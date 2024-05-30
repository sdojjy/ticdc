// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package maintainer

import (
	"context"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	pfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
)

type Maintainer struct {
	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars
	ID              model.ChangeFeedID

	info   *model.ChangeFeedInfo
	status *model.ChangeFeedStatus

	tableRanges map[model.CaptureID]*TableRange
}

func NewMaintainer(
	ID model.ChangeFeedID,
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus) *Maintainer {
	m := &Maintainer{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		ID:              ID,
		info:            info,
		status:          status,
		tableRanges:     make(map[model.CaptureID]*TableRange),
	}
	_, _ = globalVars.MessageServer.SyncAddHandler(context.Background(),
		new_arch.GetChangefeedMaintainerTopic(ID),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			m.HandleMessage(sender, message)
			return nil
		})
	return m
}

func (m *Maintainer) HandleMessage(send string, msg *new_arch.Message) {
	if msg.AddTableRangeMaintainerResponse != nil {
		resp := msg.AddTableRangeMaintainerResponse
		m.tableRanges[send].tableRangeStatus = resp.Status
		log.Info("add table range maintainer response",
			zap.String("send", send),
			zap.String("id", resp.ID),
			zap.Any("ids", m.tableRanges[send].tables))
	}
}

func (m *Maintainer) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}

func (m *Maintainer) ScheduleTableRangeManager(ctx context.Context) error {
	//load tables
	stream, _ := m.upstreamManager.GetDefaultUpstream()
	meta := kv.GetSnapshotMeta(stream.KVStorage, m.status.CheckpointTs)
	filter, err := pfilter.NewFilter(m.info.Config, "")
	snap, err := schema.NewSingleSnapshotFromMeta(
		model.DefaultChangeFeedID(m.info.ID),
		meta, m.status.CheckpointTs, m.info.Config.ForceReplicate, filter)
	if err != nil {
		return errors.Trace(err)
	}

	// list all tables
	res := make([]model.TableID, 0)
	snap.IterTables(true, func(tableInfo *model.TableInfo) {
		if filter.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
			return
		}
		// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
		// See https://github.com/pingcap/tiflow/issues/4559
		if tableInfo.IsSequence() {
			return
		}
		if pi := tableInfo.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				res = append(res, partition.ID)
			}
		} else {
			res = append(res, tableInfo.ID)
		}
	})
	// todo: load balance table id to table range maintainer
	captures := m.globalVars.CaptureManager.GetCaptures()
	var tableIDGroups = make([][]model.TableID, len(captures))
	for range captures {
		tableIDGroups = append(tableIDGroups, make([]model.TableID, 0))
	}
	for idx, tableID := range res {
		tableIDGroups[idx%len(captures)] = append(tableIDGroups[idx%len(captures)], tableID)
	}
	idx := 0
	for _, capture := range captures {
		tbls := NewTableRange(m.globalVars, m.ID, capture.ID, tableIDGroups[idx], m.info, m.status)
		m.tableRanges[capture.ID] = tbls
		idx++
		go tbls.Run(ctx)
	}
	return nil
}
