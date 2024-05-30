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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
)

type MaintainerManager struct {
	maintainers map[string]*Maintainer

	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars
}

func NewMaintainerManager(upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars) *MaintainerManager {
	m := &MaintainerManager{
		upstreamManager: upstreamManager,
		cfg:             cfg,
		globalVars:      globalVars,
		maintainers:     make(map[string]*Maintainer),
	}
	_, _ = m.globalVars.MessageServer.SyncAddHandler(context.Background(), new_arch.GetChangefeedMaintainerManagerTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			m.HandleMessage(sender, message)
			return nil
		})
	return m
}

func (m *MaintainerManager) HandleMessage(send string, msg *new_arch.Message) {
	if msg.AddMaintainerRequest != nil {
		changefeedMaintainer := NewMaintainer(model.DefaultChangeFeedID(msg.AddMaintainerRequest.Config.ID),
			m.upstreamManager, m.cfg, m.globalVars, msg.AddMaintainerRequest.Config, msg.AddMaintainerRequest.Status)
		m.maintainers[msg.AddMaintainerRequest.Config.ID] = changefeedMaintainer
		changefeedMaintainer.SendMessage(context.Background(), send, new_arch.GetCoordinatorTopic(),
			&new_arch.Message{
				AddMaintainerResponse: &new_arch.AddMaintainerResponse{
					ID:     msg.AddMaintainerRequest.Config.ID,
					Status: "running",
				},
			})
		changefeedMaintainer.ScheduleTableRangeManager(context.Background())
	}
}

func (m *MaintainerManager) SendMessage(ctx context.Context, capture string, topic string, msg *new_arch.Message) error {
	client := m.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, msg)
	return errors.Trace(err)
}
