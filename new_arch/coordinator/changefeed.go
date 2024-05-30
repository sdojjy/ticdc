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

package coordinator

import (
	"context"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"time"
)

type changefeed struct {
	maintainerCaptureID model.CaptureID
	ID                  model.ChangeFeedID
	Info                *model.ChangeFeedInfo
	Status              *model.ChangeFeedStatus

	state        model.FeedState
	checkpointTs atomic.Uint64
	errors       map[model.CaptureID]changefeedError

	maintainerStatus string
	coordinator      *coordinatorImpl
}

const (
	maintainerStatusPending  = "pending"
	maintainerStatusStarting = "starting"
	maintainerStatusRunning  = "running"
	maintainerStatusStopping = "stopping"
	maintainerStatusStopped  = "stopped"
)

func newChangefeed(captureID model.CaptureID, id model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	coordinator *coordinatorImpl) *changefeed {
	return &changefeed{
		maintainerCaptureID: captureID,
		ID:                  id,
		Info:                info,
		Status:              status,
		maintainerStatus:    maintainerStatusPending,
		coordinator:         coordinator,
	}
}

func (c *changefeed) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 50)
	loged := false
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-tick.C:
			if c.maintainerStatus == maintainerStatusPending {
				err := c.coordinator.SendMessage(ctx, c.maintainerCaptureID, new_arch.GetChangefeedMaintainerManagerTopic(),
					&new_arch.Message{
						AddMaintainerRequest: &new_arch.AddMaintainerRequest{
							Config: c.Info,
							Status: c.Status,
						},
					})
				if err != nil {
					return errors.Trace(err)
				}
				c.maintainerStatus = maintainerStatusStarting
			}
			if c.maintainerStatus == maintainerStatusRunning {
				if !loged {
					log.Info("changefeed maintainer is running",
						zap.String("ID", c.ID.String()),
						zap.String("maintainer", c.maintainerCaptureID))
					loged = true
				}
			}
		}
	}
}

func (c *changefeed) Stop(ctx context.Context) error {
	err := c.coordinator.SendMessage(ctx, c.maintainerCaptureID, new_arch.GetChangefeedMaintainerManagerTopic(),
		&new_arch.Message{
			RemoveMaintainerRequest: &new_arch.RemoveMaintainerRequest{
				ID: c.Info.ID,
			},
		})
	if err != nil {
		return errors.Trace(err)
	}
	c.maintainerStatus = maintainerStatusStopping
	return nil
}

func (c *changefeed) GetCheckpointTs(ctx context.Context) uint64 {
	return c.checkpointTs.Load()
}

func (c *changefeed) EmitCheckpointTs(ctx context.Context, uint642 uint64) error {
	return nil
}
