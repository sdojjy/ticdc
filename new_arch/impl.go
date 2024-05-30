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

package new_arch

import (
	"context"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"go.uber.org/atomic"
)

type coordinatorImpl struct {
	changefeeds map[model.ChangeFeedID]*changefeed
}

func newCoordinatorImpl() *coordinatorImpl {
	return &coordinatorImpl{}
}

type changefeed struct {
	maintainerCaptureID model.CaptureID
	ID                  model.ChangeFeedID
	Info                *model.ChangeFeedInfo
	Status              *model.ChangeFeedStatus

	state        model.FeedState
	checkpointTs atomic.Uint64
	errors       map[model.CaptureID]changefeedError
}

type changefeedError struct {
	warning *model.RunningError
	failErr *model.RunningError
}

func newChangefeed(captureID model.CaptureID, id model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus) *changefeed {
	return &changefeed{
		maintainerCaptureID: captureID,
		ID:                  id,
		Info:                info,
		Status:              status,
	}
}

func (c *changefeed) Run(ctx context.Context) error {
	return nil
}

func (c *changefeed) Stop(ctx context.Context) error {
	return nil
}
func (c *changefeed) GetCheckpointTs(ctx context.Context) uint64 {
	return c.checkpointTs.Load()
}

func (c *changefeed) EmitCheckpointTs(ctx context.Context, uint642 uint64) error {
	return nil
}

func (c *coordinatorImpl) Tick(ctx context.Context, state *orchestrator.GlobalReactorState) (nextState orchestrator.ReactorState, err error) {
	// update gc safe point
	//if err = c.updateGCSafepoint(ctx, state); err != nil {
	//	return nil, errors.Trace(err)
	//}

	var newChangefeeds = make(map[model.ChangeFeedID]struct{})
	// Tick all changefeeds.
	for changefeedID, reactor := range state.Changefeeds {
		_, exist := c.changefeeds[changefeedID]
		if !exist {
			// check if changefeed should running
			if reactor.Info.State == model.StateStopped ||
				reactor.Info.State == model.StateFailed ||
				reactor.Info.State == model.StateFinished {
				continue
			}
			// create
			newChangefeeds[changefeedID] = struct{}{}
		}
	}

	// Cleanup changefeeds that are not in the state.
	// save status to etcd
	for changefeedID, cf := range c.changefeeds {
		if reactor, exist := state.Changefeeds[changefeedID]; exist {
			//todo: handle error
			cf.EmitCheckpointTs(ctx, reactor.Status.CheckpointTs)

			checkpointTs := cf.GetCheckpointTs(ctx)
			reactor.PatchStatus(
				func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
					changed := false
					if status.CheckpointTs != checkpointTs {
						status.CheckpointTs = checkpointTs
						changed = true
					}
					return status, changed, nil
				})
			//save error info
			var lastWarning *model.RunningError
			var lastErr *model.RunningError
			for captureID, errs := range cf.errors {
				lastWarning = errs.warning
				lastErr = errs.failErr
				reactor.PatchTaskPosition(captureID,
					func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
						if position == nil {
							position = &model.TaskPosition{}
						}
						changed := false
						if position.Warning != errs.warning {
							position.Warning = errs.warning
							changed = true
						}
						if position.Error != errs.failErr {
							position.Error = errs.failErr
							changed = true
						}
						return position, changed, nil
					})
			}
			reactor.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
				info.Error = lastErr
				info.Warning = lastWarning
				return info, true, nil
			})

			// reported changefeed state
			switch cf.state {
			case model.StateFailed, model.StateFinished:
				reactor.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
					info.State = cf.state
					return info, false, nil
				})
				// stop changefeed, and remove
				cf.Stop(ctx)
			}
			continue
		}

		// stop changefeed
		cf.Stop(ctx)
		delete(c.changefeeds, changefeedID)
	}

	c.ScheduleChangefeedMaintainer(ctx, state, newChangefeeds)
	//try to rebalance tables if needed
	c.BalanceTables(ctx)
	return state, nil
}

func (c *coordinatorImpl) BalanceTables(ctx context.Context) error {
	return nil
}

func (c *coordinatorImpl) ScheduleChangefeedMaintainer(ctx context.Context,
	state *orchestrator.GlobalReactorState,
	newChangefeeds map[model.ChangeFeedID]struct{}) error {
	var captures []*model.CaptureInfo
	for _, capture := range state.Captures {
		captures = append(captures, capture)
	}

	idx := 0
	for changefeedID := range newChangefeeds {
		cf := state.Changefeeds[changefeedID]
		//todo: select a capture to schedule maintainer
		impl := newChangefeed(captures[idx%len(captures)].ID, cf.ID, cf.Info, cf.Status)
		c.changefeeds[cf.ID] = impl
		go impl.Run(ctx)
		idx++
	}
	return nil
}
