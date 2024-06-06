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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"io"
)

type coordinatorImpl struct {
	changefeeds     map[model.ChangeFeedID]*changefeed
	upstreamManager *upstream.Manager
	cfg             *config.SchedulerConfig
	globalVars      *vars.GlobalVars

	captureManager    *CaptureManager
	selfCaptureID     model.CaptureID
	schedulerManager  *Manager
	changefeedManager *ChangefeedManager
}

func NewCoordinator(
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	globalVars *vars.GlobalVars,
) owner.Owner {
	c := &coordinatorImpl{
		upstreamManager:   upstreamManager,
		cfg:               cfg,
		globalVars:        globalVars,
		changefeeds:       make(map[model.ChangeFeedID]*changefeed),
		selfCaptureID:     globalVars.CaptureInfo.ID,
		schedulerManager:  NewSchedulerManager(cfg),
		changefeedManager: NewChangefeedManager(cfg.MaxTaskConcurrency),
	}
	_, _ = globalVars.MessageServer.SyncAddHandler(context.Background(), new_arch.GetCoordinatorTopic(),
		&new_arch.Message{}, func(sender string, messageI interface{}) error {
			message := messageI.(*new_arch.Message)
			c.HandleMessage(sender, message)
			return nil
		})
	c.captureManager = NewCaptureManager(c.selfCaptureID, globalVars.OwnerRevision)
	return c
}

func (c *coordinatorImpl) HandleMessage(send string, msg *new_arch.Message) {
	if msg.AddMaintainerResponse != nil {
		rsp := msg.AddMaintainerResponse
		if rsp.Status == maintainerStatusRunning {
			c.changefeeds[model.DefaultChangeFeedID(rsp.ID)].maintainerStatus = maintainerStatusRunning
		}
	}
	//todo: 和tick 是一个多线程读写处理, 使用 actor system
	if msg.ChangefeedHeartbeatResponse != nil {
		c.captureManager.HandleMessage([]*new_arch.ChangefeedHeartbeatResponse{msg.ChangefeedHeartbeatResponse})
	}
}

func (c *coordinatorImpl) SendMessage(ctx context.Context, capture string, topic string, m *new_arch.Message) error {
	client := c.globalVars.MessageRouter.GetClient(capture)
	_, err := client.TrySendMessage(ctx, topic, m)
	return errors.Trace(err)
}

type changefeedError struct {
	warning *model.RunningError
	failErr *model.RunningError
}

func (c *coordinatorImpl) Tick(ctx context.Context,
	rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	state := rawState.(*orchestrator.GlobalReactorState)
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

	msgs := c.captureManager.HandleAliveCaptureUpdate(state.Captures)
	for _, msg := range msgs {
		client := c.globalVars.MessageRouter.GetClient(msg.To)
		_, err := client.TrySendMessage(ctx, new_arch.GetChangefeedMaintainerManagerTopic(), msg)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// only balance tables when all capture is replied bootstrap messages
	if c.captureManager.CheckAllCaptureInitialized() {
		c.ScheduleChangefeedMaintainer(ctx, state, newChangefeeds)
		//try to rebalance tables if needed
		c.BalanceTables(ctx)
	}
	return state, nil
}

func (c *coordinatorImpl) BalanceTables(ctx context.Context) error {
	return nil
}

func (c *coordinatorImpl) ScheduleChangefeedMaintainer(ctx context.Context,
	state *orchestrator.GlobalReactorState,
	newChangefeeds map[model.ChangeFeedID]struct{}) error {

	//var captures []*model.CaptureInfo
	//for _, capture := range state.Captures {
	//	captures = append(captures, capture)
	//}
	if changes := c.captureManager.TakeChanges(); changes != nil {
		c.changefeedManager.HandleCaptureChanges(changes.Init, changes.Removed)
	}

	var changefeeds []model.ChangeFeedInfo
	for _, cf := range state.Changefeeds {
		changefeeds = append(changefeeds, *cf.Info)
	}
	tasks := c.schedulerManager.Schedule(
		changefeeds,
		c.captureManager.Captures,
		c.changefeedManager.changefeeds,
		c.changefeedManager.runningTasks)

	messages, err := c.changefeedManager.HandleTasks(tasks)
	if err != nil {
		return errors.Trace(err)
	}
	if messages != nil {

	}

	//idx := 0
	//for changefeedID := range newChangefeeds {
	//	cf := state.Changefeeds[changefeedID]
	//	//todo: select a capture to schedule maintainer
	//	impl := newChangefeed(captures[idx%len(captures)].ID, cf.ID, cf.Info, cf.Status, c)
	//	c.changefeeds[cf.ID] = impl
	//	go impl.Run(ctx)
	//	idx++
	//}
	return nil
}

func (c *coordinatorImpl) EnqueueJob(adminJob model.AdminJob, done chan<- error) {

}

func (c *coordinatorImpl) RebalanceTables(cfID model.ChangeFeedID, done chan<- error) {

}

func (c *coordinatorImpl) ScheduleTable(
	cfID model.ChangeFeedID, toCapture model.CaptureID,
	tableID model.TableID, done chan<- error,
) {

}
func (c *coordinatorImpl) DrainCapture(query *scheduler.Query, done chan<- error) {

}
func (c *coordinatorImpl) WriteDebugInfo(w io.Writer, done chan<- error) {

}
func (c *coordinatorImpl) Query(query *owner.Query, done chan<- error) {

}
func (c *coordinatorImpl) AsyncStop() {

}
func (c *coordinatorImpl) UpdateChangefeedAndUpstream(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changeFeedInfo *model.ChangeFeedInfo,
) error {
	return nil
}
func (c *coordinatorImpl) UpdateChangefeed(ctx context.Context,
	changeFeedInfo *model.ChangeFeedInfo) error {
	return nil
}
func (c *coordinatorImpl) CreateChangefeed(context.Context,
	*model.UpstreamInfo,
	*model.ChangeFeedInfo,
) error {
	return nil
}
