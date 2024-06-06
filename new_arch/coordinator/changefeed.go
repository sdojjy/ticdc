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
	"encoding/json"
	"fmt"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/new_arch"
	"github.com/pingcap/tiflow/new_arch/scheduller"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Role is the role of a capture.
type Role int

const (
	// RolePrimary primary role.
	RolePrimary = 1
	// RoleSecondary secondary role.
	RoleSecondary = 2
	// RoleUndetermined means that we don't know its state, it may be
	// replicating, stopping or stopped.
	RoleUndetermined = 3
)

type changefeed struct {
	maintainerCaptureID model.CaptureID
	standbyCaptureID    model.CaptureID

	// Captures is a map of captures that has the table replica.
	// NB: Invariant, 1) at most one primary, 2) primary capture must be in
	//     CaptureRolePrimary.
	Captures map[model.CaptureID]Role

	ID     model.ChangeFeedID
	Info   *model.ChangeFeedInfo
	Status *model.ChangeFeedStatus

	state        model.FeedState
	checkpointTs atomic.Uint64
	errors       map[model.CaptureID]changefeedError

	maintainerStatus string
	coordinator      *coordinatorImpl

	scheduleState scheduller.SchedulerComponentStatus
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

func (c *changefeed) hasRemoved() bool {
	// It has been removed successfully if it's state is Removing,
	// and there is no capture has it.
	return c.scheduleState == scheduller.SchedulerComponentStatusRemoving &&
		c.maintainerCaptureID == "" && c.standbyCaptureID == ""
}

func (c *changefeed) handleRemoveChangefeed() ([]*new_arch.Message, error) {
	// Ignore remove table if it has been removed already.
	if c.hasRemoved() {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("changefeed", c.ID.ID))
		return nil, nil
	}
	// Ignore remove table if it's not in Replicating state.
	if c.scheduleState != scheduller.SchedulerComponentStatusWorking {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("changefeed", c.ID.ID))
		return nil, nil
	}
	oldState := c.scheduleState
	c.scheduleState = scheduller.SchedulerComponentStatusRemoving
	log.Info("schedulerv3: replication state transition, remove table",
		zap.String("changefeed", c.ID.ID),
		zap.Any("old", oldState))
	status := ChangefeedStatus{
		ChangefeedID:             c.ID,
		SchedulerComponentStatus: scheduller.ComponentStatusWorking,
	}
	return c.poll(&status, c.maintainerCaptureID)
}

func (c *changefeed) pollOnAbsent(
	input *ChangefeedStatus, captureID model.CaptureID) (bool, error) {
	switch input.SchedulerComponentStatus {
	case scheduller.ComponentStatusAbsent:
		c.scheduleState = scheduller.SchedulerComponentStatusPrepare
		err := c.setCapture(captureID, RoleSecondary)
		return true, errors.Trace(err)

	case scheduller.ComponentStatusStopped:
		// Ignore stopped table state as a capture may shutdown unexpectedly.
		return false, nil
	case scheduller.ComponentStatusPreparing,
		scheduller.ComponentStatusPrepared,
		scheduller.ComponentStatusWorking,
		scheduller.ComponentStatusStopping:
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("changefeed", c.ID.ID),
		zap.String("captureID", captureID))
	return false, nil
}

func (r *changefeed) isInRole(captureID model.CaptureID, role Role) bool {
	rc, ok := r.Captures[captureID]
	if !ok {
		return false
	}
	return rc == role
}
func (c *changefeed) pollOnPrepare(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	switch input.SchedulerComponentStatus {
	case scheduller.ComponentStatusAbsent:
		if c.isInRole(captureID, RoleSecondary) {
			return &new_arch.Message{
				To:                   captureID,
				AddMaintainerRequest: &new_arch.AddMaintainerRequest{},
			}, false, nil
		}
	case scheduller.ComponentStatusPreparing:
		if c.isInRole(captureID, RoleSecondary) {
			// Ignore secondary Preparing, it may take a long time.
			return nil, false, nil
		}
	case scheduller.ComponentStatusPrepared:
		if c.isInRole(captureID, RoleSecondary) {
			// Secondary is prepared, transit to Commit state.
			c.scheduleState = scheduller.SchedulerComponentStatusCommit
			return nil, true, nil
		}
	case scheduller.ComponentStatusWorking:
		if c.maintainerCaptureID == captureID {
			//r.updateCheckpointAndStats(input.Checkpoint, input.Stats)
			return nil, false, nil
		}
	case scheduller.ComponentStatusStopping, scheduller.ComponentStatusStopped:
		if c.maintainerCaptureID == captureID {
			// Primary is stopped, but we may still has secondary.
			// Clear primary and promote secondary when it's prepared.
			log.Info("schedulerv3: primary is stopped during Prepare",
				zap.String("changefeed", c.ID.ID),
				zap.Any("tableState", input),
				zap.String("captureID", captureID))
			//r.clearPrimary()
			return nil, false, nil
		}
		if c.isInRole(captureID, RoleSecondary) {
			log.Info("schedulerv3: capture is stopped during Prepare",
				zap.String("changefeed", c.ID.ID),
				zap.Any("tableState", input),
				zap.String("captureID", captureID))
			//err := c.clearCapture(captureID, RoleSecondary)
			//if err != nil {
			//	return nil, false, errors.Trace(err)
			//}
			//if r.Primary != "" {
			//	// Secondary is stopped, and we still has primary.
			//	// Transit to Replicating.
			//	r.State = ReplicationSetStateReplicating
			//} else {
			//	// Secondary is stopped, and we do not has primary.
			//	// Transit to Absent.
			//	r.State = ReplicationSetStateAbsent
			//}
			return nil, true, nil
		}
	}
	log.Warn("schedulerv3: ignore input, unexpected replication set state",
		zap.String("changefeed", c.ID.ID),
		zap.Any("tableState", input),
		zap.String("captureID", captureID),
		zap.Any("replicationSet", c))
	return nil, false, nil
}

func (c *changefeed) pollOnReplicating(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	return nil, false, nil
}

func (c *changefeed) pollOnCommit(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	return nil, false, nil
}

func (c *changefeed) pollOnRemoving(
	input *ChangefeedStatus, captureID model.CaptureID) (*new_arch.Message, bool, error) {
	return nil, false, nil
}

type ChangefeedStatus struct {
	SchedulerComponentStatus scheduller.ComponentStatus
	ChangefeedID             model.ChangeFeedID
}

// poll transit replication state based on input and the current state.
// See ReplicationSetState's comment for the state transition.
func (c *changefeed) poll(
	input *ChangefeedStatus, captureID model.CaptureID,
) ([]*new_arch.Message, error) {
	if _, ok := c.Captures[captureID]; !ok {
		return nil, nil
	}

	msgBuf := make([]*new_arch.Message, 0)
	stateChanged := true
	var err error
	for stateChanged {
		//err := r.checkInvariant(input, captureID)
		//if err != nil {
		//	return nil, errors.Trace(err)
		//}
		oldState := c.scheduleState
		var msg *new_arch.Message
		switch c.scheduleState {
		case scheduller.SchedulerComponentStatusAbsent:
			stateChanged, err = c.pollOnAbsent(input, captureID)
		case scheduller.SchedulerComponentStatusPrepare:
			msg, stateChanged, err = c.pollOnPrepare(input, captureID)
		case scheduller.SchedulerComponentStatusCommit:
			msg, stateChanged, err = c.pollOnCommit(input, captureID)
		case scheduller.SchedulerComponentStatusWorking:
			msg, stateChanged, err = c.pollOnReplicating(input, captureID)
		case scheduller.SchedulerComponentStatusRemoving:
			msg, stateChanged, err = c.pollOnRemoving(input, captureID)
		default:
			return nil, errors.New("schedulerv3: table state unknown")
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if msg != nil {
			msgBuf = append(msgBuf, msg)
		}
		if stateChanged {
			log.Info("schedulerv3: replication state transition, poll",
				zap.String("changefeed", c.ID.ID),
				zap.Any("tableState", input),
				zap.String("captureID", captureID),
				zap.Any("old", oldState),
				zap.Any("new", c.scheduleState))
		}
	}
	return msgBuf, nil
}

func (r *changefeed) handleMoveTable(
	dest model.CaptureID,
) ([]*new_arch.Message, error) {
	// Ignore move table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("schedulerv3: move table is ignored",
			zap.Any("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	// Ignore move table if
	// 1) it's not in Replicating state or
	// 2) the dest capture is the primary.
	if r.scheduleState != scheduller.SchedulerComponentStatusWorking || r.maintainerCaptureID == dest {
		log.Warn("schedulerv3: move table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	r.scheduleState = scheduller.SchedulerComponentStatusPrepare
	err := r.setCapture(dest, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("schedulerv3: replication state transition, move table",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r))
	status := ChangefeedStatus{
		ChangefeedID:             r.ID,
		SchedulerComponentStatus: scheduller.ComponentStatusAbsent,
	}
	return r.poll(&status, dest)
}

func (r *changefeed) handleAddTable(
	captureID model.CaptureID,
) ([]*new_arch.Message, error) {
	// Ignore add table if it's not in Absent state.
	if r.scheduleState != scheduller.SchedulerComponentStatusAbsent {
		log.Warn("schedulerv3: add table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	err := r.setCapture(captureID, RoleSecondary)
	if err != nil {
		return nil, errors.Trace(err)
	}
	status := ChangefeedStatus{
		ChangefeedID:             r.ID,
		SchedulerComponentStatus: scheduller.ComponentStatusAbsent,
	}
	msgs, err := r.poll(&status, captureID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("schedulerv3: replication state transition, add table",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r))
	return msgs, nil
}

func (r *changefeed) handleRemoveTable() ([]*new_arch.Message, error) {
	// Ignore remove table if it has been removed already.
	if r.hasRemoved() {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	// Ignore remove table if it's not in Replicating state.
	if r.scheduleState != scheduller.SchedulerComponentStatusWorking {
		log.Warn("schedulerv3: remove table is ignored",
			zap.String("changefeed", r.ID.ID),
			zap.Any("replicationSet", r))
		return nil, nil
	}
	r.scheduleState = scheduller.SchedulerComponentStatusRemoving
	log.Info("schedulerv3: replication state transition, remove table",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r))
	status := ChangefeedStatus{
		ChangefeedID:             r.ID,
		SchedulerComponentStatus: scheduller.ComponentStatusWorking,
	}
	return r.poll(&status, r.maintainerCaptureID)
}

// handleCaptureShutdown handle capture shutdown event.
// Besides returning messages and errors, it also returns a bool to indicate
// whether r is affected by the capture shutdown.
func (r *changefeed) handleCaptureShutdown(
	captureID model.CaptureID,
) ([]*new_arch.Message, bool, error) {
	_, ok := r.Captures[captureID]
	if !ok {
		// r is not affected by the capture shutdown.
		return nil, false, nil
	}
	// The capture has shutdown, the table has stopped.
	status := ChangefeedStatus{
		ChangefeedID:             r.ID,
		SchedulerComponentStatus: scheduller.ComponentStatusStopped,
	}
	oldState := r.scheduleState
	msgs, err := r.poll(&status, captureID)
	log.Info("schedulerv3: replication state transition, capture shutdown",
		zap.String("changefeed", r.ID.ID),
		zap.Any("replicationSet", r),
		zap.Any("old", oldState), zap.Any("new", r.scheduleState))
	return msgs, true, errors.Trace(err)
}
func (r *changefeed) setCapture(captureID model.CaptureID, role Role) error {
	cr, ok := r.Captures[captureID]
	if ok && cr != role {
		jsonR, _ := json.Marshal(r)
		return errors.ErrReplicationSetInconsistent.GenWithStackByArgs(fmt.Sprintf(
			"can not set %s as %s, it's %s, %v", captureID, role, cr, string(jsonR)))
	}
	r.Captures[captureID] = role
	return nil
}

// SetHeap is a max-heap, it implements heap.Interface.
type SetHeap []*changefeed

// NewReplicationSetHeap creates a new SetHeap.
func NewReplicationSetHeap(capacity int) SetHeap {
	if capacity <= 0 {
		panic("capacity must be positive")
	}
	return make(SetHeap, 0, capacity)
}

// Len returns the length of the heap.
func (h SetHeap) Len() int { return len(h) }

// Less returns true if the element at i is less than the element at j.
func (h SetHeap) Less(i, j int) bool {
	if h[i].ID.ID > h[j].ID.ID {
		return true
	}
	return false
}

// Swap swaps the elements with indexes i and j.
func (h SetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push pushes an element to the heap.
func (h *SetHeap) Push(x interface{}) {
	*h = append(*h, x.(*changefeed))
}

// Pop pops an element from the heap.
func (h *SetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}
