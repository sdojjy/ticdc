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

package scheduller

import (
	"github.com/pingcap/tiflow/new_arch"
)

// ComponentStatus is the state in component side
// Absent -> Working -> Stopping -> Stopped
// todo: define it in pb file
type ComponentStatus int

const (
	ComponentStatusUnknown ComponentStatus = iota
	ComponentStatusAbsent
	ComponentStatusWorking
	ComponentStatusStopping
	ComponentStatusStopped
)

// SchedulerComponentStatus is the state in scheduler side
type SchedulerComponentStatus int

const (
	SchedulerComponentStatusAbsent SchedulerComponentStatus = iota
	SchedulerComponentStatusWorking
	SchedulerComponentStatusMoving
	SchedulerComponentStatusRemoving
)

type Component struct {
	PrimaryCaptureID string
	// move component to another capture
	SecondaryCaptureID string

	Status SchedulerComponentStatus
}

func (c *Component) handleMoveComponent(client *ClientComponent) ([]*new_arch.Message, error) {
	c.Status = SchedulerComponentStatusMoving
	return c.poll(client)
}

func (c *Component) poll(client *ClientComponent) ([]*new_arch.Message, error) {
	stateChanged := true
	var err error
	for stateChanged {
		switch c.Status {
		case SchedulerComponentStatusAbsent:
			stateChanged, err = c.pollOnAbsent(client)
		case SchedulerComponentStatusMoving:
		case SchedulerComponentStatusWorking:
		case SchedulerComponentStatusRemoving:
		default:
			panic("invalid component status")
		}
	}

	return []*new_arch.Message{}, err
}

func (c *Component) pollOnAbsent(client *ClientComponent) (bool, error) {
	switch client.Status {
	case ComponentStatusAbsent:
	case ComponentStatusWorking:
	case ComponentStatusStopping:
	case ComponentStatusStopped:
	default:
		panic("invalid component status")
	}
	return true, nil
}

type ComponentManager struct {
	components []Component
}

type ClientComponent struct {
	Status ComponentStatus
}

type ClientComponentManager struct {
	components []ClientComponent
}
