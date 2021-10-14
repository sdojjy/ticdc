// Copyright 2021 PingCAP, Inc.
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

package message

import (
	"github.com/pingcap/ticdc/cdc/model"
)

// Type is the type of Message
type Type int

// types of Message
const (
	TypeUnknown Type = iota
	TypeTick
	TypeBarrier
	TypeStop
	TypeStart
	// Add a new type when adding a new message.
)

// Message is a vehicle for transferring information between nodes
type Message struct {
	// Tp is the type of Message
	Tp Type
	// BarrierTs
	BarrierTs model.Ts
	StartCh   chan struct{}
}

// TickMessage creates the message of Tick
func TickMessage() Message {
	return Message{
		Tp: TypeTick,
	}
}

// BarrierMessage creates the message of Command
func BarrierMessage(barrierTs model.Ts) Message {
	return Message{
		Tp:        TypeBarrier,
		BarrierTs: barrierTs,
	}
}

func StopMessage() Message {
	return Message{
		Tp: TypeStop,
	}
}

func StartMessage(ch chan struct{}) Message {
	return Message{
		Tp:      TypeStart,
		StartCh: ch,
	}
}
