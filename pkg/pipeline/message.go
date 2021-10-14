// Copyright 2020 PingCAP, Inc.
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

package pipeline

import "github.com/pingcap/ticdc/cdc/model"

// MessageType is the type of Message
type MessageType int

// types of Message
const (
	MessageTypeUnknown MessageType = iota
	MessageTypeCommand
	MessageTypePolymorphicEvent
	MessageTypeBarrier
	MessageTypeTick
	MessageTypeRawPolymorphicEvent
)

// Message is a vehicle for transferring information between nodes
type Message struct {
	// TODO add more kind of messages
	// Tp is the type of Message
	Tp MessageType
	// Command is the command in this message
	Command *Command
	// PolymorphicEvent represents the row change event
	PolymorphicEvent *model.PolymorphicEvent
	// BarrierTs
	BarrierTs model.Ts
}

// RawPolymorphicEventMessage creates the message of RawPolymorphicEventMessage
func RawPolymorphicEventMessage(event *model.PolymorphicEvent) Message {
	return Message{
		Tp:               MessageTypeRawPolymorphicEvent,
		PolymorphicEvent: event,
	}
}

// PolymorphicEventMessage creates the message of PolymorphicEvent
func PolymorphicEventMessage(event *model.PolymorphicEvent) Message {
	return Message{
		Tp:               MessageTypePolymorphicEvent,
		PolymorphicEvent: event,
	}
}

// CommandMessage creates the message of Command
func CommandMessage(command *Command) Message {
	return Message{
		Tp:      MessageTypeCommand,
		Command: command,
	}
}

// StartMessage creates the message of Start
func StartMessage(startCh chan struct{}) Message {
	return Message{
		Tp: MessageTypeCommand,
		Command: &Command{
			Tp:      CommandTypeStart,
			StartCh: startCh,
		},
	}
}

// StopMessage creates the message of Stop
func StopMessage() Message {
	return Message{
		Tp: MessageTypeCommand,
		Command: &Command{
			Tp: CommandTypeStop,
		},
	}
}

// BarrierMessage creates the message of Command
func BarrierMessage(barrierTs model.Ts) Message {
	return Message{
		Tp:        MessageTypeBarrier,
		BarrierTs: barrierTs,
	}
}

// TickMessage creates the message of Tick
func TickMessage() Message {
	return Message{
		Tp: MessageTypeTick,
	}
}

// CommandType is the type of Command
type CommandType int

const (
	// CommandTypeUnknown is unknown message type
	CommandTypeUnknown CommandType = iota
	// CommandTypeStopAtTs means the table pipeline should stop at the specified Ts
	CommandTypeStopAtTs
	// CommandTypeStart starts an actor.
	CommandTypeStart
	// CommandTypeStop stops an actor.
	CommandTypeStop
)

// Command is the command about table pipeline
type Command struct {
	Tp        CommandType
	StoppedTs model.Ts
	StartCh   chan struct{}
}
