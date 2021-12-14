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

package pipeline

import (
	sdtContext "context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
)

// send a tick message to actor if we get 32 pipeline messages
const messagesPerTick = 32

type actorNodeContext struct {
	sdtContext.Context
	outputCh             chan pipeline.Message
	tableActorRouter     *actor.Router
	tableActorID         actor.ID
	changefeedVars       *context.ChangefeedVars
	globalVars           *context.GlobalVars
	tickMessageThreshold int
	noTickMessageCount   int
}

func NewContext(stdCtx sdtContext.Context, tableActorRouter *actor.Router, tableActorID actor.ID, changefeedVars *context.ChangefeedVars, globalVars *context.GlobalVars) *actorNodeContext {
	return &actorNodeContext{
		Context:              stdCtx,
		outputCh:             make(chan pipeline.Message, defaultOutputChannelSize),
		tableActorRouter:     tableActorRouter,
		tableActorID:         tableActorID,
		changefeedVars:       changefeedVars,
		globalVars:           globalVars,
		tickMessageThreshold: messagesPerTick,
		noTickMessageCount:   0,
	}
}

func (c *actorNodeContext) setTickMessageThreshold(threshold int) {
	c.tickMessageThreshold = threshold
}

func (c *actorNodeContext) GlobalVars() *context.GlobalVars {
	return c.globalVars
}

func (c *actorNodeContext) ChangefeedVars() *context.ChangefeedVars {
	return c.changefeedVars
}

func (c *actorNodeContext) Throw(err error) {
	if err == nil {
		return
	}
	log.Error("puller stopped", zap.Error(err))
	_ = c.tableActorRouter.SendB(c, c.tableActorID, message.StopMessage())
}

func (c *actorNodeContext) SendToNextNode(msg pipeline.Message) {
	c.outputCh <- msg
	c.noTickMessageCount++
	if c.noTickMessageCount >= c.tickMessageThreshold {
		_ = c.tableActorRouter.Send(c.tableActorID, message.TickMessage())
		c.noTickMessageCount = 0
	}
}

func (c *actorNodeContext) tryGetProcessedMessage() *pipeline.Message {
	select {
	case msg, ok := <-c.outputCh:
		if !ok {
			return nil
		}
		return &msg
	default:
		return nil
	}
}
