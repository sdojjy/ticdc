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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	serverConfig "github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	defaultSystem *actor.System
	defaultRouter *actor.Router
)

func init() {
	defaultSystem, defaultRouter = actor.NewSystemBuilder("table").Build()
	defaultSystem.Start(context.Background())
}

type tableActor struct {
	// The ctx is only used in nodes starts.
	// TODO(neil) remove it, context should not be embedded in struct.
	ctx       context.Context
	cancel    context.CancelFunc
	wg        *errgroup.Group
	reportErr func(error)

	mb actor.Mailbox

	changefeedID string
	// quoted schema and table, used in metrics only
	tableName     string
	tableID       int64
	markTableID   int64
	cyclicEnabled bool
	memoryQuota   uint64
	mounter       entry.Mounter
	replicaInfo   *model.TableReplicaInfo
	sink          sink.Sink
	targetTs      model.Ts

	started bool
	stopped bool
	err     error

	info *cdcContext.ChangefeedVars
	vars *cdcContext.GlobalVars

	pullerNode  *pullerNode
	sorterNode  *sorterNode
	mounterNode *mounterNode
	cyclicNode  *cyclicMarkNode
	sinkNode    *sinkNode

	pullerEventsStash  []pipeline.Message
	sorterEventsStash  []pipeline.Message
	mounterEventsStash []pipeline.Message
}

var _ TablePipeline = (*tableActor)(nil)
var _ actor.Actor = (*tableActor)(nil)

func (t *tableActor) Poll(ctx context.Context, msgs []message.Message) bool {
	for i := range msgs {
		if t.stopped {
			// No need to handle remaining messages.
			break
		}
		switch msgs[i].Tp {
		case message.TypeStop:
			t.stop(nil)
			break
		case message.TypeBarrier:
			err := t.sinkNode.HandleMessages(ctx, msgs[i])
			if err != nil {
				t.stop(err)
			}
			// tick 消息是不是根据 node 的类型分类？
		case message.TypeTick:
		}

		// get message from puller
		var pullerEvents []pipeline.Message
		if len(t.pullerEventsStash) != 0 {
			pullerEvents = t.pullerEventsStash[0:]
			t.pullerEventsStash = t.pullerEventsStash[:0]
		} else {
			pullerEvents = tryReceiveFromOutput(pullerEvents, t.pullerNode.OutPut(ctx))
			//for event := range t.pullerNode.OutPut(ctx) {
			//	pullerEvents = append(pullerEvents, event)
			//}
		}
		// send message to puller
		n := 0
		for _, event := range pullerEvents {
			// TODO(dongmen) use non-blocking func here
			// add entry is a block func
			t.sorterNode.sorter.AddEntry(ctx, event.PolymorphicEvent)
			n++
		}
		t.pullerEventsStash = pullerEvents[n:]

		// get message from sorter
		var sorterEvents []pipeline.Message
		if len(t.sorterEventsStash) != 0 {
			sorterEvents = t.sorterEventsStash[0:]
			t.sorterEventsStash = t.sorterEventsStash[:0]
		} else {
			sorterEvents = tryReceiveFromOutput(sorterEvents, t.sorterNode.OutPut(ctx))
			//for event := range t.sorterNode.OutPut(ctx) {
			//	sorterEvents = append(sorterEvents, event)
			//}
		}
		// send message to mounterNode
		n = 0
		for _, event := range sorterEvents {
			t.mounterNode.Receives(event)
			n++
		}
		t.sorterEventsStash = sorterEvents[n:]

		var mounterEvents []pipeline.Message
		if len(t.mounterEventsStash) != 0 {
			mounterEvents = t.mounterEventsStash[0:]
			t.mounterEventsStash = t.mounterEventsStash[:0]
		} else {
			//for event := range t.mounterNode.OutPut(ctx) {
			//	mounterEvents = append(mounterEvents, event)
			//}
			mounterEvents = tryReceiveFromOutput(mounterEvents, t.mounterNode.OutPut(ctx))
		}
		// send message to sinkNode
		n = 0
		for _, event := range mounterEvents {
			t.sinkNode.HandleMessage(ctx, event)
			n++
		}
		t.mounterEventsStash = mounterEvents[n:]

	}
	return !t.stopped
}

func tryReceiveFromOutput(mounterEvents []pipeline.Message, output chan pipeline.Message) []pipeline.Message {
	for {
		select {
		case msg, ok := <-output:
			if !ok {
				return mounterEvents
			}
			mounterEvents = append(mounterEvents, msg)
		default:
			return mounterEvents
		}
	}
}

// Receive processes messages to the table.
func (t *tableActor) Receive(ctx context.Context, msgs []pipeline.Message) bool {
	for i := range msgs {
		if t.stopped {
			// No need to handle remaining messages.
			break
		}
		switch msgs[i].Tp {
		// Produced by Puller
		case pipeline.MessageTypeRawPolymorphicEvent:
			// Send to Sorter
			t.sorterNode.sorter.AddEntry(ctx, msgs[i].PolymorphicEvent)
		// Produced by Sorter
		case pipeline.MessageTypePolymorphicEvent:
			// Send to Sink
			fallthrough
		case pipeline.MessageTypeTick, pipeline.MessageTypeBarrier:
			err := t.sinkNode.HandleMessage(ctx, msgs[i])
			if err != nil {
				t.stop(err)
			}

		case pipeline.MessageTypeCommand:
			switch msgs[i].Command.Tp {

			case pipeline.CommandTypeStopAtTs:
				err := t.sinkNode.HandleMessage(ctx, msgs[i])
				if err != nil {
					t.stop(err)
				}
			}

		case pipeline.MessageTypeUnknown:
			fallthrough
		default:
			msg := msgs[i]
			log.Warn("find unknown message", zap.Reflect("message", msg))
		}
	}
	// Report error to processor if there is any.
	t.checkError()
	return !t.stopped
}

func (t *tableActor) start() error {
	if t.started {
		log.Panic("start an already started table",
			zap.String("changefeedID", t.changefeedID),
			zap.Int64("tableID", t.tableID),
			zap.String("tableName", t.tableName))
	}
	log.Debug("creating table flow controller",
		zap.String("changefeedID", t.changefeedID),
		zap.Int64("tableID", t.tableID),
		zap.String("tableName", t.tableName),
		zap.Uint64("quota", t.memoryQuota))

	flowController := common.NewTableFlowController(t.memoryQuota)

	t.pullerNode = newPullerNode(t.tableID, t.replicaInfo, t.tableName).(*pullerNode)
	if err := t.pullerNode.Start(t.ctx, t.wg, t.info, t.vars); err != nil {
		log.Error("puller fails to start", zap.Error(err))
		return err
	}

	t.sorterNode = newSorterNode(t.tableName, t.tableID, t.replicaInfo.StartTs, flowController, t.mounter)
	if err := t.sorterNode.Start(t.ctx, t.wg, t.info, t.vars); err != nil {
		log.Error("sorter fails to start", zap.Error(err))
		return err
	}

	t.mounterNode = newMounterNode().(*mounterNode)
	if err := t.mounterNode.Start(t.ctx); err != nil {
		log.Error("mounter fails to start")
	}

	if t.cyclicEnabled {
		// TODO(neil) support cyclic feature.
		// t.cyclicNode = newCyclicMarkNode(t.replicaInfo.MarkTableID).(*cyclicMarkNode)
	}

	t.sinkNode = newSinkNode(t.sink, t.replicaInfo.StartTs, t.targetTs, flowController)
	if err := t.sinkNode.Start(t.ctx, t.info, t.vars); err != nil {
		log.Error("sink fails to start", zap.Error(err))
		return err
	}
	t.started = true
	log.Info("table actor is started", zap.Int64("tableID", t.tableID))
	return nil
}

func (t *tableActor) stop(err error) {
	if t.stopped {
		// It's stopped already.
		return
	}
	t.stopped = true
	t.err = err
	t.cancel()
	log.Info("table actor will be stopped",
		zap.Int64("tableID", t.tableID), zap.Error(err))
}

func (t *tableActor) checkError() {
	if t.err != nil {
		t.reportErr(t.err)
		t.err = nil
	}
}

// ============ Implement TablePipline, must be threadsafe ============

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tableActor) ResolvedTs() model.Ts {
	return t.sinkNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tableActor) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tableActor) UpdateBarrierTs(ts model.Ts) {
	if t.sinkNode.barrierTs != ts {
		msg := message.BarrierMessage(ts)
		err := defaultRouter.Send(actor.ID(t.tableID), msg)
		if err != nil {
			log.Warn("send fails", zap.Reflect("msg", msg), zap.Error(err))
		}
	}
}

// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
func (t *tableActor) AsyncStop(targetTs model.Ts) bool {
	msg := message.StopMessage()
	err := defaultRouter.Send(actor.ID(t.tableID), msg)
	log.Info("send async stop signal to table", zap.Int64("tableID", t.tableID), zap.Uint64("targetTs", targetTs))
	if err != nil {
		if cerror.ErrMailboxFull.Equal(err) {
			return false
		}
		if cerror.ErrSendToClosedPipeline.Equal(err) {
			return true
		}
		log.Panic("send fails", zap.Reflect("msg", msg), zap.Error(err))
	}
	return true
}

// Workload returns the workload of this table
func (t *tableActor) Workload() model.WorkloadInfo {
	// TODO(neil)(leoppro) calculate the workload of this table
	// We temporarily set the value to constant 1
	return workload
}

// Status returns the status of this table pipeline
func (t *tableActor) Status() TableStatus {
	return t.sinkNode.Status()
}

// ID returns the ID of source table and mark table
func (t *tableActor) ID() (tableID, markTableID int64) {
	return t.tableID, t.markTableID
}

// Name returns the quoted schema and table name
func (t *tableActor) Name() string {
	return t.tableName
}

// Cancel stops this table actor immediately and destroy all resources
// created by this table pipeline
func (t *tableActor) Cancel() {
	// TODO(neil): pass context.
	if err := t.mb.SendB(context.TODO(), message.StopMessage()); err != nil {
		log.Warn("fails to send Stop message",
			zap.Uint64("tableID", uint64(t.tableID)))
	}
}

// Wait waits for table pipeline destroyed
func (t *tableActor) Wait() {
	t.wg.Wait()
}

// NewTableActor creates a table actor.
func NewTableActor(cdcCtx cdcContext.Context,
	mounter entry.Mounter,
	tableID model.TableID,
	tableName string,
	replicaInfo *model.TableReplicaInfo,
	sink sink.Sink,
	targetTs model.Ts,
) (TablePipeline, error) {
	config := cdcCtx.ChangefeedVars().Info.Config
	cyclicEnabled := config.Cyclic != nil && config.Cyclic.IsEnabled()
	info := cdcCtx.ChangefeedVars()
	vars := cdcCtx.GlobalVars()

	mb := actor.NewMailbox(actor.ID(tableID), defaultOutputChannelSize)
	// All sub-goroutines should be spawn in the wait group.
	wg, ctx := errgroup.WithContext(cdcCtx)
	// Cancel should be able to release all sub-goroutines in the actor.
	ctx, cancel := context.WithCancel(cdcCtx)
	table := &tableActor{
		ctx:       ctx,
		cancel:    cancel,
		wg:        wg,
		reportErr: cdcCtx.Throw,
		mb:        mb,

		tableID:       tableID,
		markTableID:   replicaInfo.MarkTableID,
		tableName:     tableName,
		cyclicEnabled: cyclicEnabled,
		memoryQuota:   serverConfig.GetGlobalServerConfig().PerTableMemoryQuota,
		mounter:       mounter,
		replicaInfo:   replicaInfo,
		sink:          sink,
		targetTs:      targetTs,
		started:       false,

		info: info,
		vars: vars,
	}

	// TODO add a test to make that the funcation must wait actor to be started
	//      before returning.
	//startCh := make(chan struct{})
	//err = defaultRouter.Send(actor.ID(tableID), message.StartMessage(startCh))
	//if err != nil {
	//	return nil, err
	//}
	log.Info("spawn and start table actor", zap.Int64("tableID", tableID))
	if err := table.start(); err != nil {
		return nil, err
	}
	//select {
	//case <-ctx.Done():
	//	return nil, errors.Trace(err)
	//case <-startCh:
	//}
	err := defaultSystem.Spawn(mb, table)
	if err != nil {
		return nil, err
	}
	log.Info("spawn and start table actor done", zap.Int64("tableID", tableID))
	return table, nil
}
