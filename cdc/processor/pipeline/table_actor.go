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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/cdc/sink/common"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	serverConfig "github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_       TablePipeline = (*tableActor)(nil)
	_       actor.Actor   = (*tableActor)(nil)
	stopped               = uint32(1)
)

const sinkFlushInterval = 500 * time.Millisecond

type tableActor struct {
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
	stopped uint32

	changefeedVars   *cdcContext.ChangefeedVars
	globalVars       *cdcContext.GlobalVars
	replConfig       *serverConfig.ReplicaConfig
	tableActorRouter *actor.Router

	pullerNode *pullerNode
	sortNode   *sorterNode
	sinkNode   *sinkNode

	nodes    []*ActorNode
	actorID  actor.ID
	stopFunc func(err error)

	lastFlushTime time.Time
	lck           sync.Mutex
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

	actorID := vars.TableActorSystem.ActorID()
	mb := actor.NewMailbox(actorID, defaultOutputChannelSize)
	// Cancel should be able to release all sub-goroutines in this actor.
	ctx, cancel := context.WithCancel(cdcCtx)
	// All sub-goroutines should be spawn in this wait group.
	wg, cctx := errgroup.WithContext(ctx)
	table := &tableActor{
		// all errors in table actor will be reported to processor
		reportErr: cdcCtx.Throw,
		mb:        mb,
		wg:        wg,
		cancel:    cancel,

		tableID:       tableID,
		markTableID:   replicaInfo.MarkTableID,
		tableName:     tableName,
		cyclicEnabled: cyclicEnabled,
		memoryQuota:   serverConfig.GetGlobalServerConfig().PerTableMemoryQuota,
		mounter:       mounter,
		replicaInfo:   replicaInfo,
		replConfig:    config,
		sink:          sink,
		targetTs:      targetTs,
		started:       false,

		changefeedVars:   info,
		globalVars:       vars,
		tableActorRouter: vars.TableActorSystem.Router(),
		actorID:          actorID,
	}
	table.stopFunc = stop(cctx, table)

	startTime := time.Now()
	log.Info("spawn and start table actor",
		zap.String("changfeed", info.ID),
		zap.String("tableName", tableName),
		zap.Int64("tableID", tableID))
	if err := table.start(cctx); err != nil {
		table.stopFunc(err)
		return nil, errors.Trace(err)
	}
	err := vars.TableActorSystem.System().Spawn(mb, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("spawn and start table actor done",
		zap.String("changfeed", info.ID),
		zap.String("tableName", tableName),
		zap.Int64("tableID", tableID),
		zap.Duration("duration", time.Since(startTime)))
	return table, nil
}

func (t *tableActor) Poll(ctx context.Context, msgs []message.Message) bool {
MSG:
	for i := range msgs {
		if atomic.LoadUint32(&t.stopped) == stopped {
			// No need to handle remaining messages.
			break
		}

		switch msgs[i].Tp {
		case message.TypeBarrier:
			t.sortNode.UpdateBarrierTs(msgs[i].BarrierTs)
			if err := t.sinkNode.UpdateBarrierTs(ctx, msgs[i].BarrierTs); err != nil {
				t.checkError(err)
				break
			}
		case message.TypeTick:
			// tick message flush the raw event to sink, follow the old pipeline implementation, batch flush the events  every 500ms
			if time.Since(t.lastFlushTime) > sinkFlushInterval {
				_, err := t.sinkNode.HandleMessage(ctx, pipeline.TickMessage())
				if err != nil {
					t.checkError(err)
					break
				}
				t.lastFlushTime = time.Now()
			}
		case message.TypeStop:
			// async stop the sink
			go func() {
				_, err := t.sinkNode.HandleMessage(ctx,
					pipeline.CommandMessage(&pipeline.Command{Tp: pipeline.CommandTypeStop}),
				)
				if err != nil {
					t.checkError(err)
				}
			}()
		}
		// process message for each node
		for _, n := range t.nodes {
			if err := n.TryRun(ctx); err != nil {
				log.Error("failed to process message, stop table actor ",
					zap.String("tableName", t.tableName),
					zap.Int64("tableID", t.tableID), zap.Error(err))
				t.checkError(err)
				break MSG
			}
		}
	}
	return atomic.LoadUint32(&t.stopped) != stopped
}

func (t *tableActor) start(sdtTableContext context.Context) error {
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

	pullerNode := newPullerNode(t.tableID, t.replicaInfo, t.tableName, t.changefeedVars.ID)
	pullerActorNodeContext := NewContext(sdtTableContext,
		t.tableName,
		t.globalVars.TableActorSystem.Router(),
		t.actorID, t.changefeedVars, t.globalVars, t.reportErr)
	if err := pullerNode.Init(pullerActorNodeContext); err != nil {
		log.Error("puller fails to start",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
		return err
	}
	t.pullerNode = pullerNode

	flowController := common.NewTableFlowController(t.memoryQuota)
	sorterNode := newSorterNode(t.tableName, t.tableID,
		t.replicaInfo.StartTs, flowController,
		t.mounter, t.replConfig,
	)
	sortActorNodeContext := NewContext(sdtTableContext, t.tableName,
		t.globalVars.TableActorSystem.Router(),
		t.actorID, t.changefeedVars, t.globalVars, t.reportErr)
	if err := sorterNode.StartActorNode(sortActorNodeContext, true, t.wg); err != nil {
		log.Error("sorter fails to start",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
		return err
	}

	// construct sort actor node, it gets message from pullerNode chan, and send it to sorter
	t.sortNode = sorterNode
	var messageFetchFunc AsyncMessageHolderFunc = func() *pipeline.Message { return pullerActorNodeContext.tryGetProcessedMessage() }
	var messageProcessFunc AsyncMessageProcessorFunc = func(ctx context.Context, msg pipeline.Message) (bool, error) {
		return sorterNode.TryHandleDataMessage(sortActorNodeContext, msg)
	}
	t.nodes = append(t.nodes, NewActorNode(messageFetchFunc, messageProcessFunc))

	var cyclicActorNodeContext *cyclicNodeContext
	if t.cyclicEnabled {
		cyclicNode := newCyclicMarkNode(t.markTableID)
		cyclicActorNodeContext = NewCyclicNodeContext(
			NewContext(sdtTableContext, t.tableName,
				t.globalVars.TableActorSystem.Router(),
				t.actorID, t.changefeedVars,
				t.globalVars, t.reportErr))
		if err := cyclicNode.Init(cyclicActorNodeContext); err != nil {
			log.Error("sink fails to start",
				zap.String("tableName", t.tableName),
				zap.Int64("tableID", t.tableID),
				zap.Error(err))
			return err
		}

		// construct cyclic actor node if it's enabled, it gets message from sortNode chan
		messageFetchFunc = func() *pipeline.Message { return sortActorNodeContext.tryGetProcessedMessage() }
		messageProcessFunc = func(ctx context.Context, msg pipeline.Message) (bool, error) {
			return cyclicNode.TryHandleDataMessage(cyclicActorNodeContext, msg)
		}
		t.nodes = append(t.nodes, NewActorNode(messageFetchFunc, messageProcessFunc))
	}

	actorSinkNode := newSinkNode(t.tableID, t.sink,
		t.replicaInfo.StartTs,
		t.targetTs, flowController)
	if err := actorSinkNode.InitWithReplicaConfig(true, t.replConfig); err != nil {
		log.Error("sink fails to start",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
		return err
	}
	t.sinkNode = actorSinkNode

	// construct sink actor node, it gets message from sortNode chan or cyclicNode
	if t.cyclicEnabled {
		messageFetchFunc = func() *pipeline.Message { return cyclicActorNodeContext.tryGetProcessedMessage() }
	} else {
		messageFetchFunc = func() *pipeline.Message { return sortActorNodeContext.tryGetProcessedMessage() }
	}
	messageProcessFunc = func(ctx context.Context, msg pipeline.Message) (bool, error) {
		return actorSinkNode.HandleMessage(sdtTableContext, msg)
	}
	t.nodes = append(t.nodes, NewActorNode(messageFetchFunc, messageProcessFunc))

	t.started = true
	log.Info("table actor is started",
		zap.String("tableName", t.tableName),
		zap.Int64("tableID", t.tableID))
	return nil
}

// stop the actor, it's idempotent
func stop(ctx context.Context, t *tableActor) func(err error) {
	return func(err error) {
		t.lck.Lock()
		defer t.lck.Unlock()

		if atomic.LoadUint32(&t.stopped) == stopped {
			return
		}
		atomic.StoreUint32(&t.stopped, stopped)
		if t.sortNode != nil {
			t.sortNode.ReleaseResource(ctx, t.changefeedID, t.globalVars.CaptureInfo.AdvertiseAddr)
		}
		if t.sinkNode != nil {
			if err := t.sinkNode.ReleaseResource(ctx); err != nil {
				log.Warn("close sink failed",
					zap.String("changefeed", t.changefeedID),
					zap.String("tableName", t.tableName),
					zap.Error(err), zap.Error(err))
			}
		}
		t.cancel()
		log.Info("table actor will be stopped",
			zap.String("changefeed", t.changefeedID),
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
	}
}

// any error will be reported to processor
func (t *tableActor) checkError(err error) {
	t.stopFunc(err)
	t.reportErr(err)
}

// ============ Implement TablePipline, must be threadsafe ============

// ResolvedTs returns the resolved ts in this table pipeline
func (t *tableActor) ResolvedTs() model.Ts {
	// TODO: after TiCDC introduces p2p based resolved ts mechanism, TiCDC nodes
	// will be able to cooperate replication status directly. Then we will add
	// another replication barrier for consistent replication instead of reusing
	// the global resolved-ts.
	if redo.IsConsistentEnabled(t.replConfig.Consistent.Level) {
		return t.sinkNode.ResolvedTs()
	}
	return t.sortNode.ResolvedTs()
}

// CheckpointTs returns the checkpoint ts in this table pipeline
func (t *tableActor) CheckpointTs() model.Ts {
	return t.sinkNode.CheckpointTs()
}

// UpdateBarrierTs updates the barrier ts in this table pipeline
func (t *tableActor) UpdateBarrierTs(ts model.Ts) {
	msg := message.BarrierMessage(ts)
	err := t.tableActorRouter.Send(t.actorID, msg)
	if err != nil {
		log.Warn("send fails",
			zap.Reflect("msg", msg),
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
	}
}

// AsyncStop tells the pipeline to stop, and returns true if the pipeline is already stopped.
func (t *tableActor) AsyncStop(targetTs model.Ts) bool {
	// TypeStop stop the sinkNode only ,the processor stop the sink to release some resource
	// and then stop the whole table pipeline by call Cancel
	msg := message.StopMessage()
	err := t.tableActorRouter.Send(t.actorID, msg)
	log.Info("send async stop signal to table",
		zap.String("tableName", t.tableName),
		zap.Int64("tableID", t.tableID),
		zap.Uint64("targetTs", targetTs))
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
	// cancel wait group, release resource and mark the status as stopped
	t.stopFunc(nil)
	// actor is closed, tick actor to remove this actor router
	if err := t.tableActorRouter.Send(t.mb.ID(), message.TickMessage()); err != nil {
		log.Warn("fails to send Stop message",
			zap.String("tableName", t.tableName),
			zap.Int64("tableID", t.tableID),
			zap.Error(err))
	}
}

// Wait waits for table pipeline destroyed
func (t *tableActor) Wait() {
	_ = t.wg.Wait()
}
