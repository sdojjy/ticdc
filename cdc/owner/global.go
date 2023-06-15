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

package owner

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

type GlobalOwner struct {
	bootstrapped    bool
	captures        map[model.CaptureID]*model.CaptureInfo
	upstreamManager *upstream.Manager
	changefeeds     map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState
}

func NewGlobalOwner(manager *upstream.Manager) *GlobalOwner {
	return &GlobalOwner{
		captures:        make(map[model.CaptureID]*model.CaptureInfo),
		changefeeds:     make(map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState),
		upstreamManager: manager,
	}
}

// Bootstrap checks if the state contains incompatible or incorrect information and tries to fix it.
func (o *GlobalOwner) Bootstrap(state *orchestrator.GlobalReactorState) {
	log.Info("Start bootstrapping")
	fixChangefeedInfos(state)
}

func (o *GlobalOwner) clusterVersionConsistent(captures map[model.CaptureID]*model.CaptureInfo) bool {
	versions := make(map[string]struct{}, len(captures))
	for _, capture := range captures {
		versions[capture.Version] = struct{}{}
	}

	if err := version.CheckTiCDCVersion(versions); err != nil {
		//if o.logLimiter.Allow() {
		//	log.Warn("TiCDC cluster versions not allowed",
		//		zap.String("ownerVer", version.ReleaseVersion),
		//		zap.Any("captures", captures), zap.Error(err))
		//}
		return false
	}
	return true
}

// ignoreFailedChangeFeedWhenGC checks if a failed changefeed should be ignored
// when calculating the gc safepoint of the associated upstream.
func (o *GlobalOwner) ignoreFailedChangeFeedWhenGC(
	state *orchestrator.ChangefeedReactorState,
) bool {
	upID := state.Info.UpstreamID
	us, exist := o.upstreamManager.Get(upID)
	if !exist {
		log.Warn("upstream not found", zap.Uint64("ID", upID))
		return false
	}
	// in case the changefeed failed right after it is created
	// and the status is not initialized yet.
	ts := state.Info.StartTs
	if state.Status != nil {
		ts = state.Status.CheckpointTs
	}
	return us.GCManager.IgnoreFailedChangeFeed(ts)
}

// calculateGCSafepoint calculates GCSafepoint for different upstream.
// Note: we need to maintain a TiCDC service GC safepoint for each upstream TiDB cluster
// to prevent upstream TiDB GC from removing data that is still needed by TiCDC.
// GcSafepoint is the minimum checkpointTs of all changefeeds that replicating a same upstream TiDB cluster.
func (o *GlobalOwner) calculateGCSafepoint(state *orchestrator.GlobalReactorState) (
	map[uint64]uint64, map[uint64]interface{},
) {
	minCheckpointTsMap := make(map[uint64]uint64)
	forceUpdateMap := make(map[uint64]interface{})

	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil {
			continue
		}

		switch changefeedState.Info.State {
		case model.StateNormal, model.StateStopped, model.StateError:
		case model.StateFailed:
			if o.ignoreFailedChangeFeedWhenGC(changefeedState) {
				continue
			}
		default:
			continue
		}

		checkpointTs := changefeedState.Info.GetCheckpointTs(changefeedState.Status)
		upstreamID := changefeedState.Info.UpstreamID

		if _, exist := minCheckpointTsMap[upstreamID]; !exist {
			minCheckpointTsMap[upstreamID] = checkpointTs
		}

		minCpts := minCheckpointTsMap[upstreamID]

		if minCpts > checkpointTs {
			minCpts = checkpointTs
			minCheckpointTsMap[upstreamID] = minCpts
		}
		// Force update when adding a new changefeed.
		_, exist := o.changefeeds[changefeedID]
		if !exist {
			forceUpdateMap[upstreamID] = nil
		}
	}
	return minCheckpointTsMap, forceUpdateMap
}

func (o *GlobalOwner) updateGCSafepoint(
	ctx context.Context, state *orchestrator.GlobalReactorState,
) error {
	minChekpoinTsMap, forceUpdateMap := o.calculateGCSafepoint(state)

	for upstreamID, minCheckpointTs := range minChekpoinTsMap {
		up, ok := o.upstreamManager.Get(upstreamID)
		if !ok {
			upstreamInfo := state.Upstreams[upstreamID]
			up = o.upstreamManager.AddUpstream(upstreamInfo)
		}
		if !up.IsNormal() {
			log.Warn("upstream is not ready, skip",
				zap.Uint64("id", up.ID),
				zap.Strings("pd", up.PdEndpoints))
			continue
		}

		// When the changefeed starts up, CDC will do a snapshot read at
		// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
		// bound for the GC safepoint.
		gcSafepointUpperBound := minCheckpointTs - 1

		var forceUpdate bool
		if _, exist := forceUpdateMap[upstreamID]; exist {
			forceUpdate = true
		}

		err := up.GCManager.TryUpdateGCSafePoint(ctx, gcSafepointUpperBound, forceUpdate)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Tick implements the Reactor interface
func (o *GlobalOwner) Tick(stdCtx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	failpoint.Inject("owner-run-with-error", func() {
		failpoint.Return(nil, errors.New("owner run with injected error"))
	})
	failpoint.Inject("sleep-in-owner-tick", nil)
	state := rawState.(*orchestrator.GlobalReactorState)
	// At the first Tick, we need to do a bootstrap operation.
	// Fix incompatible or incorrect meta information.
	if !o.bootstrapped {
		o.Bootstrap(state)
		o.bootstrapped = true
		return state, nil
	}

	o.captures = state.Captures

	if !o.clusterVersionConsistent(o.captures) {
		return state, nil
	}
	// Owner should update GC safepoint before initializing changefeed, so
	// changefeed can remove its "ticdc-creating" service GC safepoint during
	// initializing.
	//
	// See more gc doc.
	if err = o.updateGCSafepoint(stdCtx, state); err != nil {
		return nil, errors.Trace(err)
	}

	// Tick all changefeeds.
	captureSizeMap := make(map[model.CaptureID]int)
	for _, c := range state.Captures {
		captureSizeMap[c.ID] = 0
	}
	var needAddsignedChangefeeds []*orchestrator.ChangefeedReactorState
	totalCount := 0
	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil {
			if _, ok := o.changefeeds[changefeedID]; ok {
				delete(o.changefeeds, changefeedID)
			}
			continue
		}
		totalCount++
		o.changefeeds[changefeedID] = changefeedState
		if changefeedState.Status == nil {
			// complete the changefeed status when it is just created.
			changefeedState.PatchStatus(
				func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
					if status == nil {
						status = &model.ChangeFeedStatus{
							// changefeed status is nil when the changefeed has just created.
							ResolvedTs:        changefeedState.Info.StartTs,
							CheckpointTs:      changefeedState.Info.StartTs,
							MinTableBarrierTs: changefeedState.Info.StartTs,
							AdminJobType:      model.AdminNone,
						}
						return status, true, nil
					}
					return status, false, nil
				})
		}
		if changefeedState.Owner != nil {
			_, ok := state.Captures[changefeedState.Owner.OwnerID]
			if !ok {
				needAddsignedChangefeeds = append(needAddsignedChangefeeds, changefeedState)
			} else {
				captureSizeMap[changefeedState.Owner.OwnerID] += 1
			}
		} else {
			needAddsignedChangefeeds = append(needAddsignedChangefeeds, changefeedState)
		}
	}

	for _, c := range needAddsignedChangefeeds {
		//find smallest capture
		var minCaptureID model.CaptureID
		for captureID, _ := range captureSizeMap {
			if minCaptureID == "" {
				minCaptureID = captureID
			} else {
				if captureSizeMap[captureID] < captureSizeMap[minCaptureID] {
					minCaptureID = captureID
				}
			}
		}
		c.PatchOwner(func(owner *model.ChangeFeedOwner) (*model.ChangeFeedOwner, bool, error) {
			if owner == nil {
				owner = &model.ChangeFeedOwner{}
			}
			owner.OwnerID = minCaptureID
			return owner, true, nil
		})
		captureSizeMap[minCaptureID] += 1
	}

	// no changefeed capture llist
	//rebalance changefeed
	needReblance := false
	var needBalanceChangefeeds []*orchestrator.ChangefeedReactorState
	for _, c := range state.Captures {
		changefeedCount := captureSizeMap[c.ID]
		if changefeedCount == 0 {
			needReblance = true
		}
	}
	if needReblance {
		avgCount := totalCount / len(state.Captures)
		var captureChangefeedMap = make(map[model.CaptureID][]*orchestrator.ChangefeedReactorState)
		for _, c := range state.Changefeeds {
			if c.Info == nil {
				continue
			}
			captureChangefeedMap[c.Owner.OwnerID] = append(captureChangefeedMap[c.Owner.OwnerID], c)
		}
		for capture, changefeeds := range captureChangefeedMap {
			for _, c := range changefeeds[avgCount:] {
				captureSizeMap[capture] -= 1
				needBalanceChangefeeds = append(needBalanceChangefeeds, c)
			}
		}

		if len(needBalanceChangefeeds) > 0 {
			log.Info("relance changefeed", zap.Int("count", len(needBalanceChangefeeds)))
		}
		for _, c := range needBalanceChangefeeds {
			//find smallest capture
			var minCaptureID model.CaptureID
			for captureID, _ := range captureSizeMap {
				if minCaptureID == "" {
					minCaptureID = captureID
				} else {
					if captureSizeMap[captureID] < captureSizeMap[minCaptureID] {
						minCaptureID = captureID
					}
				}
			}
			c.PatchOwner(func(owner *model.ChangeFeedOwner) (*model.ChangeFeedOwner, bool, error) {
				if owner == nil {
					owner = &model.ChangeFeedOwner{}
				}
				owner.OwnerID = minCaptureID
				return owner, true, nil
			})
			captureSizeMap[minCaptureID] += 1
		}
	}

	// Cleanup changefeeds that are not in the state.
	if len(o.changefeeds) != len(state.Changefeeds) {
		for changefeedID, _ := range o.changefeeds {
			if _, exist := state.Changefeeds[changefeedID]; exist {
				continue
			}
			delete(o.changefeeds, changefeedID)
		}
	}

	if err := o.upstreamManager.Tick(stdCtx, state); err != nil {
		return state, errors.Trace(err)
	}
	return state, nil
}
