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
	"github.com/pingcap/tiflow/new_arch/scheduller"
	"math"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// captureIDNotDraining is the default capture ID if the drain target not set
const captureIDNotDraining = ""

var _ Scheduler = &drainCaptureScheduler{}

type drainCaptureScheduler struct {
	mu     sync.Mutex
	target model.CaptureID

	maxTaskConcurrency int
}

func newDrainCaptureScheduler(
	concurrency int,
) *drainCaptureScheduler {
	return &drainCaptureScheduler{
		target:             captureIDNotDraining,
		maxTaskConcurrency: concurrency,
	}
}

func (d *drainCaptureScheduler) Name() string {
	return "drain-capture-scheduler"
}

func (d *drainCaptureScheduler) getTarget() model.CaptureID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.target
}

func (d *drainCaptureScheduler) setTarget(target model.CaptureID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.target != captureIDNotDraining {
		return false
	}

	d.target = target
	return true
}

func (d *drainCaptureScheduler) Schedule(
	currentChangefeeds []*model.ChangeFeedInfo,
	aliveCaptures map[model.CaptureID]*CaptureStatus,
	replications map[model.ChangeFeedID]*changefeed,
) []*ScheduleTask {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.target == captureIDNotDraining {
		// There are two ways to make a capture "stopping",
		// 1. PUT /api/v1/capture/drain
		// 2. kill <TiCDC_PID>
		for id, capture := range aliveCaptures {
			if capture.IsOwner {
				// Skip draining owner.
				continue
			}
			if capture.State == CaptureStateStopping {
				d.target = id
				break
			}
		}

		if d.target == captureIDNotDraining {
			return nil
		}

		log.Info("schedulerv3: drain a stopping capture",
			zap.String("captureID", d.target))
	}

	// Currently, the workload is the number of tables in a capture.
	captureWorkload := make(map[model.CaptureID]int)
	for id := range aliveCaptures {
		if id != d.target {
			captureWorkload[id] = 0
		}
	}

	// this may happen when inject the target, there is at least 2 alive captures
	// but when schedule the task, only owner alive.
	if len(captureWorkload) == 0 {
		log.Warn("schedulerv3: drain capture scheduler ignore drain target capture, "+
			"since cannot found destination captures",
			zap.String("target", d.target), zap.Any("captures", aliveCaptures))
		d.target = captureIDNotDraining
		return nil
	}

	maxTaskConcurrency := d.maxTaskConcurrency
	// victimSpans record tables should be moved out from the target capture
	victimSpans := make([]*changefeed, 0, maxTaskConcurrency)
	skipDrain := false
	for _, rep := range replications {
		if rep.scheduleState != scheduller.SchedulerComponentStatusWorking {
			// only drain the target capture if all tables is replicating,
			log.Debug("schedulerv3: drain capture scheduler skip this tick,"+
				"not all table is replicating",
				zap.String("target", d.target),
				zap.Any("replication", rep))
			skipDrain = true
			break
		}

		if rep.primary == d.target {
			if len(victimSpans) < maxTaskConcurrency {
				victimSpans = append(victimSpans, rep)
			}
		}

		// only calculate workload of other captures not the drain target.
		if rep.primary != d.target {
			captureWorkload[rep.primary]++
		}
	}
	if skipDrain {
		return nil
	}

	// this always indicate that the whole draining process finished, and can be triggered by:
	// 1. the target capture has no table at the beginning
	// 2. all tables moved from the target capture
	// 3. the target capture cannot be found in the latest captures
	if len(victimSpans) == 0 {
		log.Info("schedulerv3: drain capture scheduler finished, since no table",
			zap.String("target", d.target))
		d.target = captureIDNotDraining
		return nil
	}

	// For each victim table, find the target for it
	result := make([]*ScheduleTask, 0, maxTaskConcurrency)
	for _, span := range victimSpans {
		target := ""
		minWorkload := math.MaxInt64
		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("schedulerv3: drain capture meet unexpected min workload",
				zap.Any("workload", captureWorkload))
		}

		result = append(result, &ScheduleTask{
			MoveChangefeed: &MoveChangefeed{
				Changefeed:  span.ID,
				DestCapture: target,
			},
			Accept: (Callback)(nil), // No need for accept callback here.
		})

		// Increase target workload to make sure tables are evenly distributed.
		captureWorkload[target]++
	}

	return result
}
