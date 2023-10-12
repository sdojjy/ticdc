// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata/sql"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	// When errors occurred, and we need to do backoff, we start an exponential backoff
	// with an interval from 10s to 30min (10s, 20s, 40s, 80s, 160s, 320s,
	//	 600s, 600s, ...).
	// To avoid thunderherd, a random factor is also added.
	defaultBackoffInitInterval        = 10 * time.Second
	defaultBackoffMaxInterval         = 10 * time.Minute
	defaultBackoffMaxElapsedTime      = 30 * time.Minute
	defaultBackoffRandomizationFactor = 0.1
	defaultBackoffMultiplier          = 2.0
)

type feedStateManagerImpl struct {
	shouldBeRunning bool
	shouldBeRemoved bool
	adminJobQueue   []*model.AdminJob
	id              model.ChangeFeedID
	ownerdb         *sql.OwnerOb[*gorm.DB]

	isRetrying                    bool
	lastErrorRetryTime            time.Time                   // time of last error for a changefeed
	lastErrorRetryCheckpointTs    model.Ts                    // checkpoint ts of last retry
	lastWarningReportCheckpointTs model.Ts                    // checkpoint ts of last warning report
	backoffInterval               time.Duration               // the interval for restarting a changefeed in 'error' state
	errBackoff                    *backoff.ExponentialBackOff // an exponential backoff for restarting a changefeed
}

func newFeedStateManager(id model.ChangeFeedID,
	ownerdb *sql.OwnerOb[*gorm.DB]) *feedStateManagerImpl {
	f := &feedStateManagerImpl{
		adminJobQueue: make([]*model.AdminJob, 0),
	}
	f.errBackoff = backoff.NewExponentialBackOff()
	f.errBackoff.InitialInterval = defaultBackoffInitInterval
	f.errBackoff.MaxInterval = defaultBackoffMaxInterval
	f.errBackoff.Multiplier = defaultBackoffMultiplier
	f.errBackoff.RandomizationFactor = defaultBackoffRandomizationFactor
	// backoff will stop once the defaultBackoffMaxElapsedTime has elapsed.
	f.errBackoff.MaxElapsedTime = defaultBackoffMaxElapsedTime

	f.resetErrRetry()
	return f
}

// resetErrRetry reset the error retry related fields
func (m *feedStateManagerImpl) resetErrRetry() {
	m.errBackoff.Reset()
	m.backoffInterval = m.errBackoff.NextBackOff()
	m.lastErrorRetryTime = time.Unix(0, 0)
}

func (f *feedStateManagerImpl) PushAdminJob(job *model.AdminJob) {
	f.pushAdminJob(job)
}

func (f *feedStateManagerImpl) Tick(resolvedTs model.Ts) bool {
	//TODO implement me
	return false
}

func (f *feedStateManagerImpl) HandleError(errs ...*model.RunningError) {
	for _, w := range errs {
		_ = f.ownerdb.SetChangefeedFailed(w)
	}
}

func (f *feedStateManagerImpl) HandleWarning(warnings ...*model.RunningError) {
	for _, w := range warnings {
		_ = f.ownerdb.SetChangefeedWarning(w)
	}
}

func (f *feedStateManagerImpl) ShouldRunning() bool {
	return f.shouldBeRunning
}

func (f *feedStateManagerImpl) ShouldRemoved() bool {
	return f.shouldBeRemoved
}

func (f *feedStateManagerImpl) MarkFinished() {
	f.pushAdminJob(&model.AdminJob{
		CfID: f.id,
		Type: model.AdminFinish,
	})
}

func (f *feedStateManagerImpl) handleAdminJob() (jobsPending bool) {
	job := f.popAdminJob()
	if job == nil || job.CfID != f.id {
		return false
	}
	log.Info("handle admin job",
		zap.String("namespace", f.id.Namespace),
		zap.String("changefeed", f.id.ID),
		zap.Any("job", job))
	switch job.Type {
	case model.AdminStop:
		//switch m.state.Info.State {
		//case model.StateNormal, model.StateWarning, model.StatePending:
		//default:
		//	log.Warn("can not pause the changefeed in the current state",
		//		zap.String("namespace", f.id.Namespace),
		//		zap.String("changefeed", f.id.ID),
		//		zap.String("changefeedState", string(m.state.Info.State)), zap.Any("job", job))
		//	return
		//}
		f.shouldBeRunning = false
		jobsPending = true
		f.ownerdb.ResumeChangefeed()
	case model.AdminRemove:
		f.shouldBeRunning = false
		f.shouldBeRemoved = true
		jobsPending = true

		f.ownerdb.SetChangefeedRemoved()

		// remove info
		//m.state.PatchInfo(func(info *model.ChangeFeedInfo) (
		//	*model.ChangeFeedInfo, bool, error,
		//) {
		//	return nil, true, nil
		//})
		//// remove changefeedStatus
		//m.state.PatchStatus(
		//	func(status *model.ChangeFeedStatus) (
		//		*model.ChangeFeedStatus, bool, error,
		//	) {
		//		return nil, true, nil
		//	})
		//checkpointTs := m.state.Info.GetCheckpointTs(m.state.Status)

		log.Info("the changefeed is removed",
			zap.String("namespace", f.id.Namespace),
			zap.String("changefeed", f.id.ID))
		//,zap.Uint64("checkpointTs", checkpointTs))
	case model.AdminResume:
		//switch m.state.Info.State {
		//case model.StateFailed, model.StateStopped, model.StateFinished:
		//default:
		//	log.Warn("can not resume the changefeed in the current state",
		//		zap.String("namespace", f.id.Namespace),
		//		zap.String("changefeed", f.id.ID),
		//		zap.String("changefeedState", string(m.state.Info.State)),
		//		zap.Any("job", job))
		//	return
		//}
		f.shouldBeRunning = true
		// when the changefeed is manually resumed, we must reset the backoff
		f.resetErrRetry()
		jobsPending = true
		f.ownerdb.ResumeChangefeed()
		//m.patchState(model.StateNormal)

		//m.state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		//	changed := false
		//	if info == nil {
		//		return nil, changed, nil
		//	}
		//	if job.OverwriteCheckpointTs > 0 {
		//		info.StartTs = job.OverwriteCheckpointTs
		//		changed = true
		//	}
		//	if info.Error != nil {
		//		info.Error = nil
		//		changed = true
		//	}
		//	return info, changed, nil
		//})

		//m.state.PatchStatus(func(status *model.ChangeFeedStatus) (
		//	*model.ChangeFeedStatus, bool, error,
		//) {
		//	if job.OverwriteCheckpointTs > 0 {
		//		oldCheckpointTs := status.CheckpointTs
		//		status = &model.ChangeFeedStatus{
		//			CheckpointTs:      job.OverwriteCheckpointTs,
		//			MinTableBarrierTs: job.OverwriteCheckpointTs,
		//			AdminJobType:      model.AdminNone,
		//		}
		//		log.Info("overwriting the tableCheckpoint ts",
		//			zap.String("namespace", m.state.ID.Namespace),
		//			zap.String("changefeed", m.state.ID.ID),
		//			zap.Any("oldCheckpointTs", oldCheckpointTs),
		//			zap.Any("newCheckpointTs", status.CheckpointTs),
		//		)
		//		return status, true, nil
		//	}
		//	return status, false, nil
		//})

	case model.AdminFinish:
		//switch m.state.Info.State {
		//case model.StateNormal, model.StateWarning:
		//default:
		//	log.Warn("can not finish the changefeed in the current state",
		//		zap.String("namespace", f.id.Namespace),
		//		zap.String("changefeed", f.id.ID),
		//		//zap.String("changefeedState", string(m.state.Info.State)),
		//		zap.Any("job", job))
		//	return
		//}
		f.shouldBeRunning = false
		jobsPending = true
		f.ownerdb.SetChangefeedFinished()
		//m.patchState(model.StateFinished)
	default:
		log.Warn("Unknown admin job", zap.Any("adminJob", job),
			zap.String("namespace", f.id.Namespace),
			zap.String("changefeed", f.id.ID))
	}
	return
}

func (f *feedStateManagerImpl) popAdminJob() *model.AdminJob {
	if len(f.adminJobQueue) == 0 {
		return nil
	}
	job := f.adminJobQueue[0]
	f.adminJobQueue = f.adminJobQueue[1:]
	return job
}

func (f *feedStateManagerImpl) pushAdminJob(job *model.AdminJob) {
	f.adminJobQueue = append(f.adminJobQueue, job)
}
