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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	msql "github.com/pingcap/tiflow/cdcv2/metadata/sql"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type changefeedImpl struct {
	uuid uint64
	ID   model.ChangeFeedID

	Info             *model.ChangeFeedInfo
	Status           *model.ChangeFeedStatus
	processor        processor.Processor
	changefeed       owner.Changefeed
	feedstateManager *feedStateManagerImpl
	captureOb        *msql.CaptureOb[*gorm.DB]
}

func newChangefeed(changefeed owner.Changefeed,
	uuid metadata.ChangefeedUUID,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	processor processor.Processor,
	feedstateManager *feedStateManagerImpl,
	captureDB *msql.CaptureOb[*gorm.DB]) *changefeedImpl {
	return &changefeedImpl{
		uuid: uuid,
		ID: model.ChangeFeedID{
			Namespace: info.Namespace,
			ID:        info.ID,
		},
		changefeed:       changefeed,
		Status:           status,
		Info:             info,
		processor:        processor,
		feedstateManager: feedstateManager,
		captureOb:        captureDB,
	}
}

// GetInfoProvider returns an InfoProvider if one is available.
func (c *changefeedImpl) GetInfoProvider() scheduler.InfoProvider {
	if provider, ok := c.changefeed.GetScheduler().(scheduler.InfoProvider); ok {
		return provider
	}
	return nil
}

func (c *changefeedImpl) Tick(ctx cdcContext.Context, info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus, captures map[model.CaptureID]*model.CaptureInfo) (model.Ts, model.Ts) {
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID: c.ID,
	})
	err, warning := c.processor.Tick(ctx, info, status)
	if warning != nil {
		c.patchProcessorWarning(c.captureOb.Self(), warning)
	}
	if err != nil {
		c.patchProcessorErr(c.captureOb.Self(), err)
		// patchProcessorErr have already patched its error to tell the owner
		// manager can just close the processor and continue to tick other processors
		err = c.processor.Close()
		if err != nil {
			log.Warn("failed to close processor",
				zap.String("namespace", c.ID.Namespace),
				zap.String("changefeed", c.ID.ID),
				zap.Error(err))
		}
	}
	return c.changefeed.Tick(ctx, info, status, captures)
}

var processorIgnorableError = []*errors.Error{
	cerror.ErrAdminStopProcessor,
	cerror.ErrReactorFinished,
}

// isProcessorIgnorableError returns true if the error means the processor exits
// normally, caused by changefeed pause, remove, etc.
func isProcessorIgnorableError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Cause(err) == context.Canceled {
		return true
	}
	for _, e := range processorIgnorableError {
		if e.Equal(err) {
			return true
		}
	}
	return false
}

func (c *changefeedImpl) patchProcessorErr(captureInfo *model.CaptureInfo,
	err error,
) {
	if isProcessorIgnorableError(err) {
		log.Info("processor exited",
			zap.String("capture", captureInfo.ID),
			zap.String("namespace", c.ID.Namespace),
			zap.String("changefeed", c.ID.ID),
			zap.Error(err))
		return
	}
	// record error information in etcd
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrProcessorUnknown.RFCCode())
	}
	c.feedstateManager.ownerdb.SetChangefeedFailed(&model.RunningError{
		Time:    time.Now(),
		Addr:    captureInfo.AdvertiseAddr,
		Code:    code,
		Message: err.Error(),
	})
	log.Error("run processor failed",
		zap.String("capture", captureInfo.ID),
		zap.String("namespace", c.ID.Namespace),
		zap.String("changefeed", c.ID.ID),
		zap.Error(err))
}

func (c *changefeedImpl) patchProcessorWarning(captureInfo *model.CaptureInfo, err error,
) {
	if err == nil {
		return
	}
	var code string
	if rfcCode, ok := cerror.RFCCode(err); ok {
		code = string(rfcCode)
	} else {
		code = string(cerror.ErrProcessorUnknown.RFCCode())
	}
	c.feedstateManager.ownerdb.SetChangefeedWarning(&model.RunningError{
		Time:    time.Now(),
		Addr:    captureInfo.AdvertiseAddr,
		Code:    code,
		Message: err.Error(),
	})
}

func (c *changefeedImpl) Close(ctx cdcContext.Context) {
	c.releaseResources(ctx)
}

func (c *changefeedImpl) releaseResources(ctx context.Context) {
	log.Info("changefeed closed",
		zap.String("namespace", c.Info.Namespace),
		zap.String("changefeed", c.Info.ID))
}
