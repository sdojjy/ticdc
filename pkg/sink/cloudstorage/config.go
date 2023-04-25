// Copyright 2022 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/config_applier"
	"go.uber.org/zap"
)

const (
	// defaultWorkerCount is the default value of worker-count.
	defaultWorkerCount = 16
	// the upper limit of worker-count.
	maxWorkerCount = 512
	// defaultFlushInterval is the default value of flush-interval.
	defaultFlushInterval = 5 * time.Second
	// the lower limit of flush-interval.
	minFlushInterval = 2 * time.Second
	// the upper limit of flush-interval.
	maxFlushInterval = 10 * time.Minute
	// defaultFileSize is the default value of file-size.
	defaultFileSize = 64 * 1024 * 1024
	// the lower limit of file size
	minFileSize = 1024 * 1024
	// the upper limit of file size
	maxFileSize = 512 * 1024 * 1024
)

// Config is the configuration for cloud storage sink.
type Config struct {
	WorkerCount              int
	FlushInterval            time.Duration
	FileSize                 int
	DateSeparator            string
	EnablePartitionSeparator bool
}

// NewConfig returns the default cloud storage sink config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:   defaultWorkerCount,
		FlushInterval: defaultFlushInterval,
		FileSize:      defaultFileSize,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (err error) {
	if sinkURI == nil {
		return cerror.ErrStorageSinkInvalidConfig.GenWithStack(
			"failed to open cloud storage sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !psink.IsStorageScheme(scheme) {
		return cerror.ErrStorageSinkInvalidConfig.GenWithStack(
			"can't create cloud storage sink with unsupported scheme: %s", scheme)
	}
	query := sinkURI.Query()
	// make sure MySQLConfig is not nil
	if replicaConfig.Sink.CloudStorageConfig == nil {
		replicaConfig.Sink.CloudStorageConfig = &config.CloudStorageConfig{}
		defer func() {
			replicaConfig.Sink.CloudStorageConfig = nil
		}()
	}
	var flushInterval string
	var appliers = []any{
		config_applier.Applier[int]{
			ValueGetter: []config_applier.ValueGetter[int]{
				config_applier.ConfigFileIntValueGetter(replicaConfig.Sink.CloudStorageConfig.WorkerCount),
				config_applier.UrlIntValGetter(query, "worker-count"),
			},
			ValidateAndAdjust: validateAndAdjustWorkerCount,
			ValueHolder:       &c.WorkerCount,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				config_applier.ConfigFileStringValueGetter(replicaConfig.Sink.CloudStorageConfig.FlushInterval),
				config_applier.UrlStringValGetter(query, "flush-interval"),
			},
			ValidateAndAdjust: validateAndAdjustFlushInterval,
			ValueHolder:       &flushInterval,
		},
		config_applier.Applier[int]{
			ValueGetter: []config_applier.ValueGetter[int]{
				config_applier.ConfigFileIntValueGetter(replicaConfig.Sink.CloudStorageConfig.FileSize),
				config_applier.UrlIntValGetter(query, "file-size"),
			},
			ValidateAndAdjust: validateAndAdjustFileSize,
			ValueHolder:       &c.FileSize,
		},
	}
	if err := config_applier.Apply(appliers); err != nil {
		return err
	}
	interval, err := time.ParseDuration(flushInterval)
	if err != nil {
		return err
	}
	c.FlushInterval = interval
	c.DateSeparator = replicaConfig.Sink.DateSeparator
	c.EnablePartitionSeparator = replicaConfig.Sink.EnablePartitionSeparator

	return nil
}

func validateAndAdjustWorkerCount(c int) (int, error) {
	if c <= 0 {
		return 0, cerror.WrapError(cerror.ErrStorageSinkInvalidConfig,
			fmt.Errorf("invalid worker-count %d, it must be greater than 0", c))
	}
	if c > maxWorkerCount {
		log.Warn("worker-count is too large",
			zap.Int("original", c), zap.Int("override", maxWorkerCount))
		c = maxWorkerCount
	}
	return c, nil
}

func validateAndAdjustFlushInterval(s string) (string, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}

	if d > maxFlushInterval {
		log.Warn("flush-interval is too large", zap.Duration("original", d),
			zap.Duration("override", maxFlushInterval))
		d = maxFlushInterval
	}
	if d < minFlushInterval {
		log.Warn("flush-interval is too small", zap.Duration("original", d),
			zap.Duration("override", minFlushInterval))
		d = minFlushInterval
	}

	return d.String(), nil
}

func validateAndAdjustFileSize(sz int) (int, error) {
	if sz > maxFileSize {
		log.Warn("file-size is too large",
			zap.Int("original", sz), zap.Int("override", maxFileSize))
		sz = maxFileSize
	}
	if sz < minFileSize {
		log.Warn("file-size is too small",
			zap.Int("original", sz), zap.Int("override", minFileSize))
		sz = minFileSize
	}
	return sz, nil
}
