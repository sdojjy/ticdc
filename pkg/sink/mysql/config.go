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

package mysql

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const (
	txnModeOptimistic  = "optimistic"
	txnModePessimistic = "pessimistic"

	// DefaultWorkerCount is the default number of workers.
	DefaultWorkerCount = 16
	// DefaultMaxTxnRow is the default max number of rows in a transaction.
	DefaultMaxTxnRow = 256
	// defaultMaxMultiUpdateRowCount is the default max number of rows in a
	// single multi update SQL.
	defaultMaxMultiUpdateRowCount = 40
	// defaultMaxMultiUpdateRowSize(1KB) defines the default value of MaxMultiUpdateRowSize
	// When row average size is larger MaxMultiUpdateRowSize,
	// disable multi update, otherwise enable multi update.
	defaultMaxMultiUpdateRowSize = 1024
	// The upper limit of max worker counts.
	maxWorkerCount = 1024
	// The upper limit of max txn rows.
	maxMaxTxnRow = 2048
	// The upper limit of max multi update rows in a single SQL.
	maxMaxMultiUpdateRowCount = 256
	// The upper limit of max multi update row size(8KB).
	maxMaxMultiUpdateRowSize = 8192

	defaultTiDBTxnMode  = txnModeOptimistic
	defaultReadTimeout  = "2m"
	defaultWriteTimeout = "2m"
	defaultDialTimeout  = "2m"
	// Note(dongmen): defaultSafeMode is set to false since v6.4.0.
	defaultSafeMode       = false
	defaultTxnIsolationRC = "READ-COMMITTED"
	defaultCharacterSet   = "utf8mb4"

	// BackoffBaseDelay indicates the base delay time for retrying.
	BackoffBaseDelay = 500 * time.Millisecond
	// BackoffMaxDelay indicates the max delay time for retrying.
	BackoffMaxDelay = 60 * time.Second

	defaultBatchDMLEnable  = true
	defaultMultiStmtEnable = true

	// defaultcachePrepStmts is the default value of cachePrepStmts
	defaultCachePrepStmts = true
)

// Config is the configs for MySQL backend.
type Config struct {
	WorkerCount            int
	MaxTxnRow              int
	MaxMultiUpdateRowCount int
	MaxMultiUpdateRowSize  int
	tidbTxnMode            string
	ReadTimeout            string
	WriteTimeout           string
	DialTimeout            string
	SafeMode               bool
	Timezone               string
	TLS                    string
	ForceReplicate         bool
	EnableOldValue         bool

	IsTiDB          bool // IsTiDB is true if the downstream is TiDB
	SourceID        uint64
	BatchDMLEnable  bool
	MultiStmtEnable bool
	CachePrepStmts  bool
}

// NewConfig returns the default mysql backend config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:            DefaultWorkerCount,
		MaxTxnRow:              DefaultMaxTxnRow,
		MaxMultiUpdateRowCount: defaultMaxMultiUpdateRowCount,
		MaxMultiUpdateRowSize:  defaultMaxMultiUpdateRowSize,
		tidbTxnMode:            defaultTiDBTxnMode,
		ReadTimeout:            defaultReadTimeout,
		WriteTimeout:           defaultWriteTimeout,
		DialTimeout:            defaultDialTimeout,
		SafeMode:               defaultSafeMode,
		BatchDMLEnable:         defaultBatchDMLEnable,
		MultiStmtEnable:        defaultMultiStmtEnable,
		CachePrepStmts:         defaultCachePrepStmts,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (err error) {
	if sinkURI == nil {
		return cerror.ErrMySQLConnectionError.GenWithStack("fail to open MySQL sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !sink.IsMySQLCompatibleScheme(scheme) {
		return cerror.ErrMySQLConnectionError.GenWithStack("can't create MySQL sink with unsupported scheme: %s", scheme)
	}
	query := sinkURI.Query()

	var appliers = []any{
		applier[int]{
			getValue: []valueGetter[int]{
				configFileIntValueGetter(replicaConfig.Sink.MySQLConfig.WorkerCount,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlIntValGetter(query, "worker-count"),
			},
			validateAndAdjust: validateAndAdjustWorkerCount,
			valueHolder:       &c.WorkerCount,
		},
		applier[int]{
			getValue: []valueGetter[int]{
				configFileIntValueGetter(replicaConfig.Sink.MySQLConfig.MaxTxRow,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlIntValGetter(query, "max-txn-row"),
			},
			validateAndAdjust: validateAndAdjustMaxTxnRow,
			valueHolder:       &c.MaxTxnRow,
		},
		applier[int]{
			getValue: []valueGetter[int]{
				configFileIntValueGetter(replicaConfig.Sink.MySQLConfig.MaxMultiUpdateRowCount,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlIntValGetter(query, "max-multi-update-row"),
			},
			validateAndAdjust: validateAndAdjustMaxMultiUpdateRowCount,
			valueHolder:       &c.MaxMultiUpdateRowCount,
		},
		applier[int]{
			getValue: []valueGetter[int]{
				configFileIntValueGetter(replicaConfig.Sink.MySQLConfig.MaxMultiUpdateRowSize,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlIntValGetter(query, "max-multi-update-row-size"),
			},
			validateAndAdjust: validateAndAdjustMaxMultiUpdateRowSize,
			valueHolder:       &c.MaxMultiUpdateRowSize,
		},
		applier[string]{
			getValue: []valueGetter[string]{
				configFileStringValueGetter(replicaConfig.Sink.MySQLConfig.TiDBTxnMode,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlStringValGetter(query, "tidb-txn-mode"),
			},
			validateAndAdjust: validateAndAdjustTiDBTxnMode,
			valueHolder:       &c.tidbTxnMode,
		},
		applier[string]{
			getValue: []valueGetter[string]{
				getSSLCAFromConfigFile(replicaConfig, changefeedID),
				getSSLCAFromURL(query, changefeedID),
			},
			validateAndAdjust: nil,
			valueHolder:       &c.TLS,
		},
		applier[bool]{
			getValue: []valueGetter[bool]{
				configFileBoolValueGetter(replicaConfig.Sink.SafeMode, replicaConfig.Sink),
				urlBoolValGetter(query, "safe-mode"),
			},
			validateAndAdjust: nil,
			valueHolder:       &c.SafeMode,
		},
		applier[string]{
			getValue: []valueGetter[string]{
				getTimezone(ctx, query, replicaConfig),
			},
			validateAndAdjust: nil,
			valueHolder:       &c.Timezone,
		},
		applier[string]{
			getValue: []valueGetter[string]{
				configFileStringValueGetter(replicaConfig.Sink.MySQLConfig.ReadTimeout,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlStringValGetter(query, "read-timeout"),
			},
			validateAndAdjust: validateDuration,
			valueHolder:       &c.ReadTimeout,
		},
		applier[string]{
			getValue: []valueGetter[string]{
				configFileStringValueGetter(replicaConfig.Sink.MySQLConfig.WriteTimeout,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlStringValGetter(query, "write-timeout"),
			},
			validateAndAdjust: validateDuration,
			valueHolder:       &c.WriteTimeout,
		},
		applier[string]{
			getValue: []valueGetter[string]{
				configFileStringValueGetter(replicaConfig.Sink.MySQLConfig.Timeout,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlStringValGetter(query, "timeout"),
			},
			validateAndAdjust: validateDuration,
			valueHolder:       &c.DialTimeout,
		},
		applier[bool]{
			getValue: []valueGetter[bool]{
				configFileBoolValueGetter(replicaConfig.Sink.MySQLConfig.EnableBatchDML,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlBoolValGetter(query, "batch-dml-enable"),
			},
			validateAndAdjust: nil,
			valueHolder:       &c.BatchDMLEnable,
		},
		applier[bool]{
			getValue: []valueGetter[bool]{
				configFileBoolValueGetter(replicaConfig.Sink.MySQLConfig.EnableMultiStatement,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlBoolValGetter(query, "multi-stmt-enable"),
			},
			validateAndAdjust: nil,
			valueHolder:       &c.MultiStmtEnable,
		},
		applier[bool]{
			getValue: []valueGetter[bool]{
				configFileBoolValueGetter(replicaConfig.Sink.MySQLConfig.EnableCachePreparedStatement,
					replicaConfig.Sink, replicaConfig.Sink.MySQLConfig),
				urlBoolValGetter(query, "cache-prep-stmts"),
			},
			validateAndAdjust: nil,
			valueHolder:       &c.CachePrepStmts,
		},
	}
	for _, ap := range appliers {
		switch v := ap.(type) {
		case *applier[int]:
			if err = v.apply(); err != nil {
				return err
			}
		case *applier[bool]:
			if err = v.apply(); err != nil {
				return err
			}
		case *applier[string]:
			if err = v.apply(); err != nil {
				return err
			}
		}
	}
	c.EnableOldValue = replicaConfig.EnableOldValue
	c.ForceReplicate = replicaConfig.ForceReplicate
	c.SourceID = replicaConfig.Sink.TiDBSourceID

	return nil
}

type applier[T any] struct {
	getValue          []valueGetter[T]
	validateAndAdjust func(v T) (T, error)
	valueHolder       *T
}

func (ap applier[T]) apply() error {
	var mergedValue T
	for _, f := range ap.getValue {
		override, v, err := f()
		if err != nil {
			return err
		}
		if !override {
			continue
		}
		nv := v
		if ap.validateAndAdjust != nil {
			nv, err = ap.validateAndAdjust(v)
			if err != nil {
				return err
			}
		}
		mergedValue = nv
	}
	ap.valueHolder = &mergedValue
	return nil
}

type valueGetter[T any] func() (bool, T, error)

func urlIntValGetter(values url.Values, key string) valueGetter[int] {
	return func() (bool, int, error) {
		s := values.Get(key)
		if len(s) == 0 {
			return false, 0, nil
		}

		c, err := strconv.Atoi(s)
		if err != nil {
			return false, 0, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		return true, c, nil
	}
}

func urlBoolValGetter(values url.Values, key string) valueGetter[bool] {
	return func() (bool, bool, error) {
		s := values.Get(key)
		if len(s) > 0 {
			enable, err := strconv.ParseBool(s)
			if err != nil {
				return false, false, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
			}
			return true, enable, nil
		}
		return false, false, nil
	}
}

func urlStringValGetter(values url.Values, key string) valueGetter[string] {
	return func() (bool, string, error) {
		s := values.Get(key)
		return len(s) > 0, s, nil
	}
}

func configFileIntValueGetter(c *int, preChecks ...interface{}) valueGetter[int] {
	return func() (bool, int, error) {
		for _, p := range preChecks {
			if p == nil {
				return false, 0, nil
			}
		}

		if c == nil {
			return false, 0, nil
		}
		return true, *c, nil
	}
}

func configFileBoolValueGetter(c *bool, preChecks ...interface{}) valueGetter[bool] {
	return func() (bool, bool, error) {
		for _, p := range preChecks {
			if p == nil {
				return false, false, nil
			}
		}

		if c == nil {
			return false, false, nil
		}
		return true, *c, nil
	}
}

func configFileStringValueGetter(c *string, preChecks ...interface{}) valueGetter[string] {
	return func() (bool, string, error) {
		for _, p := range preChecks {
			if p == nil {
				return false, "", nil
			}
		}

		if c == nil {
			return false, "", nil
		}
		return true, *c, nil
	}
}

func validateAndAdjustWorkerCount(c int) (int, error) {
	if c <= 0 {
		return 0, cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid worker-count %d, which must be greater than 0", c))
	}
	if c > maxWorkerCount {
		log.Warn("worker-count too large",
			zap.Int("original", c), zap.Int("override", maxWorkerCount))
		c = maxWorkerCount
	}
	return c, nil
}

func validateAndAdjustMaxTxnRow(c int) (int, error) {
	if c <= 0 {
		return 0, cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid max-txn-row %d, which must be greater than 0", c))
	}
	if c > maxMaxTxnRow {
		log.Warn("max-txn-row too large",
			zap.Int("original", c), zap.Int("override", maxMaxTxnRow))
		c = maxMaxTxnRow
	}
	return c, nil
}

func validateAndAdjustMaxMultiUpdateRowCount(c int) (int, error) {
	if c <= 0 {
		return 0, cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid max-multi-update-row %d, which must be greater than 0", c))
	}
	if c > maxMaxMultiUpdateRowCount {
		log.Warn("max-multi-update-row too large",
			zap.Int("original", c), zap.Int("override", maxMaxMultiUpdateRowCount))
		c = maxMaxMultiUpdateRowCount
	}
	return c, nil
}

func validateAndAdjustMaxMultiUpdateRowSize(c int) (int, error) {
	if c < 0 {
		return 0, cerror.WrapError(cerror.ErrMySQLInvalidConfig,
			fmt.Errorf("invalid max-multi-update-row-size %d, "+
				"which must be greater than or equal to 0", c))
	}
	if c > maxMaxMultiUpdateRowSize {
		log.Warn("max-multi-update-row-size too large",
			zap.Int("original", c), zap.Int("override", maxMaxMultiUpdateRowSize))
		c = maxMaxMultiUpdateRowSize
	}
	return c, nil
}

func validateAndAdjustTiDBTxnMode(s string) (string, error) {
	if s == txnModeOptimistic || s == txnModePessimistic {
		return s, nil
	}
	log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic",
		zap.String("default", defaultTiDBTxnMode))
	return defaultTiDBTxnMode, nil
}

func getSSLCAFromConfigFile(replicaConfig *config.ReplicaConfig, changefeedID model.ChangeFeedID) valueGetter[string] {
	return func() (bool, string, error) {
		if replicaConfig.Sink == nil || replicaConfig.Sink.MySQLConfig == nil {
			return false, "", nil
		}
		mysqlConfig := replicaConfig.Sink.MySQLConfig
		if mysqlConfig.SSLCa == nil || mysqlConfig.SSLCert == nil || mysqlConfig.SSLKey == nil {
			return false, "", nil
		}
		credential := security.Credential{
			CAPath:   *mysqlConfig.SSLCa,
			CertPath: *mysqlConfig.SSLCert,
			KeyPath:  *mysqlConfig.SSLKey,
		}
		tlsCfg, err := credential.ToTLSConfig()
		if err != nil {
			return false, "", errors.Trace(err)
		}

		name := "cdc_mysql_tls" + changefeedID.Namespace + "_" + changefeedID.ID
		err = dmysql.RegisterTLSConfig(name, tlsCfg)
		if err != nil {
			return false, "", cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		return true, "?tls=" + name, nil
	}
}

func getSSLCAFromURL(values url.Values, changefeedID model.ChangeFeedID) valueGetter[string] {
	return func() (bool, string, error) {
		s := values.Get("ssl-ca")
		if len(s) == 0 {
			return false, "", nil
		}

		credential := security.Credential{
			CAPath:   values.Get("ssl-ca"),
			CertPath: values.Get("ssl-cert"),
			KeyPath:  values.Get("ssl-key"),
		}
		tlsCfg, err := credential.ToTLSConfig()
		if err != nil {
			return false, "", errors.Trace(err)
		}

		name := "cdc_mysql_tls" + changefeedID.Namespace + "_" + changefeedID.ID
		err = dmysql.RegisterTLSConfig(name, tlsCfg)
		if err != nil {
			return false, "", cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
		}
		return true, "?tls=" + name, nil
	}
}

func getTimezone(ctxWithTimezone context.Context, values url.Values,
	replicaConfig *config.ReplicaConfig) valueGetter[string] {
	const pleaseSpecifyTimezone = "We recommend that you specify the time-zone explicitly. " +
		"Please make sure that the timezone of the TiCDC server, " +
		"sink-uri and the downstream database are consistent. " +
		"If the downstream database does not load the timezone information, " +
		"you can refer to https://dev.mysql.com/doc/refman/8.0/en/mysql-tzinfo-to-sql.html."
	return func() (bool, string, error) {
		serverTimezone := contextutil.TimezoneFromCtx(ctxWithTimezone)
		var configFileTimezone *string
		if replicaConfig.Sink != nil && replicaConfig.Sink.MySQLConfig != nil {
			configFileTimezone = replicaConfig.Sink.MySQLConfig.TimeZone
		}

		if _, ok := values["time-zone"]; !ok && configFileTimezone == nil {
			// If time-zone is not specified, use the timezone of the server.
			log.Warn("Because time-zone is not specified, "+
				"the timezone of the TiCDC server will be used. "+
				pleaseSpecifyTimezone,
				zap.String("timezone", serverTimezone.String()))
			return true, fmt.Sprintf(`"%s"`, serverTimezone.String()), nil
		}

		var timezoneConfig string
		if _, ok := values["time-zone"]; ok {
			timezoneConfig = values.Get("time-zone")
		} else if configFileTimezone != nil {
			timezoneConfig = *configFileTimezone
		}
		if len(timezoneConfig) == 0 {
			log.Warn("Because time-zone is empty, " +
				"the timezone of the downstream database will be used. " +
				pleaseSpecifyTimezone)
			return true, "", nil
		}

		changefeedTimezone, err := util.GetTimezone(timezoneConfig)
		if err != nil {
			return false, "", cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		timezone := fmt.Sprintf(`"%s"`, changefeedTimezone.String())
		// We need to check whether the timezone of the TiCDC server and the sink-uri are consistent.
		// If they are inconsistent, it may cause the data to be inconsistent.
		if changefeedTimezone.String() != serverTimezone.String() {
			return false, "", cerror.WrapError(cerror.ErrMySQLInvalidConfig, errors.Errorf(
				"the timezone of the TiCDC server and the sink-uri are inconsistent. "+
					"TiCDC server timezone: %s, sink-uri timezone: %s. "+
					"Please make sure that the timezone of the TiCDC server, "+
					"sink-uri and the downstream database are consistent.",
				serverTimezone.String(), changefeedTimezone.String()))
		}

		return true, timezone, nil
	}
}

func validateDuration(s string) (string, error) {
	_, err := time.ParseDuration(s)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}
	return s, nil
}
