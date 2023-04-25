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
	"github.com/pingcap/tiflow/pkg/sink/config_applier"
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
	// make sure MySQLConfig is not nil
	if replicaConfig.Sink.MySQLConfig == nil {
		replicaConfig.Sink.MySQLConfig = &config.MySQLConfig{}
		defer func() {
			replicaConfig.Sink.MySQLConfig = nil
		}()
	}

	var appliers = []any{
		config_applier.Applier[int]{
			ValueGetter: []config_applier.ValueGetter[int]{
				config_applier.ConfigFileIntValueGetter(replicaConfig.Sink.MySQLConfig.WorkerCount),
				config_applier.UrlIntValGetter(query, "worker-count"),
			},
			ValidateAndAdjust: validateAndAdjustWorkerCount,
			ValueHolder:       &c.WorkerCount,
		},
		config_applier.Applier[int]{
			ValueGetter: []config_applier.ValueGetter[int]{
				config_applier.ConfigFileIntValueGetter(replicaConfig.Sink.MySQLConfig.MaxTxRow),
				config_applier.UrlIntValGetter(query, "max-txn-row"),
			},
			ValidateAndAdjust: validateAndAdjustMaxTxnRow,
			ValueHolder:       &c.MaxTxnRow,
		},
		config_applier.Applier[int]{
			ValueGetter: []config_applier.ValueGetter[int]{
				config_applier.ConfigFileIntValueGetter(replicaConfig.Sink.MySQLConfig.MaxMultiUpdateRowCount),
				config_applier.UrlIntValGetter(query, "max-multi-update-row"),
			},
			ValidateAndAdjust: validateAndAdjustMaxMultiUpdateRowCount,
			ValueHolder:       &c.MaxMultiUpdateRowCount,
		},
		config_applier.Applier[int]{
			ValueGetter: []config_applier.ValueGetter[int]{
				config_applier.ConfigFileIntValueGetter(replicaConfig.Sink.MySQLConfig.MaxMultiUpdateRowSize),
				config_applier.UrlIntValGetter(query, "max-multi-update-row-size"),
			},
			ValidateAndAdjust: validateAndAdjustMaxMultiUpdateRowSize,
			ValueHolder:       &c.MaxMultiUpdateRowSize,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				config_applier.ConfigFileStringValueGetter(replicaConfig.Sink.MySQLConfig.TiDBTxnMode),
				config_applier.UrlStringValGetter(query, "tidb-txn-mode"),
			},
			ValidateAndAdjust: validateAndAdjustTiDBTxnMode,
			ValueHolder:       &c.tidbTxnMode,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				getSSLCAFromConfigFile(replicaConfig, changefeedID),
				getSSLCAFromURL(query, changefeedID),
			},
			ValidateAndAdjust: nil,
			ValueHolder:       &c.TLS,
		},
		config_applier.Applier[bool]{
			ValueGetter: []config_applier.ValueGetter[bool]{
				config_applier.ConfigFileBoolValueGetter(replicaConfig.Sink.SafeMode),
				config_applier.UrlBoolValGetter(query, "safe-mode"),
			},
			ValidateAndAdjust: nil,
			ValueHolder:       &c.SafeMode,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				getTimezone(ctx, query, replicaConfig),
			},
			ValidateAndAdjust: nil,
			ValueHolder:       &c.Timezone,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				config_applier.ConfigFileStringValueGetter(replicaConfig.Sink.MySQLConfig.ReadTimeout),
				config_applier.UrlStringValGetter(query, "read-timeout"),
			},
			ValidateAndAdjust: validateDuration,
			ValueHolder:       &c.ReadTimeout,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				config_applier.ConfigFileStringValueGetter(replicaConfig.Sink.MySQLConfig.WriteTimeout),
				config_applier.UrlStringValGetter(query, "write-timeout"),
			},
			ValidateAndAdjust: validateDuration,
			ValueHolder:       &c.WriteTimeout,
		},
		config_applier.Applier[string]{
			ValueGetter: []config_applier.ValueGetter[string]{
				config_applier.ConfigFileStringValueGetter(replicaConfig.Sink.MySQLConfig.Timeout),
				config_applier.UrlStringValGetter(query, "timeout"),
			},
			ValidateAndAdjust: validateDuration,
			ValueHolder:       &c.DialTimeout,
		},
		config_applier.Applier[bool]{
			ValueGetter: []config_applier.ValueGetter[bool]{
				config_applier.ConfigFileBoolValueGetter(replicaConfig.Sink.MySQLConfig.EnableBatchDML),
				config_applier.UrlBoolValGetter(query, "batch-dml-enable"),
			},
			ValidateAndAdjust: nil,
			ValueHolder:       &c.BatchDMLEnable,
		},
		config_applier.Applier[bool]{
			ValueGetter: []config_applier.ValueGetter[bool]{
				config_applier.ConfigFileBoolValueGetter(replicaConfig.Sink.MySQLConfig.EnableMultiStatement),
				config_applier.UrlBoolValGetter(query, "multi-stmt-enable"),
			},
			ValidateAndAdjust: nil,
			ValueHolder:       &c.MultiStmtEnable,
		},
		config_applier.Applier[bool]{
			ValueGetter: []config_applier.ValueGetter[bool]{
				config_applier.ConfigFileBoolValueGetter(replicaConfig.Sink.MySQLConfig.EnableCachePreparedStatement),
				config_applier.UrlBoolValGetter(query, "cache-prep-stmts"),
			},
			ValidateAndAdjust: nil,
			ValueHolder:       &c.CachePrepStmts,
		},
	}
	if err := config_applier.Apply(appliers); err != nil {
		return err
	}
	c.EnableOldValue = replicaConfig.EnableOldValue
	c.ForceReplicate = replicaConfig.ForceReplicate
	c.SourceID = replicaConfig.Sink.TiDBSourceID

	return nil
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

func getSSLCAFromConfigFile(replicaConfig *config.ReplicaConfig, changefeedID model.ChangeFeedID) config_applier.ValueGetter[string] {
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

func getSSLCAFromURL(values url.Values, changefeedID model.ChangeFeedID) config_applier.ValueGetter[string] {
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
	replicaConfig *config.ReplicaConfig) config_applier.ValueGetter[string] {
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
