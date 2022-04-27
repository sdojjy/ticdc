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

package migration

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
)

func MigrateData(stdCtx context.Context, cli *etcd.CDCEtcdClient) (bool, error) {
	schemaVersionKey := etcd.CDCMetaBase() + etcd.SchemaVersionKey
	//do etcd
	resp, err := cli.Client.Get(stdCtx, schemaVersionKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	shouldMigration := false
	destVersion := schemaVersion
	initVersion := false
	var oldVersion int
	if len(resp.Kvs) == 0 {
		shouldMigration = true
		initVersion = true
	} else {
		oldVersion, err = strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			return false, errors.Trace(err)
		}
		shouldMigration = oldVersion < destVersion
	}
	if shouldMigration {
		sess, err := concurrency.NewSession(cli.Client.Unwrap(),
			concurrency.WithTTL(5))
		if err != nil {
			return false, errors.Trace(err)
		}
		//campaign old cluster owner
		election := concurrency.NewElection(sess, "/tidb/cdc/owner")
		electionCtx, cancel := context.WithTimeout(stdCtx, time.Second*2)
		defer cancel()
		defer func() {
			_ = sess.Close()
		}()
		if err := election.Campaign(electionCtx, "migration"); err != nil {
			switch errors.Cause(err) {
			case context.Canceled, mvcc.ErrCompacted:
			default:
				// if campaign owner failed, restart capture
				log.Warn("campaign owner failed", zap.Error(err))
			}
			return false, nil
		}
		// I'm the leader now, migrate date, migration following keys
		//1. /tidb/cdc/changefeed/info/<changfeed-id>
		//2. /tidb/cdc/job/<changfeed-id>
		var cmps []clientv3.Cmp
		var opsThen []clientv3.Op
		var txnEmptyOpsElse = []clientv3.Op{}
		for _, mkey := range []string{"/tidb/cdc/changefeed/info", "/tidb/cdc/job"} {
			resp, err = cli.Client.Get(stdCtx, mkey, clientv3.WithPrefix())
			if err != nil {
				return false, errors.Trace(err)
			}
			for _, v := range resp.Kvs {
				oldKey := string(v.Key)
				//update key
				newKey := "/tidb/cdc/default/default" + oldKey[len("/tidb/cdc"):]
				//check new is not exists and old is exists
				cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(newKey), "=", 0))
				cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(oldKey), "!=", 0))
				// migrate data
				opsThen = append(opsThen, clientv3.OpPut(newKey, string(v.Value)))
				// delete old key?
				//opsThen = append(opsThen, clientv3.OpDelete(oldKey))
			}
		}
		//check if the version is not changed
		if initVersion {
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(schemaVersionKey), "=", 0))
		} else {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(schemaVersionKey), "=", fmt.Sprintf("%d", oldVersion)))
		}
		//update the schema version
		opsThen = append(opsThen, clientv3.OpPut(schemaVersionKey, fmt.Sprintf("%d", destVersion)))

		txnResp, err := cli.Client.Txn(stdCtx, cmps, opsThen, txnEmptyOpsElse)
		if err != nil {
			return false, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
		if !txnResp.Succeeded {
			log.Warn("migration compare failed")
			return false, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
		log.Info("etcd data migration done")
	}
	return true, nil
}
