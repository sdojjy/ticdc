package migration

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
)

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

func WaitSchemaVersionMatched(stdCtx context.Context, cli *etcd.CDCEtcdClient) (int, error) {
	tick := time.NewTicker(time.Second)
	//do etcd
	version, err := getSchemaVersion(stdCtx, cli)
	if err != nil {
		return version, errors.Trace(err)
	}
	if version == schemaVersion {
		return version, nil
	}
	for {
		select {
		case <-stdCtx.Done():
			return 0, stdCtx.Err()
		case <-tick.C:
			version, err := getSchemaVersion(stdCtx, cli)
			if err != nil {
				return 0, errors.Trace(err)
			}
			if version == schemaVersion {
				return version, nil
			}
		}
	}
}

func getSchemaVersion(stdCtx context.Context, cli *etcd.CDCEtcdClient) (int, error) {
	schemaVersionKey := etcd.CDCMetaBase() + etcd.SchemaVersionKey
	//do etcd
	resp, err := cli.Client.Get(stdCtx, schemaVersionKey)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	} else {
		version, err := strconv.Atoi(string(resp.Kvs[0].Value))
		if err != nil {
			return 0, errors.Trace(err)
		}
		return version, nil
	}
}
