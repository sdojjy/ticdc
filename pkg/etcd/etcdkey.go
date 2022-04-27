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

package etcd

import (
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	CDCMetaPrefix = "/__cdc_meta__"
	// EtcdKeyBase is the common prefix of the keys in CDC
	//EtcdKeyBase = "/tidb/cdc"
	ownerKey   = "/owner"
	captureKey = "/capture"

	taskKey         = "/task"
	taskWorkloadKey = taskKey + "/workload"
	taskStatusKey   = taskKey + "/status"
	taskPositionKey = taskKey + "/position"

	changefeedInfoKey = "/changefeed/info"
	jobKey            = "/job"
	upstreamInfoKey   = "/upstream"
	SchemaVersionKey  = "/meta/schema-version"
)

// CDCKeyType is the type of etcd key
type CDCKeyType = int

// the types of etcd key
const (
	CDCKeyTypeUnknown CDCKeyType = iota
	CDCKeyTypeOwner
	CDCKeyTypeCapture
	CDCKeyTypeChangefeedInfo
	CDCKeyTypeChangeFeedStatus
	CDCKeyTypeTaskPosition
	CDCKeyTypeTaskStatus
	CDCKeyTypeTaskWorkload
	CDCKeyTypeUpstream
	CDCKeyTypeSchemaVersion
)

// CDCKey represents a etcd key which is defined by TiCDC
/*
 Usage:
 we can parse a raw etcd key:
 ```
 	k := new(CDCKey)
 	rawKey := "/tidb/cdc/changefeed/info/test/changefeed"
	err := k.Parse(rawKey)
 	c.Assert(k, check.DeepEquals, &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: "test/changefeed",
	})
 ```

 and we can generate a raw key from CDCKey
 ```
 	k := &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: "test/changefeed",
	}
 	c.Assert(k.String(), check.Equals, "/tidb/cdc/changefeed/info/test/changefeed")
 ```

*/
type CDCKey struct {
	Tp                CDCKeyType
	ChangefeedID      string
	CaptureID         string
	OwnerLeaseID      string
	Namespace         string
	ClusterID         string
	UpstreamClusterID string
}

// Parse parses the given etcd key
func (k *CDCKey) Parse(key string) error {
	if !strings.HasPrefix(key, EtcdKeyBase()) {
		return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
	}
	key = key[len("/tidb/cdc"):]
	parts := strings.Split(key, "/")
	k.ClusterID = parts[1]
	key = key[len(k.ClusterID)+1:]
	if strings.HasPrefix(key, CDCMetaPrefix) {
		key = key[len(CDCMetaPrefix):]
		switch {
		case strings.HasPrefix(key, ownerKey):
			k.Tp = CDCKeyTypeOwner
			k.CaptureID = ""
			k.ChangefeedID = ""
			key = key[len(ownerKey):]
			if len(key) > 0 {
				key = key[1:]
			}
			k.OwnerLeaseID = key
		case strings.HasPrefix(key, captureKey):
			k.Tp = CDCKeyTypeCapture
			k.CaptureID = key[len(captureKey)+1:]
			k.ChangefeedID = ""
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, upstreamInfoKey):
			k.Tp = CDCKeyTypeUpstream
			k.CaptureID = ""
			k.UpstreamClusterID = key[len(upstreamInfoKey)+1:]
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, SchemaVersionKey):
			k.Tp = CDCKeyTypeSchemaVersion
			k.CaptureID = ""
			k.UpstreamClusterID = ""
			k.OwnerLeaseID = ""
		default:
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
	} else {
		k.Namespace = parts[2]
		key = key[len(k.Namespace)+1:]
		switch {
		case strings.HasPrefix(key, changefeedInfoKey):
			k.Tp = CDCKeyTypeChangefeedInfo
			k.CaptureID = ""
			k.ChangefeedID = key[len(changefeedInfoKey)+1:]
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, jobKey):
			k.Tp = CDCKeyTypeChangeFeedStatus
			k.CaptureID = ""
			k.ChangefeedID = key[len(jobKey)+1:]
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, taskStatusKey):
			splitKey := strings.SplitN(key[len(taskStatusKey)+1:], "/", 2)
			if len(splitKey) != 2 {
				return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
			}
			k.Tp = CDCKeyTypeTaskStatus
			k.CaptureID = splitKey[0]
			k.ChangefeedID = splitKey[1]
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, taskPositionKey):
			splitKey := strings.SplitN(key[len(taskPositionKey)+1:], "/", 2)
			if len(splitKey) != 2 {
				return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
			}
			k.Tp = CDCKeyTypeTaskPosition
			k.CaptureID = splitKey[0]
			k.ChangefeedID = splitKey[1]
			k.OwnerLeaseID = ""
		case strings.HasPrefix(key, taskWorkloadKey):
			splitKey := strings.SplitN(key[len(taskWorkloadKey)+1:], "/", 2)
			if len(splitKey) != 2 {
				return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
			}
			k.Tp = CDCKeyTypeTaskWorkload
			k.CaptureID = splitKey[0]
			k.ChangefeedID = splitKey[1]
			k.OwnerLeaseID = ""
		default:
			return cerror.ErrInvalidEtcdKey.GenWithStackByArgs(key)
		}
	}
	return nil
}

func CDCClusterBase(id string) string {
	return fmt.Sprintf("/tidb/cdc/%s", id)
}

func EtcdKeyBase() string {
	return fmt.Sprintf("/tidb/cdc/%s", config.GetGlobalServerConfig().ClusterID)
}

func CDCMetaBase() string {
	return fmt.Sprintf("%s%s", EtcdKeyBase(), CDCMetaPrefix)
}

func NamespacedPrefix(namespace string) string {
	return EtcdKeyBase() + "/" + namespace
}

func (k *CDCKey) String() string {
	switch k.Tp {
	case CDCKeyTypeOwner:
		if len(k.OwnerLeaseID) == 0 {
			return EtcdKeyBase() + CDCMetaPrefix + ownerKey
		}
		return EtcdKeyBase() + CDCMetaPrefix + ownerKey + "/" + k.OwnerLeaseID
	case CDCKeyTypeCapture:
		return EtcdKeyBase() + CDCMetaPrefix + captureKey + "/" + k.CaptureID
	case CDCKeyTypeChangefeedInfo:
		return NamespacedPrefix(k.Namespace) + changefeedInfoKey + "/" + k.ChangefeedID
	case CDCKeyTypeChangeFeedStatus:
		return NamespacedPrefix(k.Namespace) + jobKey + "/" + k.ChangefeedID
	case CDCKeyTypeTaskPosition:
		return NamespacedPrefix(k.Namespace) + taskPositionKey + "/" + k.CaptureID + "/" + k.ChangefeedID
	case CDCKeyTypeTaskStatus:
		return NamespacedPrefix(k.Namespace) + taskStatusKey + "/" + k.CaptureID + "/" + k.ChangefeedID
	case CDCKeyTypeTaskWorkload:
		return NamespacedPrefix(k.Namespace) + taskWorkloadKey + "/" + k.CaptureID + "/" + k.ChangefeedID
	case CDCKeyTypeUpstream:
		return NamespacedPrefix(k.Namespace) + upstreamInfoKey + "/" + k.UpstreamClusterID
	}
	log.Panic("unreachable")
	return ""
}
