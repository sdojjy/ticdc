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

package new_arch

import "github.com/pingcap/tiflow/cdc/model"

type Message struct {
	MasterVersion int64  `json:"master_version,omitempty"`
	Sender        string `json:"sender,omitempty"`
	To            string `json:"to,omitempty"`

	AddMaintainerRequest    *AddMaintainerRequest    `json:"add_maintainer_request,omitempty"`
	AddMaintainerResponse   *AddMaintainerResponse   `json:"add_maintainer_response,omitempty"`
	RemoveMaintainerRequest *RemoveMaintainerRequest `json:"remove_maintainer_request,omitempty"`

	AddTableRangeMaintainerRequest  *AddTableRangeMaintainerRequest  `json:"add_table_range_maintainer_request,omitempty"`
	AddTableRangeMaintainerResponse *AddTableRangeMaintainerResponse `json:"add_table_range_maintainer_response,omitempty"`

	BootstrapRequest  *BootstrapRequest  `json:"bootstrap_request,omitempty"`
	BootstrapResponse *BootstrapResponse `json:"bootstrap_response,omitempty"`

	DispatchComponentRequest *DispatchComponentRequest `json:"msg_dispatch_component_request,omitempty"`

	ChangefeedHeartbeatRequest  *ChangefeedHeartbeatRequest  `json:"changefeed_heartbeat_request,omitempty"`
	ChangefeedHeartbeatResponse *ChangefeedHeartbeatResponse `json:"changefeed_heartbeat_response,omitempty"`
}

type DispatchComponentRequest struct {
	AddComponent    []byte `json:"add_component,omitempty"`
	RemoveComponent []byte `json:"remove_component,omitempty"`
}

type BootstrapRequest struct {
}

type BootstrapResponse struct {
}

type AddTableRangeMaintainerRequest struct {
	Tables []model.TableID         `json:"tables,omitempty"`
	Config *model.ChangeFeedInfo   `json:"config,omitempty"`
	Status *model.ChangeFeedStatus `json:"status,omitempty"`
}

type AddTableRangeMaintainerResponse struct {
	Status string `json:"status,omitempty"`
	ID     string `json:"id,omitempty"`
}

type AddMaintainerRequest struct {
	Config *model.ChangeFeedInfo   `json:"config,omitempty"`
	Status *model.ChangeFeedStatus `json:"status,omitempty"`
}

type AddMaintainerResponse struct {
	Status string `json:"status,omitempty"`
	ID     string `json:"id,omitempty"`
}
type RemoveMaintainerRequest struct {
	ID string `json:"id,omitempty"`
}

type ChangefeedHeartbeatRequest struct {
	LivenessTimestamp int64 `json:"liveness_time,omitempty"`
}

type ChangefeedHeartbeatResponse struct {
	From        string              `json:"from,omitempty"`
	Liveness    int32               `json:"liveness,omitempty"`
	Changefeeds []*ChangefeedStatus `json:"changefeeds,omitempty"`
}

type ChangefeedStatus struct {
	ID model.ChangeFeedID `json:"id,omitempty"`
}
