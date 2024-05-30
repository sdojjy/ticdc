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
	AddMaintainerRequest    *AddMaintainerRequest    `json:"add_maintainer_request"`
	AddMaintainerResponse   *AddMaintainerResponse   `json:"add_maintainer_response"`
	RemoveMaintainerRequest *RemoveMaintainerRequest `json:"remove_maintainer_request"`

	AddTableRangeMaintainerRequest  *AddTableRangeMaintainerRequest  `json:"add_table_range_maintainer_request"`
	AddTableRangeMaintainerResponse *AddTableRangeMaintainerResponse `json:"add_table_range_maintainer_response"`
}

type AddTableRangeMaintainerRequest struct {
	Tables []model.TableID         `json:"tables"`
	Config *model.ChangeFeedInfo   `json:"config"`
	Status *model.ChangeFeedStatus `json:"status"`
}

type AddTableRangeMaintainerResponse struct {
	Status string `json:"status"`
	ID     string `json:"id"`
}

type AddMaintainerRequest struct {
	Config *model.ChangeFeedInfo   `json:"config"`
	Status *model.ChangeFeedStatus `json:"status"`
}

type AddMaintainerResponse struct {
	Status string `json:"status"`
	ID     string `json:"id"`
}
type RemoveMaintainerRequest struct {
	ID string `json:"id"`
}
