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

package debezium

import "github.com/pingcap/tiflow/cdc/model"

type DMLPayload struct {
	Before      map[string]interface{} `json:"before,omitempty"`
	After       map[string]interface{} `json:"after,omitempty"`
	Source      *DMLSource             `json:"source,omitempty"`
	Op          string                 `json:"op"`
	TsMs        int64                  `json:"ts_ms"`
	Transaction *Transaction           `json:"transaction,omitempty"`
}

type DMLSource struct {
	Version   string      `json:"version"`
	Connector string      `json:"connector"`
	Name      string      `json:"name"`
	TsMs      int64       `json:"ts_ms"`
	Snapshot  string      `json:"snapshot"`
	Db        string      `json:"db"`
	Sequence  interface{} `json:"sequence"`
	Table     string      `json:"table"`
	ServerId  int         `json:"server_id"`
	Gtid      interface{} `json:"gtid"`
	File      string      `json:"file"`
	Pos       int         `json:"pos"`
	Row       int         `json:"row"`
	Thread    int         `json:"thread"`
	Query     interface{} `json:"query"`
}

type Transaction struct {
	ID                  string `json:"id"`
	TotalOrder          int64  `json:"total_order"`
	DataCollectionOrder int64  `json:"data_collection_order"`
}

type DMLPayloadBuilder struct {
	msg *DMLPayload
}

func NewDMLPayloadBuilder() *DMLPayloadBuilder {
	return &DMLPayloadBuilder{}
}

func (d *DMLPayloadBuilder) WithRowChangedEvent(e *model.RowChangedEvent) *DMLPayloadBuilder {
	d.msg = &DMLPayload{}
	return d
}

func (d *DMLPayloadBuilder) Build() *DMLPayload {
	return &DMLPayload{}
}
