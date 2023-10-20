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

import (
	"encoding/base64"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
)

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
	d.msg = &DMLPayload{
		Op:          getOp(e),
		TsMs:        int64(e.CommitTs),
		Transaction: nil,
		Source: &DMLSource{
			Version:   "",
			Connector: "",
			Name:      "",
			TsMs:      0,
			Snapshot:  "",
			Db:        "",
			Sequence:  nil,
			Table:     "",
			ServerId:  1,
			Gtid:      nil,
			File:      "",
			Pos:       0,
			Row:       0,
			Thread:    0,
			Query:     nil,
		},
	}
	if e.IsDelete() {
		d.msg.Before = make(map[string]interface{})
		for _, col := range e.PreColumns {
			d.msg.Before[col.Name] = col.Value
		}
	} else if e.IsUpdate() {
		d.msg.Before = make(map[string]interface{})
		for _, col := range e.PreColumns {
			d.msg.Before[col.Name] = col.Value
		}
		d.msg.After = make(map[string]interface{})
		for _, col := range e.Columns {
			d.msg.After[col.Name] = col.Value
		}
	} else {
		d.msg.After = make(map[string]interface{})
		for _, col := range e.Columns {
			d.msg.After[col.Name] = convertColumnValue(col)
		}
	}
	return d
}

func convertColumnValue(col *model.Column) interface{} {
	if col.Value == nil {
		return nil
	}
	switch col.Type {
	case mysql.TypeNull:
		return nil
	case mysql.TypeBit:
		return col.Value
	case mysql.TypeBlob:
		// convert to base64
		return base64.StdEncoding.EncodeToString(col.Value.([]byte))

	case mysql.TypeTiny | mysql.TypeShort | mysql.TypeLong | mysql.TypeLonglong | mysql.TypeInt24:
		return col.Value
	default:
		return col.Value
	}
}

func getOp(e *model.RowChangedEvent) string {
	if e.IsDelete() {
		return "d"
	} else if e.IsUpdate() {
		return "u"
	}
	return "c"
}

func (d *DMLPayloadBuilder) Build() *DMLPayload {
	return d.msg
}
