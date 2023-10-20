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
	parsermodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
)

type DDLPayload struct {
	Source       *ddlSource      `json:"source"`
	Position     *binlogPosition `json:"position"`
	TsMs         int64           `json:"ts_ms"`
	DatabaseName string          `json:"databaseName"`
	DDL          string          `json:"ddl"`
	TableChanges []tableChange   `json:"tableChanges"`
}

type ddlSource struct {
	Server string `json:"server"`
}

type binlogPosition struct {
}

type tableChange struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Table table  `json:"table"`
}

type table struct {
	DefaultCharsetName    string   `json:"defaultCharsetName"`
	PrimaryKeyColumnNames []string `json:"primaryKeyColumnNames"`
	Columns               []column `json:"columns"`
}

type column struct {
	Name            string   `json:"name"`
	JdbcType        int      `json:"jdbcType"`
	TypeName        string   `json:"typeName"`
	TypeExpression  string   `json:"typeExpression"`
	CharsetName     string   `json:"charsetName"`
	Position        int      `json:"position"`
	Optional        bool     `json:"optional"`
	AutoIncremented bool     `json:"autoIncremented"`
	Generated       bool     `json:"generated"`
	Comment         string   `json:"comment"`
	HasDefaultValue bool     `json:"hasDefaultValue"`
	EnumValues      []string `json:"enumValues"`
}

type DDLPayloadBuilder struct {
	msg *DDLPayload
}

func NewDDLPayloadBuilder() *DDLPayloadBuilder {
	return &DDLPayloadBuilder{}
}

func (d *DDLPayloadBuilder) Build(e *model.DDLEvent) *DDLPayload {
	d.msg = &DDLPayload{
		TsMs:         int64(e.CommitTs),
		DatabaseName: e.TableInfo.TableName.Schema,
		DDL:          e.Query,
	}
	d.msg.Source = &ddlSource{}
	d.msg.Position = &binlogPosition{}
	change := tableChange{}
	d.msg.TableChanges = []tableChange{change}
	switch e.Type {
	case parsermodel.ActionCreateTable:
		change.Type = "CREATE"
	case parsermodel.ActionDropTable:
		change.Type = "DROP"
	default:
		change.Type = "ALTER"
	}
	primaryKeys := make([]string, 0)
	for _, col := range e.TableInfo.GetPrimaryKey().Columns {
		primaryKeys = append(primaryKeys, col.Name.O)
	}
	cols := make([]column, 0, len(e.TableInfo.Columns))
	for _, col := range e.TableInfo.Columns {
		cols = append(cols, column{
			Name:            col.Name.O,
			JdbcType:        int(col.GetType()),
			TypeName:        col.GetTypeDesc(),
			TypeExpression:  col.GeneratedExprString,
			CharsetName:     col.GetCharset(),
			Position:        col.Offset,
			Optional:        false,
			AutoIncremented: mysql.HasAutoIncrementFlag(col.GetFlag()),
			Generated:       col.IsGenerated(),
			Comment:         col.Comment,
			HasDefaultValue: col.DefaultValue != nil,
			EnumValues:      col.GetElems(),
		})
	}
	change.Table = table{
		DefaultCharsetName:    e.Charset,
		PrimaryKeyColumnNames: primaryKeys,
		Columns:               cols,
	}
	return d.msg
}
