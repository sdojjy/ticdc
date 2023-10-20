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
	"fmt"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
)

type DmlMessageBuilder struct {
	Schema       *ConnectSchema
	beforeFields *ConnectSchema
	afterFields  *ConnectSchema

	Message *Message

	schemaMsg  *Message
	payloadMsg *Message

	columnTypeMeg  []interface{}
	beforeMsg      []interface{}
	afterMsg       []interface{}
	sourceMsg      *Message
	Op             string
	TsMs           int64
	transactionMsg *Message

	connectName string
}

func (d *DmlMessageBuilder) WithChangeEvent(e *model.RowChangedEvent) *DmlMessageBuilder {
	d.Op = getOperation(e)
	d.TsMs = int64(e.CommitTs)
	var values []interface{}
	if e.IsDelete() {
		values = append(values, getBeforeSchemaMsg(e, "before", d.connectName, e.PreColumns))
	} else if e.IsInsert() {
		values = append(values, getBeforeSchemaMsg(e, "after", d.connectName, e.Columns))
	} else {
		values = append(values, getBeforeSchemaMsg(e, "before", d.connectName, e.PreColumns))
		values = append(values, getBeforeSchemaMsg(e, "after", d.connectName, e.Columns))
	}
	values = append(values,
		&Message{
			Values: []interface{}{
				"struct",
				true,
				[]interface{}{
					&Message{
						Values: []interface{}{
							"int32",
							false,
							"id",
						},
					},
					&Message{
						Values: []interface{}{
							"STRING",
							true,
							"name",
						},
					},
				},
				"io.debezium.connector.mysql.Source",
				"source",
			},
		},
		&Message{
			Values: []interface{}{
				"string",
				false,
				nil,
				"op",
				"op",
			},
		},
		&Message{
			Values: []interface{}{
				"int64",
				true,
				nil,
				"ts_ms",
				"ts_ms",
			},
		},
		&Message{
			Values: []interface{}{
				"struct",
				true,
				[]interface{}{&Message{
					Values: []interface{}{
						"int32",
						false,
						"id",
					},
				}},
				"event.block",
				"transaction",
			},
		})

	d.schemaMsg = &Message{
		Values: []interface{}{
			"struct",
			values,
			true,
			fmt.Sprintf("%s.%s.%s", d.connectName, e.TableInfo.TableName.Schema, e.TableInfo.TableName.Table),
			int32(1),
		},
	}

	return d
}

var type2TiDBType = map[byte]string{
	mysql.TypeTiny:       "INT",
	mysql.TypeShort:      "INT",
	mysql.TypeInt24:      "INT",
	mysql.TypeLong:       "INT",
	mysql.TypeLonglong:   "BIGINT",
	mysql.TypeFloat:      "FLOAT",
	mysql.TypeDouble:     "DOUBLE",
	mysql.TypeBit:        "BIT",
	mysql.TypeNewDecimal: "DECIMAL",
	mysql.TypeTinyBlob:   "TEXT",
	mysql.TypeMediumBlob: "TEXT",
	mysql.TypeBlob:       "TEXT",
	mysql.TypeLongBlob:   "TEXT",
	mysql.TypeVarchar:    "TEXT",
	mysql.TypeVarString:  "TEXT",
	mysql.TypeString:     "TEXT",
	mysql.TypeEnum:       "ENUM",
	mysql.TypeSet:        "SET",
	mysql.TypeJSON:       "JSON",
	mysql.TypeDate:       "DATE",
	mysql.TypeDatetime:   "DATETIME",
	mysql.TypeTimestamp:  "TIMESTAMP",
	mysql.TypeDuration:   "TIME",
	mysql.TypeYear:       "YEAR",
}

func getTiDBTypeFromColumn(col *model.Column) string {
	tt := type2TiDBType[col.Type]
	if col.Flag.IsUnsigned() && (tt == "INT" || tt == "BIGINT") {
		return tt + " UNSIGNED"
	}
	if col.Flag.IsBinary() && tt == "TEXT" {
		return "BLOB"
	}
	return tt
}

func typeToConnectType(t byte) string {
	switch t {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
		return "int32"
	case mysql.TypeLonglong:
		return "int64"
	case mysql.TypeFloat:
		return "float32"
	case mysql.TypeDouble:
		return "float64"
	case mysql.TypeBit:
		return "int64"
	case mysql.TypeNewDecimal:
		return "string"
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		return "string"
	case mysql.TypeEnum:
		return "string"
	case mysql.TypeSet:
		return "string"
	case mysql.TypeJSON:
		return "string"
	case mysql.TypeDate:
		return "string"
	case mysql.TypeDatetime:
		return "string"
	case mysql.TypeTimestamp:
		return "string"
	case mysql.TypeDuration:
		return "string"
	case mysql.TypeYear:
		return "int32"
	}
	return "string"
}

func getBeforeSchemaMsg(e *model.RowChangedEvent,
	fieldName string,
	connectName string,
	data []*model.Column) *Message {
	var columns []interface{}
	if e.IsDelete() {
		for _, c := range data {
			columns = append(columns, &Message{
				Values: []interface{}{
					typeToConnectType(c.Type),
					false,
					c.Name,
				},
			},
			)
		}
	}

	return &Message{
		Values: []interface{}{
			"struct",
			true,
			columns,
			fmt.Sprintf("%s.%s.%s.Value", connectName, e.TableInfo.TableName.Schema, e.TableInfo.TableName.Table),
			fieldName,
		},
	}
}

func getOperation(e *model.RowChangedEvent) string {
	if e.IsInsert() {
		return "c"
	} else if e.IsUpdate() {
		return "u"
	}
	return "d"
}

func NewDmlMessageBuilder() *DmlMessageBuilder {
	return &DmlMessageBuilder{
		beforeFields: &ConnectSchema{
			t: STRUCT,
		},
		afterFields: &ConnectSchema{
			t: STRUCT,
		},
	}
}

func (d *DmlMessageBuilder) WithBeforeField(field *Field) *DmlMessageBuilder {
	d.beforeFields.WithField(field)
	return d
}

func (d *DmlMessageBuilder) WithAfterField(field *Field) *DmlMessageBuilder {
	d.afterFields.WithField(field)
	return d
}

func (d *DmlMessageBuilder) WithBeforeValues(values []interface{}) *DmlMessageBuilder {
	d.beforeMsg = values
	return d
}

func (d *DmlMessageBuilder) WithAfterValues(values []interface{}) *DmlMessageBuilder {
	d.afterMsg = values
	return d
}

func (d *DmlMessageBuilder) BuildSource() *DmlMessageBuilder {
	return d
}

func (d *DmlMessageBuilder) BuildSchema() *DmlMessageBuilder {
	d.Schema = &ConnectSchema{}
	d.Schema.WithType(STRUCT).
		WithField(&Field{
			Name:  "schema",
			Index: 0,
			Schema: &ConnectSchema{
				t: STRUCT,
				fields: map[string]*Field{
					"type": {Name: "type", Index: 0, Schema: STRING_SCHEMA},
					"fields": {Name: "fields", Index: 1, Schema: &ConnectSchema{
						t: ARRAY,
						valueSchema: &ConnectSchema{
							t: STRUCT,
							fields: map[string]*Field{
								"type":     {Name: "type", Index: 0, Schema: STRING_SCHEMA},
								"optional": {Name: "optional", Index: 1, Schema: BOOLEAN_SCHEMA},
								"fields": {Name: "fields", Index: 2, Schema: &ConnectSchema{
									t: ARRAY,
									valueSchema: &ConnectSchema{
										t: STRUCT,
										fields: map[string]*Field{
											"type":     {Name: "type", Index: 0, Schema: STRING_SCHEMA},
											"optional": {Name: "optional", Index: 1, Schema: BOOLEAN_SCHEMA},
											"field":    {Name: "field", Index: 2, Schema: STRING_SCHEMA},
										},
									},
								}},
								"name":  {Name: "name", Index: 3, Schema: STRING_SCHEMA},
								"field": {Name: "field", Index: 4, Schema: STRING_SCHEMA},
							},
						},
					}},
					"optional": {Name: "optional", Index: 2, Schema: BOOLEAN_SCHEMA},
					"name":     {Name: "name", Index: 3, Schema: STRING_SCHEMA},
					"version":  {Name: "version", Index: 4, Schema: INT32_SCHEMA},
				},
			},
		}).WithField(&Field{
		Name:  "payload",
		Index: 1,
		Schema: &ConnectSchema{
			t: STRUCT,
			fields: map[string]*Field{
				"before": {Name: "before", Index: 0, Schema: d.beforeFields},
				"after":  {Name: "after", Index: 1, Schema: d.afterFields},
				"source": {Name: "source", Index: 2,
					Schema: &ConnectSchema{
						t: STRUCT,
						fields: map[string]*Field{
							"server": {Name: "server", Index: 0, Schema: STRING_SCHEMA},
						},
					},
				},
				"op":    {Name: "op", Index: 3, Schema: STRING_SCHEMA},
				"ts_ms": {Name: "ts_ms", Index: 4, Schema: INT64_SCHEMA},
				"transaction": {
					Name:  "transaction",
					Index: 5,
					Schema: &ConnectSchema{
						t: STRUCT,
						fields: map[string]*Field{
							"lsn": {Name: "lsn", Index: 0, Schema: INT64_SCHEMA},
						},
					},
				},
			},
		},
	})
	return d
}

func (d *DmlMessageBuilder) BuildMsg() *DmlMessageBuilder {
	d.schemaMsg = &Message{
		Values: []interface{}{
			"struct",
			[]interface{}{
				&Message{
					Values: []interface{}{
						"struct",
						true,
						[]interface{}{
							&Message{
								Values: []interface{}{
									"int32",
									false,
									"id",
								},
							},
							&Message{
								Values: []interface{}{
									"STRING",
									true,
									"name",
								},
							},
						},
						"tutorial.test.t5.Value",
						"before",
					},
				},
				&Message{
					Values: []interface{}{
						"struct",
						true,
						[]interface{}{
							&Message{
								Values: []interface{}{
									"int32",
									false,
									"id",
								},
							},
							&Message{
								Values: []interface{}{
									"STRING",
									true,
									"name",
								},
							},
						},
						"tutorial.test.t5.Value",
						"after",
					},
				},
				&Message{
					Values: []interface{}{
						"struct",
						true,
						[]interface{}{
							&Message{
								Values: []interface{}{
									"int32",
									false,
									"id",
								},
							},
							&Message{
								Values: []interface{}{
									"STRING",
									true,
									"name",
								},
							},
						},
						"io.debezium.connector.mysql.Source",
						"source",
					},
				},
				&Message{
					Values: []interface{}{
						"string",
						false,
						nil,
						"op",
						"op",
					},
				},
				&Message{
					Values: []interface{}{
						"int64",
						true,
						nil,
						"ts_ms",
						"ts_ms",
					},
				},
				&Message{
					Values: []interface{}{
						"struct",
						true,
						[]interface{}{&Message{
							Values: []interface{}{
								"int32",
								false,
								"id",
							},
						}},
						"event.block",
						"transaction",
					},
				},
			},
			true,
			"tu.test.t1",
			int32(1),
		},
	}
	beforeMsg := &Message{
		Values: d.beforeMsg,
	}
	afterMsg := &Message{
		Values: d.afterMsg,
	}
	d.sourceMsg = &Message{
		Values: []interface{}{
			"mysql",
		},
	}
	d.transactionMsg = &Message{
		Values: []interface{}{
			//lsn
			int64(1),
		},
	}
	d.payloadMsg = &Message{
		Values: []interface{}{
			//before
			beforeMsg,
			//after
			afterMsg,
			//source
			d.sourceMsg,
			//op
			d.Op,
			//ts_ms
			d.TsMs,
			//transaction
			d.transactionMsg,
		},
	}
	d.Message = &Message{
		Values: []interface{}{
			//schema
			d.schemaMsg,
			// payload
			d.payloadMsg,
		},
	}
	return d
}
