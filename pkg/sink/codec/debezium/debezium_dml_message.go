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

type DmlMessageBuilder struct {
	Schema       *ConnectSchema
	beforeFields *ConnectSchema
	afterFields  *ConnectSchema

	Message *Message

	schemaMsg  *Message
	payloadMsg *Message

	beforeMsg      []interface{}
	afterMsg       []interface{}
	sourceMsg      *Message
	Op             string
	TsMs           int64
	transactionMsg *Message
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
