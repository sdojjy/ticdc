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

	"github.com/mailru/easyjson/jwriter"
)

type schema struct {
	Type     string  `json:"type"`
	Fields   []field `json:"fields"`
	Optional bool    `json:"optional"`
	Name     string  `json:"name"`
	Version  int     `json:"version"`
}

type field struct {
	Type     string  `json:"type"`
	Fields   []field `json:"fields,omitempty"`
	Optional bool    `json:"optional"`
	Name     string  `json:"name"`
}

func tableId(catalog, schema, table string) string {
	if len(catalog) == 0 {
		if len(schema) == 0 {
			return table
		}
		return schema + "." + table
	}
	if len(schema) == 0 {
		return catalog + "." + table
	}
	return catalog + "." + schema + "." + table
}

func fullColumnName(tableID, columnName string) string {
	return tableID + "." + columnName
}

type Message struct {
	Schema Schema
	Values []interface{}
}

func (m *Message) get(fieldName string) interface{} {
	field := m.lookupField(fieldName)
	return m.getByField(field)
}

func (m *Message) getByField(field *Field) interface{} {
	val := m.Values[field.Index]
	if val == nil && field.Schema.DefaultValue() != nil {
		val = field.Schema.DefaultValue()
	}
	return val
}

func (m *Message) lookupField(fieldName string) *Field {
	field := m.Schema.Field(fieldName)
	if field == nil {
		panic("Field not found: " + fieldName)
	}
	return field
}

func ToJSON(out *jwriter.Writer, schema Schema, value interface{}) {
	switch schema.Type() {
	case INT8:
		out.Int8(value.(int8))
	case INT16:
		out.Int16(value.(int16))
	case INT32:
		out.Int32(value.(int32))
	case INT64:
		out.Int64(value.(int64))
	case FLOAT32:
		out.Float32(value.(float32))
	case FLOAT64:
		out.Float64(value.(float64))
	case BOOLEAN:
		out.Bool(value.(bool))
	case STRING:
		out.String(fmt.Sprintf("%s", value))
	case ARRAY:
		if value == nil {
			out.RawString("[]")
			return
		}
		out.RawByte('[')
		first := true
		for _, v := range value.([]interface{}) {
			if !first {
				out.RawByte(',')
			}
			ToJSON(out, schema.ValueSchema(), v)
			first = false
		}
		out.RawByte(']')
	case MAP:
		// todo: use reflect.ValueOf(value).MapRange().
		valueMap := value.(map[interface{}]interface{})
		// map to object or a json array
		obj := schema.KeySchema().Type() == STRING
		if obj {
			out.RawByte('{')
		} else {
			out.RawByte('[')
		}
		first := true

		for k, v := range valueMap {
			if !first {
				out.RawByte(',')
			}
			if obj {
				keyOut := &jwriter.Writer{}
				ToJSON(keyOut, schema.KeySchema(), k)
				keyBytes, err := keyOut.BuildBytes()
				if err != nil {
					panic(err)
				}
				out.RawString(fmt.Sprintf("\"%s\":", string(keyBytes)))
				ToJSON(out, schema.ValueSchema(), v)
			} else {
				out.RawByte('[')
				ToJSON(out, schema.KeySchema(), k)
				out.RawByte(',')
				ToJSON(out, schema.ValueSchema(), v)
				out.RawByte(']')
			}
			first = false
		}
		if obj {
			out.RawByte('}')
		} else {
			out.RawByte(']')
		}
	case STRUCT:
		if value == nil {
			out.RawString("null")
			return
		}
		m := value.(*Message)
		out.RawByte('{')
		first := true
		for _, field := range schema.Fields() {
			if !first {
				out.RawByte(',')
			}
			out.RawString(fmt.Sprintf("\"%s\":", field.Name))
			ToJSON(out, field.Schema, m.getByField(field))
			first = false
		}
		out.RawByte('}')
	}
}
