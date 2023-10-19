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

type Type string

type Schema interface {
	Type() Type

	IsOptional() bool

	DefaultValue() interface{}

	Name() string

	Version() int

	Doc() string

	Parameters() map[string]string

	KeySchema() Schema

	ValueSchema() Schema

	Fields() []*Field

	Field(name string) *Field

	Schema() Schema
}

type Field struct {
	Name   string
	Index  int
	Schema Schema
}

var (
	INT8    Type = "INT8"
	INT16   Type = "INT16"
	INT32   Type = "INT32"
	INT64   Type = "INT64"
	FLOAT32 Type = "FLOAT32"
	FLOAT64 Type = "FLOAT64"
	BOOLEAN Type = "BOOLEAN"
	STRING  Type = "STRING"
	BYTES   Type = "BYTES"
	ARRAY   Type = "ARRAY"
	MAP     Type = "MAP"
	STRUCT  Type = "STRUCT"
)

var (
	INT8_SCHEMA             = &ConnectSchema{t: INT8}
	INT16_SCHEMA            = &ConnectSchema{t: INT16}
	INT32_SCHEMA            = &ConnectSchema{t: INT32}
	INT64_SCHEMA            = &ConnectSchema{t: INT64}
	FLOAT32_SCHEMA          = &ConnectSchema{t: FLOAT32}
	FLOAT64_SCHEMA          = &ConnectSchema{t: FLOAT64}
	BOOLEAN_SCHEMA          = &ConnectSchema{t: BOOLEAN}
	STRING_SCHEMA           = &ConnectSchema{t: STRING}
	BYTES_SCHEMA            = &ConnectSchema{t: BYTES}
	OPTIONAL_INT8_SCHEMA    = &ConnectSchema{t: INT8, optional: true}
	OPTIONAL_INT16_SCHEMA   = &ConnectSchema{t: INT16, optional: true}
	OPTIONAL_INT32_SCHEMA   = &ConnectSchema{t: INT32, optional: true}
	OPTIONAL_INT64_SCHEMA   = &ConnectSchema{t: INT64, optional: true}
	OPTIONAL_FLOAT32_SCHEMA = &ConnectSchema{t: FLOAT32, optional: true}
	OPTIONAL_FLOAT64_SCHEMA = &ConnectSchema{t: FLOAT64, optional: true}
	OPTIONAL_BOOLEAN_SCHEMA = &ConnectSchema{t: BOOLEAN, optional: true}
	OPTIONAL_STRING_SCHEMA  = &ConnectSchema{t: STRING, optional: true}
	OPTIONAL_BYTES_SCHEMA   = &ConnectSchema{t: BYTES, optional: true}
)

type ConnectSchema struct {
	t            Type
	optional     bool
	defaultValue interface{}
	fields       map[string]*Field
	keySchema    Schema
	valueSchema  Schema
	name         string
	version      int
	doc          string
	parameters   map[string]string
}

func (s *ConnectSchema) Type() Type {
	return s.t
}

func (s *ConnectSchema) WithType(t Type) *ConnectSchema {
	s.t = t
	return s
}

func (s *ConnectSchema) IsOptional() bool {
	return s.optional
}

func (s *ConnectSchema) DefaultValue() interface{} {
	return s.defaultValue
}

func (s *ConnectSchema) Name() string {
	return s.name
}

func (s *ConnectSchema) Version() int {
	return s.version
}

func (s *ConnectSchema) Doc() string {
	return s.doc
}

func (s *ConnectSchema) Parameters() map[string]string {
	return s.parameters
}

func (s *ConnectSchema) WithKeySchema(keySchema Schema) *ConnectSchema {
	s.keySchema = keySchema
	return s
}

func (s *ConnectSchema) KeySchema() Schema {
	return s.keySchema
}

func (s *ConnectSchema) WithValueSchema(valueSchema Schema) *ConnectSchema {
	s.valueSchema = valueSchema
	return s
}

func (s *ConnectSchema) ValueSchema() Schema {
	return s.valueSchema
}

func (s *ConnectSchema) Fields() []*Field {
	fields := make([]*Field, len(s.fields))
	for _, field := range s.fields {
		fields[field.Index] = field
	}
	return fields
}

func (s *ConnectSchema) WithFields(fields []*Field) *ConnectSchema {
	if s.fields == nil {
		s.fields = make(map[string]*Field)
	}
	for _, field := range fields {
		s.WithField(field)
	}
	return s
}

func (s *ConnectSchema) WithField(field *Field) *ConnectSchema {
	if s.fields == nil {
		s.fields = make(map[string]*Field)
	}
	s.fields[field.Name] = field
	return s
}

func (s *ConnectSchema) Field(name string) *Field {
	return s.fields[name]
}

func (s *ConnectSchema) Schema() Schema {
	return s
}
