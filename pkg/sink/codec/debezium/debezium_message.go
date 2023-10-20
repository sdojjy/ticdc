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
