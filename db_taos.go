// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orm

import (
	"fmt"
)

// taos operators.
var taosOperators = map[string]string{
	"exact":       "= ?",
	"iexact":      "LIKE ? ESCAPE '\\'",
	"contains":    "LIKE ? ESCAPE '\\'",
	"icontains":   "LIKE ? ESCAPE '\\'",
	"gt":          "> ?",
	"gte":         ">= ?",
	"lt":          "< ?",
	"lte":         "<= ?",
	"eq":          "= ?",
	"ne":          "!= ?",
	"startswith":  "LIKE ? ESCAPE '\\'",
	"endswith":    "LIKE ? ESCAPE '\\'",
	"istartswith": "LIKE ? ESCAPE '\\'",
	"iendswith":   "LIKE ? ESCAPE '\\'",
}

// taos column types.
var taosTypes = map[string]string{
	"auto":            "integer NOT NULL PRIMARY KEY AUTOINCREMENT",
	"pk":              "NOT NULL PRIMARY KEY",
	"bool":            "bool",
	"string":          "binary(%d)",
	"binary":     	   "nchar(%d)",
	"time.Time":       "timestamp",
	"int8":            "tinyint",
	"int16":           "smallint",
	"int32":           "int",
	"int64":           "bigint",
	"float32":         "float",
	"float64":         "double",
}

// taos dbBaser.
type dbBaseTaos struct {
	dbBase
}

var _ dbBaser = new(dbBaseTaos)

// get taos operator.
func (d *dbBaseTaos) OperatorSQL(operator string) string {
	return taosOperators[operator]
}

// generate functioned sql for taos.
// only support DATE(text).
func (d *dbBaseTaos) GenerateOperatorLeftCol(fi *fieldInfo, operator string, leftCol *string) {
	if fi.fieldType == TypeDateField {
		*leftCol = fmt.Sprintf("DATE(%s)", *leftCol)
	}
}

// unable updating joined record in taos.
func (d *dbBaseTaos) SupportUpdateJoin() bool {
	return false
}

// max int in taos.
func (d *dbBaseTaos) MaxLimit() uint64 {
	return 9223372036854775807
}

// get column types in taos.
func (d *dbBaseTaos) DbTypes() map[string]string {
	return taosTypes
}


// get columns in taos.
func (d *dbBaseTaos) GetColumns(db dbQuerier, table string) (map[string][3]string, error) {
	query := d.ins.ShowColumnsQuery(table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	columns := make(map[string][3]string)

	for rows.Next() {
		var (
			name string
			typ  string
			null string
		)
		err := rows.Scan(&name, &typ, &null)
		if err != nil {
			return columns, err
		}
		null = "NOT NULL"
		columns[name] = [3]string{name, typ, null}
	}

	return columns, nil
}

// get show columns sql in taos.
func (d *dbBaseTaos) ShowColumnsQuery(table string) string {
	return fmt.Sprintf("describe %s", table)
}

// create new taos dbBaser.
func newdbBaseTaos() dbBaser {
	b := new(dbBaseTaos)
	b.ins = b
	return b
}
