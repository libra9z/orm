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
	"strconv"
	"strings"
)

// sqlserver operators.
var sqlserverOperators = map[string]string{
	"exact":       "= ?",
	"gt":          "> ?",
	"gte":         ">= ?",
	"lt":          "< ?",
	"lte":         "<= ?",
	"//iendswith": "LIKE ?",
}

// sqlserver column field types.
var sqlserverTypes = map[string]string{
	"auto":            "IDENTITY(1,1) NOT NULL PRIMARY KEY",
	"pk":              "NOT NULL PRIMARY KEY",
	"bool":            "bool",
	"string":          "varchar(%d)",
	"string-char":     "char(%d)",
	"string-text":     "text",
	"time.Time-date":  "date",
	"time.Time":       "datetime",
	"int8":            "tinyint",
	"int16":           "smallint",
	"int32":           "int",
	"int64":           "bigint",
	"uint8":           "tinyint",
	"uint16":          "smallint",
	"uint32":          "int",
	"uint64":          "bigint",
	"float64":         "float(53)",
	"float64-decimal": "number(%d, %d)",
}

// sqlserver dbBaser
type dbBaseSqlserver struct {
	dbBase
}

var _ dbBaser = new(dbBaseSqlserver)

// create Sqlserver dbBaser.
func newdbBaseSqlserver() dbBaser {
	b := new(dbBaseSqlserver)
	b.ins = b
	return b
}

// OperatorSQL get Sqlserver operator.
func (d *dbBaseSqlserver) OperatorSQL(operator string) string {
	return sqlserverOperators[operator]
}

// DbTypes get Sqlserver table field types.
func (d *dbBaseSqlserver) DbTypes() map[string]string {
	return sqlserverTypes
}

//ShowTablesQuery show all the tables in database
func (d *dbBaseSqlserver) ShowTablesQuery() string {
	return "SELECT NAME FROM SYSOBJECTS WHERE XTYPE='U'"
}

// Sqlserver
func (d *dbBaseSqlserver) ShowColumnsQuery(table string) string {
	return fmt.Sprintf("SELECT NAME FROM SYSCOLUMNs "+
		"WHERE id=Object_Id('%s')", strings.ToUpper(table))
}

// check index is exist
func (d *dbBaseSqlserver) IndexExists(db dbQuerier, table string, name string) bool {
	row := db.QueryRow("SELECT COUNT(*) FROM USER_IND_COLUMNS, USER_INDEXES "+
		"WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME "+
		"AND  USER_IND_COLUMNS.TABLE_NAME = ? AND USER_IND_COLUMNS.INDEX_NAME = ?", strings.ToUpper(table), strings.ToUpper(name))

	var cnt int
	row.Scan(&cnt)
	return cnt > 0
}

func (d *dbBaseSqlserver) TableQuote() string {
	return ""
}

func (d *dbBaseSqlserver) ReplaceMarks(query *string) {
	ss := strings.Split(*query, "?")
	if len(ss) > 1 {
		*query = ""
		for i := 0; i < len(ss)-1; i++ {
			*query = *query + ss[i] + "@p" + strconv.FormatInt(int64(i+1), 10)
		}

		*query = *query + ss[len(ss)-1]
	}
}
