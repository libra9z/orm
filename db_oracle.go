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
	"reflect"
	"strconv"
	"strings"
	"time"
)

// oracle operators.
var oracleOperators = map[string]string{
	"exact":       "= ?",
	"gt":          "> ?",
	"gte":         ">= ?",
	"lt":          "< ?",
	"lte":         "<= ?",
	"//iendswith": "LIKE ?",
}

// oracle column field types.
var oracleTypes = map[string]string{
	"auto":            "NOT NULL PRIMARY KEY",
	"pk":              "NOT NULL PRIMARY KEY",
	"bool":            "bool",
	"string":          "VARCHAR2(%d)",
	"string-char":     "CHAR(%d)",
	"string-text":     "VARCHAR2(%d)",
	"time.Time-date":  "DATE",
	"time.Time":       "TIMESTAMP",
	"int8":            "INTEGER",
	"int16":           "INTEGER",
	"int32":           "INTEGER",
	"int64":           "INTEGER",
	"uint8":           "INTEGER",
	"uint16":          "INTEGER",
	"uint32":          "INTEGER",
	"uint64":          "INTEGER",
	"float64":         "NUMBER",
	"float64-decimal": "NUMBER(%d, %d)",
}

// oracle dbBaser
type dbBaseOracle struct {
	dbBase
}

var _ dbBaser = new(dbBaseOracle)

// create oracle dbBaser.
func newdbBaseOracle() dbBaser {
	b := new(dbBaseOracle)
	b.ins = b
	return b
}

// OperatorSQL get oracle operator.
func (d *dbBaseOracle) OperatorSQL(operator string) string {
	return oracleOperators[operator]
}

// DbTypes get oracle table field types.
func (d *dbBaseOracle) DbTypes() map[string]string {
	return oracleTypes
}

//ShowTablesQuery show all the tables in database
func (d *dbBaseOracle) ShowTablesQuery() string {
	return "select TABLE_NAME, OWNER from SYS.ALL_TABLES order by OWNER, TABLE_NAME"
	//return "SELECT TABLE_NAME FROM USER_TABLES"
}

// Oracle
func (d *dbBaseOracle) ShowColumnsQuery(table string) string {
	ss := strings.Split(table, ".")
	if len(ss) > 1 {
		table = ss[1]
	}
	return fmt.Sprintf("SELECT COLUMN_NAME,DATA_TYPE,NULLABLE FROM ALL_TAB_COLUMNS "+
		"WHERE TABLE_NAME ='%s'", strings.ToUpper(table))
}

// check index is exist
func (d *dbBaseOracle) IndexExists(db dbQuerier, table string, name string) bool {
	ss := strings.Split(table, ".")
	if len(ss) > 1 {
		table = ss[1]
	}

	row := db.QueryRow("SELECT COUNT(*) FROM USER_IND_COLUMNS, USER_INDEXES "+
		"WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME "+
		"AND  USER_IND_COLUMNS.TABLE_NAME = :1 AND USER_IND_COLUMNS.INDEX_NAME = :2 ", strings.ToUpper(table), strings.ToUpper(name))

	var cnt int
	row.Scan(&cnt)
	return cnt > 0
}

// gt all tables.
func (d *dbBaseOracle) GetTables(db dbQuerier) (map[string]bool, error) {
	tables := make(map[string]bool)
	query := d.ins.ShowTablesQuery()
	rows, err := db.Query(query)
	if err != nil {
		return tables, err
	}

	defer rows.Close()

	for rows.Next() {
		var table, schema string
		err := rows.Scan(&table, &schema)
		if err != nil {
			return tables, err
		}
		if table != "" {
			tables[strings.ToLower(schema)+"."+strings.ToLower(table)] = true
		}
	}

	return tables, nil
}

// get all cloumns in table.
func (d *dbBaseOracle) GetColumns(db dbQuerier, table string) (map[string][3]string, error) {
	columns := make(map[string][3]string)
	query := d.ins.ShowColumnsQuery(table)
	rows, err := db.Query(query)
	if err != nil {
		return columns, err
	}

	defer rows.Close()

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
		columns[strings.ToLower(name)] = [3]string{strings.ToLower(name), strings.ToLower(typ), strings.ToLower(null)}
	}

	return columns, nil
}

func (d *dbBaseOracle) TableQuote() string {
	return ""
}

func (d *dbBaseOracle) ReplaceMarks(query *string) {
	ss := strings.Split(*query, "?")
	if len(ss) > 1 {
		*query = ""
		for i := 0; i < len(ss)-1; i++ {
			*query = *query + ss[i] + ":" + strconv.FormatInt(int64(i+1), 10)
		}
		*query = *query + ss[len(ss)-1]
	}
}

// execute insert sql dbQuerier with given struct reflect.Value.
func (d *dbBaseOracle) Insert(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location) (int64, error) {
	names := make([]string, 0, len(mi.fields.dbcols))
	values, autoFields, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
	if err != nil {
		return 0, err
	}

	id, err := d.InsertValue(q, mi, false, names, values)
	if err != nil {
		return 0, err
	}

	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}
	return id, err
}

// execute insert sql with given struct and given values.
// insert the given values, not the field values in struct.
func (d *dbBaseOracle) InsertValue(q dbQuerier, mi *modelInfo, isMulti bool, names []string, values []interface{}) (int64, error) {
	Q := d.ins.TableQuote()

	marks := make([]string, len(names))
	for i := range marks {
		marks[i] = "?"
	}

	sep := fmt.Sprintf("%s, %s", Q, Q)
	qmarks := strings.Join(marks, ", ")
	columns := strings.Join(names, sep)

	multi := len(values) / len(names)

	if isMulti {
		qmarks = strings.Repeat(qmarks+"), (", multi-1) + qmarks
	}

	query := fmt.Sprintf("INSERT INTO %s%s%s (%s%s%s) VALUES (%s)", Q, mi.table, Q, Q, columns, Q, qmarks)

	d.ins.ReplaceMarks(&query)

	if isMulti || !d.ins.HasReturningID(mi, &query) {
		res, err := q.Exec(query, values...)
		if err == nil {
			if isMulti {
				return res.RowsAffected()
			}
			return 0, nil
		}
		return 0, err
	}
	row := q.QueryRow(query, values...)
	var id int64
	err := row.Scan(&id)
	return id, err
}

func (d *dbBaseOracle) SetSequence(mi *modelInfo, names []string, query *string) {
	pkname := ""
	value := ""
	for _, column := range mi.fields.dbcols {
		var fi *fieldInfo
		if fi, _ = mi.fields.GetByAny(column); fi != nil {
			column = fi.column
		} else {
			panic(fmt.Errorf("wrong db field/column name `%s` for model `%s`", column, mi.fullName))
		}
		if fi.sequence && fi.colDefault {
			pkname = column
			value = fi.initial.String()
			break
		}
	}
	index := 0
	for i := 0; i < len(names); i++ {
		if names[i] == pkname {
			index = i + 1
			break
		}
	}
	*query = strings.Replace(*query, ":"+strconv.FormatInt(int64(index), 10), value, -1)
}
