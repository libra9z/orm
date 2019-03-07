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
	"database/sql"
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
	return "SELECT TABLE_NAME FROM USER_TABLES"
}

// Oracle
func (d *dbBaseOracle) ShowColumnsQuery(table string) string {
	return fmt.Sprintf("SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS "+
		"WHERE TABLE_NAME ='%s'", strings.ToUpper(table))
}

// check index is exist
func (d *dbBaseOracle) IndexExists(db dbQuerier, table string, name string) bool {
	row := db.QueryRow("SELECT COUNT(*) FROM USER_IND_COLUMNS, USER_INDEXES "+
		"WHERE USER_IND_COLUMNS.INDEX_NAME = USER_INDEXES.INDEX_NAME "+
		"AND  USER_IND_COLUMNS.TABLE_NAME = ? AND USER_IND_COLUMNS.INDEX_NAME = ?", strings.ToUpper(table), strings.ToUpper(name))

	var cnt int
	row.Scan(&cnt)
	return cnt > 0
}

// execute insert sql with given struct and given values.
// insert the given values, not the field values in struct.
func (d *dbBaseOracle) InsertValue(q dbQuerier, mi *modelInfo, isMulti bool, names []string, values []interface{}) (int64, error) {
	Q := d.ins.TableQuote()

	marks := make([]string, len(names))
	idx := 0
	for i := range marks {
		idx = idx + 1
		marks[i] = ":" + strconv.FormatInt(int64(idx), 10)
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
		fmt.Printf("err=%v\n", err)
		if err == nil {
			if isMulti {
				return res.RowsAffected()
			}
			return res.LastInsertId()
		} else {
			return 0, err
		}
		return 0, nil
	}
	row := q.QueryRow(query, values...)
	var id int64
	err := row.Scan(&id)
	return id, err
}

// query sql ,read records and persist in dbBaser.
func (d *dbBaseOracle) Read(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string, isForUpdate bool) error {
	var whereCols []string
	var args []interface{}

	// if specify cols length > 0, then use it for where condition.
	if len(cols) > 0 {
		var err error
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return err
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s, %s", Q, Q)
	sels := strings.Join(mi.fields.dbcols, sep)
	colsNum := len(mi.fields.dbcols)

	sep = fmt.Sprintf("%s = ? AND %s", Q, Q)
	wheres := strings.Join(whereCols, sep)

	forUpdate := ""
	if isForUpdate {
		forUpdate = "FOR UPDATE"
	}

	query := fmt.Sprintf("SELECT %s%s%s FROM %s%s%s WHERE %s%s%s = :1 %s", Q, sels, Q, Q, mi.table, Q, Q, wheres, Q, forUpdate)

	refs := make([]interface{}, colsNum)
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}

	d.ins.ReplaceMarks(&query)

	row := q.QueryRow(query, args...)
	if err := row.Scan(refs...); err != nil {
		if err == sql.ErrNoRows {
			return ErrNoRows
		}
		return err
	}
	elm := reflect.New(mi.addrField.Elem().Type())
	mind := reflect.Indirect(elm)
	d.setColsValues(mi, &mind, mi.fields.dbcols, refs, tz)
	ind.Set(mind)
	return nil
}

// multi-insert sql with given slice struct reflect.Value.
func (d *dbBaseOracle) InsertMulti(q dbQuerier, mi *modelInfo, sind reflect.Value, bulk int, tz *time.Location) (int64, error) {
	var (
		cnt    int64
		nums   int
		values []interface{}
		names  []string
	)

	// typ := reflect.Indirect(mi.addrField).Type()

	length, autoFields := sind.Len(), make([]string, 0, 1)

	for i := 1; i <= length; i++ {

		ind := reflect.Indirect(sind.Index(i - 1))

		// Is this needed ?
		// if !ind.Type().AssignableTo(typ) {
		// 	return cnt, ErrArgs
		// }

		if i == 1 {
			var (
				vus []interface{}
				err error
			)
			vus, autoFields, err = d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
			if err != nil {
				return cnt, err
			}
			values = make([]interface{}, bulk*len(vus))
			nums += copy(values, vus)
		} else {
			vus, _, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, nil, tz)
			if err != nil {
				return cnt, err
			}

			if len(vus) != len(names) {
				return cnt, ErrArgs
			}

			nums += copy(values[nums:], vus)
		}

		if i > 1 && i%bulk == 0 || length == i {
			num, err := d.InsertValue(q, mi, true, names, values[:nums])
			if err != nil {
				return cnt, err
			}
			cnt += num
			nums = 0
		}
	}

	var err error
	if len(autoFields) > 0 {
		err = d.ins.setval(q, mi, autoFields)
	}

	return cnt, err
}

// execute insert sql dbQuerier with given struct reflect.Value.
func (d *dbBaseOracle) Insert(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location) (int64, error) {
	names := make([]string, 0, len(mi.fields.dbcols))
	values, autoFields, err := d.collectValues(mi, ind, mi.fields.dbcols, false, true, &names, tz)
	if err != nil {
		return 0, err
	}

	if values == nil {

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

// execute update sql dbQuerier with given struct reflect.Value.
func (d *dbBaseOracle) Update(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (int64, error) {
	pkName, pkValue, ok := getExistPk(mi, ind)
	if !ok {
		return 0, ErrMissPK
	}

	var setNames []string

	// if specify cols length is zero, then commit all columns.
	if len(cols) == 0 {
		cols = mi.fields.dbcols
		setNames = make([]string, 0, len(mi.fields.dbcols)-1)
	} else {
		setNames = make([]string, 0, len(cols))
	}

	setValues, _, err := d.collectValues(mi, ind, cols, true, false, &setNames, tz)
	if err != nil {
		return 0, err
	}

	setValues = append(setValues, pkValue)

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s = ?, %s ", Q, Q)
	setColumns := strings.Join(setNames, sep)

	query := fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = ? WHERE %s%s%s = ? ", Q, mi.table, Q, Q, setColumns, Q, Q, pkName, Q)

	d.ins.ReplaceMarks(&query)

	res, err := q.Exec(query, setValues...)
	if err == nil {
		return res.RowsAffected()
	}
	return 0, err
}

// execute delete sql dbQuerier with given struct reflect.Value.
// delete index is pk.
func (d *dbBaseOracle) Delete(q dbQuerier, mi *modelInfo, ind reflect.Value, tz *time.Location, cols []string) (int64, error) {
	var whereCols []string
	var args []interface{}
	// if specify cols length > 0, then use it for where condition.
	if len(cols) > 0 {
		var err error
		whereCols = make([]string, 0, len(cols))
		args, _, err = d.collectValues(mi, ind, cols, false, false, &whereCols, tz)
		if err != nil {
			return 0, err
		}
	} else {
		// default use pk value as where condtion.
		pkColumn, pkValue, ok := getExistPk(mi, ind)
		if !ok {
			return 0, ErrMissPK
		}
		whereCols = []string{pkColumn}
		args = append(args, pkValue)
	}

	Q := d.ins.TableQuote()

	sep := fmt.Sprintf("%s = ? AND %s ", Q, Q)
	wheres := strings.Join(whereCols, sep)

	query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s = ? ", Q, mi.table, Q, Q, wheres, Q)

	d.ins.ReplaceMarks(&query)
	res, err := q.Exec(query, args...)
	if err == nil {
		num, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if num > 0 {
			if mi.fields.pk.auto {
				if mi.fields.pk.fieldType&IsPositiveIntegerField > 0 {
					ind.FieldByIndex(mi.fields.pk.fieldIndex).SetUint(0)
				} else {
					ind.FieldByIndex(mi.fields.pk.fieldIndex).SetInt(0)
				}
			}
			err := d.deleteRels(q, mi, args, tz)
			if err != nil {
				return num, err
			}
		}
		return num, err
	}
	return 0, err
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
	}
}
