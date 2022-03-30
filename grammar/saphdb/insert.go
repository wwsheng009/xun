package saphdb

import (
	"errors"
	"fmt"
	"time"

	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/utils"
)

// CompileInsertOrIgnore Compile an insert ignore statement into SQL.
func (grammarSQL Hdb) CompileInsertOrIgnore(query *dbal.Query, columns []interface{}, values [][]interface{}) (string, []interface{}) {
	sql, bindings := grammarSQL.CompileInsert(query, columns, values)
	// sql = fmt.Sprintf("%s", sql)
	return sql, bindings
}

// CompileInsertGetID Compile an insert and get ID statement into SQL.
func (grammarSQL Hdb) CompileInsertGetID(query *dbal.Query, columns []interface{}, values [][]interface{}, sequence string) (string, []interface{}) {
	sql, bindings := grammarSQL.CompileInsert(query, columns, values)
	// sql = fmt.Sprintf("%s returning %s", sql, grammarSQL.ID(sequence))
	return sql, bindings
}

// ProcessInsertGetID Execute an insert and get ID statement and return the id
func (grammarSQL Hdb) ProcessInsertGetID(sql string, bindings []interface{}, sequence string) (int64, error) {
	// var seq int64
	// err := grammarSQL.DB.Get(&seq, sql, bindings...)
	// if err != nil {
	// 	return 0, err
	// }
	// return seq, nil

	stmt, err := grammarSQL.DB.Prepare(sql)
	if err != nil {
		return 0, err
	}

	defer stmt.Close()
	// res, err := stmt.Exec(bindings...)
	_, err = utils.StmtExec(stmt, bindings)
	if err != nil {
		return 0, err
	}

	rows := []int64{}
	err = grammarSQL.DB.Select(&rows, "select current_identity_value() FROM DUMMY;")
	if err != nil {
		return 0, err
	}

	return rows[0], err
}

// CompileInsert Compile an insert statement into SQL.
func (grammarSQL Hdb) CompileInsert(query *dbal.Query, columns []interface{}, values [][]interface{}) (string, []interface{}) {

	table := grammarSQL.WrapTable(query.From)
	if len(values) == 0 {
		return fmt.Sprintf("insert into %s default values", table), nil
	}

	offset := 0
	parameters := ""
	bindings := []interface{}{}
	for idx, value := range values {
		row := []interface{}{}
		if idx == 0 {
			parameters = fmt.Sprintf("(%s)", grammarSQL.Parameterize(value, offset))
		}
		for _, v := range value {
			if !dbal.IsExpression(v) {
				// if v1, err := value2time(v); err == nil {
				// 	row = append(row, v1)
				// } else {
				row = append(row, v)
				// }
				offset++
			}
		}
		bindings = append(bindings, row)
	}

	return fmt.Sprintf("insert into %s (%s) values %s", table, grammarSQL.Columnize(columns), parameters), bindings
}

func value2time(v interface{}) (time.Time, error) {

	formats := []string{
		"2006-01-02T15:04:05-0700",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"15:04:05",
	}

	dateValue := time.Now()
	err := errors.New("not datetime value")
	switch strValue := v.(type) {
	case string:
		// v is a string here, so e.g. v + " Yeah!" is possible.
		fmt.Printf("String: %v", strValue)
		for _, format := range formats {
			dateValue, err = time.Parse(format, strValue)
			if err == nil {
				return dateValue, nil
			}
		}
	}
	return dateValue, err
}

// CompileInsertUsing Compile an insert statement using a subquery into SQL.
func (grammarSQL Hdb) CompileInsertUsing(query *dbal.Query, columns []interface{}, sql string) string {
	return fmt.Sprintf("INSERT INTO %s (%s) %s from dummy", grammarSQL.WrapTable(query.From), grammarSQL.Columnize(columns), sql)
}
