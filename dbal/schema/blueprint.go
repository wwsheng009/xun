package schema

import (
	"errors"
	"fmt"
	"strings"
)

// NewBlueprint create a new blueprint intance
func NewBlueprint(name string, builder *Builder) *Blueprint {
	return &Blueprint{
		Name:      name,
		Builder:   builder,
		Columns:   []*Column{},
		ColumnMap: map[string]*Column{},
		Indexes:   []*Index{},
		IndexMap:  map[string]*Index{},
	}
}

// Create table
func (table *Blueprint) Create() {
	table.validate().Builder.Conn.Write.
		MustExec(table.sqlCreate())
}

// Drop table
func (table *Blueprint) Drop() {
	table.validate().Builder.Conn.Write.
		MustExec(table.sqlDrop())
}

// Exists check if the table is exists
func (table *Blueprint) Exists() bool {
	row := table.validate().Builder.Conn.Write.
		QueryRowx(table.sqlExists())
	if row.Err() != nil {
		panic(row.Err())
	}

	res, err := row.SliceScan()
	if err != nil {
		return false
	}
	return table.Name == fmt.Sprintf("%s", res[0])
}

// BigInteger Create a new auto-incrementing big integer (8-byte) column on the table.
func (table *Blueprint) BigInteger() {}

// String Create a new string column on the table.
func (table *Blueprint) String(name string, length int) *Column {
	column := table.NewColumn(name)
	column.Length = &length
	column.Type = "string"
	table.addColumn(column)
	return column
}

// NewIndex New index instance
func (table *Blueprint) NewIndex(name string, columns ...*Column) *Index {
	index := &Index{Name: name, Columns: []*Column{}}
	index.Columns = append(index.Columns, columns...)
	index.Table = table
	return index
}

// NewColumn New column instance
func (table *Blueprint) NewColumn(name string) *Column {
	return &Column{Name: name, Table: table}
}

func (table *Blueprint) addColumn(column *Column) {
	column.validate()
	table.Columns = append(table.Columns, column)
	table.ColumnMap[column.Name] = column
}

func (table *Blueprint) addIndex(index *Index) {
	index.validate()
	table.Indexes = append(index.Table.Indexes, index)
	table.IndexMap[index.Name] = index
}

func (table *Blueprint) validate() *Blueprint {
	if table.Builder == nil {
		err := errors.New("the table " + table.Name + "does not bind the builder")
		panic(err)
	}
	return table
}

func (table *Blueprint) sqlExists() string {
	sql := fmt.Sprintf("SHOW TABLES like '%s'", table.Name)
	return sql
}

func (table *Blueprint) sqlDrop() string {
	sql := fmt.Sprintf("DROP Table `%s`", table.Name)
	return sql
}

func (table *Blueprint) sqlCreate() string {

	sql := "CREATE Table `" + table.Name + "` (\n"

	// columns
	stmts := []string{}
	for _, column := range table.Columns {
		stmts = append(stmts, column.sqlCreate())
	}

	for _, index := range table.Indexes {
		stmts = append(stmts, index.sqlCreate())
	}

	// indexes
	sql = sql + strings.Join(stmts, ",\n")
	sql = sql + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

	return sql
}
