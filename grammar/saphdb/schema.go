package saphdb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/yaoapp/kun/log"
	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/utils"
)

// GetVersion get the version of the connection database
func (grammarSQL Hdb) GetVersion() (*dbal.Version, error) {
	sql := fmt.Sprintf("select VERSION  from \"SYS\".\"M_DATABASE\";")
	// defer logger.Debug(logger.RETRIEVE, sql).TimeCost(time.Now())
	rows := []string{}
	err := grammarSQL.DB.Select(&rows, sql)
	if err != nil {
		return nil, err
	}
	if len(rows) < 1 {
		return nil, fmt.Errorf("Can't get the version")
	}

	verArr := strings.Split(rows[0], ".")
	if len(verArr) < 4 {
		return nil, fmt.Errorf("Can't parse the version: %s", rows[0])
	}

	verstr := fmt.Sprintf("%s.%s.%s", strings.TrimLeft(verArr[0], "0"), strings.TrimLeft(verArr[2], "0"), strings.TrimLeft(verArr[4], "0"))
	ver, err := semver.Make(verstr)
	if err == nil {
		return &dbal.Version{
			Version: ver,
			Driver:  grammarSQL.Driver,
		}, nil
	}

	defer log.With(log.F{"version": ver}).Trace(sql)
	// if strings.Contains(verArr[0], ".") {
	// 	ver, err = semver.Make(verArr[0] + ".0")
	// }

	if err == nil {
		return &dbal.Version{
			Version: ver,
			Driver:  grammarSQL.Driver,
		}, nil
	}

	ver, err = semver.Make(verArr[0])
	if err == nil {
		return &dbal.Version{
			Version: ver,
			Driver:  grammarSQL.Driver,
		}, nil
	}

	return &dbal.Version{
		Version: ver,
		Driver:  grammarSQL.Driver,
	}, nil
}

// GetTables Get all of the table names for the database.
func (grammarSQL Hdb) GetTables() ([]string, error) {
	sql := fmt.Sprintf(
		"SELECT table_name AS name FROM public.tables WHERE schema_name=%s",
		grammarSQL.VAL(grammarSQL.GetSchema()),
	)
	defer log.Debug(sql)
	tables := []string{}
	err := grammarSQL.DB.Select(&tables, sql)
	if err != nil {
		return nil, err
	}
	return tables, nil
}

// TableExists check if the table exists
func (grammarSQL Hdb) TableExists(name string) (bool, error) {
	table_name := name
	schema_name := grammarSQL.GetSchema()

	if strings.Contains(name, ".") { //只要有权限，可以获取别的schema的表
		arrs := strings.Split(name, ".")
		schema_name = arrs[0]
		table_name = arrs[1]
	}
	sql := fmt.Sprintf(
		"SELECT table_name AS name FROM public.tables WHERE schema_name=%s AND table_name = %s",
		grammarSQL.VAL(schema_name),
		grammarSQL.VAL(table_name),
	)
	defer log.Debug(sql)
	rows := []string{}
	err := grammarSQL.DB.Select(&rows, sql)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	return table_name == rows[0], nil
}

// CreateType create user defined type
func (grammarSQL Hdb) CreateType(table *dbal.Table, types map[string][]string) error {
	// Create Types
	// for name, option := range types {
	// 	typ := fmt.Sprintf("ENUM('%s')", strings.Join(option, "','"))
	// 	typeSQL := fmt.Sprintf(`
	// DO $$ BEGIN
	// 	CREATE TYPE %s.%s AS %s;
	// EXCEPTION
	// 	WHEN duplicate_object THEN null;
	// END $$;
	// `, table.SchemaName, name, typ)
	// 	defer log.Debug(typeSQL)
	// 	_, err := grammarSQL.DB.Exec(typeSQL)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}

// CreateTable create a new table on the schema
func (grammarSQL Hdb) CreateTable(table *dbal.Table, options ...dbal.CreateTableOption) error {
	name := grammarSQL.ID(table.TableName)
	sql := fmt.Sprintf("CREATE TABLE %s (\n", name)

	if len(options) > 0 {
		option := options[0]
		if option.Temporary {
			sql = fmt.Sprintf("CREATE GLOBAL TEMPORARY TABLE %s (\n", name)
		}
	}

	stmts := []string{}
	commentStmts := []string{}

	var primary *dbal.Primary = nil
	columns := []*dbal.Column{}
	indexes := []*dbal.Index{}
	cbCommands := []*dbal.Command{}
	// Commands
	// The commands must be:
	//    AddColumn(column *Column)    for adding a column
	//    ChangeColumn(column *Column) for modifying a colu
	//    RenameColumn(old string,new string)  for renaming a column
	//    DropColumn(name string)  for dropping a column
	//    CreateIndex(index *Index) for creating a index
	//    DropIndex( name string) for  dropping a index
	//    RenameIndex(old string,new string)  for renaming a index
	for _, command := range table.Commands {
		switch command.Name {
		case "AddColumn":
			columns = append(columns, command.Params[0].(*dbal.Column))
			cbCommands = append(cbCommands, command)
			break
		case "CreateIndex":
			indexes = append(indexes, command.Params[0].(*dbal.Index))
			cbCommands = append(cbCommands, command)
			break
		case "CreatePrimary":
			primary = command.Params[0].(*dbal.Primary)
			cbCommands = append(cbCommands, command)
			break
		}
	}

	err := grammarSQL.createTableAddColumn(table, &stmts, &commentStmts, columns)
	if err != nil {
		return err
	}

	// Primary key
	if primary != nil {
		stmts = append(stmts, grammarSQL.SQLAddPrimary(primary))
	}
	sql = sql + strings.Join(stmts, ",\n")
	sql = sql + fmt.Sprintf("\n)")

	// Create table
	defer log.Debug(sql)
	_, err = grammarSQL.DB.Exec(sql)
	if err != nil {
		return err
	}

	// indexes
	err = grammarSQL.createTableCreateIndex(table, indexes)
	if err != nil {
		return err
	}

	// Comments
	err = grammarSQL.createTableAddComment(table, commentStmts)
	if err != nil {
		return err
	}

	// Callback
	for _, cmd := range cbCommands {
		cmd.Callback(err)
	}

	return nil
}

func (grammarSQL Hdb) createTableAddColumn(table *dbal.Table, stmts *[]string, commentStmts *[]string, columns []*dbal.Column) error {
	// Enum types
	types := map[string][]string{}

	// Columns
	for _, column := range columns {
		*stmts = append(*stmts,
			grammarSQL.SQLAddColumn(column),
		)

		commentStmt := grammarSQL.SQLAddComment(column)
		if commentStmt != "" {
			*commentStmts = append(*commentStmts, commentStmt)
		}
		// if column.Type == "enum" {
		// 	typeName := strings.ToLower("ENUM__" + strings.Join(column.Option, "_EOPT_"))
		// 	types[typeName] = column.Option
		// }
	}

	// Create Types
	return grammarSQL.CreateType(table, types)
}

func (grammarSQL Hdb) createTableCreateIndex(table *dbal.Table, indexes []*dbal.Index) error {
	indexStmts := []string{}

	for _, index := range indexes {
		if index.Type == "primary" {
			continue
		}
		indexStmt := grammarSQL.SQLAddIndex(index)
		if indexStmt != "" {
			indexStmts = append(indexStmts, indexStmt)
		}
	}
	if len(indexStmts) > 0 {
		// sql := strings.Join(indexStmts, ";\n")
		for _, sql := range indexStmts {
			defer log.Debug(sql)
			_, err := grammarSQL.DB.Exec(sql)
			if err != nil {
				return err
			}

		}
		// defer log.Debug(sql)
		// _, err := grammarSQL.DB.Exec(sql)

	}
	return nil
}

func (grammarSQL Hdb) createTableAddComment(table *dbal.Table, commentStmts []string) error {
	if len(commentStmts) > 0 {
		// sql := strings.Join(commentStmts, ";\n")
		// defer log.Debug(sql)
		// _, err := grammarSQL.DB.Exec(sql)
		// return err

		for _, sql := range commentStmts {
			defer log.Debug(sql)
			_, err := grammarSQL.DB.Exec(sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// RenameTable rename a table on the schema.
func (grammarSQL Hdb) RenameTable(old string, new string) error {
	sql := fmt.Sprintf("RENAME TABLE %s TO %s", grammarSQL.ID(old), grammarSQL.ID(new))
	defer log.Debug(sql)
	_, err := grammarSQL.DB.Exec(sql)
	return err
}

// GetTable get a table on the schema
func (grammarSQL Hdb) GetTable(name string) (*dbal.Table, error) {
	has, err := grammarSQL.TableExists(name)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, fmt.Errorf("the table %s does not exists", name)
	}
	schema_name := grammarSQL.GetSchema()
	table_name := name
	if strings.Contains(name, ".") { //只要有权限，可以获取别的schema的表
		arrs := strings.Split(name, ".")
		schema_name = arrs[0]
		table_name = arrs[1]
	}

	table := dbal.NewTable(table_name, schema_name, grammarSQL.GetDatabase())
	columns, err := grammarSQL.GetColumnListing(schema_name, table.TableName)
	if err != nil {
		return nil, err
	}
	indexes, err := grammarSQL.GetIndexListing(schema_name, table.TableName)
	if err != nil {
		return nil, err
	}

	primaryKeyName := ""

	// attaching columns
	for _, column := range columns {
		column.Indexes = []*dbal.Index{}
		table.PushColumn(column)
	}

	// attaching indexes
	for i := range indexes {
		idx := indexes[i]
		if !table.HasColumn(idx.ColumnName) {
			return nil, fmt.Errorf("the column %s does not exists", idx.ColumnName)
		}
		column := table.ColumnMap[idx.ColumnName]
		if !table.HasIndex(idx.Name) {
			index := *idx
			index.Columns = []*dbal.Column{}
			column.Indexes = append(column.Indexes, &index)
			table.PushIndex(&index)
		}
		index := table.IndexMap[idx.Name]
		index.Columns = append(index.Columns, column)
		if index.Type == "primary" {
			primaryKeyName = idx.Name
		}
	}

	// attaching primary
	if _, has := table.IndexMap[primaryKeyName]; has {
		idx := table.IndexMap[primaryKeyName]
		table.Primary = &dbal.Primary{
			Name:      idx.Name,
			TableName: idx.TableName,
			DBName:    idx.DBName,
			Table:     idx.Table,
			Columns:   idx.Columns,
		}
		delete(table.IndexMap, idx.Name)
		for _, column := range table.Primary.Columns {
			column.Primary = true
			column.Indexes = []*dbal.Index{}
		}
	}

	return table, nil
}

// AlterTable alter a table on the schema
func (grammarSQL Hdb) AlterTable(table *dbal.Table) error {

	sql := fmt.Sprintf("ALTER TABLE %s ", grammarSQL.ID(table.TableName))
	stmts := []string{}
	errs := []error{}

	// Commands
	// The commands must be:
	//    AddColumn(column *Column)    for adding a column
	//    ChangeColumn(column *Column) for modifying a colu
	//    RenameColumn(old string,new string)  for renaming a column
	//    DropColumn(name string)  for dropping a column
	//    CreateIndex(index *Index) for creating a index
	//    DropIndex(name string) for  dropping a index
	//    RenameIndex(old string,new string)  for renaming a index
	for _, command := range table.Commands {
		switch command.Name {
		case "AddColumn":
			grammarSQL.alterTableAddColumn(table, command, sql, &stmts, &errs)

		case "ChangeColumn":
			grammarSQL.alterTableChangeColumn(table, command, sql, &stmts, &errs)

		case "RenameColumn":
			grammarSQL.alterTableRenameColumn(table, command, sql, &stmts, &errs)

		case "DropColumn":
			grammarSQL.alterTableDropColumn(table, command, sql, &stmts, &errs)

		case "CreateIndex":
			grammarSQL.alterTableCreateIndex(table, command, sql, &stmts, &errs)

		case "RenameIndex":
			grammarSQL.alterTableRenameIndex(table, command, sql, &stmts, &errs)

		case "DropIndex":
			grammarSQL.alterTableDropIndex(table, command, sql, &stmts, &errs)

		case "CreatePrimary":
			grammarSQL.alterTableCreatePrimary(table, command, sql, &stmts, &errs)

		case "DropPrimary":
			grammarSQL.alterTableDropPrimary(table, command, sql, &stmts, &errs)

		}
	}

	defer log.Debug(strings.Join(stmts, "\n"))

	// Return Errors
	if len(errs) > 0 {
		message := ""
		for _, err := range errs {
			message = message + err.Error() + "\n"
		}
		return errors.New(message)
	}

	return nil
}

func (grammarSQL Hdb) alterTableAddColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	column := command.Params[0].(*dbal.Column)
	stmt := "ADD ( " + grammarSQL.SQLAddColumn(column) + " )"
	*stmts = append(*stmts, sql+stmt)
	if column.Type == "enum" {
		typeName := strings.ToLower("ENUM__" + strings.Join(column.Option, "_EOPT_"))
		types := map[string][]string{}
		types[typeName] = column.Option
		err := grammarSQL.CreateType(table, types)
		if err != nil {
			*errs = append(*errs, err)
			return
		}
	}
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, err)
	}

	commentStmt := grammarSQL.SQLAddComment(column)
	if commentStmt != "" {
		err := grammarSQL.ExecSQL(table, commentStmt)
		if err != nil {
			*errs = append(*errs, err)
		}
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableChangeColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	column := command.Params[0].(*dbal.Column)
	stmt := "ALTER (" + grammarSQL.SQLAlterColumnType(column) + " )"
	*stmts = append(*stmts, sql+stmt)
	if column.Type == "enum" {
		typeName := strings.ToLower("ENUM__" + strings.Join(column.Option, "_EOPT_"))
		types := map[string][]string{}
		types[typeName] = column.Option
		err := grammarSQL.CreateType(table, types)
		if err != nil {
			*errs = append(*errs, err)
			return
		}
	}
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, err)
	}

	commentStmt := grammarSQL.SQLAddComment(column)
	if commentStmt != "" {
		err := grammarSQL.ExecSQL(table, commentStmt)
		if err != nil {
			*errs = append(*errs, err)
		}
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableRenameColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	old := command.Params[0].(string)
	new := command.Params[1].(string)
	column, has := table.ColumnMap[old]
	if !has {
		*errs = append(*errs, fmt.Errorf("the column "+old+" not exists"))
		return
	}
	column.Name = new
	stmt := fmt.Sprintf("RENAME COLUMN \"%s\".%s TO %s", table.TableName,
		grammarSQL.ID(old),
		grammarSQL.ID(new),
	)
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, err)
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableDropColumn(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	name := command.Params[0].(string)
	stmt := fmt.Sprintf("DROP (%s)", grammarSQL.ID(name))
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, err)
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableCreateIndex(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	index := command.Params[0].(*dbal.Index)
	stmt := grammarSQL.SQLAddIndex(index)
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, err)
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableDropIndex(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	name := fmt.Sprintf("%s_%s", table.TableName, command.Params[0])
	stmt := fmt.Sprintf(
		"DROP INDEX %s",
		grammarSQL.ID(name),
		// grammarSQL.Quoter.ID(table.TableName, db),
	)
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, err)
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableCreatePrimary(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	primary := command.Params[0].(*dbal.Primary)
	stmt := "ADD " + grammarSQL.SQLAddPrimary(primary)
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, err)
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableDropPrimary(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	stmt := fmt.Sprintf(
		"DROP PRIMARY KEY",
		// grammarSQL.ID(fmt.Sprintf("%s_pkey", table.GetName())),
	)
	*stmts = append(*stmts, sql+stmt)
	err := grammarSQL.ExecSQL(table, sql+stmt)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("DropPrimary: %s", err))
	}
	command.Callback(err)
}

func (grammarSQL Hdb) alterTableRenameIndex(table *dbal.Table, command *dbal.Command, sql string, stmts *[]string, errs *[]error) {
	old := fmt.Sprintf("%s_%s", table.TableName, command.Params[0])
	new := fmt.Sprintf("%s_%s", table.TableName, command.Params[1])
	stmt := fmt.Sprintf(
		"RENAME INDEX %s TO %s",
		grammarSQL.ID(old),
		grammarSQL.ID(new),
		// grammarSQL.Quoter.ID(table.TableName, db),
	)
	*stmts = append(*stmts, stmt)
	err := grammarSQL.ExecSQL(table, stmt)
	if err != nil {
		*errs = append(*errs, err)
	}
	command.Callback(err)
}

// ExecSQL execute sql then update table structure
func (grammarSQL Hdb) ExecSQL(table *dbal.Table, sql string) error {
	_, err := grammarSQL.DB.Exec(sql)
	if err != nil {
		return err
	}
	// update table structure
	new, err := grammarSQL.GetTable(table.TableName)
	if err != nil {
		return err
	}

	*table = *new
	return nil
}

// SQLAlterColumnType return the add column sql for table alter
func (grammarSQL Hdb) SQLAlterColumnType(Column *dbal.Column) string {
	// `id` bigint(20) unsigned NOT NULL,
	types := grammarSQL.Types
	quoter := grammarSQL.Quoter

	typ, has := types[Column.Type]
	if !has {
		typ = "VARCHAR"
	}

	decimalTypes := []string{"DECIMAL"}
	if Column.Precision != nil && Column.Scale != nil && utils.StringHave(decimalTypes, typ) {
		typ = fmt.Sprintf("%s(%d,%d)", typ, utils.IntVal(Column.Precision), utils.IntVal(Column.Scale))
	} else if strings.Contains(typ, "TIMESTAMP(%d)") || strings.Contains(typ, "TIME(%d)") {
		DateTimePrecision := utils.IntVal(Column.DateTimePrecision, 0)
		typ = fmt.Sprintf(typ, DateTimePrecision)
	} else if typ == "BYTEA" {
		typ = "BYTEA"
	} else if typ == "ENUM" {
		typ = strings.ToLower("ENUM__" + strings.Join(Column.Option, "_EOPT_"))
	} else if Column.Length != nil {
		typ = fmt.Sprintf("%s(%d)", typ, utils.IntVal(Column.Length))
	} else if typ == "IPADDRESS" { // ipAddress
		typ = "integer"
	} else if typ == "YEAR" { // year
		typ = "SMALLINT"
	}

	if utils.StringVal(Column.Extra) != "" {
		if typ == "BIGINT" {
			typ = "BIGSERIAL"
		} else {
			typ = "SERIAL"
		}
	}

	// sql := fmt.Sprintf(
	// 	"%s SET DATA TYPE %s ",
	// 	quoter.ID(Column.Name, db), typ)

	nameQuoter := quoter.ID(Column.Name)
	sql := fmt.Sprintf(
		"%s %s",
		nameQuoter, typ)

	sql = strings.Trim(sql, " ")
	return sql
}

// SQLAlterIndex  return the add index sql for table alter
func (grammarSQL Hdb) SQLAlterIndex(index *dbal.Index) string {
	indexTypes := grammarSQL.IndexTypes
	quoter := grammarSQL.Quoter
	typ, has := indexTypes[index.Type]
	if !has {
		typ = "KEY"
	}

	// UNIQUE KEY `unionid` (`unionid`) COMMENT 'xxxx'
	columns := []string{}
	for _, Column := range index.Columns {
		columns = append(columns, quoter.ID(Column.Name))
	}

	name := quoter.ID(index.Name)
	sql := fmt.Sprintf(
		"CREATE %s %s ON %s (%s)",
		typ, name, quoter.ID(index.TableName), strings.Join(columns, ","))

	if typ == "PRIMARY KEY" {
		sql = fmt.Sprintf(
			"%s (%s) ",
			typ, strings.Join(columns, ","))
	}
	return sql
}

// GetIndexListing get a table indexes structure
func (grammarSQL Hdb) GetIndexListing(dbName string, tableName string) ([]*dbal.Index, error) {
	selectColumns := []string{
		`I.SCHEMA_NAME AS "db_name"`,
		`I.TABLE_NAME AS "table_name"`,
		`I.INDEX_NAME AS "index_name"`,
		`C.COLUMN_NAME AS "column_name"`,
		`'' as "collation"`,
		`'false' as "nullable"`,
		`CASE I.CONSTRAINT 
			WHEN 'NOT NULL UNIQUE' THEN 'true'
			WHEN 'PRIMARY KEY' THEN 'true'
			WHEN 'UNIQUE' THEN 'true'
			ELSE 'false'
		END "unique"`,
		`CASE I.CONSTRAINT 
			WHEN 'PRIMARY KEY' THEN 'true'
			ELSE 'false'
		END "primary"`,
		`'' as "comment"`,
		`'BTREE' as "index_type"`,
		`POSITION "seq_in_index"`,
		`'' as "index_comment"`,
	}
	sql := fmt.Sprintf(`
			SELECT 
				%s
			FROM
				INDEXES I ,
				INDEX_COLUMNS C
			WHERE
				C.SCHEMA_NAME = %s
				AND C.TABLE_NAME = %s
				AND I.INDEX_OID = C.INDEX_OID
			`,
		strings.Join(selectColumns, ","),
		grammarSQL.VAL(dbName),
		grammarSQL.VAL(tableName),
	)
	defer log.Debug(sql)
	indexes := []*dbal.Index{}
	err := grammarSQL.DB.Select(&indexes, sql)
	if err != nil {
		return nil, err
	}

	// counting the type of indexes
	for _, index := range indexes {
		if index.Primary {
			index.Type = "primary"
			index.Name = "PRIMARY"
		} else if index.Unique {
			index.Type = "unique"
		} else {
			index.Type = "index"
		}
		index.Name = strings.TrimPrefix(index.Name, tableName+"_")
	}
	return indexes, nil
}

// GetColumnListing get a table columns structure
func (grammarSQL Hdb) GetColumnListing(dbName string, tableName string) ([]*dbal.Column, error) {
	selectColumns := []string{
		"SCHEMA_NAME AS \"db_name\"",
		"TABLE_NAME AS \"table_name\"",
		"COLUMN_NAME AS \"name\"",
		"POSITION AS \"position\"",
		"DEFAULT_VALUE AS \"default\"",
		`CASE
			WHEN IS_NULLABLE = 'TRUE' THEN true
			WHEN IS_NULLABLE = 'FALSE' THEN false
			ELSE false
		END AS "nullable"`,
		`'false' AS "unsigned"`,
		`INDEX_TYPE AS "type_name"`,
		`UPPER(DATA_TYPE_NAME) AS "type"`,
		`LENGTH AS "length"`,
		`'0' AS "octet_length"`,
		`LENGTH AS "precision"`,
		`SCALE AS "scale"`,
		`'0' AS "datetime_precision"`,
		`'' AS "charset"`,
		`'' AS "collation"`,
		`'' AS "key"`,
		`'false' AS "primary"`,
		`CASE
			WHEN GENERATED_ALWAYS_AS = '' THEN 'auto_increment'
			ELSE ''
		 END AS "extra"`,
		`COMMENTS AS "comment"`,
	}
	sql := fmt.Sprintf(`
			SELECT %s
			FROM TABLE_COLUMNS
			WHERE  SCHEMA_NAME = %s AND TABLE_NAME = %s
			ORDER BY POSITION;
		`,
		strings.Join(selectColumns, ","),
		grammarSQL.VAL(dbName),
		grammarSQL.VAL(tableName),
	)
	defer log.Debug(sql)
	columns := []*dbal.Column{}
	err := grammarSQL.DB.Select(&columns, sql)
	if err != nil {
		return nil, err
	}

	// Cast the database data type to DBAL data type
	for _, column := range columns {
		typ, has := grammarSQL.FlipTypes[column.Type]
		if has {
			column.Type = typ
		}

		if column.Comment != nil {
			typ = grammarSQL.GetTypeFromComment(column.Comment)
			if typ != "" {
				column.Type = typ
			}
		}
		//转换默认值
		switch column.Default.(type) {
		case []uint8:
			{
				column.Default = string(column.Default.([]uint8))
			}
		}

		// user defined types
		if column.Type == "USER-DEFINED" {

			// enum options
			enumOptions := map[string][]string{}
			if strings.Contains(column.TypeName, "enum__") {
				column.Type = "enum"
				if _, has := enumOptions[column.TypeName]; !has {
					optionRange := []string{}
					err := grammarSQL.DB.Select(&optionRange, fmt.Sprintf("select enum_range(null::%s.%s)", dbName, column.TypeName))
					if err != nil {
						return nil, err
					}
					optionStr := strings.TrimPrefix(optionRange[0], "{")
					optionStr = strings.TrimRight(optionStr, "}")
					enumOptions[column.TypeName] = strings.Split(optionStr, ",")
				}
				column.Option = enumOptions[column.TypeName]
			}
		}

		if utils.StringVal(column.Extra) == "auto_increment" {
			column.Extra = utils.StringPtr("AutoIncrement")
		}
	}
	return columns, nil
}

// DropTable a table from the schema.
func (grammarSQL Hdb) DropTable(name string) error {
	sql := fmt.Sprintf("DROP TABLE %s CASCADE", grammarSQL.ID(name))
	defer log.Debug(sql)
	_, err := grammarSQL.DB.Exec(sql)
	return err
}

// DropTableIfExists if the table exists, drop it from the schema.
func (grammarSQL Hdb) DropTableIfExists(name string) error {
	sql := fmt.Sprintf("DROP TABLE %s CASCADE", grammarSQL.ID(name))
	defer log.Debug(sql)
	grammarSQL.DB.Exec(sql)
	return nil
}
