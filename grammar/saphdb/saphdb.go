package saphdb

import (
	"fmt"
	"net/url"
	"path/filepath"

	_ "github.com/SAP/go-hdb/driver"
	"github.com/jmoiron/sqlx"
	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/grammar/sql"
	"github.com/yaoapp/xun/utils"
)

type Hdb struct {
	sql.SQL
}

func init() {
	dbal.Register("hdb", New())
}

// setup the method will be executed when db server was connected
func (grammarSQL *Hdb) setup(db *sqlx.DB, config *dbal.Config, option *dbal.Option) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	if config == nil {
		return fmt.Errorf("config is nil")
	}

	grammarSQL.DB = db
	grammarSQL.Config = config
	grammarSQL.Option = option

	uinfo, err := url.Parse(grammarSQL.Config.DSN)
	if err != nil {
		return err
	}
	grammarSQL.DatabaseName = filepath.Base(uinfo.Path)
	schema := uinfo.Query().Get("defaultSchema")
	if schema == "" {
		schema = "PUBLIC"
	}
	grammarSQL.SchemaName = schema
	return nil
}

// NewWith Create a new grammar interface, using the given *sqlx.DB, *dbal.Config and *dbal.Option.
func (grammarSQL Hdb) NewWith(db *sqlx.DB, config *dbal.Config, option *dbal.Option) (dbal.Grammar, error) {
	err := grammarSQL.setup(db, config, option)
	if err != nil {
		return nil, err
	}
	grammarSQL.Quoter.Bind(db, option.Prefix)
	return grammarSQL, nil
}

// NewWithRead Create a new grammar interface, using the given *sqlx.DB, *dbal.Config and *dbal.Option.
func (grammarSQL Hdb) NewWithRead(write *sqlx.DB, writeConfig *dbal.Config, read *sqlx.DB, readConfig *dbal.Config, option *dbal.Option) (dbal.Grammar, error) {
	err := grammarSQL.setup(write, writeConfig, option)
	if err != nil {
		return nil, err
	}

	grammarSQL.Read = read
	grammarSQL.ReadConfig = readConfig
	grammarSQL.Quoter.Bind(write, option.Prefix, read)
	return grammarSQL, nil
}

func New() dbal.Grammar {
	hdb := Hdb{
		SQL: sql.NewSQL(&Quoter{}),
	}
	hdb.Driver = "hdb"
	hdb.IndexTypes = map[string]string{
		"unique": "UNIQUE INDEX",
		"index":  "INDEX",
	}
	// overwrite types
	types := hdb.SQL.Types
	types["tinyInteger"] = "SMALLINT"
	types["bigInteger"] = "BIGINT"
	types["string"] = "NVARCHAR"
	types["integer"] = "INTEGER"
	types["decimal"] = "DECIMAL"
	types["float"] = "FLOAT"
	types["double"] = "DOUBLE"
	types["char"] = "VARCHAR"
	types["mediumText"] = "NVARCHAR(5000)"
	types["longText"] = "NVARCHAR(5000)"
	types["dateTime"] = "SECONDDATE"
	types["dateTimeTz"] = "TIMESTAMP"
	types["enum"] = "ENUM"
	types["time"] = "TIME"
	types["timeTz"] = "TIMESTAMP"
	types["timestamp"] = "SECONDDATE"
	types["timestampTz"] = "TIMESTAMP"
	types["binary"] = "VARBINARY"
	types["macAddress"] = "NVARCHAR(100)"
	hdb.Types = types

	// set fliptypes
	flipTypes, ok := utils.MapFilp(hdb.Types)
	if ok {
		hdb.FlipTypes = flipTypes.(map[string]string)
		hdb.FlipTypes["TEXT"] = "text"
		hdb.FlipTypes["TTIMESTAMP"] = "timestamp"
		hdb.FlipTypes["TIME"] = "time"
		hdb.FlipTypes["SMALLINT"] = "smallInteger"
	}

	return hdb
}

// GetOperators get the operators
func (grammarSQL Hdb) GetOperators() []string {
	return []string{
		"=", "<", ">", "<=", ">=", "<>", "!=",
		"like", "not like", "between", "ilike", "not ilike",
		"~", "&", "|", "#", "<<", ">>", "<<=", ">>=",
		"&&", "@>", "<@", "?", "?|", "?&", "||", "-", "@?", "@@", "#-",
		"is distinct from", "is not distinct from",
	}
}
