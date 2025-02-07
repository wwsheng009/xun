package postgres

import (
	"fmt"
	"strings"

	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/utils"
)

// SQLAddColumn return the add column sql for table create
func (grammarSQL Postgres) SQLAddColumn(column *dbal.Column) string {
	types := grammarSQL.Types
	quoter := grammarSQL.Quoter

	// `id` bigint(20) unsigned NOT NULL,
	typ, has := types[column.Type]
	if !has {
		typ = "VARCHAR"
	}

	decimalTypes := []string{"DECIMAL", "FLOAT", "NUMBERIC", "DOUBLE"}

	if column.Precision != nil && column.Scale != nil && utils.StringHave(decimalTypes, typ) {
		typ = fmt.Sprintf("%s(%d,%d)", typ, utils.IntVal(column.Precision), utils.IntVal(column.Scale))
	} else if strings.Contains(typ, "TIMESTAMP(%d)") || strings.Contains(typ, "TIME(%d)") {
		DateTimePrecision := utils.IntVal(column.DateTimePrecision, 0)
		typ = fmt.Sprintf(typ, DateTimePrecision)
	} else if typ == "BYTEA" {
		typ = "BYTEA"
	} else if typ == "ENUM" {
		typ = strings.ToLower("ENUM__" + strings.Join(column.Option, "_EOPT_"))
	} else if column.Length != nil {
		typ = fmt.Sprintf("%s(%d)", typ, utils.IntVal(column.Length))
	}

	unsigned := ""
	nullable := utils.GetIF(column.Nullable, "NULL", "NOT NULL").(string)

	defaultValue := grammarSQL.GetDefaultValue(column)
	// comment := utils.GetIF(utils.StringVal(column.Comment) != "", fmt.Sprintf("COMMENT %s", quoter.VAL(column.Comment)), "").(string)
	collation := utils.GetIF(utils.StringVal(column.Collation) != "", fmt.Sprintf("COLLATE %s", utils.StringVal(column.Collation)), "").(string)
	extra := ""
	if utils.StringVal(column.Extra) != "" {
		if typ == "BIGINT" {
			typ = "BIGSERIAL"
		} else if typ == "SMALLINT" {
			typ = "SMALLSERIAL"
		} else {
			typ = "SERIAL"
		}
		nullable = ""
		defaultValue = ""
	}

	if typ == "IPADDRESS" { // ipAddress
		typ = "integer"
	} else if typ == "YEAR" { // 2021 -1046 smallInt (2-byte)
		typ = "SMALLINT"
	}

	sql := fmt.Sprintf(
		"%s %s %s %s %s %s %s",
		quoter.ID(column.Name), typ, unsigned, nullable, defaultValue, extra, collation)

	sql = strings.Trim(sql, " ")
	return sql
}

// SQLAddComment return the add comment sql for table create
func (grammarSQL Postgres) SQLAddComment(column *dbal.Column) string {
	comment := utils.GetIF(
		utils.StringVal(column.Comment) != "",
		fmt.Sprintf(
			"COMMENT on column %s.%s is %s;",
			grammarSQL.ID(column.TableName),
			grammarSQL.ID(column.Name),
			grammarSQL.VAL(column.Comment),
		), "").(string)

	mappingTypes := []string{"ipAddress", "year", "vector"}
	if utils.StringHave(mappingTypes, column.Type) {
		comment = fmt.Sprintf("COMMENT on column %s.%s is %s;",
			grammarSQL.ID(column.TableName),
			grammarSQL.ID(column.Name),
			grammarSQL.VAL(fmt.Sprintf("T:%s|%s", column.Type, utils.StringVal(column.Comment))),
		)
	}
	return comment
}

// SQLAddIndex  return the add index sql for table create
func (grammarSQL Postgres) SQLAddIndex(index *dbal.Index) string {
	quoter := grammarSQL.Quoter
	indexTypes := grammarSQL.IndexTypes
	typ, has := indexTypes[index.Type]
	if !has {
		typ = "KEY"
	}

	// UNIQUE KEY `unionid` (`unionid`) COMMENT 'xxxx'
	// IS JSON
	columns := []string{}
	isJSON := false

	sql := ""
	name := quoter.ID(fmt.Sprintf("%s_%s", index.TableName, index.Name))

	for _, column := range index.Columns {
		columns = append(columns, quoter.ID(column.Name))
		if column.Type == "json" || column.Type == "jsonb" {
			isJSON = true
		} else if column.Type == "vector" {
			// use pgvector/pgvector cannot support 2000+ dim index for vector type
			// if *column.Length > 2000 {
			// 	sql += fmt.Sprintf(
			// 		"CREATE INDEX ON %s USING hnsw ((binary_quantize(%s)::bit(%d)) bit_hamming_ops);\n",
			// 		quoter.ID(index.TableName), quoter.ID(column.Name), *column.Length)
			// } else {

			// get all the index methods from pg for extension pgvecto.rs
			// SELECT am.amname AS index_method,
			// 	opc.opcname AS opclass_name,
			// 	opc.opcintype::regtype AS indexed_type,
			// 	opc.opcdefault AS is_default
			// 	FROM pg_am am, pg_opclass opc
			// 	WHERE opc.opcmethod = am.oid AND am.amname = 'vectors'
			// 	ORDER BY index_method, opclass_name;

			// use tensorchord/pgvecto.rs as extension
			sql += fmt.Sprintf(
				"CREATE INDEX %s ON %s USING vectors (%s vector_cos_ops) WITH (options='[indexing.hnsw]\nm = 16\nef_construction = 200');\n",
				name, quoter.ID(index.TableName), quoter.ID(column.Name))
			// }

		} else if column.Type == "vecf16" {
			// use tensorchord/pgvecto.rs as extension
			sql += fmt.Sprintf(
				"CREATE INDEX %s ON %s USING vectors (%s vecf16_cos_ops) WITH (options='[indexing.hnsw]\nm = 16\nef_construction = 200');\n",
				name, quoter.ID(index.TableName), quoter.ID(column.Name))
		} else if column.Type == "halfvec" {
			sql += fmt.Sprintf(
				"CREATE INDEX ON %s USING hnsw (%s halfvec_cosine_ops);\n",
				quoter.ID(index.TableName), quoter.ID(column.Name))
		}
	}
	if sql != "" {
		return sql
	}
	if isJSON {
		return ""
	}

	comment := ""
	if index.Comment != nil {
		comment = fmt.Sprintf("COMMENT %s", quoter.VAL(index.Comment))
	}

	if typ == "PRIMARY KEY" {
		sql += fmt.Sprintf(
			"%s (%s) %s",
			typ, strings.Join(columns, ","), comment)
	} else {
		sql += fmt.Sprintf(
			"CREATE %s %s ON %s (%s)",
			typ, name, quoter.ID(index.TableName), strings.Join(columns, ","))
	}
	return sql
}

// SQLAddPrimary return the add primary key sql for table create
func (grammarSQL Postgres) SQLAddPrimary(primary *dbal.Primary) string {
	quoter := grammarSQL.Quoter

	// PRIMARY KEY `unionid` (`unionid`) COMMENT 'xxxx'
	columns := []string{}
	for _, column := range primary.Columns {
		columns = append(columns, quoter.ID(column.Name))
	}

	sql := fmt.Sprintf(
		// "CONSTRAINT %s PRIMARY KEY (%s)",
		"PRIMARY KEY (%s)",
		// grammarSQL.ID(primary.Name),
		strings.Join(columns, ","))

	return sql
}
