package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yaoapp/xun"
	"github.com/yaoapp/xun/dbal"
	"github.com/yaoapp/xun/dbal/schema"
	"github.com/yaoapp/xun/unit"
)

func TestSelectSelectColumns(t *testing.T) {
	NewTableFoSelectTest()
	qb := getTestBuilder()
	qb.From("table_test_select as t").
		Where("email", "like", "%@yao.run").
		Select("id", "t.email as wid", "t.cate as category").
		OrderBy("id")
	checktestSelectSelect(t, qb)
}

func TestSelectSelectColumnsWithComma(t *testing.T) {
	NewTableFoSelectTest()
	qb := getTestBuilder()
	qb.From("table_test_select as t").
		Where("email", "like", "%@yao.run").
		Select("id, t.email as wid, t.cate as category").
		OrderBy("id")

	checktestSelectSelect(t, qb)
}

func TestSelectSelectColumnsMixed(t *testing.T) {
	NewTableFoSelectTest()
	qb := getTestBuilder()
	qb.From("table_test_select as t").
		Where("email", "like", "%@yao.run").
		Select("id", "t.email as wid, t.cate as category").
		OrderBy("id")

	checktestSelectSelect(t, qb)
}

func TestSelectSelectRaw(t *testing.T) {
	NewTableFoSelectTest()
	qb := getTestBuilder()
	if unit.DriverIs("postgres") {
		qb.From("table_test_select as t").
			Where("email", "like", "%@yao.run").
			Select("id", "t.email as wid").
			SelectRaw(`"t"."cate" as "category"`).
			OrderBy("id")
	} else {
		qb.From("table_test_select as t").
			Where("email", "like", "%@yao.run").
			Select("id", "t.email as wid").
			SelectRaw("`t`.`cate` as `category`").
			OrderBy("id")
	}
	checktestSelectSelect(t, qb)
}

func TestSelectSelectColumnsMixRaw(t *testing.T) {
	NewTableFoSelectTest()
	qb := getTestBuilder()
	if unit.DriverIs("postgres") {
		qb.From("table_test_select as t").
			Where("email", "like", "%@yao.run").
			Select(dbal.Raw(`"id"`), "t.email as wid, t.cate as category").
			OrderBy("id")
	} else {
		qb.From("table_test_select as t").
			Where("email", "like", "%@yao.run").
			Select(dbal.Raw("`id`"), "t.email as wid, t.cate as category").
			OrderBy("id")
	}
	checktestSelectSelect(t, qb)
}

func TestSelectSelectSub(t *testing.T) {
	NewTableFoSelectTest()
	qb := getTestBuilder()
	if unit.DriverIs("postgres") {
		qb.From("table_test_select as t").
			Where("email", "like", "%@yao.run").
			Select("id").
			SelectSub(func(qb Query) {
				qb.SelectRaw("'cate'")
			}, "category").
			OrderBy("id")
	} else {
		qb.From("table_test_select as t").
			Where("email", "like", "%@yao.run").
			Select("id").
			SelectSub(func(qb Query) {
				qb.SelectRaw("?", "cate")
			}, "category").
			OrderBy("id")
	}

	// select "id", (select 'cate') as "category" from "table_test_select" as "t" where "email" like $1 order by "id" asc
	// select `id`, (select ?) as `category` from `table_test_select` as `t` where `email` like ? order by `id` asc
	// fmt.Println(qb.ToSQL())
	// utils.Println(qb.GetBindings())
	// utils.Println(qb.MustGet())

	checktestSelectSelectSub(t, qb)
}

// clean the test data
func TestSelectClean(t *testing.T) {
	builder := getTestSchemaBuilder()
	builder.DropTableIfExists("table_test_select")
}

func NewTableFoSelectTest() {
	defer unit.Catch()
	builder := getTestSchemaBuilder()
	builder.DropTableIfExists("table_test_select")
	builder.MustCreateTable("table_test_select", func(table schema.Blueprint) {
		table.ID("id")
		table.String("email")
		table.String("name")
		table.String("cate")
		table.Integer("vote")
		table.Float("score", 5, 2)
		table.Enum("status", []string{"WAITING", "PENDING", "DONE"}).SetDefault("WAITING")
		table.Timestamps()
		table.SoftDeletes()
	})

	qb := getTestBuilder()
	qb.Table("table_test_select").Insert([]xun.R{
		{"email": "john@yao.run", "cate": "cat", "name": "John", "vote": 8, "score": 96.32, "status": "WAITING", "created_at": "2021-03-25 00:21:16"},
		{"email": "lee@yao.run", "cate": "dog", "name": "Lee", "vote": 5, "score": 64.56, "status": "PENDING", "created_at": "2021-03-25 08:30:15"},
		{"email": "ken@yao.run", "cate": "dog", "name": "Ken", "vote": 5, "score": 99.27, "status": "DONE", "created_at": "2021-03-25 09:40:23"},
		{"email": "ben@yao.run", "cate": "cat", "name": "Ben", "vote": 6, "score": 48.12, "status": "DONE", "created_at": "2021-03-25 18:15:29"},
	})
}

func checktestSelectSelect(t *testing.T, qb Query) {
	// checking sql
	sql := qb.ToSQL()
	if unit.DriverIs("postgres") {
		assert.Equal(t, `select "id", "t"."email" as "wid", "t"."cate" as "category" from "table_test_select" as "t" where "email" like $1 order by "id" asc`, sql, "the query sql not equal")
	} else {
		assert.Equal(t, "select `id`, `t`.`email` as `wid`, `t`.`cate` as `category` from `table_test_select` as `t` where `email` like ? order by `id` asc", sql, "the query sql not equal")
	}

	// checking result
	rows := qb.MustGet()
	assert.Equal(t, 4, len(rows), "the return value should be have 4 rows")
	if len(rows) == 4 {
		assert.Equal(t, int64(1), rows[0]["id"].(int64), "the id of first row should be 1")
		assert.Equal(t, "john@yao.run", rows[0]["wid"].(string), "the wid of first row should be john@yao.run")
		assert.Equal(t, "cat", rows[0]["category"].(string), "the category of first row should be 1")
	}
}

func checktestSelectSelectSub(t *testing.T, qb Query) {

	// select "id", (select 'cate') as "category" from "table_test_select" as "t" where "email" like $1 order by "id" asc
	// select `id`, (select ?) as `category` from `table_test_select` as `t` where `email` like ? order by `id` asc

	// checking sql
	sql := qb.ToSQL()
	if unit.DriverIs("postgres") {
		assert.Equal(t, `select "id", (select 'cate') as "category" from "table_test_select" as "t" where "email" like $1 order by "id" asc`, sql, "the query sql not equal")
	} else {
		assert.Equal(t, "select `id`, (select ?) as `category` from `table_test_select` as `t` where `email` like ? order by `id` asc", sql, "the query sql not equal")
	}

	// checking result
	rows := qb.MustGet()
	assert.Equal(t, 4, len(rows), "the return value should be have 4 rows")
	if len(rows) == 4 {
		assert.Equal(t, int64(1), rows[0]["id"].(int64), "the id of first row should be 1")
		assert.Equal(t, "cate", rows[0]["category"].(string), "the category of first row should be 1")
	}
}