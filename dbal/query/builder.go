package query

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/yaoapp/xun/dbal"

	_ "github.com/yaoapp/xun/grammar/mysql"    // Load the MySQL Grammar
	_ "github.com/yaoapp/xun/grammar/postgres" // Load the Postgres Grammar
	_ "github.com/yaoapp/xun/grammar/sqlite3"  // Load the SQLite3 Grammar
	_ "github.com/yaoapp/xun/grammar/saphdb"
)

// New create a new schema interface using the given driver and DSN
func New(driver string, dsn string) Query {
	builder := newBuilder(driver, dsn)
	return builder
}

// NewBuilder create a new schema interface using the given driver and DSN
func NewBuilder(driver string, dsn string) *Builder {
	return newBuilder(driver, dsn)
}

// Use create a new schema interface using the given connection
func Use(conn *Connection) Query {
	builder := useBuilder(conn)
	return builder
}

// UseBuilder create a new schema interface using the given connection
func UseBuilder(conn *Connection) *Builder {
	return useBuilder(conn)
}

// Driver Get the driver name
func (builder *Builder) Driver() (string, error) {
	if builder.Conn == nil {
		return "", fmt.Errorf("the connection is nil")
	}

	if builder.Conn.WriteConfig != nil {
		return builder.Conn.WriteConfig.Driver, nil
	}

	if builder.Conn.ReadConfig != nil {
		return builder.Conn.ReadConfig.Driver, nil
	}

	return "", fmt.Errorf("the connection is nil")
}

// Clone create a new builder instance with current builder
func (builder *Builder) Clone() Query {
	return builder.clone()
}

// Builder get the query builder instance
func (builder *Builder) Builder() *Builder {
	return builder
}

// New create a new builder instance with current builder
func (builder *Builder) New() Query {
	return builder.new()
}

// Reset Reset query
func (builder *Builder) Reset() Query {
	builder.Query = dbal.NewQuery()
	return builder
}

// NewBuilder create a new builder instance with current builder
func (builder *Builder) NewBuilder() *Builder {
	return builder.new()
}

// clone create a new builder instance with current builder
func (builder *Builder) clone() *Builder {
	new := builder.new()
	*new = *builder
	new.Query = builder.Query.Clone()
	return new
}

// new create a new builder instance
func (builder *Builder) new() *Builder {
	new := *builder
	new.Query = dbal.NewQuery()
	return &new
}

// newBuilder create a new schema builder interface using the given driver and DSN
func newBuilder(driver string, dsn string) *Builder {
	db, err := sqlx.Connect(driver, dsn)
	if err != nil {
		panic(err)
	}
	conn := &Connection{
		Write: db,
		WriteConfig: &dbal.Config{
			DSN:    dsn,
			Driver: driver,
			Name:   "primary",
		},
		Read: db,
		ReadConfig: &dbal.Config{
			DSN:      dsn,
			Driver:   driver,
			Name:     "secondary",
			ReadOnly: true,
		},
		Option: &dbal.Option{},
	}
	return useBuilder(conn)
}

// useBuilder create a new schema builder instance using the given connection
func useBuilder(conn *Connection) *Builder {
	grammar := newGrammar(conn)
	return &Builder{
		Mode:     "production",
		Conn:     conn,
		Grammar:  grammar,
		Database: grammar.GetDatabase(),
		Schema:   grammar.GetSchema(),
		Query:    dbal.NewQuery(),
	}
}

// newGrammar create a new grammar interface
func newGrammar(conn *Connection) dbal.Grammar {
	var driver string
	var err error
	if conn.WriteConfig != nil {
		driver = conn.WriteConfig.Driver
	} else if conn.ReadConfig != nil {
		driver = conn.ReadConfig.Driver
	}

	grammar, has := dbal.Grammars[driver]
	if !has {
		panic(fmt.Errorf("The %s driver not import", driver))
	}

	if conn.Write != nil {
		// create new grammar using the registered grammars ( read and write)
		grammar, err = grammar.NewWithRead(conn.Write, conn.WriteConfig, conn.Read, conn.ReadConfig, conn.Option)
		if err != nil {
			panic(fmt.Errorf("grammar setup error. (%s)", err))
		}

	} else if conn.Read != nil {
		// create new grammar using the registered grammars ( readonly )
		grammar, err = grammar.NewWith(conn.Read, conn.ReadConfig, conn.Option)
		if err != nil {
			panic(fmt.Errorf("grammar setup error. (%s)", err))
		}
	}

	err = grammar.OnConnected()
	if err != nil {
		panic(fmt.Errorf("the OnConnected event error. (%s)", err))
	}
	return grammar
}
