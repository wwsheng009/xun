package schema

// HasColumn Determine if the table has a given column.
func (table *Table) HasColumn(name ...string) bool {
	has := true
	for _, n := range name {
		_, has = table.ColumnMap[n]
		if !has {
			return has
		}
	}
	return has
}

// HasIndex Determine if the table has a given index.
func (table *Table) HasIndex(name ...string) bool {
	has := true
	for _, n := range name {
		_, has = table.IndexMap[n]
		if !has {
			return has
		}
	}
	return has
}

// GetName get the table name
func (table *Table) GetName() string {
	return table.Name
}

// AddColumn add or modify a column to the table
func (table *Table) AddColumn(column *Column) *Column {
	if table.HasColumn(column.Name) {
		table.ModifyColumnCommand(&column.Column)
		table.onChange("ModifyColumn", column)
		return column
	}
	table.AddColumnCommand(&column.Column)
	table.onChange("AddColumn", column)
	return column
}

// DropColumn Indicate that the given columns should be dropped.
func (table *Table) DropColumn(name ...string) {
	for _, n := range name {
		table.DropColumnCommand(n)
	}
	table.onChange("DropColumn", name)
}

// RenameColumn Indicate that the given column should be renamed.
func (table *Table) RenameColumn(old string, new string) *Column {
	table.RenameColumnCommand(old, new)
	column := table.GetColumn(old)
	column.Name = new
	table.onChange("RenameColumn", old, new)
	return column
}

// CreateIndex Indicate that the given index should be created.
func (table *Table) CreateIndex(key string, columnNames ...string) {
	columns := []*Column{}
	for _, name := range columnNames {
		columns = append(columns, table.GetColumn(name))
	}
	index := table.NewIndex(key, columns...)
	index.Type = "index"
	table.CreateIndexCommand(&index.Index)
	table.onChange("CreateIndex", index)
}

// CreateUnique Indicate that the given unique index should be created.
func (table *Table) CreateUnique(key string, columnNames ...string) {
	columns := []*Column{}
	for _, name := range columnNames {
		columns = append(columns, table.GetColumn(name))
	}
	index := table.NewIndex(key, columns...)
	index.Type = "unique"
	table.CreateIndexCommand(&index.Index)
	table.onChange("CreateIndex", index)
}

// CreatePrimary Indicate that the given column should be a primary index.
func (table *Table) CreatePrimary(columnName string) {
	column := table.GetColumn(columnName)
	column.Primary()
}

// DropPrimary Indicate that dropping the primary index
func (table *Table) DropPrimary() {
	if table.Primary != nil {
		if table.Primary != nil {
			table.DropIndex(table.Primary.Name + "_primary")
		}
	}
}

// DropIndex Indicate that the given indexes should be dropped.
func (table *Table) DropIndex(key ...string) {
	for _, n := range key {
		table.DropIndexCommand(n)
	}
	table.onChange("DropIndex", key)
}

// RenameIndex Indicate that the given indexes should be renamed.
func (table *Table) RenameIndex(old string, new string) *Index {
	table.RenameIndexCommand(old, new)
	index := table.GetIndex(old)
	index.Name = new
	table.onChange("RenameIndex", old, new)
	return index
}

// String Create a new string column on the table.
func (table *Table) String(name string, length int) *Column {
	column := table.NewColumn(name)
	column.Length = &length
	column.Type = "string"
	table.AddColumn(column)
	return column
}

// BigInteger Create a new auto-incrementing big integer (8-byte) column on the table.
func (table *Table) BigInteger(name string) *Column {
	column := table.NewColumn(name)
	column.Type = "bigInteger"
	table.AddColumn(column)
	return column
}

// UnsignedBigInteger Create a new unsigned big integer (8-byte) column on the table.
func (table *Table) UnsignedBigInteger(name string) *Column {
	return table.BigInteger(name).Unsigned()
}

// BigIncrements Create a new auto-incrementing big integer (8-byte) column on the table.
func (table *Table) BigIncrements(name string) *Column {
	return table.UnsignedBigInteger(name).AutoIncrement()
}

// ID Alias BigIncrements. Create a new auto-incrementing big integer (8-byte) column on the table.
func (table *Table) ID(name string) *Column {
	return table.BigIncrements(name).Primary()
}
