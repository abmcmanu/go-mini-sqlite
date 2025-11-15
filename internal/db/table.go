package db

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

type ColType string

const (
	TypeInt    ColType = "INT"
	TypeString ColType = "STRING"
)

type Column struct {
	Name       string
	Type       ColType
	PrimaryKey bool
	NotNull    bool
	Unique     bool
}

type Schema struct {
	Columns []Column
}

type Table struct {
	Name     string
	Schema   Schema
	FilePath string
	Index    *BPTree
}

func (t *Table) Save() error {
	if err := os.MkdirAll(filepath.Dir(t.FilePath), 0755); err != nil {
		return err
	}
	f, err := os.Create(t.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	data := struct {
		Name   string
		Schema Schema
		Rows   []map[string]string
	}{
		Name:   t.Name,
		Schema: t.Schema,
		Rows:   t.Index.GetAll(),
	}
	return enc.Encode(&data)
}

func LoadTable(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var data struct {
		Name   string
		Schema Schema
		Rows   []map[string]string
	}

	dec := gob.NewDecoder(f)
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}

	table := &Table{
		Name:     data.Name,
		Schema:   data.Schema,
		FilePath: path,
		Index:    NewBPTree(3),
	}

	for _, row := range data.Rows {
		pk := row[table.PrimaryKey()]
		table.Index.Insert(pk, row)
	}

	return table, nil
}

func (t *Table) PrimaryKey() string {
	for _, c := range t.Schema.Columns {
		if c.PrimaryKey {
			return c.Name
		}
	}
	return ""
}

func (t *Table) Insert(values map[string]string) error {
	cols := t.Schema.ColumnsMap()
	for name, col := range cols {
		v := values[name]
		if col.NotNull && v == "" {
			return fmt.Errorf("column '%s' cannot be NULL", name)
		}
		if col.Unique {
			rows, _ := t.SelectWhere(name, v)
			if len(rows) > 0 {
				return fmt.Errorf("value '%s' already exists for UNIQUE column '%s'", v, name)
			}
		}
	}

	pkName := t.PrimaryKey()
	pkVal := values[pkName]
	if pkVal == "" {
		pkVal = fmt.Sprintf("%d", len(t.Index.GetAll())+1)
		values[pkName] = pkVal
	}

	t.Index.Insert(pkVal, values)

	return t.Save()
}

func (t *Table) SelectWhere(column, value string) ([]map[string]string, error) {
	all := t.Index.GetAll()
	var results []map[string]string
	for _, row := range all {
		if row[column] == value {
			results = append(results, row)
		}
	}
	return results, nil
}

func (t *Table) SelectAll() []map[string]string {
	return t.Index.GetAll()
}

func (t *Table) Update(whereColumn, whereValue string, updates map[string]string) (int, error) {
	rows, err := t.SelectWhere(whereColumn, whereValue)
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 {
		return 0, nil
	}

	cols := t.Schema.ColumnsMap()
	pkName := t.PrimaryKey()

	for colName, newVal := range updates {
		col, exists := cols[colName]
		if !exists {
			return 0, fmt.Errorf("column '%s' does not exist", colName)
		}

		if colName == pkName {
			return 0, fmt.Errorf("cannot modify primary key '%s'", pkName)
		}

		if col.NotNull && newVal == "" {
			return 0, fmt.Errorf("column '%s' cannot be NULL", colName)
		}

		if col.Unique && newVal != "" {
			existing, _ := t.SelectWhere(colName, newVal)
			for _, ex := range existing {
				isCurrentRow := false
				for _, row := range rows {
					if ex[pkName] == row[pkName] {
						isCurrentRow = true
						break
					}
				}
				if !isCurrentRow {
					return 0, fmt.Errorf("value '%s' already exists for UNIQUE column '%s'", newVal, colName)
				}
			}
		}
	}

	count := 0
	for _, row := range rows {
		pk := row[pkName]
		currentRow, found := t.Index.Get(pk)
		if !found {
			continue
		}

		for colName, newVal := range updates {
			currentRow[colName] = newVal
		}

		t.Index.Insert(pk, currentRow)
		count++
	}

	if err := t.Save(); err != nil {
		return 0, err
	}

	return count, nil
}

func (t *Table) Delete(whereColumn, whereValue string) (int, error) {
	rows, err := t.SelectWhere(whereColumn, whereValue)
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 {
		return 0, nil
	}

	pkName := t.PrimaryKey()
	count := 0

	for _, row := range rows {
		pk := row[pkName]
		t.Index.Delete(pk)
		count++
	}

	if err := t.Save(); err != nil {
		return 0, err
	}

	return count, nil
}

// ColumnsMap returns a map of column names to Column for fast lookup
func (s Schema) ColumnsMap() map[string]Column {
	m := make(map[string]Column)
	for _, c := range s.Columns {
		m[c.Name] = c
	}
	return m
}

func convertSchema(cols []Column) Schema {
	return Schema{Columns: cols}
}