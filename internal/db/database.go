package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Database struct {
	RootPath string
	ActiveDB string
	Tables   map[string]*Table
}

func NewDatabase(root string) (*Database, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Database{
		RootPath: root,
		Tables:   make(map[string]*Table),
	}, nil
}

func (d *Database) CreateDatabase(name string) error {
	dbPath := filepath.Join(d.RootPath, name)
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		return fmt.Errorf("database '%s' already exists", name)
	}
	return os.MkdirAll(dbPath, 0o755)
}

func (d *Database) SetActiveDB(name string) error {
	dbPath := filepath.Join(d.RootPath, name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	d.ActiveDB = name
	d.Tables = make(map[string]*Table)

	files, err := os.ReadDir(dbPath)
	if err != nil {
		return err
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".tbl") {
			t, err := LoadTable(filepath.Join(dbPath, f.Name()))
			if err == nil {
				d.Tables[t.Name] = t
			}
		}
	}
	return nil
}

func (d *Database) CreateTable(name string, schema Schema) error {
	if d.ActiveDB == "" {
		return fmt.Errorf("no active database (use USE <db>)")
	}

	if _, exists := d.Tables[name]; exists {
		return fmt.Errorf("table '%s' already exists", name)
	}

	dbPath := filepath.Join(d.RootPath, d.ActiveDB)
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return err
	}

	tblPath := filepath.Join(dbPath, name+".tbl")
	t := &Table{
		Name:     name,
		Schema:   schema,
		FilePath: tblPath,
		Index:    NewBPTree(4),
	}

	d.Tables[name] = t
	return t.Save()
}

func (d *Database) Table(name string) (*Table, error) {
	t, ok := d.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table '%s' not found", name)
	}
	return t, nil
}

func (d *Database) ActivePath() (string, error) {
	if d.ActiveDB == "" {
		return "", fmt.Errorf("no active database")
	}
	return filepath.Join(d.RootPath, d.ActiveDB), nil
}

func (d *Database) GetTable(name string) (*Table, error) {
	t, ok := d.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table '%s' not found", name)
	}
	return t, nil
}

func (d *Database) ListDatabases() ([]string, error) {
	entries, err := os.ReadDir(d.RootPath)
	if err != nil {
		return nil, err
	}

	var databases []string
	for _, entry := range entries {
		if entry.IsDir() {
			databases = append(databases, entry.Name())
		}
	}
	return databases, nil
}

func (d *Database) ListTables() ([]string, error) {
	if d.ActiveDB == "" {
		return nil, fmt.Errorf("no database selected — use USE <database>")
	}

	var tables []string
	for tableName := range d.Tables {
		tables = append(tables, tableName)
	}
	return tables, nil
}

func (d *Database) DropDatabase(name string) error {
	dbPath := filepath.Join(d.RootPath, name)

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	if d.ActiveDB == name {
		return fmt.Errorf("cannot drop active database '%s' — use USE to switch first", name)
	}

	return os.RemoveAll(dbPath)
}

func (d *Database) DropTable(name string) error {
	if d.ActiveDB == "" {
		return fmt.Errorf("no database selected — use USE <database>")
	}

	t, exists := d.Tables[name]
	if !exists {
		return fmt.Errorf("table '%s' does not exist", name)
	}

	if err := os.Remove(t.FilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error deleting file: %v", err)
	}

	delete(d.Tables, name)

	return nil
}