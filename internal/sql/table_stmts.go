package sql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
	"github.com/abmcmanu/go-mini-sqlite/internal/util"
)

type ShowTablesStmt struct{}

func parseShowTables(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)SHOW\s+TABLES;?`)
	if !re.MatchString(query) {
		return nil, errors.New("invalid SHOW TABLES syntax")
	}
	return &ShowTablesStmt{}, nil
}

func (s *ShowTablesStmt) Exec(d *db.Database) error {
	tables, err := d.ListTables()
	if err != nil {
		return err
	}

	var rows []map[string]string
	for _, tableName := range tables {
		rows = append(rows, map[string]string{
			"Table": tableName,
		})
	}

	columns := []string{"Table"}
	util.PrintTable(columns, rows)
	return nil
}

type DescribeStmt struct {
	Table string
}

func parseDescribe(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)(?:DESCRIBE|DESC)\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid DESCRIBE syntax")
	}
	return &DescribeStmt{Table: m[1]}, nil
}

func (s *DescribeStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("no database selected — use USE <database>")
	}

	t, err := d.GetTable(s.Table)
	if err != nil {
		return err
	}

	var rows []map[string]string
	for _, col := range t.Schema.Columns {
		row := map[string]string{
			"Field": col.Name,
			"Type":  string(col.Type),
			"Key":   "",
			"Null":  "YES",
			"Extra": "",
		}

		if col.PrimaryKey {
			row["Key"] = "PRI"
		}

		if col.NotNull {
			row["Null"] = "NO"
		}

		var extras []string
		if col.Unique {
			extras = append(extras, "UNIQUE")
		}
		if len(extras) > 0 {
			row["Extra"] = strings.Join(extras, ", ")
		}

		rows = append(rows, row)
	}

	columns := []string{"Field", "Type", "Key", "Null", "Extra"}
	util.PrintTable(columns, rows)
	return nil
}

type CreateTableStmt struct {
	Name    string
	Columns []db.Column
}

func parseCreateTable(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s*\((.+)\);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 3 {
		return nil, errors.New("invalid CREATE TABLE syntax")
	}

	name := m[1]
	colDefs := strings.Split(m[2], ",")
	var cols []db.Column
	for _, def := range colDefs {
		def = strings.TrimSpace(def)
		parts := strings.Fields(def)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column: %s", def)
		}

		col := db.Column{Name: parts[0], Type: db.ColType(strings.ToUpper(parts[1]))}
		defUpper := strings.ToUpper(def)

		if strings.Contains(defUpper, "PRIMARY KEY") {
			col.PrimaryKey = true
		}
		if strings.Contains(defUpper, "NOT NULL") {
			col.NotNull = true
		}
		if strings.Contains(defUpper, "UNIQUE") {
			col.Unique = true
		}
		cols = append(cols, col)
	}
	return &CreateTableStmt{Name: name, Columns: cols}, nil
}

func (s *CreateTableStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("no database selected — use USE <database>")
	}
	return d.CreateTable(s.Name, db.Schema{Columns: s.Columns})
}

type DropTableStmt struct {
	Name string
}

func parseDropTable(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)DROP\s+TABLE\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid DROP TABLE syntax")
	}
	return &DropTableStmt{Name: m[1]}, nil
}

func (s *DropTableStmt) Exec(d *db.Database) error {
	err := d.DropTable(s.Name)
	if err != nil {
		return err
	}
	fmt.Printf("Table '%s' deleted successfully.\n", s.Name)
	return nil
}