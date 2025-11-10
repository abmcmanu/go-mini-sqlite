package sql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
	"github.com/abmcmanu/go-mini-sqlite/internal/util"
)

// Statement représente une commande SQL analysée et prête à exécuter
type Statement interface {
	Exec(database *db.Database) error
}

// Parse analyse la commande SQL et retourne un Statement prêt à exécuter
func Parse(query string) (Statement, error) {
	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	switch {
	case strings.HasPrefix(queryUpper, "CREATE DATABASE"):
		return parseCreateDatabase(query)
	case strings.HasPrefix(queryUpper, "USE "):
		return parseUseDatabase(query)
	case strings.HasPrefix(queryUpper, "CREATE TABLE"):
		return parseCreateTable(query)
	case strings.HasPrefix(queryUpper, "INSERT INTO"):
		return parseInsert(query)
	case strings.HasPrefix(queryUpper, "SELECT"):
		return parseSelect(query)
	default:
		return nil, fmt.Errorf("commande SQL non reconnue: %s", query)
	}
}

// ─── CREATE DATABASE ───────────────────────────────────────────────────────────
type CreateDatabaseStmt struct {
	Name string
}

func parseCreateDatabase(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+DATABASE\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("syntaxe CREATE DATABASE invalide")
	}
	return &CreateDatabaseStmt{Name: m[1]}, nil
}

func (s *CreateDatabaseStmt) Exec(d *db.Database) error {
	return d.CreateDatabase(s.Name)
}

// ─── USE DATABASE ──────────────────────────────────────────────────────────────
type UseDatabaseStmt struct {
	Name string
}

func parseUseDatabase(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)USE\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("syntaxe USE invalide")
	}
	return &UseDatabaseStmt{Name: m[1]}, nil
}

func (s *UseDatabaseStmt) Exec(d *db.Database) error {
	return d.SetActiveDB(s.Name)
}

// ─── CREATE TABLE ──────────────────────────────────────────────────────────────
type CreateTableStmt struct {
	Name    string
	Columns []db.Column
}

func parseCreateTable(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+([a-zA-Z0-9_]+)\s*\((.+)\);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 3 {
		return nil, errors.New("syntaxe CREATE TABLE invalide")
	}

	name := m[1]
	colDefs := strings.Split(m[2], ",")
	var cols []db.Column
	for _, def := range colDefs {
		def = strings.TrimSpace(def)
		parts := strings.Fields(def)
		if len(parts) < 2 {
			return nil, fmt.Errorf("colonne invalide: %s", def)
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
		return errors.New("aucune base sélectionnée — utilisez USE <database>")
	}
	return d.CreateTable(s.Name, db.Schema{Columns: s.Columns})
}

// ─── INSERT INTO ───────────────────────────────────────────────────────────────
type InsertStmt struct {
	Table  string
	Cols   []string
	Values []string
}

func parseInsert(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([a-zA-Z0-9_]+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 4 {
		return nil, errors.New("syntaxe INSERT invalide")
	}

	table := m[1]
	cols := splitAndTrim(m[2])
	vals := splitAndTrim(m[3])

	if len(cols) != len(vals) {
		return nil, errors.New("nombre de colonnes ≠ nombre de valeurs")
	}
	return &InsertStmt{Table: table, Cols: cols, Values: vals}, nil
}

func (s *InsertStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("aucune base sélectionnée — utilisez USE <database>")
	}

	t, err := d.GetTable(s.Table)
	if err != nil {
		return err
	}

	row := make(map[string]string)
	for i, col := range s.Cols {
		row[col] = strings.Trim(s.Values[i], `"`)
	}

	return t.Insert(row)
}

// ─── SELECT ─────────────────────────────────────────────────────────────────────
type SelectStmt struct {
	Table    string
	Column   string
	Value    string
	HasWhere bool
}

func parseSelect(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^"]+)"?)?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("syntaxe SELECT invalide")
	}

	stmt := &SelectStmt{Table: m[1]}
	if len(m) >= 4 && m[2] != "" {
		stmt.HasWhere = true
		stmt.Column = m[2]
		stmt.Value = m[3]
	}
	return stmt, nil
}

func (s *SelectStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("aucune base sélectionnée — utilisez USE <database>")
	}

	t, err := d.GetTable(s.Table)
	if err != nil {
		return err
	}

	var rows []map[string]string
	if s.HasWhere {
		rows, err = t.SelectWhere(s.Column, s.Value)
	} else {
		rows = t.SelectAll()
	}
	if err != nil {
		return err
	}

	// Affiche le tableau avec colonnes dans l’ordre du schéma
	var cols []string
	for _, c := range t.Schema.Columns {
		cols = append(cols, c.Name)
	}

	util.PrintTable(cols, rows)
	return nil
}

// ─── Helpers ───────────────────────────────────────────────────────────────────
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
