package sql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
	"github.com/abmcmanu/go-mini-sqlite/internal/util"
)

type Statement interface {
	Exec(database *db.Database) error
}

func Parse(query string) (Statement, error) {
	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	switch {
	case strings.HasPrefix(queryUpper, "CREATE DATABASE"):
		return parseCreateDatabase(query)
	case strings.HasPrefix(queryUpper, "DROP DATABASE"):
		return parseDropDatabase(query)
	case strings.HasPrefix(queryUpper, "SHOW DATABASES"):
		return parseShowDatabases(query)
	case strings.HasPrefix(queryUpper, "SHOW TABLES"):
		return parseShowTables(query)
	case strings.HasPrefix(queryUpper, "DESCRIBE "), strings.HasPrefix(queryUpper, "DESC "):
		return parseDescribe(query)
	case strings.HasPrefix(queryUpper, "USE "):
		return parseUseDatabase(query)
	case strings.HasPrefix(queryUpper, "CREATE TABLE"):
		return parseCreateTable(query)
	case strings.HasPrefix(queryUpper, "DROP TABLE"):
		return parseDropTable(query)
	case strings.HasPrefix(queryUpper, "INSERT INTO"):
		return parseInsert(query)
	case strings.HasPrefix(queryUpper, "SELECT"):
		return parseSelect(query)
	case strings.HasPrefix(queryUpper, "UPDATE"):
		return parseUpdate(query)
	case strings.HasPrefix(queryUpper, "DELETE FROM"):
		return parseDelete(query)
	default:
		return nil, fmt.Errorf("unknown SQL command: %s", query)
	}
}

type CreateDatabaseStmt struct {
	Name string
}

func parseCreateDatabase(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+DATABASE\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid CREATE DATABASE syntax")
	}
	return &CreateDatabaseStmt{Name: m[1]}, nil
}

func (s *CreateDatabaseStmt) Exec(d *db.Database) error {
	return d.CreateDatabase(s.Name)
}

type DropDatabaseStmt struct {
	Name string
}

func parseDropDatabase(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)DROP\s+DATABASE\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid DROP DATABASE syntax")
	}
	return &DropDatabaseStmt{Name: m[1]}, nil
}

func (s *DropDatabaseStmt) Exec(d *db.Database) error {
	err := d.DropDatabase(s.Name)
	if err != nil {
		return err
	}
	fmt.Printf("Database '%s' deleted successfully.\n", s.Name)
	return nil
}

type ShowDatabasesStmt struct{}

func parseShowDatabases(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)SHOW\s+DATABASES;?`)
	if !re.MatchString(query) {
		return nil, errors.New("invalid SHOW DATABASES syntax")
	}
	return &ShowDatabasesStmt{}, nil
}

func (s *ShowDatabasesStmt) Exec(d *db.Database) error {
	databases, err := d.ListDatabases()
	if err != nil {
		return err
	}

	if len(databases) == 0 {
		fmt.Println("No databases found.")
		return nil
	}

	fmt.Println("Databases:")
	for _, dbName := range databases {
		fmt.Printf("  - %s\n", dbName)
	}
	return nil
}

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

type UseDatabaseStmt struct {
	Name string
}

func parseUseDatabase(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)USE\s+([a-zA-Z0-9_]+);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid USE syntax")
	}
	return &UseDatabaseStmt{Name: m[1]}, nil
}

func (s *UseDatabaseStmt) Exec(d *db.Database) error {
	return d.SetActiveDB(s.Name)
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

type InsertStmt struct {
	Table  string
	Cols   []string
	Values []string
}

func parseInsert(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+([a-zA-Z0-9_]+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\);?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 4 {
		return nil, errors.New("invalid INSERT syntax")
	}

	table := m[1]
	cols := splitAndTrim(m[2])
	vals := splitAndTrim(m[3])

	if len(cols) != len(vals) {
		return nil, errors.New("column count != value count")
	}
	return &InsertStmt{Table: table, Cols: cols, Values: vals}, nil
}

func (s *InsertStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("no database selected — use USE <database>")
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

type SelectStmt struct {
	Table            string
	Column           string
	Value            string
	HasWhere         bool
	OrderByColumn    string
	OrderByDirection string
	Limit            int
}

func parseSelect(query string) (Statement, error) {
	// SELECT * FROM table [WHERE col=val] [ORDER BY col [ASC|DESC]] [LIMIT n]
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^"]+)"?)?(?:\s+ORDER\s+BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+))?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid SELECT syntax")
	}

	stmt := &SelectStmt{Table: m[1]}

	if len(m) >= 4 && m[2] != "" {
		stmt.HasWhere = true
		stmt.Column = m[2]
		stmt.Value = m[3]
	}

	if len(m) >= 5 && m[4] != "" {
		stmt.OrderByColumn = m[4]
		if len(m) >= 6 && m[5] != "" {
			stmt.OrderByDirection = strings.ToUpper(m[5])
		} else {
			stmt.OrderByDirection = "ASC"
		}
	}

	if len(m) >= 7 && m[6] != "" {
		limit, err := parseNumber(m[6])
		if err != nil || limit < 0 {
			return nil, errors.New("LIMIT must be a positive number")
		}
		stmt.Limit = int(limit)
	}

	return stmt, nil
}

func (s *SelectStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("no database selected — use USE <database>")
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

	if s.OrderByColumn != "" {
		sortRows(rows, s.OrderByColumn, s.OrderByDirection)
	}

	if s.Limit > 0 && s.Limit < len(rows) {
		rows = rows[:s.Limit]
	}

	var cols []string
	for _, c := range t.Schema.Columns {
		cols = append(cols, c.Name)
	}

	util.PrintTable(cols, rows)
	return nil
}

type UpdateStmt struct {
	Table       string
	Updates     map[string]string
	WhereColumn string
	WhereValue  string
}

func parseUpdate(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^";]+)"?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 5 {
		return nil, errors.New("invalid UPDATE syntax (expected: UPDATE table SET col=val WHERE col=val)")
	}

	table := m[1]
	setPart := m[2]
	whereCol := m[3]
	whereVal := m[4]

	updates := make(map[string]string)
	assignments := splitAndTrim(setPart)
	for _, assign := range assignments {
		parts := strings.Split(assign, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid assignment: %s", assign)
		}
		colName := strings.TrimSpace(parts[0])
		colValue := strings.Trim(strings.TrimSpace(parts[1]), `"`)
		updates[colName] = colValue
	}

	return &UpdateStmt{
		Table:       table,
		Updates:     updates,
		WhereColumn: whereCol,
		WhereValue:  whereVal,
	}, nil
}

func (s *UpdateStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("no database selected — use USE <database>")
	}

	t, err := d.GetTable(s.Table)
	if err != nil {
		return err
	}

	count, err := t.Update(s.WhereColumn, s.WhereValue, s.Updates)
	if err != nil {
		return err
	}

	fmt.Printf("%d row(s) updated.\n", count)
	return nil
}

type DeleteStmt struct {
	Table       string
	WhereColumn string
	WhereValue  string
}

func parseDelete(query string) (Statement, error) {
	re := regexp.MustCompile(`(?i)DELETE\s+FROM\s+([a-zA-Z0-9_]+)\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^";]+)"?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 4 {
		return nil, errors.New("invalid DELETE syntax (expected: DELETE FROM table WHERE col=val)")
	}

	return &DeleteStmt{
		Table:       m[1],
		WhereColumn: m[2],
		WhereValue:  m[3],
	}, nil
}

func (s *DeleteStmt) Exec(d *db.Database) error {
	if d.ActiveDB == "" {
		return errors.New("no database selected — use USE <database>")
	}

	t, err := d.GetTable(s.Table)
	if err != nil {
		return err
	}

	count, err := t.Delete(s.WhereColumn, s.WhereValue)
	if err != nil {
		return err
	}

	fmt.Printf("%d row(s) deleted.\n", count)
	return nil
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// sortRows sorts rows by column in ASC or DESC direction
func sortRows(rows []map[string]string, column, direction string) {
	less := func(i, j int) bool {
		valI := rows[i][column]
		valJ := rows[j][column]

		numI, errI := parseNumber(valI)
		numJ, errJ := parseNumber(valJ)

		if errI == nil && errJ == nil {
			if direction == "DESC" {
				return numI > numJ
			}
			return numI < numJ
		}

		if direction == "DESC" {
			return valI > valJ
		}
		return valI < valJ
	}

	// Bubble sort
	for i := 0; i < len(rows)-1; i++ {
		for j := i + 1; j < len(rows); j++ {
			if !less(i, j) {
				rows[i], rows[j] = rows[j], rows[i]
			}
		}
	}
}

func parseNumber(s string) (float64, error) {
	var num float64
	_, err := fmt.Sscanf(s, "%f", &num)
	return num, err
}