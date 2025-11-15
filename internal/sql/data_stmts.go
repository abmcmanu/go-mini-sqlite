package sql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
	"github.com/abmcmanu/go-mini-sqlite/internal/util"
)

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
	AggregateFunc    string // COUNT, SUM, AVG
	AggregateColumn  string // column name or "*" for COUNT(*)
}

func parseSelect(query string) (Statement, error) {
	queryUpper := strings.ToUpper(query)

	// Check for aggregate functions: COUNT, SUM, AVG
	if strings.Contains(queryUpper, "COUNT(") || strings.Contains(queryUpper, "SUM(") || strings.Contains(queryUpper, "AVG(") {
		// SELECT COUNT(*) FROM table [WHERE col=val]
		// SELECT SUM(column) FROM table [WHERE col=val]
		// SELECT AVG(column) FROM table [WHERE col=val]
		re := regexp.MustCompile(`(?i)SELECT\s+(COUNT|SUM|AVG)\s*\(\s*([a-zA-Z0-9_*]+)\s*\)\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^"]+)"?)?;?`)
		m := re.FindStringSubmatch(query)
		if len(m) < 4 {
			return nil, errors.New("invalid aggregate SELECT syntax")
		}

		stmt := &SelectStmt{
			Table:           m[3],
			AggregateFunc:   strings.ToUpper(m[1]),
			AggregateColumn: m[2],
		}

		if len(m) >= 6 && m[4] != "" {
			stmt.HasWhere = true
			stmt.Column = m[4]
			stmt.Value = m[5]
		}

		return stmt, nil
	}

	// Regular SELECT * FROM table [WHERE col=val] [ORDER BY col [ASC|DESC]] [LIMIT n]
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

	// Handle aggregate functions
	if s.AggregateFunc != "" {
		return s.execAggregate(rows)
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

func (s *SelectStmt) execAggregate(rows []map[string]string) error {
	switch s.AggregateFunc {
	case "COUNT":
		fmt.Printf("%d\n", len(rows))
		return nil

	case "SUM":
		if s.AggregateColumn == "*" {
			return errors.New("SUM requires a column name, not *")
		}
		var sum float64
		for _, row := range rows {
			val, exists := row[s.AggregateColumn]
			if !exists {
				return fmt.Errorf("column '%s' not found", s.AggregateColumn)
			}
			num, err := parseNumber(val)
			if err != nil {
				return fmt.Errorf("cannot SUM non-numeric value: %s", val)
			}
			sum += num
		}
		fmt.Printf("%.2f\n", sum)
		return nil

	case "AVG":
		if s.AggregateColumn == "*" {
			return errors.New("AVG requires a column name, not *")
		}
		if len(rows) == 0 {
			fmt.Println("0")
			return nil
		}
		var sum float64
		for _, row := range rows {
			val, exists := row[s.AggregateColumn]
			if !exists {
				return fmt.Errorf("column '%s' not found", s.AggregateColumn)
			}
			num, err := parseNumber(val)
			if err != nil {
				return fmt.Errorf("cannot AVG non-numeric value: %s", val)
			}
			sum += num
		}
		avg := sum / float64(len(rows))
		fmt.Printf("%.2f\n", avg)
		return nil

	default:
		return fmt.Errorf("unknown aggregate function: %s", s.AggregateFunc)
	}
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