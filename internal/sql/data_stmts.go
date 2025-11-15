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
	Where            *WhereClause
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
		return parseAggregateSelect(query)
	}

	// Check for AND/OR in WHERE clause
	if strings.Contains(queryUpper, " AND ") || strings.Contains(queryUpper, " OR ") {
		return parseSelectWithMultipleConditions(query)
	}

	// Regular SELECT * FROM table [WHERE col=val] [ORDER BY col [ASC|DESC]] [LIMIT n]
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^"]+)"?)?(?:\s+ORDER\s+BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+))?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil, errors.New("invalid SELECT syntax")
	}

	stmt := &SelectStmt{Table: m[1]}

	if len(m) >= 4 && m[2] != "" {
		stmt.Where = &WhereClause{
			Conditions: []Condition{{Column: m[2], Operator: "=", Value: m[3]}},
			Operator:   "AND",
		}
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

func parseAggregateSelect(query string) (Statement, error) {
	// SELECT COUNT(*) FROM table [WHERE conditions]
	// SELECT SUM(column) FROM table [WHERE conditions]
	// SELECT AVG(column) FROM table [WHERE conditions]

	queryUpper := strings.ToUpper(query)

	// Check for AND/OR in WHERE clause
	if strings.Contains(queryUpper, " AND ") || strings.Contains(queryUpper, " OR ") {
		re := regexp.MustCompile(`(?i)SELECT\s+(COUNT|SUM|AVG)\s*\(\s*([a-zA-Z0-9_*]+)\s*\)\s+FROM\s+([a-zA-Z0-9_]+)\s+WHERE\s+(.+?);?$`)
		m := re.FindStringSubmatch(query)
		if len(m) < 5 {
			return nil, errors.New("invalid aggregate SELECT syntax")
		}

		whereClause, err := parseWhereClause(m[4])
		if err != nil {
			return nil, err
		}

		return &SelectStmt{
			Table:           m[3],
			AggregateFunc:   strings.ToUpper(m[1]),
			AggregateColumn: m[2],
			Where:           whereClause,
		}, nil
	}

	// Single condition or no WHERE
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
		stmt.Where = &WhereClause{
			Conditions: []Condition{{Column: m[4], Operator: "=", Value: m[5]}},
			Operator:   "AND",
		}
	}

	return stmt, nil
}

func parseSelectWithMultipleConditions(query string) (Statement, error) {
	// SELECT * FROM table WHERE col1=val1 AND/OR col2=val2 [ORDER BY col [ASC|DESC]] [LIMIT n]
	re := regexp.MustCompile(`(?i)SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_]+)\s+WHERE\s+(.+?)(?:\s+ORDER\s+BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?)?\s*(?:LIMIT\s+(\d+))?\s*;?$`)
	m := re.FindStringSubmatch(query)
	if len(m) < 3 {
		return nil, errors.New("invalid SELECT syntax with multiple conditions")
	}

	whereClause, err := parseWhereClause(m[2])
	if err != nil {
		return nil, err
	}

	stmt := &SelectStmt{
		Table: m[1],
		Where: whereClause,
	}

	if len(m) >= 4 && m[3] != "" {
		stmt.OrderByColumn = m[3]
		if len(m) >= 5 && m[4] != "" {
			stmt.OrderByDirection = strings.ToUpper(m[4])
		} else {
			stmt.OrderByDirection = "ASC"
		}
	}

	if len(m) >= 6 && m[5] != "" {
		limit, err := parseNumber(m[5])
		if err != nil || limit < 0 {
			return nil, errors.New("LIMIT must be a positive number")
		}
		stmt.Limit = int(limit)
	}

	return stmt, nil
}

func parseWhereClause(whereStr string) (*WhereClause, error) {
	whereStr = strings.TrimSpace(whereStr)
	queryUpper := strings.ToUpper(whereStr)

	var operator string
	var conditionParts []string

	if strings.Contains(queryUpper, " AND ") {
		operator = "AND"
		conditionParts = strings.Split(whereStr, " AND ")
		// Fallback to lowercase
		if len(conditionParts) == 1 {
			conditionParts = strings.Split(whereStr, " and ")
		}
	} else if strings.Contains(queryUpper, " OR ") {
		operator = "OR"
		conditionParts = strings.Split(whereStr, " OR ")
		// Fallback to lowercase
		if len(conditionParts) == 1 {
			conditionParts = strings.Split(whereStr, " or ")
		}
	} else {
		return nil, errors.New("no AND/OR operator found")
	}

	var conditions []Condition
	for _, part := range conditionParts {
		part = strings.TrimSpace(part)

		// Try LIKE first
		reLike := regexp.MustCompile(`(?i)([a-zA-Z0-9_]+)\s+LIKE\s+"?([^"]+)"?`)
		m := reLike.FindStringSubmatch(part)
		if len(m) >= 3 {
			conditions = append(conditions, Condition{
				Column:   m[1],
				Operator: "LIKE",
				Value:    strings.Trim(m[2], `"`),
			})
			continue
		}

		// Try = operator
		reEq := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*=\s*"?([^"]+)"?`)
		m = reEq.FindStringSubmatch(part)
		if len(m) < 3 {
			return nil, fmt.Errorf("invalid condition: %s", part)
		}
		conditions = append(conditions, Condition{
			Column:   m[1],
			Operator: "=",
			Value:    strings.Trim(m[2], `"`),
		})
	}

	return &WhereClause{
		Conditions: conditions,
		Operator:   operator,
	}, nil
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
	if s.Where != nil {
		// Filter rows using WhereClause
		allRows := t.SelectAll()
		for _, row := range allRows {
			if s.Where.Evaluate(row) {
				rows = append(rows, row)
			}
		}
	} else {
		rows = t.SelectAll()
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
	Table   string
	Updates map[string]string
	Where   *WhereClause
}

func parseUpdate(query string) (Statement, error) {
	queryUpper := strings.ToUpper(query)

	// Check for AND/OR in WHERE clause
	if strings.Contains(queryUpper, " AND ") || strings.Contains(queryUpper, " OR ") {
		re := regexp.MustCompile(`(?i)UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+(.+?);?$`)
		m := re.FindStringSubmatch(query)
		if len(m) < 4 {
			return nil, errors.New("invalid UPDATE syntax with multiple conditions")
		}

		table := m[1]
		setPart := m[2]

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

		whereClause, err := parseWhereClause(m[3])
		if err != nil {
			return nil, err
		}

		return &UpdateStmt{
			Table:   table,
			Updates: updates,
			Where:   whereClause,
		}, nil
	}

	// Single condition
	re := regexp.MustCompile(`(?i)UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^";]+)"?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 5 {
		return nil, errors.New("invalid UPDATE syntax (expected: UPDATE table SET col=val WHERE col=val)")
	}

	table := m[1]
	setPart := m[2]

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
		Table:   table,
		Updates: updates,
		Where: &WhereClause{
			Conditions: []Condition{{Column: m[3], Operator: "=", Value: m[4]}},
			Operator:   "AND",
		},
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

	// Get matching rows
	allRows := t.SelectAll()
	var matchingRows []map[string]string
	for _, row := range allRows {
		if s.Where.Evaluate(row) {
			matchingRows = append(matchingRows, row)
		}
	}

	if len(matchingRows) == 0 {
		fmt.Println("0 row(s) updated.")
		return nil
	}

	// Validate and update
	cols := t.Schema.ColumnsMap()
	pkName := t.PrimaryKey()

	for colName, newVal := range s.Updates {
		col, exists := cols[colName]
		if !exists {
			return fmt.Errorf("column '%s' does not exist", colName)
		}
		if col.PrimaryKey {
			return fmt.Errorf("cannot update PRIMARY KEY column '%s'", colName)
		}
		if col.NotNull && newVal == "" {
			return fmt.Errorf("column '%s' is NOT NULL", colName)
		}
	}

	count := 0
	for _, row := range matchingRows {
		pkVal := row[pkName]
		updatedRow := make(map[string]string)
		for k, v := range row {
			updatedRow[k] = v
		}
		for k, v := range s.Updates {
			updatedRow[k] = v
		}

		t.Index.Insert(pkVal, updatedRow)
		count++
	}

	if err := t.Save(); err != nil {
		return err
	}

	fmt.Printf("%d row(s) updated.\n", count)
	return nil
}

type DeleteStmt struct {
	Table string
	Where *WhereClause
}

func parseDelete(query string) (Statement, error) {
	queryUpper := strings.ToUpper(query)

	// Check for AND/OR in WHERE clause
	if strings.Contains(queryUpper, " AND ") || strings.Contains(queryUpper, " OR ") {
		re := regexp.MustCompile(`(?i)DELETE\s+FROM\s+([a-zA-Z0-9_]+)\s+WHERE\s+(.+?);?$`)
		m := re.FindStringSubmatch(query)
		if len(m) < 3 {
			return nil, errors.New("invalid DELETE syntax with multiple conditions")
		}

		whereClause, err := parseWhereClause(m[2])
		if err != nil {
			return nil, err
		}

		return &DeleteStmt{
			Table: m[1],
			Where: whereClause,
		}, nil
	}
	// Single condition
	re := regexp.MustCompile(`(?i)DELETE\s+FROM\s+([a-zA-Z0-9_]+)\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*"?([^";]+)"?;?`)
	m := re.FindStringSubmatch(query)
	if len(m) < 4 {
		return nil, errors.New("invalid DELETE syntax (expected: DELETE FROM table WHERE col=val)")
	}

	return &DeleteStmt{
		Table: m[1],
		Where: &WhereClause{
			Conditions: []Condition{{Column: m[2], Operator: "=", Value: m[3]}},
			Operator:   "AND",
		},
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

	// Get matching rows
	allRows := t.SelectAll()
	var matchingRows []map[string]string
	for _, row := range allRows {
		if s.Where.Evaluate(row) {
			matchingRows = append(matchingRows, row)
		}
	}

	if len(matchingRows) == 0 {
		fmt.Println("0 row(s) deleted.")
		return nil
	}

	pkName := t.PrimaryKey()
	count := 0

	for _, row := range matchingRows {
		pk := row[pkName]
		t.Index.Delete(pk)
		count++
	}

	if err := t.Save(); err != nil {
		return err
	}

	fmt.Printf("%d row(s) deleted.\n", count)
	return nil
}
