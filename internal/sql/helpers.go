package sql

import (
	"fmt"
	"strings"
)

type Condition struct {
	Column   string
	Operator string // "=" or "LIKE"
	Value    string
}

type WhereClause struct {
	Conditions []Condition
	Operator   string // "AND" or "OR"
}

func (w *WhereClause) Evaluate(row map[string]string) bool {
	if len(w.Conditions) == 0 {
		return true
	}

	if w.Operator == "AND" {
		for _, cond := range w.Conditions {
			if !evaluateCondition(row[cond.Column], cond.Operator, cond.Value) {
				return false
			}
		}
		return true
	}

	// OR operator
	for _, cond := range w.Conditions {
		if evaluateCondition(row[cond.Column], cond.Operator, cond.Value) {
			return true
		}
	}
	return false
}

func evaluateCondition(rowValue, operator, condValue string) bool {
	switch operator {
	case "=":
		return rowValue == condValue
	case "LIKE":
		return matchPattern(rowValue, condValue)
	default:
		return false
	}
}

func matchPattern(value, pattern string) bool {
	// Convert SQL LIKE pattern to regex-like matching
	// % = any number of characters
	// _ = exactly one character

	i, j := 0, 0
	valueLen, patternLen := len(value), len(pattern)
	starIdx, matchIdx := -1, 0
	for i < valueLen {
		if j < patternLen {
			if pattern[j] == '%' {
				starIdx = j
				matchIdx = i
				j++
				continue
			} else if pattern[j] == '_' || pattern[j] == value[i] {
				i++
				j++
				continue
			}
		}

		if starIdx != -1 {
			j = starIdx + 1
			matchIdx++
			i = matchIdx
			continue
		}
		return false
	}

	for j < patternLen && pattern[j] == '%' {
		j++
	}

	return j == patternLen
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
