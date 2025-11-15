package sql

import (
	"fmt"
	"strings"
)

type Condition struct {
	Column string
	Value  string
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
			if row[cond.Column] != cond.Value {
				return false
			}
		}
		return true
	}

	// OR operator
	for _, cond := range w.Conditions {
		if row[cond.Column] == cond.Value {
			return true
		}
	}
	return false
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