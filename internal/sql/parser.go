package sql

import (
	"fmt"
	"strings"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
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