package sql

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
)

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