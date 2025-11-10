package db

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

// -------- Types & structures --------

type ColType string

const (
	TypeInt    ColType = "INT"
	TypeString ColType = "STRING"
)

type Column struct {
	Name       string
	Type       ColType
	PrimaryKey bool
	NotNull    bool
	Unique     bool
}

type Schema struct {
	Columns []Column
}

type Table struct {
	Name     string
	Schema   Schema
	FilePath string
	Index    *BPTree // B+ Tree en mémoire
}

// -------- Gestion de la table --------

// Save enregistre la table sur disque (schéma + lignes)
func (t *Table) Save() error {
	if err := os.MkdirAll(filepath.Dir(t.FilePath), 0755); err != nil {
		return err
	}
	f, err := os.Create(t.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	data := struct {
		Name   string
		Schema Schema
		Rows   []map[string]string
	}{
		Name:   t.Name,
		Schema: t.Schema,
		Rows:   t.Index.GetAll(),
	}
	return enc.Encode(&data)
}

// LoadTable recharge une table depuis un fichier gob
func LoadTable(path string) (*Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var data struct {
		Name   string
		Schema Schema
		Rows   []map[string]string
	}

	dec := gob.NewDecoder(f)
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}

	table := &Table{
		Name:     data.Name,
		Schema:   data.Schema,
		FilePath: path,
		Index:    NewBPTree(3),
	}

	for _, row := range data.Rows {
		pk := row[table.PrimaryKey()]
		table.Index.Insert(pk, row)
	}

	return table, nil
}

// PrimaryKey retourne le nom de la clé primaire
func (t *Table) PrimaryKey() string {
	for _, c := range t.Schema.Columns {
		if c.PrimaryKey {
			return c.Name
		}
	}
	return ""
}

// -------- Opérations SQL --------

// Insert ajoute une ligne avec vérification des contraintes
func (t *Table) Insert(values map[string]string) error {
	// Vérifie que toutes les colonnes existent et contraintes NOT NULL / UNIQUE
	cols := t.Schema.ColumnsMap()
	for name, col := range cols {
		v := values[name]
		if col.NotNull && v == "" {
			return fmt.Errorf("colonne '%s' ne peut pas être NULL", name)
		}
		if col.Unique {
			rows, _ := t.SelectWhere(name, v)
			if len(rows) > 0 {
				return fmt.Errorf("valeur '%s' déjà présente pour colonne UNIQUE '%s'", v, name)
			}
		}
	}

	// Gère la clé primaire automatique si nécessaire
	pkName := t.PrimaryKey()
	pkVal := values[pkName]
	if pkVal == "" {
		pkVal = fmt.Sprintf("%d", len(t.Index.GetAll())+1)
		values[pkName] = pkVal
	}

	// Insère dans l’index mémoire
	t.Index.Insert(pkVal, values)

	// Sauvegarde sur disque
	return t.Save()
}

// SelectWhere filtre sur une colonne avec une valeur
func (t *Table) SelectWhere(column, value string) ([]map[string]string, error) {
	all := t.Index.GetAll()
	var results []map[string]string
	for _, row := range all {
		if row[column] == value {
			results = append(results, row)
		}
	}
	return results, nil
}

// SelectAll retourne toutes les lignes
func (t *Table) SelectAll() []map[string]string {
	return t.Index.GetAll()
}

// -------- Helpers --------

// ColumnsMap retourne un map[name]Column pour lookup rapide
func (s Schema) ColumnsMap() map[string]Column {
	m := make(map[string]Column)
	for _, c := range s.Columns {
		m[c.Name] = c
	}
	return m
}

// convertSchema convertit slice de Column en Schema
func convertSchema(cols []Column) Schema {
	return Schema{Columns: cols}
}
