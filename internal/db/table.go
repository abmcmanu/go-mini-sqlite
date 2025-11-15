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

// Update modifie les lignes correspondant à une condition WHERE
func (t *Table) Update(whereColumn, whereValue string, updates map[string]string) (int, error) {
	// Récupère toutes les lignes qui correspondent à la clause WHERE
	rows, err := t.SelectWhere(whereColumn, whereValue)
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 {
		return 0, nil // Aucune ligne à mettre à jour
	}

	// Vérifie les contraintes pour les nouvelles valeurs
	cols := t.Schema.ColumnsMap()
	pkName := t.PrimaryKey()

	for colName, newVal := range updates {
		col, exists := cols[colName]
		if !exists {
			return 0, fmt.Errorf("colonne '%s' n'existe pas", colName)
		}

		// Interdit la modification de la clé primaire
		if colName == pkName {
			return 0, fmt.Errorf("impossible de modifier la clé primaire '%s'", pkName)
		}

		// Vérifie NOT NULL
		if col.NotNull && newVal == "" {
			return 0, fmt.Errorf("colonne '%s' ne peut pas être NULL", colName)
		}

		// Vérifie UNIQUE
		if col.Unique && newVal != "" {
			existing, _ := t.SelectWhere(colName, newVal)
			// Autorise la mise à jour si la valeur existe déjà dans la ligne qu'on modifie
			for _, ex := range existing {
				isCurrentRow := false
				for _, row := range rows {
					if ex[pkName] == row[pkName] {
						isCurrentRow = true
						break
					}
				}
				if !isCurrentRow {
					return 0, fmt.Errorf("valeur '%s' déjà présente pour colonne UNIQUE '%s'", newVal, colName)
				}
			}
		}
	}

	// Applique les mises à jour
	count := 0
	for _, row := range rows {
		pk := row[pkName]
		// Récupère la ligne actuelle depuis l'index
		currentRow, found := t.Index.Get(pk)
		if !found {
			continue
		}

		// Applique les modifications
		for colName, newVal := range updates {
			currentRow[colName] = newVal
		}

		// Met à jour dans l'index (Insert remplace si la clé existe)
		t.Index.Insert(pk, currentRow)
		count++
	}

	// Sauvegarde sur disque
	if err := t.Save(); err != nil {
		return 0, err
	}

	return count, nil
}

// Delete supprime les lignes correspondant à une condition WHERE
func (t *Table) Delete(whereColumn, whereValue string) (int, error) {
	// Récupère toutes les lignes qui correspondent à la clause WHERE
	rows, err := t.SelectWhere(whereColumn, whereValue)
	if err != nil {
		return 0, err
	}

	if len(rows) == 0 {
		return 0, nil // Aucune ligne à supprimer
	}

	pkName := t.PrimaryKey()
	count := 0

	// Supprime chaque ligne de l'index
	for _, row := range rows {
		pk := row[pkName]
		t.Index.Delete(pk)
		count++
	}

	// Sauvegarde sur disque
	if err := t.Save(); err != nil {
		return 0, err
	}

	return count, nil
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
