package db

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Database représente le gestionnaire global qui pilote plusieurs bases
// Chaque base est un dossier contenant des fichiers .tbl (tables)
type Database struct {
	RootPath string            // dossier principal ./data
	ActiveDB string            // nom de la base de données courante
	Tables   map[string]*Table // tables chargées en mémoire pour la base active
}

// NewDatabase initialise le gestionnaire racine (ex: ./data)
func NewDatabase(root string) (*Database, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Database{
		RootPath: root,
		Tables:   make(map[string]*Table),
	}, nil
}

// CreateDatabase crée physiquement un dossier pour une nouvelle base
func (d *Database) CreateDatabase(name string) error {
	dbPath := filepath.Join(d.RootPath, name)
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		return fmt.Errorf("la base '%s' existe déjà", name)
	}
	return os.MkdirAll(dbPath, 0o755)
}

// SetActiveDB permet de changer la base de données courante (USE <db>)
func (d *Database) SetActiveDB(name string) error {
	dbPath := filepath.Join(d.RootPath, name)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' inexistante", name)
	}

	d.ActiveDB = name
	d.Tables = make(map[string]*Table)

	files, err := os.ReadDir(dbPath)
	if err != nil {
		return err
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".tbl") {
			t, err := LoadTable(filepath.Join(dbPath, f.Name()))
			if err == nil {
				d.Tables[t.Name] = t
			}
		}
	}
	return nil
}

// CreateTable crée et enregistre une table dans la base active
func (d *Database) CreateTable(name string, schema Schema) error {
	if d.ActiveDB == "" {
		return fmt.Errorf("aucune base active (faites USE <db>)")
	}

	// vérifie doublon
	if _, exists := d.Tables[name]; exists {
		return fmt.Errorf("table '%s' existe déjà", name)
	}

	dbPath := filepath.Join(d.RootPath, d.ActiveDB)
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		return err
	}

	tblPath := filepath.Join(dbPath, name+".tbl")
	t := &Table{
		Name:     name,
		Schema:   schema,
		FilePath: tblPath,
		Index:    NewBPTree(4),
	}

	d.Tables[name] = t
	return t.Save()
}

// Table retourne une table par son nom si elle existe
func (d *Database) Table(name string) (*Table, error) {
	t, ok := d.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table '%s' introuvable", name)
	}
	return t, nil
}

// ActivePath retourne le chemin absolu de la base active
func (d *Database) ActivePath() (string, error) {
	if d.ActiveDB == "" {
		return "", fmt.Errorf("aucune base active")
	}
	return filepath.Join(d.RootPath, d.ActiveDB), nil
}

func (d *Database) GetTable(name string) (*Table, error) {
	t, ok := d.Tables[name]
	if !ok {
		return nil, fmt.Errorf("table '%s' introuvable", name)
	}
	return t, nil
}
