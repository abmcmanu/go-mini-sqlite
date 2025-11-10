package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/abmcmanu/go-mini-sqlite/internal/db"
	intsql "github.com/abmcmanu/go-mini-sqlite/internal/sql"
)

func main() {
	fmt.Println("go-mini-sqlite shell — tapez .exit pour quitter")

	// Dossier racine des bases de données
	basePath := "./data"

	// Initialise le gestionnaire global de bases
	database, err := db.NewDatabase(basePath)
	if err != nil {
		log.Fatalf("Erreur d'initialisation du répertoire base de données: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Commandes internes (hors SQL)
		if line == ".exit" || strings.EqualFold(line, "exit") {
			fmt.Println("bye 👋")
			break
		}

		// Analyse de la requête SQL
		stmt, err := intsql.Parse(line)
		if err != nil {
			fmt.Println("❌ Erreur de parsing:", err)
			continue
		}

		// Exécution de la commande SQL
		if err := stmt.Exec(database); err != nil {
			fmt.Println("❌ Erreur d'exécution:", err)
			continue
		}
	}
}
