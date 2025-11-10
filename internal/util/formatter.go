package util

import (
	"fmt"
	"strings"
)

// PrintTable affiche un tableau bien formaté dans le terminal.
// rows : slice de maps représentant les lignes de la table
// columns : ordre des colonnes à afficher
func PrintTable(columns []string, rows []map[string]string) {
	if len(columns) == 0 {
		fmt.Println("(aucune colonne)")
		return
	}

	// Calcul de la largeur max de chaque colonne
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}

	for _, row := range rows {
		for i, col := range columns {
			val := row[col]
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	// Fonction pour afficher une ligne séparatrice
	printSeparator := func() {
		fmt.Print("+")
		for _, w := range widths {
			fmt.Print(strings.Repeat("-", w+2))
			fmt.Print("+")
		}
		fmt.Println()
	}

	// Affichage de l'en-tête
	printSeparator()
	fmt.Print("|")
	for i, col := range columns {
		fmt.Printf(" %-*s |", widths[i], col)
	}
	fmt.Println()
	printSeparator()

	// Affichage des lignes
	if len(rows) == 0 {
		// Si aucun résultat
		fmt.Printf("| %-*s |\n", totalWidth(widths)+3*(len(columns)-1), "(aucune ligne)")
		printSeparator()
		return
	}

	for _, row := range rows {
		fmt.Print("|")
		for i, col := range columns {
			val := row[col]
			fmt.Printf(" %-*s |", widths[i], val)
		}
		fmt.Println()
	}
	printSeparator()
}

// totalWidth calcule la largeur totale d'une ligne pour gestion du cas vide
func totalWidth(widths []int) int {
	total := 0
	for _, w := range widths {
		total += w
	}
	return total
}
