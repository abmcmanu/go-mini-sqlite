package db

import (
	"sort"
	"sync"
)

// Entry représente une paire clé → ligne (enregistrements)
type Entry struct {
	Key   string
	Value map[string]string
}

// BPTree est une version simplifiée d’un B+ Tree :
// - Les clés sont stockées triées pour recherche rapide
// - Chaque clé pointe vers une ligne complète (map[string]string)
// - Pas de pages physiques ni de balance complexe (simulation logique)
type BPTree struct {
	order int
	data  []Entry
	mu    sync.RWMutex
}

// NewBPTree crée un nouvel arbre avec un ordre donné (ordre = nb max d’enfants / 2)
func NewBPTree(order int) *BPTree {
	return &BPTree{
		order: order,
		data:  make([]Entry, 0),
	}
}

// Insert ajoute ou met à jour une entrée (clé unique)
func (b *BPTree) Insert(key string, value map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Si clé existe déjà, on met à jour
	for i, e := range b.data {
		if e.Key == key {
			b.data[i].Value = value
			return
		}
	}

	// Sinon on insère trié
	b.data = append(b.data, Entry{Key: key, Value: value})
	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Key < b.data[j].Key
	})
}

// Get recherche une clé dans l’arbre (logique)
func (b *BPTree) Get(key string) (map[string]string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, e := range b.data {
		if e.Key == key {
			return e.Value, true
		}
	}
	return nil, false
}

// Delete supprime une entrée selon la clé
func (b *BPTree) Delete(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, e := range b.data {
		if e.Key == key {
			b.data = append(b.data[:i], b.data[i+1:]...)
			return
		}
	}
}

// GetAll renvoie toutes les lignes stockées dans l’arbre
func (b *BPTree) GetAll() []map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows := make([]map[string]string, 0, len(b.data))
	for _, e := range b.data {
		rows = append(rows, e.Value)
	}
	return rows
}

// Keys renvoie la liste des clés triées
func (b *BPTree) Keys() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := make([]string, len(b.data))
	for i, e := range b.data {
		keys[i] = e.Key
	}
	return keys
}

// Rebuild est appelé après rechargement depuis le disque
func (b *BPTree) Rebuild() {
	b.mu.Lock()
	defer b.mu.Unlock()
	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Key < b.data[j].Key
	})
}
