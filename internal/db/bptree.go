package db

import (
	"sort"
	"sync"
)

type Entry struct {
	Key   string
	Value map[string]string
}

// BPTree est une version simplifiée d’un B+ Tree logique.
type BPTree struct {
	order int     // ordre logique
	data  []Entry // liste triée de (clé, valeur)
	mu    sync.RWMutex
}

func NewBPTree(order int) *BPTree {
	return &BPTree{
		order: order,
		data:  make([]Entry, 0),
	}
}

func (b *BPTree) Insert(key string, value map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Si la clé existe déjà, mise à jour
	for i, e := range b.data {
		if e.Key == key {
			b.data[i].Value = value
			return
		}
	}

	// Sinon, on insère et on trie
	b.data = append(b.data, Entry{Key: key, Value: value})
	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Key < b.data[j].Key
	})
}

// Get recherche une clé dans l’arbre.
func (b *BPTree) Get(key string) (map[string]string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Recherche binaire pour plus d’efficacité
	i := sort.Search(len(b.data), func(i int) bool {
		return b.data[i].Key >= key
	})

	if i < len(b.data) && b.data[i].Key == key {
		return b.data[i].Value, true
	}
	return nil, false
}

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

func (b *BPTree) GetAll() []map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	rows := make([]map[string]string, 0, len(b.data))
	for _, e := range b.data {
		rows = append(rows, e.Value)
	}
	return rows
}

// Keys renvoie la liste des clés triées.
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
// pour s'assurer que les clés sont bien triées.
func (b *BPTree) Rebuild() {
	b.mu.Lock()
	defer b.mu.Unlock()

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Key < b.data[j].Key
	})
}
