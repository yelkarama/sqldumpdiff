package store

import (
	"github.com/younes/sqldumpdiff/internal/parser"
)

// MemoryStore is a simple in-memory store for rows
type MemoryStore struct {
	data map[string]*parser.InsertRow
}

// NewMemoryStore creates a new memory store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]*parser.InsertRow),
	}
}

// Set stores a row by its hash
func (ms *MemoryStore) Set(hash string, row *parser.InsertRow) {
	ms.data[hash] = row
}

// Get retrieves a row by its hash
func (ms *MemoryStore) Get(hash string) *parser.InsertRow {
	return ms.data[hash]
}

// GetAll returns all stored rows
func (ms *MemoryStore) GetAll() map[string]*parser.InsertRow {
	return ms.data
}

// Count returns the number of stored rows
func (ms *MemoryStore) Count() int {
	return len(ms.data)
}
