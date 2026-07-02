package litekv

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type KVStore[V any] interface {
	Set(key string, value V) error
	Get(key string) (value V, found bool, err error)
	Delete(key string) error
	Close() error
}

type SQLiteStore[V any] struct {
	db      *sql.DB
	stmtSet *sql.Stmt
	stmtGet *sql.Stmt
	stmtDel *sql.Stmt
}

// _busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL
func NewSQLiteStore[V any](db *sql.DB) (*SQLiteStore[V], error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}

	store := &SQLiteStore[V]{db: db}

	if err := store.initTable(); err != nil {
		return nil, err
	}
	if err := store.prepareStatements(); err != nil {
		store.closeStatements()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore[V]) initTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS kv_store (
		key TEXT PRIMARY KEY,
		value TEXT,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := s.db.Exec(query)
	return err
}

func (s *SQLiteStore[V]) prepareStatements() error {
	var err error

	s.stmtSet, err = s.db.Prepare(`REPLACE INTO kv_store (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP);`)
	if err != nil {
		return err
	}

	s.stmtGet, err = s.db.Prepare(`SELECT value FROM kv_store WHERE key = ?;`)
	if err != nil {
		return err
	}

	s.stmtDel, err = s.db.Prepare(`DELETE FROM kv_store WHERE key = ?;`)
	if err != nil {
		return err
	}

	return nil
}

func (s *SQLiteStore[V]) closeStatements() {
	if s.stmtSet != nil {
		s.stmtSet.Close()
	}
	if s.stmtGet != nil {
		s.stmtGet.Close()
	}
	if s.stmtDel != nil {
		s.stmtDel.Close()
	}
}

func (s *SQLiteStore[V]) Set(key string, value V) error {
	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	_, err = s.stmtSet.Exec(key, string(bytes))
	return err
}

func (s *SQLiteStore[V]) Get(key string) (V, bool, error) {
	var zero V
	var valStr string

	err := s.stmtGet.QueryRow(key).Scan(&valStr)
	if err == sql.ErrNoRows {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}

	var value V
	if err := json.Unmarshal([]byte(valStr), &value); err != nil {
		return zero, false, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return value, true, nil
}

func (s *SQLiteStore[V]) Delete(key string) error {
	_, err := s.stmtDel.Exec(key)
	return err
}

func (s *SQLiteStore[V]) Close() error {
	s.closeStatements()
	return nil
}

type JSONStore[V any] struct {
	mu       sync.RWMutex
	filePath string
	data     map[string]V
}

func NewJSONStore[V any](filePath string) (*JSONStore[V], error) {
	store := &JSONStore[V]{
		filePath: filePath,
		data:     make(map[string]V),
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return store, nil
	}

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	return store, json.NewDecoder(fd).Decode(&store.data)
}

func (j *JSONStore[V]) saveToDisk() error {
	fileData, err := json.MarshalIndent(j.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(j.filePath, fileData, 0644)
}

func (j *JSONStore[V]) Get(key string) (V, bool, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	val, found := j.data[key]
	return val, found, nil
}

func (j *JSONStore[V]) Set(key string, value V) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.data[key] = value
	return j.saveToDisk()
}

func (j *JSONStore[V]) Delete(key string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, found := j.data[key]; !found {
		return nil
	}
	delete(j.data, key)
	return j.saveToDisk()
}

func (j *JSONStore[V]) Close() error {
	return nil
}
