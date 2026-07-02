package litekv

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type KVStore interface {
	Set(key, value string) error
	Get(key string) (value string, found bool, err error)
	Delete(key string) error
	Close() error
}

type SQLiteStore struct {
	db      *sql.DB
	stmtSet *sql.Stmt
	stmtGet *sql.Stmt
	stmtDel *sql.Stmt
}

// _busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL
func NewSQLiteStore(db *sql.DB) (*SQLiteStore, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}

	store := &SQLiteStore{db: db}

	if err := store.initTable(); err != nil {
		return nil, err
	}
	if err := store.prepareStatements(); err != nil {
		store.closeStatements()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) initTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS kv_store (
		key TEXT PRIMARY KEY,
		value TEXT,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := s.db.Exec(query)
	return err
}

func (s *SQLiteStore) prepareStatements() error {
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

func (s *SQLiteStore) closeStatements() {
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

func (s *SQLiteStore) Set(key, value string) error {
	_, err := s.stmtSet.Exec(key, value)
	return err
}

func (s *SQLiteStore) Get(key string) (string, bool, error) {
	var value string
	err := s.stmtGet.QueryRow(key).Scan(&value)

	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (s *SQLiteStore) Delete(key string) error {
	_, err := s.stmtDel.Exec(key)
	return err
}

func (s *SQLiteStore) Close() error {
	s.closeStatements()
	return nil
}

type JSONStore struct {
	mu       sync.RWMutex
	filePath string
	data     map[string]string
}

func NewJSONStore(filePath string) (*JSONStore, error) {
	store := &JSONStore{
		filePath: filePath,
		data:     make(map[string]string),
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return store, nil
	}

	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	return store, json.NewDecoder(fd).Decode(&store.data)
}

func (j *JSONStore) saveToDisk() error {
	fileData, err := json.MarshalIndent(j.data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(j.filePath, fileData, 0644)
}

func (j *JSONStore) Get(key string) (string, bool, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	val, found := j.data[key]
	return val, found, nil
}

func (j *JSONStore) Set(key, value string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.data[key] = value
	return j.saveToDisk()
}

func (j *JSONStore) Delete(key string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, found := j.data[key]; !found {
		return nil
	}
	delete(j.data, key)
	return j.saveToDisk()
}

func (j *JSONStore) Close() error {
	return nil
}
