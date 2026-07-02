package litekv

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

func testStoreInterface(t *testing.T, store KVStore[string]) {
	// 1. 测试 Get 一个不存在的 Key
	val, found, err := store.Get("non-existent")
	if err != nil {
		t.Fatalf("unexpected error on Get: %v", err)
	}
	if found || val != "" {
		t.Errorf("expected not found, got val=%s, found=%t", val, found)
	}

	// 2. 测试 Set 和 Get
	err = store.Set("name", "Go Gopher")
	if err != nil {
		t.Fatalf("failed to Set: %v", err)
	}

	val, found, err = store.Get("name")
	if err != nil {
		t.Fatalf("failed to Get: %v", err)
	}
	if !found || val != "Go Gopher" {
		t.Errorf("expected 'Go Gopher', got val=%s, found=%t", val, found)
	}

	// 3. 测试覆盖更新 (Update/Replace)
	err = store.Set("name", "New Gopher")
	if err != nil {
		t.Fatalf("failed to update Set: %v", err)
	}

	val, found, err = store.Get("name")
	if err != nil {
		t.Fatalf("failed to Get after update: %v", err)
	}
	if val != "New Gopher" {
		t.Errorf("expected updated value 'New Gopher', got '%s'", val)
	}

	// 4. 测试 Delete
	err = store.Delete("name")
	if err != nil {
		t.Fatalf("failed to Delete: %v", err)
	}

	val, found, err = store.Get("name")
	if err != nil {
		t.Fatalf("failed to Get after Delete: %v", err)
	}
	if found {
		t.Errorf("expected key to be deleted, but it was still found")
	}

	// 5. 重复删除不应该报错
	err = store.Delete("name")
	if err != nil {
		t.Errorf("deleting a non-existent key should not return error: %v", err)
	}
}

// 测试 SQLiteStore 实现
func TestSQLiteStore(t *testing.T) {
	// 异常边界测试：传入 nil db
	_, err := NewSQLiteStore[any](nil)
	if err == nil {
		t.Error("expected error when passing nil db to NewSQLiteStore, got nil")
	}

	// 使用 modernc 的驱动名称 "sqlite"（注意：mattn 的是 "sqlite3"）
	// 并使用内存模式进行隔离测试
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()

	store, err := NewSQLiteStore[string](db)
	if err != nil {
		t.Fatalf("failed to create SQLiteStore: %v", err)
	}
	defer store.Close()

	// 运行通用接口测试
	testStoreInterface(t, store)
}

// 测试 JSONStore 实现
func TestJSONStore(t *testing.T) {
	tmpDir := t.TempDir()
	jsonPath := filepath.Join(tmpDir, "test_store.json")

	store, err := NewJSONStore[string](jsonPath)
	if err != nil {
		t.Fatalf("failed to create JSONStore: %v", err)
	}

	testStoreInterface(t, store)
	store.Close()

	// 验证持久化行为
	store2, err := NewJSONStore[string](jsonPath)
	if err != nil {
		t.Fatalf("failed to re-open JSONStore: %v", err)
	}

	err = store2.Set("persistent_key", "hello_disk")
	if err != nil {
		t.Fatalf("failed to set persistent_key: %v", err)
	}
	store2.Close()

	store3, err := NewJSONStore[string](jsonPath)
	if err != nil {
		t.Fatalf("failed to re-open JSONStore third time: %v", err)
	}
	defer store3.Close()

	val, found, err := store3.Get("persistent_key")
	if err != nil || !found || val != "hello_disk" {
		t.Errorf("failed to recover data from disk, got val=%s, found=%t, err=%v", val, found, err)
	}
}

// 并发安全测试
func TestStoreConcurrency(t *testing.T) {
	// 同样使用 "sqlite" 驱动名
	db, _ := sql.Open("sqlite", ":memory:")
	defer db.Close()
	sqliteStore, _ := NewSQLiteStore[string](db)
	defer sqliteStore.Close()

	tmpDir := t.TempDir()
	jsonStore, _ := NewJSONStore[string](filepath.Join(tmpDir, "concurrent.json"))
	defer jsonStore.Close()

	stores := map[string]KVStore[string]{
		"SQLiteStore": sqliteStore,
		"JSONStore":   jsonStore,
	}

	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			workerCount := 20
			opsPerWorker := 50

			for i := 0; i < workerCount; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := 0; j < opsPerWorker; j++ {
						key := fmt.Sprintf("worker_%d_key_%d", workerID, j)
						val := fmt.Sprintf("value_%d", j)

						_ = store.Set(key, val)
						_, _, _ = store.Get(key)
						_ = store.Delete(key)
					}
				}(i)
			}
			wg.Wait()
		})
	}
}
