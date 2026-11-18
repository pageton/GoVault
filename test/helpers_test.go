// Package govaulttest holds GoVault's integration test suite: everything
// that needs a real SQLite driver. It lives in its own module so the
// library's go.mod stays dependency-free; a replace directive points it at
// the sibling library source.
package govaulttest

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // the driver under test

	"github.com/pageton/GoVault"
)

// openTest opens a Store on a fresh temporary database, closes it when the
// test finishes, and returns the store plus its path.
func openTest(t *testing.T, opts ...govault.Option) (*govault.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := govault.Open(path, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s, path
}

// openRaw opens a second database/sql connection to the same file so tests
// can observe physical rows the API hides, such as expired-but-unpurged keys.
func openRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("sql.Open raw: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func rawCount(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("raw %q: %v", query, err)
	}
	return n
}
