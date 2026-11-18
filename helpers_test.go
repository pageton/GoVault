package govault

import (
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // default driver for tests
)

// openTest opens a Store on a fresh temporary database and closes it when
// the test finishes.
func openTest(t *testing.T, opts ...Option) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "test.db"), opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// rawRowCount counts physical rows, including expired ones the API hides.
// Used by tests to observe what the janitor actually deleted.
func rawRowCount(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("raw query %q: %v", query, err)
	}
	return n
}
