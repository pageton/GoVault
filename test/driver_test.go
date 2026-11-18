package govaulttest

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pageton/GoVault"
)

// moderncDSN builds a modernc-dialect DSN with the settings GoVault expects,
// the way a consuming project would write it by hand.
func moderncDSN(path string) string {
	return "file:" + path + "?_txlock=immediate&_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
}

func TestWithDriverExplicit(t *testing.T) {
	dsn := moderncDSN(filepath.Join(t.TempDir(), "explicit.db"))
	s, err := govault.Open("", govault.WithDriver("sqlite", dsn))
	if err != nil {
		t.Fatalf("Open with explicit driver: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	if err := s.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := s.Get(ctx, "k"); err != nil || string(got) != "v" {
		t.Fatalf("Get: got %q, %v", got, err)
	}
}

func TestWithDriverUnregistered(t *testing.T) {
	_, err := govault.Open("", govault.WithDriver("nosuchdriver", "file:x.db"))
	if err == nil || !strings.Contains(err.Error(), "unknown driver") {
		t.Fatalf("Open with unregistered driver: got %v, want unknown-driver error", err)
	}
}

func TestWithDriverRequiresBoth(t *testing.T) {
	for _, tc := range []struct{ name, dsn string }{
		{"sqlite", ""},
		{"", "file:x.db"},
	} {
		if _, err := govault.Open("unused.db", govault.WithDriver(tc.name, tc.dsn)); err == nil ||
			!strings.Contains(err.Error(), "WithDriver requires") {
			t.Errorf("WithDriver(%q, %q): got %v, want usage error", tc.name, tc.dsn, err)
		}
	}
}

func TestAttach(t *testing.T) {
	db, err := sql.Open("sqlite", moderncDSN(filepath.Join(t.TempDir(), "attach.db")))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}

	s, err := govault.Attach(db)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}

	ctx := context.Background()
	if err := s.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := s.Get(ctx, "k"); err != nil || string(got) != "v" {
		t.Fatalf("Get: got %q, %v", got, err)
	}
	if n, err := s.Count(ctx, ""); err != nil || n != 1 {
		t.Fatalf("Count: got %d, %v", n, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Attach transfers ownership: Close also closed the caller's handle.
	if err := db.PingContext(ctx); err == nil {
		t.Fatal("attached db should be closed after Store.Close")
	}
}

func TestAttachNilDB(t *testing.T) {
	if _, err := govault.Attach(nil); err == nil {
		t.Fatal("Attach(nil) should fail")
	}
}

func TestAttachRejectsNonWAL(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "delete-mode.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	_, err = govault.Attach(db)
	if err == nil || !strings.Contains(err.Error(), "not WAL") {
		t.Fatalf("Attach without WAL: got %v, want journal-mode error", err)
	}
}
