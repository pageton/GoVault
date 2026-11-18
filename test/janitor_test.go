package govaulttest

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pageton/GoVault"
)

func TestJanitorPurgesExpiredRows(t *testing.T) {
	s, path := openTest(t, govault.WithJanitorInterval(60*time.Millisecond))
	raw := openRaw(t, path)
	ctx := context.Background()

	if err := s.Set(ctx, "ephemeral", []byte("v"), 80*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Set(ctx, "durable", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// The row must be physically deleted, not just hidden.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if n := rawCount(t, raw, "SELECT COUNT(*) FROM kv WHERE key = ?", "ephemeral"); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("janitor did not purge the expired row in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := rawCount(t, raw, "SELECT COUNT(*) FROM kv WHERE key = ?", "durable"); n != 1 {
		t.Fatalf("janitor removed a non-expired row (count=%d)", n)
	}
}

func TestJanitorDisabled(t *testing.T) {
	s, path := openTest(t, govault.WithJanitorInterval(0))
	raw := openRaw(t, path)
	ctx := context.Background()

	if err := s.Set(ctx, "ephemeral", []byte("v"), 80*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Hidden by the API ...
	if ok, err := s.Exists(ctx, "ephemeral"); err != nil || ok {
		t.Fatalf("Exists after expiry: got %v, %v", ok, err)
	}
	// ... but still on disk, waiting to be overwritten or purged later.
	if n := rawCount(t, raw, "SELECT COUNT(*) FROM kv WHERE key = ?", "ephemeral"); n != 1 {
		t.Fatalf("expired row should remain on disk when janitor is off (count=%d)", n)
	}
}

func TestPurgeOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "purge.db")
	ctx := context.Background()

	// Close the store before the TTL elapses, with no janitor running.
	s1, err := govault.Open(path, govault.WithJanitorInterval(0))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Set(ctx, "ephemeral", []byte("v"), 100*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s1.Set(ctx, "durable", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	// Reopening purges keys that expired while the store was closed.
	s2, err := govault.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	raw := openRaw(t, path)
	if n := rawCount(t, raw, "SELECT COUNT(*) FROM kv WHERE key = ?", "ephemeral"); n != 0 {
		t.Fatalf("expired row survived reopen purge (count=%d)", n)
	}
	if n := rawCount(t, raw, "SELECT COUNT(*) FROM kv WHERE key = ?", "durable"); n != 1 {
		t.Fatalf("durable row lost on reopen (count=%d)", n)
	}
}
