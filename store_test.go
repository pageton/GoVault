package govault

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenClose(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil { // idempotent
		t.Fatalf("second Close: %v", err)
	}
}

func TestOpenEmptyPath(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("Open(\"\") should fail")
	}
}

func TestOpenMissingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does", "not", "exist", "test.db")
	if _, err := Open(path); err == nil {
		t.Fatal("Open on a path with a missing parent directory should fail")
	}
}

func TestOpsAfterClose(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := s.Get(ctx, "k"); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close: got %v, want ErrClosed", err)
	}
	if err := s.Set(ctx, "k", nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Set after Close: got %v, want ErrClosed", err)
	}
	if _, err := s.Delete(ctx, "k"); !errors.Is(err, ErrClosed) {
		t.Errorf("Delete after Close: got %v, want ErrClosed", err)
	}
	if _, err := s.Exists(ctx, "k"); !errors.Is(err, ErrClosed) {
		t.Errorf("Exists after Close: got %v, want ErrClosed", err)
	}
	if _, err := s.Keys(ctx, ""); !errors.Is(err, ErrClosed) {
		t.Errorf("Keys after Close: got %v, want ErrClosed", err)
	}
	if err := s.Scan(ctx, "", func(string, []byte) bool { return true }); !errors.Is(err, ErrClosed) {
		t.Errorf("Scan after Close: got %v, want ErrClosed", err)
	}
	if _, err := s.Count(ctx, ""); !errors.Is(err, ErrClosed) {
		t.Errorf("Count after Close: got %v, want ErrClosed", err)
	}
	if err := s.Batch(ctx, func(tx *Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("Batch after Close: got %v, want ErrClosed", err)
	}
}

func TestInMemory(t *testing.T) {
	ctx := context.Background()

	a, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	defer a.Close()
	if err := a.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := a.Get(ctx, "k"); err != nil || string(got) != "v" {
		t.Fatalf("Get: got %q, %v; want \"v\", nil", got, err)
	}

	// A second in-memory store must not see the first one's data.
	b, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	defer b.Close()
	if _, err := b.Get(ctx, "k"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get from second :memory: store: got %v, want ErrKeyNotFound", err)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.db")
	ctx := context.Background()

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s1.Set(ctx, "plain", []byte("keeps")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := s1.Set(ctx, "temporary", []byte("expires"), 2*time.Second); err != nil {
		t.Fatalf("Set with TTL: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got, err := s2.Get(ctx, "plain"); err != nil || string(got) != "keeps" {
		t.Fatalf("Get after reopen: got %q, %v", got, err)
	}
	if got, err := s2.Get(ctx, "temporary"); err != nil || string(got) != "expires" {
		t.Fatalf("Get TTL key after reopen (well before expiry): got %q, %v", got, err)
	}
}

func TestTwoStoresSameFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	ctx := context.Background()

	a, err := Open(path)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer a.Close()
	b, err := Open(path)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer b.Close()

	if err := a.Set(ctx, "from-a", []byte("1")); err != nil {
		t.Fatalf("Set via A: %v", err)
	}
	if got, err := b.Get(ctx, "from-a"); err != nil || string(got) != "1" {
		t.Fatalf("Get via B: got %q, %v", got, err)
	}
	if err := b.Set(ctx, "from-b", []byte("2")); err != nil {
		t.Fatalf("Set via B: %v", err)
	}
	if got, err := a.Get(ctx, "from-b"); err != nil || string(got) != "2" {
		t.Fatalf("Get via A: got %q, %v", got, err)
	}
}
