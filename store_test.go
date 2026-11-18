package govault

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
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

func TestOpenFileURI(t *testing.T) {
	path := "file:" + filepath.Join(t.TempDir(), "uri.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open with file: URI: %v", err)
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

func TestWithMaxOpenConns(t *testing.T) {
	s := openTest(t, WithMaxOpenConns(2))
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				key := fmt.Sprintf("g%d:%02d", g, i)
				if err := s.Set(ctx, key, []byte("v")); err != nil {
					t.Errorf("Set %s: %v", key, err)
					return
				}
				if _, err := s.Get(ctx, key); err != nil {
					t.Errorf("Get %s: %v", key, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if n, err := s.Count(ctx, ""); err != nil || n != 100 {
		t.Fatalf("Count: got %d, %v; want 100", n, err)
	}
}

func TestContextCancellation(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := s.Get(cancelled, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with cancelled context: got %v", err)
	}
	if err := s.Set(cancelled, "k2", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Set with cancelled context: got %v", err)
	}
	if _, err := s.Delete(cancelled, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Delete with cancelled context: got %v", err)
	}
	if _, err := s.Exists(cancelled, "k"); !errors.Is(err, context.Canceled) {
		t.Errorf("Exists with cancelled context: got %v", err)
	}
	if _, err := s.Keys(cancelled, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("Keys with cancelled context: got %v", err)
	}
	if err := s.Scan(cancelled, "", func(string, []byte) bool { return true }); !errors.Is(err, context.Canceled) {
		t.Errorf("Scan with cancelled context: got %v", err)
	}
	if _, err := s.Count(cancelled, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("Count with cancelled context: got %v", err)
	}
	if err := s.Batch(cancelled, func(tx *Tx) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("Batch with cancelled context: got %v", err)
	}

	// A cancelled call must not poison the pool for later operations.
	if got, err := s.Get(ctx, "k"); err != nil || string(got) != "v" {
		t.Errorf("Get after cancelled operations: got %q, %v", got, err)
	}
}
