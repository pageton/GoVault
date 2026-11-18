package govaulttest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pageton/GoVault"
)

func TestConcurrentOps(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	const goroutines = 8
	const keysPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			prefix := fmt.Sprintf("g%d:", g)
			for i := 0; i < keysPerGoroutine; i++ {
				key := fmt.Sprintf("%s%03d", prefix, i)
				if err := s.Set(ctx, key, []byte(key)); err != nil {
					t.Errorf("Set %s: %v", key, err)
					return
				}
			}
			for i := 0; i < keysPerGoroutine; i++ {
				key := fmt.Sprintf("%s%03d", prefix, i)
				got, err := s.Get(ctx, key)
				if err != nil || string(got) != key {
					t.Errorf("Get %s: got %q, %v", key, got, err)
					return
				}
			}
			// Delete the even-numbered keys; half must remain.
			for i := 0; i < keysPerGoroutine; i += 2 {
				key := fmt.Sprintf("%s%03d", prefix, i)
				if _, err := s.Delete(ctx, key); err != nil {
					t.Errorf("Delete %s: %v", key, err)
					return
				}
			}
			if n, err := s.Count(ctx, prefix); err != nil || n != keysPerGoroutine/2 {
				t.Errorf("Count %s: got %d, %v; want %d", prefix, n, err, keysPerGoroutine/2)
			}
		}(g)
	}
	wg.Wait()

	if n, err := s.Count(ctx, ""); err != nil || n != goroutines*keysPerGoroutine/2 {
		t.Fatalf("final Count: got %d, %v; want %d", n, err, goroutines*keysPerGoroutine/2)
	}
}

func TestConcurrentBatches(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	const goroutines = 4
	const setsPerBatch = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			err := s.Batch(ctx, func(tx *govault.Tx) error {
				for i := 0; i < setsPerBatch; i++ {
					key := fmt.Sprintf("g%d:%03d", g, i)
					if err := tx.Set(ctx, key, []byte(key)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Errorf("Batch g%d: %v", g, err)
			}
		}(g)
	}
	wg.Wait()

	if n, err := s.Count(ctx, ""); err != nil || n != goroutines*setsPerBatch {
		t.Fatalf("Count after batches: got %d, %v; want %d", n, err, goroutines*setsPerBatch)
	}
}

func TestConcurrentMixedWorkload(t *testing.T) {
	// Writers, readers, and scanners hammering overlapping prefixes at the
	// same time; every operation must succeed or report ErrKeyNotFound.
	s, _ := openTest(t)
	ctx := context.Background()

	const writers = 4
	const keysPerWriter = 50
	stop := make(chan struct{})

	var workers sync.WaitGroup
	for w := 0; w < writers; w++ {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			for i := 0; i < keysPerWriter; i++ {
				key := fmt.Sprintf("shared:%d:%03d", w, i)
				if err := s.Set(ctx, key, []byte("v")); err != nil {
					t.Errorf("Set %s: %v", key, err)
					return
				}
			}
		}(w)
	}

	for r := 0; r < 4; r++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.Get(ctx, "shared:0:000"); err != nil && err != govault.ErrKeyNotFound {
					t.Errorf("Get: %v", err)
					return
				}
				if _, err := s.Exists(ctx, "shared:1:010"); err != nil {
					t.Errorf("Exists: %v", err)
					return
				}
			}
		}()
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.Scan(ctx, "shared:", func(string, []byte) bool { return true }); err != nil {
				t.Errorf("Scan: %v", err)
				return
			}
		}
	}()

	// Run the mixed phase until the writers finish, then stop the readers.
	deadline := time.After(2 * time.Second)
	for {
		if n, err := s.Count(ctx, "shared:"); err != nil {
			t.Fatalf("Count: %v", err)
		} else if n == writers*keysPerWriter {
			break
		}
		select {
		case <-deadline:
			t.Fatal("writers did not finish in time")
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(stop)
	workers.Wait()

	if n, err := s.Count(ctx, "shared:"); err != nil || n != writers*keysPerWriter {
		t.Fatalf("final Count: got %d, %v; want %d", n, err, writers*keysPerWriter)
	}
}
