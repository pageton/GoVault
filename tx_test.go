package govault

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var errDeliberate = errors.New("deliberate failure")

func TestBatchCommit(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	err := s.Batch(ctx, func(tx *Tx) error {
		for _, k := range []string{"a", "b", "c"} {
			if err := tx.Set(ctx, k, []byte(k)); err != nil {
				return err
			}
		}
		// The transaction reads its own writes.
		got, err := tx.Get(ctx, "b")
		if err != nil || string(got) != "b" {
			return fmt.Errorf("tx.Get own write: got %q, %v", got, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if got, err := s.Get(ctx, k); err != nil || string(got) != k {
			t.Fatalf("Get %s after commit: got %q, %v", k, got, err)
		}
	}
}

func TestBatchRollback(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	if err := s.Set(ctx, "keep", []byte("me")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	err := s.Batch(ctx, func(tx *Tx) error {
		if err := tx.Set(ctx, "transient", []byte("x")); err != nil {
			return err
		}
		if _, err := tx.Delete(ctx, "keep"); err != nil {
			return err
		}
		return errDeliberate
	})
	if !errors.Is(err, errDeliberate) {
		t.Fatalf("Batch error: got %v, want errDeliberate", err)
	}
	if _, err := s.Get(ctx, "transient"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("rolled-back write is visible: got %v", err)
	}
	if got, err := s.Get(ctx, "keep"); err != nil || string(got) != "me" {
		t.Fatalf("rolled-back delete lost data: got %q, %v", got, err)
	}
}

func TestBatchPanicRollsBack(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Fatalf("Batch should re-panic the original value: got %v", r)
			}
		}()
		_ = s.Batch(ctx, func(tx *Tx) error {
			if err := tx.Set(ctx, "transient", []byte("x")); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	// The write made before the panic must not survive the rollback.
	if _, err := s.Get(ctx, "transient"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("write survived panic rollback: got %v", err)
	}
}

func TestBatchOperations(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	for _, k := range []string{"p:1", "p:2", "q:1", "gone"} {
		if err := s.Set(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	err := s.Batch(ctx, func(tx *Tx) error {
		if n, err := tx.Delete(ctx, "gone"); err != nil || n != 1 {
			return fmt.Errorf("tx.Delete: %d, %v", n, err)
		}
		if err := tx.Set(ctx, "p:3", []byte("v")); err != nil {
			return err
		}
		if err := tx.Set(ctx, "t:1", []byte("v"), time.Minute); err != nil {
			return err
		}
		if ok, err := tx.Exists(ctx, "gone"); err != nil || ok {
			return fmt.Errorf("tx.Exists after delete: %v, %v", ok, err)
		}
		if ok, err := tx.Exists(ctx, "p:3"); err != nil || !ok {
			return fmt.Errorf("tx.Exists after set: %v, %v", ok, err)
		}
		if n, err := tx.Count(ctx, "p:"); err != nil || n != 3 {
			return fmt.Errorf("tx.Count: %d, %v", n, err)
		}
		keys, err := tx.Keys(ctx, "p:")
		if err != nil || fmt.Sprint(keys) != "[p:1 p:2 p:3]" {
			return fmt.Errorf("tx.Keys: %v, %v", keys, err)
		}
		scanned := 0
		if err := tx.Scan(ctx, "p:", func(string, []byte) bool { scanned++; return true }); err != nil {
			return err
		}
		if scanned != 3 {
			return fmt.Errorf("tx.Scan visited %d keys, want 3", scanned)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
}

func TestBatchNilFn(t *testing.T) {
	s := openTest(t)
	if err := s.Batch(context.Background(), nil); err == nil {
		t.Fatal("Batch(nil) should fail")
	}
}

func TestNestedBatchFails(t *testing.T) {
	// A nested Batch cannot take the write lock the outer one already holds;
	// it must fail with the busy timeout instead of deadlocking.
	s := openTest(t, WithBusyTimeout(150*time.Millisecond))
	ctx := context.Background()

	err := s.Batch(ctx, func(tx *Tx) error {
		if err := tx.Set(ctx, "outer", []byte("v")); err != nil {
			return err
		}
		inner := s.Batch(ctx, func(tx *Tx) error {
			return tx.Set(ctx, "inner", []byte("v"))
		})
		if inner == nil {
			return errors.New("nested Batch unexpectedly succeeded")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("outer Batch: %v", err)
	}
	if got, err := s.Get(ctx, "outer"); err != nil || string(got) != "v" {
		t.Fatalf("outer write lost: got %q, %v", got, err)
	}
}

func TestReadDuringOpenBatch(t *testing.T) {
	// WAL mode: readers proceed while a write transaction is open, seeing the
	// pre-transaction snapshot.
	s := openTest(t)
	ctx := context.Background()

	if err := s.Set(ctx, "committed", []byte("before")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	inside := make(chan struct{})
	release := make(chan struct{})
	batchErr := make(chan error, 1)
	go func() {
		batchErr <- s.Batch(ctx, func(tx *Tx) error {
			if err := tx.Set(ctx, "pending", []byte("during")); err != nil {
				return err
			}
			close(inside)
			<-release
			return nil
		})
	}()

	<-inside
	if got, err := s.Get(ctx, "committed"); err != nil || string(got) != "before" {
		t.Errorf("read during open write tx: got %q, %v", got, err)
	}
	if _, err := s.Get(ctx, "pending"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("uncommitted write is visible outside tx: got %v", err)
	}
	close(release)
	if err := <-batchErr; err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if got, err := s.Get(ctx, "pending"); err != nil || string(got) != "during" {
		t.Fatalf("committed write missing: got %q, %v", got, err)
	}
}
