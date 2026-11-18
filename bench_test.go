package govault

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// benchStore opens a Store on a fresh temporary database and pre-populates it
// with size keys of the form "key:<n>%07d" mapped to 128-byte values.
func benchStore(b *testing.B, size int) (*Store, []byte) {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "bench.db"))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { s.Close() })

	value := make([]byte, 128)
	for i := range value {
		value[i] = byte(i)
	}
	ctx := context.Background()
	err = s.Batch(ctx, func(tx *Tx) error {
		for i := 0; i < size; i++ {
			if err := tx.Set(ctx, fmt.Sprintf("key:%07d", i), value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatalf("populate: %v", err)
	}
	return s, value
}

func BenchmarkSet(b *testing.B) {
	s, value := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set(ctx, fmt.Sprintf("key:%07d", i), value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	s, value := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.Set(ctx, fmt.Sprintf("key:%07d", i), value, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get(ctx, fmt.Sprintf("key:%07d", i%1000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetMissing(b *testing.B) {
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Get(ctx, "absent"); err != ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func BenchmarkExists(b *testing.B) {
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Exists(ctx, fmt.Sprintf("key:%07d", i%1000)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	s, _ := benchStore(b, b.N) // each iteration deletes its own key
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Delete(ctx, fmt.Sprintf("key:%07d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScan100of1000(b *testing.B) {
	// "key:0000099" is the 100th key with prefix "key:00000" in a table of
	// 1000 keys, so each scan walks 100 streamed rows.
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seen := 0
		if err := s.Scan(ctx, "key:00000", func(string, []byte) bool { seen++; return true }); err != nil {
			b.Fatal(err)
		}
		if seen != 100 {
			b.Fatalf("scan visited %d keys, want 100", seen)
		}
	}
}

func BenchmarkKeysAll1000(b *testing.B) {
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keys, err := s.Keys(ctx, "")
		if err != nil {
			b.Fatal(err)
		}
		if len(keys) != 1000 {
			b.Fatalf("got %d keys, want 1000", len(keys))
		}
	}
}

func BenchmarkBatch100Sets(b *testing.B) {
	s, value := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := s.Batch(ctx, func(tx *Tx) error {
			for j := 0; j < 100; j++ {
				if err := tx.Set(ctx, fmt.Sprintf("batch:%d:%03d", i, j), value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := s.Get(ctx, fmt.Sprintf("key:%07d", i%1000)); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

func BenchmarkSetParallel(b *testing.B) {
	// Concurrent writers serialize on SQLite's single write lock; this
	// measures sustained write throughput under contention.
	s, _ := benchStore(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := s.Set(ctx, fmt.Sprintf("par:%07d", i), []byte("value")); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}
