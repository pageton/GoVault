// Command concurrent demonstrates safe concurrent use of one Store: parallel
// writers, readers, and scanners at the same time. WAL journaling means
// readers proceed while writes are in flight; writers queue on the busy
// timeout instead of corrupting each other.
//
//	go run github.com/pageton/GoVault/examples/concurrent
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pageton/GoVault"
	_ "modernc.org/sqlite" // registers the default SQLite driver
)

const (
	writers         = 4
	keysPerWriter   = 250
	readerLoops     = 4
	scansPerScanner = 20
)

func main() {
	ctx := context.Background()

	v, err := govault.Open(filepath.Join(os.TempDir(), "govault-concurrent.vault"),
		govault.WithBusyTimeout(10*time.Second))
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer v.Close()

	start := time.Now()
	var wg sync.WaitGroup

	// Writers: each owns its own key prefix.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				key := fmt.Sprintf("writer%d:%04d", w, i)
				if err := v.Set(ctx, key, []byte(key)); err != nil {
					fmt.Println("set:", err)
					os.Exit(1)
				}
			}
		}(w)
	}

	// Readers: hammer reads and existence checks while writes land.
	for r := 0; r < readerLoops; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				key := fmt.Sprintf("writer%d:%04d", r%writers, i)
				if _, err := v.Get(ctx, key); err != nil && err != govault.ErrKeyNotFound {
					fmt.Println("get:", err)
					os.Exit(1)
				}
				if _, err := v.Exists(ctx, key); err != nil {
					fmt.Println("exists:", err)
					os.Exit(1)
				}
			}
		}(r)
	}

	// A scanner: full prefix scans racing against the writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < scansPerScanner; i++ {
			if err := v.Scan(ctx, "writer", func(string, []byte) bool { return true }); err != nil {
				fmt.Println("scan:", err)
				os.Exit(1)
			}
		}
	}()

	wg.Wait()

	n, err := v.Count(ctx, "writer")
	if err != nil {
		fmt.Println("count:", err)
		os.Exit(1)
	}
	fmt.Printf("writers=%d x %d keys, %d readers, %d concurrent scans\n",
		writers, keysPerWriter, readerLoops, scansPerScanner)
	fmt.Printf("final count = %d (want %d) in %v\n", n, writers*keysPerWriter, time.Since(start).Round(time.Millisecond))
}
