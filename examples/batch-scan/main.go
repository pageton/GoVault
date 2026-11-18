// Command batch-scan demonstrates atomic writes with Batch (commit and
// rollback) and streamed, ordered prefix scans with early termination.
//
//	go run github.com/pageton/GoVault/examples/batch-scan
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pageton/GoVault"
	_ "github.com/pageton/GoVault/purego" // registers the default SQLite driver
)

var errSimulated = errors.New("simulated failure mid-batch")

func main() {
	ctx := context.Background()

	v, err := govault.Open(filepath.Join(os.TempDir(), "govault-batch.vault"))
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer v.Close()

	// A batch that fails partway rolls back completely: neither its writes
	// nor its deletes are kept.
	err = v.Batch(ctx, func(tx *govault.Tx) error {
		_ = tx.Set(ctx, "user:1", []byte("would-be-alice"))
		_, _ = tx.Delete(ctx, "user:1")
		return errSimulated // pretend something went wrong
	})
	fmt.Printf("failed batch        = %v\n", err)
	n, _ := v.Count(ctx, "user:")
	fmt.Printf("live user keys after rollback = %d\n", n)

	// A batch that returns nil commits everything atomically, and the
	// transaction reads its own writes as it goes.
	err = v.Batch(ctx, func(tx *govault.Tx) error {
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("user:%d", i)
			value := fmt.Sprintf("member-%d", i)
			if err := tx.Set(ctx, key, []byte(value)); err != nil {
				return err
			}
		}
		ok, err := tx.Exists(ctx, "user:3")
		if err != nil {
			return err
		}
		seen, err := tx.Count(ctx, "user:")
		if err != nil {
			return err
		}
		fmt.Printf("inside the tx       = user:3 exists? %v, %d users written\n", ok, seen)
		return nil
	})
	fmt.Printf("committed batch     = %v\n", err)

	// Scan streams matching entries in key order; return false to stop early.
	visited := 0
	fmt.Println("scan \"user:\" (stopping after 2 entries):")
	err = v.Scan(ctx, "user:", func(key string, value []byte) bool {
		fmt.Printf("  %s -> %s\n", key, value)
		visited++
		return visited < 2
	})
	if err != nil {
		fmt.Println("scan:", err)
		os.Exit(1)
	}

	keys, _ := v.Keys(ctx, "user:")
	fmt.Printf("all live user keys  = %v\n", keys)
}
