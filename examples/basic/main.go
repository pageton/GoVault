// Command basic demonstrates the GoVault lifecycle: open a store, write and
// read values, overwrite, check existence, delete, close — and reopen to see
// that everything survived on disk.
//
//	go run github.com/pageton/GoVault/examples/basic
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pageton/GoVault"
	_ "github.com/pageton/GoVault/purego" // registers the default SQLite driver
)

// freshPath returns a database path with no leftovers from earlier runs, so
// the example's output is deterministic.
func freshPath(name string) string {
	p := filepath.Join(os.TempDir(), name)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(p + suffix)
	}
	return p
}

func main() {
	ctx := context.Background()
	path := freshPath("govault-basic.vault")

	v, err := govault.Open(path)
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}

	// Store a couple of values.
	if err := v.Set(ctx, "greeting", []byte("hello")); err != nil {
		fmt.Println("set:", err)
		os.Exit(1)
	}
	_ = v.Set(ctx, "user:1", []byte("alice"))

	// Read one back. Get returns a caller-owned copy of the bytes.
	value, err := v.Get(ctx, "greeting")
	fmt.Printf("get greeting     = %q (err = %v)\n", value, err)

	// Set upserts: writing again replaces the value.
	_ = v.Set(ctx, "greeting", []byte("hello, GoVault"))
	value, _ = v.Get(ctx, "greeting")
	fmt.Printf("after overwrite  = %q\n", value)

	// Existence checks and deletes. Delete reports how many keys existed.
	exists, _ := v.Exists(ctx, "greeting")
	n, _ := v.Delete(ctx, "greeting", "never-existed")
	fmt.Printf("existed?         = %v, deleted %d key(s)\n", exists, n)
	if _, err := v.Get(ctx, "greeting"); err == govault.ErrKeyNotFound {
		fmt.Println("get after delete = govault.ErrKeyNotFound")
	}

	count, _ := v.Count(ctx, "")
	fmt.Printf("keys in store    = %d\n", count)
	v.Close()

	// Reopen: durable keys are exactly where we left them.
	v2, err := govault.Open(path)
	if err != nil {
		fmt.Println("reopen:", err)
		os.Exit(1)
	}
	defer v2.Close()
	value, _ = v2.Get(ctx, "user:1")
	fmt.Printf("after reopen     = user:1 -> %q\n", value)
}
