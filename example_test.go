package govault_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pageton/GoVault"
)

func Example() {
	v, err := govault.Open(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer v.Close()

	ctx := context.Background()

	// Store values, optionally with a time-to-live.
	_ = v.Set(ctx, "user:1", []byte("alice"), 24*time.Hour)
	_ = v.Set(ctx, "user:2", []byte("bob"))

	// Read them back.
	value, _ := v.Get(ctx, "user:1")
	fmt.Println("user:1 =", string(value))

	// Scan everything under a prefix, streaming in key order.
	_ = v.Scan(ctx, "user:", func(key string, value []byte) bool {
		fmt.Println(key, "->", string(value))
		return true
	})

	// Group writes atomically.
	err = v.Batch(ctx, func(tx *govault.Tx) error {
		_ = tx.Set(ctx, "session:abc", []byte("payload"), 5*time.Minute)
		_, _ = tx.Delete(ctx, "user:2")
		return nil
	})
	fmt.Println("batch err:", err)

	n, _ := v.Count(ctx, "user:")
	fmt.Println("users left:", n)

	// Output:
	// user:1 = alice
	// user:1 -> alice
	// user:2 -> bob
	// batch err: <nil>
	// users left: 1
}
