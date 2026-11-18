// Command ttl demonstrates key expiration: an optional TTL on Set, lazy
// expiry (expired keys vanish from every operation), and the background
// janitor that physically reclaims the rows.
//
//	go run github.com/pageton/GoVault/examples/ttl
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pageton/GoVault"
	_ "github.com/pageton/GoVault/purego" // registers the default SQLite driver
)

func main() {
	ctx := context.Background()

	// Sweep expired rows every second so the demo is quick to watch.
	v, err := govault.Open(filepath.Join(os.TempDir(), "govault-ttl.vault"),
		govault.WithJanitorInterval(time.Second))
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer v.Close()

	// A session key that lives for 1.5s, and a counter that lives forever.
	if err := v.Set(ctx, "session:token", []byte("secret"), 1500*time.Millisecond); err != nil {
		fmt.Println("set:", err)
		os.Exit(1)
	}
	_ = v.Set(ctx, "counter", []byte("42"))

	// Poll the session until it expires. Nothing needs to wake it up: the
	// moment the TTL elapses, the key is invisible to every operation.
	for i := 0; i <= 8; i++ {
		ok, _ := v.Exists(ctx, "session:token")
		fmt.Printf("t=%4.1fs  exists(session:token) = %-5v  store keys = %d\n",
			float64(i)*300*time.Millisecond.Seconds(), ok, mustCount(v, "session:"))
		time.Sleep(300 * time.Millisecond)
	}

	// The durable key never noticed any of this.
	value, _ := v.Get(ctx, "counter")
	fmt.Printf("counter still readable = %q\n", value)

	// Overwriting a TTL key without a TTL removes its expiry.
	_ = v.Set(ctx, "session:token", []byte("renewed"), time.Second)
	_ = v.Set(ctx, "session:token", []byte("permanent"))
	time.Sleep(1200 * time.Millisecond) // past the original deadline
	value, _ = v.Get(ctx, "session:token")
	fmt.Printf("after TTL-stripping overwrite = %q\n", value)
}

func mustCount(v *govault.Store, prefix string) int64 {
	n, err := v.Count(context.Background(), prefix)
	if err != nil {
		fmt.Println("count:", err)
		os.Exit(1)
	}
	return n
}
