# GoVault

A lightweight, high-performance embedded key-value store for Go, powered by SQLite.

GoVault maps string keys to `[]byte` values in a single SQLite database file —
durable, crash-safe, and safe for concurrent use from many goroutines, with
zero configuration and zero CGo.

```go
v, err := govault.Open("app.vault")
if err != nil {
	log.Fatal(err)
}
defer v.Close()

ctx := context.Background()
_ = v.Set(ctx, "session:abc", []byte("payload"), 5*time.Minute)

value, err := v.Get(ctx, "session:abc") // → []byte("payload")
```

## Features

- **Simple, idiomatic Go API** — context-aware, `[]byte` values, sentinel errors
- **Persistent** — one SQLite file, WAL journaling, crash-safe
- **Core operations** — `Get`, `Set`, `Delete`, `Exists`, plus `Keys`, `Scan`, `Count`
- **Atomic batches** — `Batch` groups writes into one transaction (commit, rollback, or panic → rollback)
- **TTL / key expiration** — optional per-key TTL, expired keys are hidden instantly and physically purged by a background janitor
- **Prefix scanning** — streamed, ordered, early-exit; no `LIKE`, pure B-tree range scans
- **Concurrency-safe** — many concurrent readers and writers; WAL mode means readers never block the writer
- **Efficient** — prepared statements on the hot path, `WITHOUT ROWID` clustered storage, partial expiry index

## Install

```sh
go get github.com/pageton/GoVault
```

GoVault uses [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite), a
pure-Go (CGo-free) SQLite driver, so it cross-compiles anywhere Go does.

## Usage

### Open and close

```go
v, err := govault.Open("app.vault") // created if missing; parent dir must exist
if err != nil {
	log.Fatal(err)
}
defer v.Close() // stops the janitor, checkpoints the WAL, closes the pool
```

`Open(":memory:")` gives you a private in-memory store (the pool is pinned to
one connection automatically).

### Get, Set, Delete, Exists

```go
ctx := context.Background()

// Set upserts; overwrite any previous value and TTL.
err := v.Set(ctx, "user:1", []byte("alice"))

// Optional TTL (at most one) — zero or negative expires immediately.
err = v.Set(ctx, "session:abc", []byte("..."), 30*time.Minute)

value, err := v.Get(ctx, "user:1") // []byte("alice"), caller-owned copy
// err == govault.ErrKeyNotFound when absent or expired

ok, err := v.Exists(ctx, "user:1") // true

n, err := v.Delete(ctx, "user:1", "user:2") // deletes atomically; n = how many existed
```

Values are arbitrary bytes; encode JSON/protobuf/whatever at the call site.

### Batch (transactions)

```go
err := v.Batch(ctx, func(tx *govault.Tx) error {
	for i := 0; i < 1000; i++ {
		if err := tx.Set(ctx, fmt.Sprintf("row:%04d", i), data(i)); err != nil {
			return err // returning non-nil rolls everything back
		}
	}
	return nil // returning nil commits
})
```

A `Tx` exposes the same operations as the `Store` (`Get`, `Set`, `Delete`,
`Exists`, `Keys`, `Scan`, `Count`) and reads its own uncommitted writes. If
the function panics, the transaction is rolled back and the panic re-raised.

Batches run as `BEGIN IMMEDIATE`: concurrent batches queue on the busy timeout
rather than failing mid-transaction. Don't nest `Batch` calls — the inner one
fails once the busy timeout elapses (see `WithBusyTimeout`).

### Prefix scanning

```go
// Streams in lexicographic key order; constant memory; fn returning false stops.
err := v.Scan(ctx, "user:", func(key string, value []byte) bool {
	fmt.Println(key, string(value))
	return true
})

keys, err := v.Keys(ctx, "user:") // collected variant
n, err := v.Count(ctx, "user:")   // fast COUNT via the primary key index
```

### TTL / expiration

- `Set` with a TTL hides the key from every operation the moment it expires.
- Expired rows are physically deleted by a background janitor (default: every
  30s) using a partial index, and purged once at `Open` for keys that expired
  while the store was closed.
- `WithJanitorInterval(d)` tunes the sweep; a non-positive `d` disables it
  (expired keys then linger on disk, hidden, until overwritten).

### Options

```go
v, err := govault.Open("app.vault",
	govault.WithBusyTimeout(10*time.Second), // write-lock patience, default 5s
	govault.WithMaxOpenConns(8),             // pool cap, default uncapped
	govault.WithJanitorInterval(time.Minute), // sweep cadence, default 30s
)
```

## Storage layout

```sql
CREATE TABLE kv (
	key        TEXT NOT NULL PRIMARY KEY, -- BINARY collation: byte order
	value      BLOB NOT NULL,
	expires_at INTEGER                     -- Unix nanoseconds; NULL = forever
) WITHOUT ROWID;
CREATE INDEX kv_expires_at ON kv (expires_at) WHERE expires_at IS NOT NULL;
```

- `WITHOUT ROWID` makes the table a pure B-tree keyed by the primary key:
  point lookups and ordered range scans are single seeks.
- Prefix scans are computed key ranges (`key >= prefix AND key < upper`),
  never `LIKE` — case-safe and index-native.
- The database runs in WAL mode with `synchronous=NORMAL`; pragmas are set
  per connection via the DSN so every pooled connection applies them.
- `Close` truncates the WAL back into the main file, leaving a self-contained
  database you can copy or back up.

## Benchmarks

Ryzen 5 7600X, Go 1.27, linux/amd64, 128-byte values, 1000-key store
(`go test -bench=. -benchmem`):

| Benchmark            | ns/op   | B/op  | allocs/op |
|----------------------|---------|-------|-----------|
| Set                  | 19,371  | 312   | 10        |
| SetWithTTL           | 29,362  | 320   | 11        |
| Get                  | 4,187   | 944   | 24        |
| GetMissing           | 3,627   | 648   | 20        |
| Exists               | 4,025   | 636   | 19        |
| Delete               | 17,791  | 176   | 8         |
| Scan100of1000        | 45,844  | 36104 | 724       |
| Batch100Sets         | 329,049 | 32786 | 1123      |
| GetParallel          | 2,678   | 1079  | 27        |
| SetParallel          | 7,959   | 312   | 11        |

That's roughly 240k reads/s single-threaded, 370k/s across goroutines, and
~50k writes/s single-threaded (writes serialize on SQLite's single write
lock, as in any SQLite-backed store).

## Testing

```sh
go test -race ./...                    # full suite incl. concurrency + WAL behavior
go test -bench=. -benchmem ./...       # benchmarks
```

## Notes & limits

- One writer at a time (SQLite): writes queue on the busy timeout; reads run
  concurrently with writes thanks to WAL.
- Keys and values: keys are Go strings (compared as bytes), values are
  arbitrary `[]byte` up to SQLite's ~1 GB blob limit (keep them small for
  latency).
- The store directory must exist before `Open`.
- Swapping in a CGo driver (e.g. `mattn/go-sqlite3`) is a future extension if
  raw throughput ever trumps build portability.

## License

[MIT](LICENSE) © pageton
