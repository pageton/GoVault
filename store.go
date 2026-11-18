// Package govault provides a lightweight, embedded key-value store backed by
// SQLite.
//
// A Store maps string keys to byte-slice values in a single SQLite database
// file, journaling in WAL mode so readers never block the writer and vice
// versa. Keys may carry a time-to-live: they are hidden as soon as they expire
// and are physically removed shortly after by an optional background janitor.
// Writes can be grouped atomically with Batch.
//
// A Store is safe for concurrent use by multiple goroutines:
//
//	v, err := govault.Open("app.vault")
//	if err != nil {
//		return err
//	}
//	defer v.Close()
//
//	if err := v.Set(ctx, "greeting", []byte("hello")); err != nil {
//		return err
//	}
//
// All operations accept a context and abort when that context is cancelled.
package govault

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

// schemaStatements create the single table backing the store:
//
//	key        TEXT    primary key, compared as raw bytes (BINARY collation)
//	value      BLOB    arbitrary bytes, never NULL
//	expires_at INTEGER absolute expiry as Unix nanoseconds, NULL = no TTL
//
// WITHOUT ROWID keeps the table a pure B-tree keyed by the primary key, which
// makes point lookups and ordered scans single seek operations. The partial
// index lets the janitor delete expired rows without scanning the table.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS kv (
		key        TEXT NOT NULL PRIMARY KEY,
		value      BLOB NOT NULL,
		expires_at INTEGER
	) WITHOUT ROWID`,
	`CREATE INDEX IF NOT EXISTS kv_expires_at ON kv (expires_at) WHERE expires_at IS NOT NULL`,
}

// Store is a persistent key-value store backed by one SQLite database.
// All methods are safe for concurrent use by multiple goroutines.
type Store struct {
	db    *sql.DB
	stmts *stmtSet
	cfg   config

	closeOnce sync.Once
	closed    atomic.Bool

	janitorStop chan struct{}
	janitorDone chan struct{}
}

// Open opens (creating if necessary) the SQLite database at path and returns
// a ready-to-use Store. The parent directory of path must already exist.
//
// The database is opened in WAL mode with synchronous=NORMAL and an immediate
// transaction lock, and runs one cleanup pass for keys that expired while the
// store was closed. Unless disabled with WithJanitorInterval, a background
// janitor keeps removing expired keys until Close is called.
func Open(path string, opts ...Option) (*Store, error) {
	if path == "" {
		return nil, errors.New("govault: path must not be empty")
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	// An in-memory database is private to each connection, so pin the pool to
	// a single connection unless the caller made an explicit choice.
	if isMemoryPath(path) && cfg.maxOpenConns == 0 {
		cfg.maxOpenConns = 1
	}

	db, err := sql.Open("sqlite", dataSourceName(path, cfg))
	if err != nil {
		return nil, fmt.Errorf("govault: open %q: %w", path, err)
	}
	if cfg.maxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.maxOpenConns)
	}

	s := &Store{db: db, cfg: cfg}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.busyTimeout+5*time.Second)
	defer cancel()
	for _, stmt := range schemaStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("govault: initialize schema: %w", err)
		}
	}
	stmts, err := prepareStmts(ctx, db)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("govault: prepare statements: %w", err)
	}
	s.stmts = stmts
	_, _ = s.purgeExpired(ctx) // best effort: keys that expired while closed

	if cfg.janitorEnabled {
		s.startJanitor()
	}
	return s, nil
}

// Close stops the janitor, checkpoints the WAL back into the main database
// file, and closes the underlying connection pool. It is idempotent; later
// calls return nil.
func (s *Store) Close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.janitorStop != nil {
			close(s.janitorStop)
			<-s.janitorDone
		}
		s.closed.Store(true)
		s.stmts.close()
		s.checkpoint()
		err = s.db.Close()
	})
	return err
}

// checkpoint folds the WAL back into the database file so the on-disk file is
// self-contained. Failures are ignored: with other connections open the
// checkpoint simply cannot complete, and the WAL is recovered on next open.
func (s *Store) checkpoint() {
	var busy, log, checkpointed int
	_ = s.db.QueryRowContext(context.Background(),
		"PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &checkpointed)
}

func (s *Store) errClosed() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}

// purgeExpired physically deletes expired rows and returns how many went.
func (s *Store) purgeExpired(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, purgeExpiredSQL, nowNanos())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// startJanitor launches a goroutine that deletes expired keys every interval
// until Close signals janitorStop.
func (s *Store) startJanitor() {
	s.janitorStop = make(chan struct{})
	s.janitorDone = make(chan struct{})
	interval := s.cfg.janitorInterval
	go func() {
		defer close(s.janitorDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.janitorStop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				_, _ = s.purgeExpired(ctx)
				cancel()
			}
		}
	}()
}

// dataSourceName builds the SQLite DSN. Pragmas live in the DSN so that every
// pooled connection applies them, not just the first one. _txlock=immediate
// makes Batch transactions acquire the write lock up front, so concurrent
// writers queue on busy_timeout instead of failing mid-transaction.
func dataSourceName(path string, cfg config) string {
	var base string
	switch {
	case strings.HasPrefix(path, "file:"):
		base = path // caller-supplied URI, used as is
	case path == ":memory:":
		base = "file::memory:"
	default:
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
		base = (&url.URL{Scheme: "file", Path: path}).String()
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return fmt.Sprintf(
		"%s%s_txlock=immediate&_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)",
		base, sep, cfg.busyTimeout.Milliseconds(),
	)
}

func isMemoryPath(path string) bool {
	return path == ":memory:" || strings.Contains(path, "mode=memory")
}
