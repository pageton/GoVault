package govault

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// runner is implemented by both *sql.DB and *sql.Tx, letting Store and Tx
// share one implementation of the streaming and counting operations.
type runner interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// preparer is implemented by both *sql.DB and *sql.Tx.
type preparer interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}

// aliveCond matches rows whose TTL has not elapsed yet. Its argument is the
// current time as Unix nanoseconds.
const aliveCond = "(expires_at IS NULL OR expires_at > ?)"

const (
	getSQL = "SELECT value FROM kv WHERE key = ? AND " + aliveCond
	setSQL = `INSERT INTO kv (key, value, expires_at) VALUES (?, ?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value, expires_at = excluded.expires_at`
	deleteSQL       = "DELETE FROM kv WHERE key = ?"
	existsSQL       = "SELECT 1 FROM kv WHERE key = ? AND " + aliveCond + " LIMIT 1"
	purgeExpiredSQL = "DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?"
)

func nowNanos() int64 { return time.Now().UnixNano() }

// stmtSet holds prepared statements for the point operations, which are the
// hot path: reusing them lets every connection skip re-compiling the SQL.
type stmtSet struct {
	get    *sql.Stmt
	set    *sql.Stmt
	del    *sql.Stmt
	exists *sql.Stmt
}

func prepareStmts(ctx context.Context, p preparer) (*stmtSet, error) {
	var ss stmtSet
	prepare := func(dst **sql.Stmt, query string) error {
		stmt, err := p.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		*dst = stmt
		return nil
	}
	if err := prepare(&ss.get, getSQL); err != nil {
		return nil, err
	}
	if err := prepare(&ss.set, setSQL); err != nil {
		ss.close()
		return nil, err
	}
	if err := prepare(&ss.del, deleteSQL); err != nil {
		ss.close()
		return nil, err
	}
	if err := prepare(&ss.exists, existsSQL); err != nil {
		ss.close()
		return nil, err
	}
	return &ss, nil
}

func (ss *stmtSet) close() {
	if ss == nil {
		return
	}
	for _, stmt := range []*sql.Stmt{ss.get, ss.set, ss.del, ss.exists} {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
}

// Get returns the value stored under key. The returned slice is owned by the
// caller. It returns ErrKeyNotFound if the key was never set or its TTL has
// already elapsed.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.errClosed(); err != nil {
		return nil, err
	}
	return getValue(ctx, s.stmts, key)
}

// Set stores value under key, overwriting any previous value and TTL. A nil
// value is stored as empty. An optional TTL (at most one) makes the key
// invisible to all operations once it elapses; a zero or negative TTL expires
// the key immediately.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	if err := s.errClosed(); err != nil {
		return err
	}
	return setValue(ctx, s.stmts, key, value, ttl...)
}

// Delete removes keys and returns how many of them existed (expired keys
// count as absent). Deleting one or more keys is atomic: either all listed
// keys are gone afterwards, or none are.
func (s *Store) Delete(ctx context.Context, keys ...string) (int64, error) {
	if err := s.errClosed(); err != nil {
		return 0, err
	}
	switch len(keys) {
	case 0:
		return 0, nil
	case 1:
		return deleteKeys(ctx, s.stmts, keys...)
	default:
		var n int64
		err := s.Batch(ctx, func(tx *Tx) error {
			var derr error
			n, derr = deleteKeys(ctx, tx.stmts, keys...)
			return derr
		})
		return n, err
	}
}

// Exists reports whether key is present and has not expired.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	if err := s.errClosed(); err != nil {
		return false, err
	}
	return existsValue(ctx, s.stmts, key)
}

// Keys returns all live keys that start with prefix, in lexicographic byte
// order. An empty prefix matches every key. For large data sets prefer Scan,
// which streams instead of collecting.
func (s *Store) Keys(ctx context.Context, prefix string) ([]string, error) {
	if err := s.errClosed(); err != nil {
		return nil, err
	}
	return collectKeys(ctx, s.db, prefix)
}

// Scan iterates live entries whose key starts with prefix, in lexicographic
// byte order, calling fn for each one. When fn returns false the iteration
// stops. Rows are streamed, so memory use stays constant regardless of how
// many keys match. An empty prefix scans the whole store.
func (s *Store) Scan(ctx context.Context, prefix string, fn func(key string, value []byte) bool) error {
	if err := s.errClosed(); err != nil {
		return err
	}
	return scanPrefix(ctx, s.db, prefix, fn)
}

// Count returns how many live keys start with prefix. An empty prefix counts
// every key in the store.
func (s *Store) Count(ctx context.Context, prefix string) (int64, error) {
	if err := s.errClosed(); err != nil {
		return 0, err
	}
	return countPrefix(ctx, s.db, prefix)
}

// getValue, setValue, and friends implement the operations once so Store and
// Tx do not duplicate SQL.

func getValue(ctx context.Context, ss *stmtSet, key string) ([]byte, error) {
	var value []byte
	err := ss.get.QueryRowContext(ctx, key, nowNanos()).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("govault: get %q: %w", key, err)
	}
	return value, nil
}

func setValue(ctx context.Context, ss *stmtSet, key string, value []byte, ttl ...time.Duration) error {
	if value == nil {
		value = []byte{} // the value column is NOT NULL
	}
	var expiresAt any // nil binds to SQL NULL: no expiry
	if len(ttl) > 0 {
		expiresAt = time.Now().Add(ttl[0]).UnixNano()
	}
	if _, err := ss.set.ExecContext(ctx, key, value, expiresAt); err != nil {
		return fmt.Errorf("govault: set %q: %w", key, err)
	}
	return nil
}

func deleteKeys(ctx context.Context, ss *stmtSet, keys ...string) (int64, error) {
	var total int64
	for _, key := range keys {
		res, err := ss.del.ExecContext(ctx, key)
		if err != nil {
			return total, fmt.Errorf("govault: delete %q: %w", key, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			total += n
		}
	}
	return total, nil
}

func existsValue(ctx context.Context, ss *stmtSet, key string) (bool, error) {
	var one int
	err := ss.exists.QueryRowContext(ctx, key, nowNanos()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("govault: exists %q: %w", key, err)
	}
	return true, nil
}

func collectKeys(ctx context.Context, r runner, prefix string) ([]string, error) {
	query, args := prefixQuery("SELECT key FROM kv", prefix)
	rows, err := r.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("govault: keys %q: %w", prefix, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("govault: keys %q: %w", prefix, err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("govault: keys %q: %w", prefix, err)
	}
	return keys, nil
}

func scanPrefix(ctx context.Context, r runner, prefix string, fn func(key string, value []byte) bool) error {
	if fn == nil {
		return errors.New("govault: Scan requires a non-nil function")
	}
	query, args := prefixQuery("SELECT key, value FROM kv", prefix)
	rows, err := r.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("govault: scan %q: %w", prefix, err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("govault: scan %q: %w", prefix, err)
		}
		if !fn(key, value) {
			return nil
		}
	}
	return rows.Err()
}

func countPrefix(ctx context.Context, r runner, prefix string) (int64, error) {
	query, args := prefixQuery("SELECT COUNT(*) FROM kv", prefix)
	var n int64
	if err := r.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("govault: count %q: %w", prefix, err)
	}
	return n, nil
}

// prefixQuery appends a key range and liveness filter to selectStmt. A prefix
// range is two comparisons against the computed upper bound, which SQLite
// serves from the primary key B-tree without LIKE or table scans.
func prefixQuery(selectStmt string, prefix string) (string, []any) {
	now := nowNanos()
	if prefix == "" {
		return selectStmt + " WHERE " + aliveCond + " ORDER BY key", []any{now}
	}
	if upper, ok := prefixUpperBound(prefix); ok {
		return selectStmt + " WHERE key >= ? AND key < ? AND " + aliveCond + " ORDER BY key",
			[]any{prefix, upper, now}
	}
	// Every byte of the prefix is 0xFF: no greater bound exists, but the
	// lower bound still narrows the scan.
	return selectStmt + " WHERE key >= ? AND " + aliveCond + " ORDER BY key", []any{prefix, now}
}

// prefixUpperBound returns the smallest string sorting after every string
// with the given prefix, and whether such a bound exists. It is the
// byte-wise analogue of incrementing the last non-maximal byte.
func prefixUpperBound(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xFF {
			up := make([]byte, i+1)
			copy(up, b[:i+1])
			up[i]++
			return string(up), true
		}
	}
	return "", false
}
