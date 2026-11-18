package govault

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Tx is a read/write transaction. Obtain one with Store.Batch; do not
// construct it yourself. Its methods behave like the corresponding Store
// methods, but reads inside the transaction also see its own uncommitted
// writes.
type Tx struct {
	tx *sql.Tx
}

// Batch executes fn inside a single write transaction (BEGIN IMMEDIATE, so
// the write lock is taken up front and concurrent batches queue on the busy
// timeout instead of failing mid-transaction). If fn returns nil, every
// change it made is committed; if it returns an error or panics, everything
// is rolled back. A panic is re-raised after the rollback.
//
// Do not call Batch from within fn: the nested transaction cannot acquire the
// write lock held by the outer one and fails once the busy timeout elapses.
func (s *Store) Batch(ctx context.Context, fn func(tx *Tx) error) error {
	if err := s.errClosed(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("govault: Batch requires a non-nil function")
	}
	sqlTx, err := s.db.BeginTx(ctx, nil) // IMMEDIATE via the DSN
	if err != nil {
		return fmt.Errorf("govault: begin transaction: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			_ = sqlTx.Rollback()
			panic(r)
		}
	}()
	if err := fn(&Tx{tx: sqlTx}); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("govault: commit: %w", err)
	}
	return nil
}

// Get returns the value stored under key, including values written earlier
// in this transaction. It returns ErrKeyNotFound for missing or expired keys.
func (t *Tx) Get(ctx context.Context, key string) ([]byte, error) {
	return getValue(ctx, t.tx, key)
}

// Set stores value under key, overwriting any previous value and TTL. See
// Store.Set for the semantics of the optional TTL.
func (t *Tx) Set(ctx context.Context, key string, value []byte, ttl ...time.Duration) error {
	return setValue(ctx, t.tx, key, value, ttl...)
}

// Delete removes keys and returns how many existed. Expired keys count as
// absent.
func (t *Tx) Delete(ctx context.Context, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return deleteKeys(ctx, t.tx, keys...)
}

// Exists reports whether key is present, has not expired, and was not deleted
// in this transaction.
func (t *Tx) Exists(ctx context.Context, key string) (bool, error) {
	return existsValue(ctx, t.tx, key)
}

// Keys returns all live keys starting with prefix in lexicographic byte
// order, including keys written earlier in this transaction.
func (t *Tx) Keys(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	err := iterateKeys(ctx, t.tx, prefix, func(key string) error {
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// Scan iterates live entries starting with prefix in lexicographic byte
// order, streaming rows and stopping early when fn returns false.
func (t *Tx) Scan(ctx context.Context, prefix string, fn func(key string, value []byte) bool) error {
	if fn == nil {
		return errors.New("govault: Scan requires a non-nil function")
	}
	return iterateEntries(ctx, t.tx, prefix, func(key string, value []byte) (bool, error) {
		return fn(key, value), nil
	})
}

// Count returns how many live keys start with prefix.
func (t *Tx) Count(ctx context.Context, prefix string) (int64, error) {
	return countPrefix(ctx, t.tx, prefix)
}
