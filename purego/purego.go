// Package purego registers GoVault's default SQLite driver: the pure-Go
// (CGo-free) modernc.org/sqlite. Import it solely for its side effect:
//
//	import (
//		"github.com/pageton/GoVault"
//		_ "github.com/pageton/GoVault/purego"
//	)
//
// Projects that prefer a different database/sql SQLite driver (for example
// github.com/mattn/go-sqlite3) skip this package entirely and select their
// own with govault.WithDriver or govault.Attach instead, keeping this driver
// out of their binary.
package purego

import _ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
