// Command customdriver demonstrates selecting the SQLite driver from the
// consuming project rather than accepting the library's default: opening via
// WithDriver with an explicit DSN, and adopting an existing *sql.DB with
// Attach (including the WAL check failing loudly on a misconfigured DSN).
//
// Here both paths use the bundled pure-Go driver, but the same code shape
// applies to any database/sql SQLite driver such as mattn/go-sqlite3 — just
// swap the driver name, its import, and the DSN dialect.
//
//	go run github.com/pageton/GoVault/examples/customdriver
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pageton/GoVault"
	_ "modernc.org/sqlite" // this project's driver choice
)

func main() {
	ctx := context.Background()
	path := filepath.Join(os.TempDir(), "govault-driver.vault")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(path + suffix)
	}

	// --- Attach: the project owns the *sql.DB and its DSN. -----------------
	dsn := "file:" + path +
		"?_txlock=immediate&_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		fmt.Println("sql.Open:", err)
		os.Exit(1)
	}
	v, err := govault.Attach(db) // verifies WAL; takes ownership of db
	if err != nil {
		fmt.Println("attach:", err)
		os.Exit(1)
	}
	if err := v.Set(ctx, "via-attach", []byte("written through an attached pool")); err != nil {
		fmt.Println("set:", err)
		os.Exit(1)
	}
	v.Close() // also closes db

	// --- WithDriver: GoVault opens the driver by name and DSN. -------------
	v2, err := govault.Open("", govault.WithDriver("sqlite", dsn))
	if err != nil {
		fmt.Println("open with driver:", err)
		os.Exit(1)
	}
	defer v2.Close()
	value, err := v2.Get(ctx, "via-attach")
	fmt.Printf("reopened via WithDriver: via-attach -> %q (err = %v)\n", value, err)

	// --- The WAL guard: a DSN without WAL is rejected with instructions. ----
	badPath := filepath.Join(os.TempDir(), "govault-driver-bad.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(badPath + suffix)
	}
	badDB, err := sql.Open("sqlite", "file:"+badPath) // no journal_mode pragma
	if err != nil {
		fmt.Println("sql.Open:", err)
		os.Exit(1)
	}
	defer badDB.Close()
	_, err = govault.Attach(badDB)
	fmt.Printf("attach without WAL: %v\n", err)
	if err == nil || !strings.Contains(err.Error(), "not WAL") {
		fmt.Println("expected a WAL rejection with setup instructions")
		os.Exit(1)
	}

	// The DSN is driver-specific: mattn/go-sqlite3 spells the same settings
	// differently. Reference recipe (not run here):
	//
	//	import _ "github.com/mattn/go-sqlite3"
	//	govault.Open("", govault.WithDriver("sqlite3",
	//		"file:app.db?_busy_timeout=5000&_journal_mode=WAL" +
	//		"&_synchronous=NORMAL&_txlock=immediate"))
}
