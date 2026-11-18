package govault

import "time"

// Option configures a Store at Open time.
type Option func(*config)

type config struct {
	busyTimeout     time.Duration
	maxOpenConns    int
	janitorEnabled  bool
	janitorInterval time.Duration
	driverName      string // database/sql driver to open; default "sqlite"
	dsn             string // when set, used verbatim instead of the built-in DSN
}

func defaultConfig() config {
	return config{
		busyTimeout:     5 * time.Second,
		maxOpenConns:    0, // no explicit cap
		janitorEnabled:  true,
		janitorInterval: 30 * time.Second,
	}
}

// WithBusyTimeout sets how long a writer waits for the SQLite database lock
// before failing, trading throughput under heavy write contention for
// resilience. The default is 5s.
func WithBusyTimeout(d time.Duration) Option {
	return func(c *config) { c.busyTimeout = d }
}

// WithMaxOpenConns caps the size of the connection pool. Zero (the default)
// leaves the pool uncapped. Open uses a pool of one connection automatically
// for ":memory:" databases, since those are private to each connection.
func WithMaxOpenConns(n int) Option {
	return func(c *config) { c.maxOpenConns = n }
}

// WithJanitorInterval sets how often the background janitor physically
// removes expired keys. A non-positive interval disables the janitor;
// expired keys are then hidden on read but stay on disk until overwritten,
// deleted, or the store is reopened with the janitor enabled.
func WithJanitorInterval(d time.Duration) Option {
	return func(c *config) {
		if d <= 0 {
			c.janitorEnabled = false
			return
		}
		c.janitorInterval = d
	}
}

// WithDriver opens the store with the given database/sql driver and DSN
// instead of GoVault's built-in default. The consuming project imports the
// driver it wants; GoVault only references it by name. Both arguments are
// required, and the DSN is passed to the driver verbatim, so it must carry
// the settings GoVault relies on. For github.com/mattn/go-sqlite3:
//
//	file:app.db?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL&_txlock=immediate
//
// For modernc.org/sqlite with custom pragmas:
//
//	file:app.db?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)
//
// WithBusyTimeout does not apply to a caller-provided DSN. See also Attach
// for handing over an already-open *sql.DB.
func WithDriver(name, dsn string) Option {
	return func(c *config) {
		c.driverName = name
		c.dsn = dsn
	}
}
