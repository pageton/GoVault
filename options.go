package govault

import "time"

// Option configures a Store at Open time.
type Option func(*config)

type config struct {
	busyTimeout     time.Duration
	maxOpenConns    int
	janitorEnabled  bool
	janitorInterval time.Duration
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
