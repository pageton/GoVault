package govault

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Tests in the root module cover only the pure internals; everything that
// needs a real SQLite driver lives in the separate test module.

func TestPrefixUpperBound(t *testing.T) {
	cases := []struct {
		prefix string
		want   string
		ok     bool
	}{
		{"", "", false}, // no bound: matches everything
		{"a", "b", true},
		{"abc", "abd", true},
		{"a\x00", "a\x01", true},
		{"a\xff", "b", true},
		{"a\xff\xff", "b", true},
		{"\xff\xff", "", false}, // no greater string exists
	}
	for _, tc := range cases {
		got, ok := prefixUpperBound(tc.prefix)
		if got != tc.want || ok != tc.ok {
			t.Errorf("prefixUpperBound(%q): got (%q, %v), want (%q, %v)",
				tc.prefix, got, ok, tc.want, tc.ok)
		}
	}
}

func TestDataSourceName(t *testing.T) {
	abs := func(p string) string {
		a, _ := filepath.Abs(p)
		return a
	}
	cases := []struct {
		name, path, wantPrefix string
	}{
		{"absolute path", "/tmp/x.db", "file:///tmp/x.db?"},
		{"relative path", "app.db", "file://" + abs("app.db") + "?"},
		{"memory", ":memory:", "file::memory:?"},
		{"uri passthrough", "file:e.db?_pragma=x", "file:e.db?_pragma=x&"},
	}
	for _, tc := range cases {
		got := dataSourceName(tc.path, defaultConfig())
		if !strings.HasPrefix(got, tc.wantPrefix) {
			t.Errorf("%s: got %q, want prefix %q", tc.name, got, tc.wantPrefix)
		}
	}
	for _, frag := range []string{
		"_txlock=immediate",
		"_pragma=busy_timeout(5000)",
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(NORMAL)",
	} {
		if got := dataSourceName("/tmp/x.db", defaultConfig()); !strings.Contains(got, frag) {
			t.Errorf("default DSN %q missing %q", got, frag)
		}
	}
	if got := dataSourceName("/tmp/x.db", config{busyTimeout: 1500 * time.Millisecond}); !strings.Contains(got, "busy_timeout(1500)") {
		t.Errorf("custom busy timeout not reflected: %q", got)
	}
}

func TestIsMemoryPath(t *testing.T) {
	yes := []string{":memory:", "file::memory:", "file:x.db?mode=memory"}
	no := []string{"x.db", "memory.db", "file:mode-memory.db"}
	for _, s := range yes {
		if !isMemoryPath(s) {
			t.Errorf("isMemoryPath(%q) = false, want true", s)
		}
	}
	for _, s := range no {
		if isMemoryPath(s) {
			t.Errorf("isMemoryPath(%q) = true, want false", s)
		}
	}
}
