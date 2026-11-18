package govaulttest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pageton/GoVault"
)

func TestSetGet(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	if err := s.Set(ctx, "hello", []byte("world")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := s.Get(ctx, "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Fatalf("Get: got %q, want %q", got, "world")
	}

	// Overwrite replaces the value.
	if err := s.Set(ctx, "hello", []byte("govault")); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	if got, err := s.Get(ctx, "hello"); err != nil || string(got) != "govault" {
		t.Fatalf("Get after overwrite: got %q, %v", got, err)
	}
}

func TestGetMissing(t *testing.T) {
	s, _ := openTest(t)
	if _, err := s.Get(context.Background(), "nope"); !errors.Is(err, govault.ErrKeyNotFound) {
		t.Fatalf("Get missing key: got %v, want ErrKeyNotFound", err)
	}
}

func TestSetEdgeCases(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	cases := []struct {
		name string
		key  string
		val  []byte
	}{
		{"empty key", "", []byte("v")},
		{"empty value", "empty", []byte{}},
		{"nil value", "nil", nil},
		{"binary value", "bin", []byte{0x00, 0x01, 0xFF, 0x00}},
		{"unicode key", "日本語/ключ", []byte("idempotent")},
		{"1MB value", "big", bytes.Repeat([]byte{0xAB}, 1<<20)},
	}
	for _, tc := range cases {
		if err := s.Set(ctx, tc.key, tc.val); err != nil {
			t.Fatalf("%s: Set: %v", tc.name, err)
		}
		want := tc.val
		if want == nil {
			want = []byte{}
		}
		got, err := s.Get(ctx, tc.key)
		if err != nil {
			t.Fatalf("%s: Get: %v", tc.name, err)
		}
		if len(got) != len(want) || !bytes.Equal(got, want) {
			t.Fatalf("%s: roundtrip mismatch: got %d bytes, want %d", tc.name, len(got), len(want))
		}
	}
}

func TestExists(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	if err := s.Set(ctx, "there", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ok, err := s.Exists(ctx, "there"); err != nil || !ok {
		t.Fatalf("Exists present: got %v, %v", ok, err)
	}
	if ok, err := s.Exists(ctx, "absent"); err != nil || ok {
		t.Fatalf("Exists absent: got %v, %v", ok, err)
	}
	if _, err := s.Delete(ctx, "there"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, err := s.Exists(ctx, "there"); err != nil || ok {
		t.Fatalf("Exists after delete: got %v, %v", ok, err)
	}
}

func TestDelete(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	for _, k := range []string{"a", "b", "c"} {
		if err := s.Set(ctx, k, []byte(k)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	if n, err := s.Delete(ctx, "a"); err != nil || n != 1 {
		t.Fatalf("Delete one: got %d, %v; want 1, nil", n, err)
	}
	if n, err := s.Delete(ctx, "missing"); err != nil || n != 0 {
		t.Fatalf("Delete missing: got %d, %v; want 0, nil", n, err)
	}
	if n, err := s.Delete(ctx); err != nil || n != 0 {
		t.Fatalf("Delete nothing: got %d, %v; want 0, nil", n, err)
	}
	// Multi-delete is atomic and reports how many keys existed.
	if n, err := s.Delete(ctx, "b", "missing", "c"); err != nil || n != 2 {
		t.Fatalf("Delete multi: got %d, %v; want 2, nil", n, err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, err := s.Get(ctx, k); !errors.Is(err, govault.ErrKeyNotFound) {
			t.Fatalf("Get %s after delete: got %v, want ErrKeyNotFound", k, err)
		}
	}
}

func TestTTL(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	// A TTL key is visible immediately and gone after it elapses.
	if err := s.Set(ctx, "ephemeral", []byte("v"), 150*time.Millisecond); err != nil {
		t.Fatalf("Set with TTL: %v", err)
	}
	if ok, err := s.Exists(ctx, "ephemeral"); err != nil || !ok {
		t.Fatalf("Exists before expiry: got %v, %v", ok, err)
	}
	if _, err := s.Get(ctx, "ephemeral"); err != nil {
		t.Fatalf("Get before expiry: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := s.Get(ctx, "ephemeral"); !errors.Is(err, govault.ErrKeyNotFound) {
		t.Fatalf("Get after expiry: got %v, want ErrKeyNotFound", err)
	}
	if ok, err := s.Exists(ctx, "ephemeral"); err != nil || ok {
		t.Fatalf("Exists after expiry: got %v, %v", ok, err)
	}
	if keys, err := s.Keys(ctx, ""); err != nil || len(keys) != 0 {
		t.Fatalf("Keys after expiry: got %v, %v; want none", keys, err)
	}

	// A key without TTL outlives neighbors that expire.
	if err := s.Set(ctx, "durable", []byte("v")); err != nil {
		t.Fatalf("Set without TTL: %v", err)
	}
	if err := s.Set(ctx, "ephemeral", []byte("v"), 150*time.Millisecond); err != nil {
		t.Fatalf("Set with TTL: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := s.Get(ctx, "durable"); err != nil {
		t.Fatalf("durable key expired: %v", err)
	}

	// Overwriting a TTL key without a TTL removes the expiry.
	if err := s.Set(ctx, "revived", []byte("v"), 150*time.Millisecond); err != nil {
		t.Fatalf("Set with TTL: %v", err)
	}
	if err := s.Set(ctx, "revived", []byte("v2")); err != nil {
		t.Fatalf("Set overwrite: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got, err := s.Get(ctx, "revived"); err != nil || string(got) != "v2" {
		t.Fatalf("Get revived: got %q, %v; want \"v2\", nil", got, err)
	}

	// Zero and negative TTLs expire immediately.
	if err := s.Set(ctx, "zero", []byte("v"), 0); err != nil {
		t.Fatalf("Set zero TTL: %v", err)
	}
	if _, err := s.Get(ctx, "zero"); !errors.Is(err, govault.ErrKeyNotFound) {
		t.Fatalf("Get zero TTL: got %v, want ErrKeyNotFound", err)
	}
	if err := s.Set(ctx, "negative", []byte("v"), -time.Second); err != nil {
		t.Fatalf("Set negative TTL: %v", err)
	}
	if _, err := s.Get(ctx, "negative"); !errors.Is(err, govault.ErrKeyNotFound) {
		t.Fatalf("Get negative TTL: got %v, want ErrKeyNotFound", err)
	}
}

func TestKeys(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	for _, k := range []string{"b", "a:c", "a", "a:b:a", "c"} {
		if err := s.Set(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	want := []string{"a", "a:b:a", "a:c", "b", "c"}
	got, err := s.Keys(ctx, "")
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Keys: got %v, want %v", got, want)
	}

	got, err = s.Keys(ctx, "a:")
	if err != nil {
		t.Fatalf("Keys prefix: %v", err)
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{"a:b:a", "a:c"}) {
		t.Fatalf("Keys prefix: got %v", got)
	}

	if got, err := s.Keys(ctx, "zzz"); err != nil || len(got) != 0 {
		t.Fatalf("Keys no match: got %v, %v", got, err)
	}
}

func TestScan(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	entries := map[string]string{
		"user:1":   "alice",
		"user:2":   "bob",
		"session:": "x",
	}
	for k, v := range entries {
		if err := s.Set(ctx, k, []byte(v)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	type pair struct {
		k, v string
	}
	var got []pair
	if err := s.Scan(ctx, "user:", func(key string, value []byte) bool {
		got = append(got, pair{key, string(value)})
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := []pair{{"user:1", "alice"}, {"user:2", "bob"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Scan: got %v, want %v", got, want)
	}

	// Returning false stops the iteration early.
	calls := 0
	if err := s.Scan(ctx, "", func(key string, value []byte) bool {
		calls++
		return false
	}); err != nil {
		t.Fatalf("Scan stop: %v", err)
	}
	if calls != 1 {
		t.Fatalf("Scan stop: got %d calls, want 1", calls)
	}

	// Scanning a prefix with no matches is not an error.
	calls = 0
	if err := s.Scan(ctx, "zzz:", func(string, []byte) bool { calls++; return true }); err != nil {
		t.Fatalf("Scan no match: %v", err)
	}
	if calls != 0 {
		t.Fatalf("Scan no match: got %d calls", calls)
	}

	if err := s.Scan(ctx, "", nil); err == nil {
		t.Fatal("Scan with nil fn should fail")
	}
}

func TestCount(t *testing.T) {
	s, _ := openTest(t)
	ctx := context.Background()

	if n, err := s.Count(ctx, ""); err != nil || n != 0 {
		t.Fatalf("Count empty store: got %d, %v", n, err)
	}
	for _, k := range []string{"a", "ab", "b"} {
		if err := s.Set(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	if n, err := s.Count(ctx, ""); err != nil || n != 3 {
		t.Fatalf("Count all: got %d, %v; want 3", n, err)
	}
	if n, err := s.Count(ctx, "a"); err != nil || n != 2 {
		t.Fatalf("Count prefix: got %d, %v; want 2", n, err)
	}
	if _, err := s.Delete(ctx, "ab"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n, err := s.Count(ctx, "a"); err != nil || n != 1 {
		t.Fatalf("Count after delete: got %d, %v; want 1", n, err)
	}

	// Expired keys do not count.
	if err := s.Set(ctx, "temp", []byte("v"), 150*time.Millisecond); err != nil {
		t.Fatalf("Set TTL: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if n, err := s.Count(ctx, ""); err != nil || n != 2 {
		t.Fatalf("Count with expired key: got %d, %v; want 2", n, err)
	}
}

func TestScanByteOrderMatchesPrefixBound(t *testing.T) {
	// Keys around the upper-bound seam of prefix "user:" must be included or
	// excluded exactly.
	s, _ := openTest(t)
	ctx := context.Background()
	for _, k := range []string{"user:1", "user:\xff", "user;", "vser:"} {
		if err := s.Set(ctx, k, []byte("v")); err != nil {
			t.Fatalf("Set %q: %v", k, err)
		}
	}
	keys, err := s.Keys(ctx, "user:")
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if fmt.Sprint(keys) != fmt.Sprint([]string{"user:1", "user:\xff"}) {
		t.Fatalf("Keys around bound: got %q", keys)
	}
}
