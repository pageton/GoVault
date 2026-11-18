package govault

import "errors"

// Errors returned by Store and Tx operations. Test for them with errors.Is.
var (
	// ErrKeyNotFound is returned by Get when a key does not exist or its TTL
	// has already elapsed.
	ErrKeyNotFound = errors.New("govault: key not found")

	// ErrClosed is returned by Store operations attempted after Close.
	ErrClosed = errors.New("govault: store is closed")
)
