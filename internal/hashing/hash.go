package hashing

import (
	"github.com/cespare/xxhash/v2"
)

// HashString returns a fast, deterministic 64-bit hash of the given string.
// It is heavily optimized for zero-allocations and rapid inlining.
func HashString(s string) uint64 {
	return xxhash.Sum64String(s)
}

// HashBytes returns a fast, deterministic 64-bit hash of a byte slice.
// This prevents callers from allocating memory when casting []byte to string.
func HashBytes(b []byte) uint64 {
	return xxhash.Sum64(b)
}

// Hash is a convenience wrapper for HashString.
func Hash(s string) uint64 {
	return HashString(s)
}
