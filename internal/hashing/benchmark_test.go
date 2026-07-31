package hashing

import (
	"hash/crc32"
	"hash/fnv"
	"testing"

	"github.com/cespare/xxhash/v2"
)

var testString = "AAPL.NASDAQ"

// BenchmarkHash_xxHash benchmarks the chosen xxHash implementation.
func BenchmarkHash_xxHash(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = xxhash.Sum64String(testString)
	}
}

// BenchmarkHash_CRC32 benchmarks the standard library CRC32.
func BenchmarkHash_CRC32(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// CRC32 requires a byte slice allocation for strings in the standard lib wrapper
		_ = crc32.ChecksumIEEE([]byte(testString))
	}
}

// BenchmarkHash_FNV benchmarks the standard library FNV-1a.
func BenchmarkHash_FNV(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := fnv.New64a()
		_, _ = h.Write([]byte(testString))
		_ = h.Sum64()
	}
}
