package hashing

import (
	"testing"
)

func TestHashDeterministic(t *testing.T) {
	input := "AAPL"
	expected := Hash(input)

	// Ensure it returns the same result consistently
	for i := 0; i < 100; i++ {
		if got := Hash(input); got != expected {
			t.Fatalf("Hash is not deterministic: expected %d, got %d", expected, got)
		}
	}
}

func TestHashDifferent(t *testing.T) {
	hash1 := Hash("AAPL")
	hash2 := Hash("MSFT")

	if hash1 == hash2 {
		t.Fatalf("Hash collision for distinct strings: %d", hash1)
	}
}
