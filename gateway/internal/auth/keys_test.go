package auth

import (
	"regexp"
	"testing"
)

func TestNewKeyFormat(t *testing.T) {
	raw, hash, prefix, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^sk-forge-[0-9A-Za-z]{43}$`).MatchString(raw) {
		t.Fatalf("raw = %q", raw)
	}
	if hash != HashKey(raw) || len(hash) != 64 {
		t.Fatalf("hash = %q", hash)
	}
	if prefix != raw[9:17] {
		t.Fatalf("prefix = %q, want %q", prefix, raw[9:17])
	}
	raw2, _, _, _ := NewKey()
	if raw2 == raw {
		t.Fatal("two keys are identical")
	}
}

func TestHashKeyKnownValue(t *testing.T) {
	if HashKey("sk-test") != skTestHash {
		t.Fatalf("HashKey = %s", HashKey("sk-test"))
	}
}
