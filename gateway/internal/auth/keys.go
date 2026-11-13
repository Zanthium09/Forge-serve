package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"strings"
)

const (
	keyPrefix = "sk-forge-"
	base62    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	keyChars  = 43 // 62^43 > 2^256, so 32 random bytes always fit
)

// NewKey mints a key: sk-forge- + base62(32 random bytes). Only the hash
// is ever stored; the raw key is shown once, at creation.
func NewKey() (raw, hashHex, prefix string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", "", err
	}
	n := new(big.Int).SetBytes(b)
	sixtyTwo := big.NewInt(62)
	mod := new(big.Int)
	var sb strings.Builder
	digits := make([]byte, 0, keyChars)
	for n.Sign() > 0 {
		n.DivMod(n, sixtyTwo, mod)
		digits = append(digits, base62[mod.Int64()])
	}
	for len(digits) < keyChars {
		digits = append(digits, '0')
	}
	for i := len(digits) - 1; i >= 0; i-- {
		sb.WriteByte(digits[i])
	}
	raw = keyPrefix + sb.String()
	return raw, HashKey(raw), raw[len(keyPrefix) : len(keyPrefix)+8], nil
}

// HashKey is the stored form of a key: lowercase SHA-256 hex.
func HashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
