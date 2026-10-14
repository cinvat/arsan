package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// readSecretFile reads a credential from disk.
//
// It refuses anything group- or world-readable. A bootstrap token left
// readable is the same as no token at all, and failing loudly here beats
// discovering it from an incident.
func readSecretFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read secret %s: %w", path, err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return nil, fmt.Errorf("secret %s is mode %o, want 0600 or stricter", path, mode)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read secret %s: %w", path, err)
	}
	return b, nil
}

// secretFile returns a configured credential, trimmed, or "" when unset.
//
// An unreadable file is a warning rather than a failure so the process still
// starts: a missing bootstrap token disables agent registration, which is a
// legitimate state for an api that is not accepting agents yet.
// secretFile is secret with the file flag named explicitly, for the case where
// it is not the value key plus "-file" (the api and the agent spell it
// differently). Deriving the file key from the value key is what let the
// agent's --bootstrap-token-file be ignored without any error.
func secretFile(key, fileKey, valueKey string) string {
	if f := viper.GetString(fileKey); f != "" {
		b, err := readSecretFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "warn:", err)
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	return strings.TrimSpace(viper.GetString(valueKey))
}
