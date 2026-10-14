package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

// The api and the agent spell the secret flags differently: the api uses a
// dotted key (--bootstrap.token-file), the agent a hyphenated one
// (--bootstrap-token-file). secretFile takes both explicitly for that reason.
func TestSecretFileReadsTheNamedFlag(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("  s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	// Only the hyphenated flag is set, as on the agent.
	viper.Set("bootstrap-token-file", path)

	if got := secretFile("bootstrap.token", "bootstrap-token-file", "bootstrap-token"); got != "s3cret" {
		t.Fatalf("token = %q, want %q", got, "s3cret")
	}
}

// The value flag is a fallback, not a second source: a token file wins.
func TestSecretFilePrefersTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("from-file"), 0o600); err != nil {
		t.Fatal(err)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("bootstrap-token-file", path)
	viper.Set("bootstrap-token", "from-flag")

	if got := secretFile("bootstrap.token", "bootstrap-token-file", "bootstrap-token"); got != "from-file" {
		t.Fatalf("token = %q, want the file to win", got)
	}
}

// A world-readable file is refused, since a token on disk that anyone on the
// host can read is not a secret.
func TestSecretFileRejectsLoosePermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte("s3cret"), 0o644); err != nil {
		t.Fatal(err)
	}

	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("bootstrap-token-file", path)

	// Returns "" rather than failing: an api with no token is a legitimate
	// state, it just cannot accept agents.
	if got := secretFile("bootstrap.token", "bootstrap-token-file", "bootstrap-token"); got != "" {
		t.Fatalf("token = %q, want a loose-permission file refused", got)
	}
}

// With neither flag set the helper must not invent a token.
func TestSecretFileUnset(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	if got := secretFile("bootstrap.token", "bootstrap-token-file", "bootstrap-token"); got != "" {
		t.Fatalf("token = %q, want empty when unset", got)
	}
}
