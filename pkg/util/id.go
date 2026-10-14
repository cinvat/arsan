// Package util holds small helpers shared by the api, controller and agent
// subcommands.
//
// Domain types and the command bus are NOT here. Those are shared too, but they
// are arsan's own concepts, so they live under internal/ and are imported
// directly within the module. What belongs in pkg is the opposite: code with no
// opinion about VMs, nodes, or scheduling.
package util

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// NewID returns a random identifier with a short readable prefix.
//
// Both the api (which creates instance records) and the controller (which
// creates node records) need stable IDs, and a shared generator keeps the two
// from drifting into different formats.
func NewID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice. Panicking is right here:
		// generating an ID is a prerequisite for accepting any work, and a
		// predictable fallback would be worse than refusing to start.
		panic(fmt.Sprintf("util: entropy unavailable: %v", err))
	}
	if prefix == "" {
		return hex.EncodeToString(b[:])
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// Slug turns a human name into a stable identifier fragment: lowercase,
// non-alphanumerics collapsed to single dashes, no leading or trailing dash.
//
// Node names are operator-supplied, so "Compute Node 1" and "compute-node-1"
// must resolve to the same node rather than creating two.
func Slug(name string) string {
	var b strings.Builder
	b.Grow(len(name))

	lastDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastDash = false
		default:
			// Collapse any run of separators into one dash.
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}
