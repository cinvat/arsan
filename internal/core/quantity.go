package core

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Quantity is a size in MiB. It exists so flavors read as "32MB" or "2GB"
// instead of a bare int whose unit the reader must guess.
//
// Only MB and GB are accepted — nothing smaller can host a VM, nothing larger
// fits one. Bare numbers are rejected: with memory once meaning GiB and disk
// once meaning GiB, a bare "10" is a 1024x mistake waiting to happen, so the
// unit is required out loud.
type Quantity int

var quantityRe = regexp.MustCompile(`^([0-9]+)\s*(m|mb|g|gb)$`)

// ParseQuantity parses "32M", "32MB", "2G", "2GB" (case-insensitive) into MiB.
func ParseQuantity(s string) (Quantity, error) {
	m := quantityRe.FindStringSubmatch(strings.ToLower(strings.TrimSpace(s)))
	if m == nil {
		return 0, fmt.Errorf("size %q: want <n>MB or <n>GB: %w", s, ErrInvalidRequest)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("size %q must be positive: %w", s, ErrInvalidRequest)
	}
	if strings.HasPrefix(m[2], "g") {
		return Quantity(n * 1024), nil
	}
	return Quantity(n), nil
}

// MiB returns the value in MiB.
func (q Quantity) MiB() int { return int(q) }

// FreeCPUs returns the lowest-numbered count CPUs not in pinned, drawn from a
// host with total CPUs. It is the shared first-fit policy for dedicated
// claims: every store uses it, so placement agrees on which CPUs a claim
// holds regardless of backend. Shortage is ErrCapacityConflict.
func FreeCPUs(total int, pinned []int, count int) ([]int, error) {
	held := make(map[int]bool, len(pinned))
	for _, c := range pinned {
		held[c] = true
	}
	var out []int
	for c := 0; c < total && len(out) < count; c++ {
		if !held[c] {
			out = append(out, c)
		}
	}
	if len(out) < count {
		return nil, fmt.Errorf("claim %d cpus: %w", count, ErrCapacityConflict)
	}
	return out, nil
}

func (q Quantity) String() string {
	if int(q)%1024 == 0 {
		return fmt.Sprintf("%dGB", int(q)/1024)
	}
	return fmt.Sprintf("%dMB", int(q))
}

// UnmarshalJSON accepts "32MB"/"2GB" strings. Numbers are rejected so a
// legacy bare value cannot silently mean something 1024x off.
func (q *Quantity) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return fmt.Errorf("size %s: want <n>MB or <n>GB: %w", string(raw), ErrInvalidRequest)
	}
	if strings.TrimSpace(s) == "" {
		*q = 0
		return nil
	}
	v, err := ParseQuantity(s)
	if err != nil {
		return err
	}
	*q = v
	return nil
}

// MarshalJSON writes the normalized form ("32MB", "2GB").
func (q Quantity) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(q.String())), nil
}

// UnmarshalYAML accepts "32MB"/"2GB" strings (gopkg.in/yaml.v3 calls this).
func (q *Quantity) UnmarshalYAML(unmarshal func(any) error) error {
	var v any
	if err := unmarshal(&v); err != nil {
		return err
	}
	s, ok := v.(string)
	if !ok {
		// An absent size (nil) is allowed for image/cdrom volumes.
		if v == nil {
			*q = 0
			return nil
		}
		return fmt.Errorf("size %v: want <n>MB or <n>GB: %w", v, ErrInvalidRequest)
	}
	if strings.TrimSpace(s) == "" {
		*q = 0
		return nil
	}
	parsed, err := ParseQuantity(s)
	if err != nil {
		return err
	}
	*q = parsed
	return nil
}
