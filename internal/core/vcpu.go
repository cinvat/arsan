package core

import (
	"encoding/json"
	"fmt"
)

// VCPU sizes a VM's CPUs as topology, not a bare count.
//
// A count alone cannot express what KVM actually runs: sockets matter for
// per-socket licensed guests, and threads matter for SMT layouts. The
// schedulable count is the product; Dedicated selects pinning:
//
//   - shared (default): vCPUs float across host CPUs, placement counts.
//   - dedicated: vCPUs are pinned to exclusive host CPUs recorded on the
//     node, with no overcommit. The pins travel in Placement.PinnedCPUs,
//     are stored on the instance, and are freed on teardown.
type VCPU struct {
	Sockets   int  `json:"sockets"`
	Cores     int  `json:"cores"`
	Threads   int  `json:"threads"`
	Dedicated bool `json:"dedicated"`
}

// VCPUCount is the legacy shape: N vCPUs as one socket of N cores, shared.
// It is what a bare `vcpu: 2` (or `"vcpu": 2`) decodes to.
func VCPUCount(n int) VCPU { return VCPU{Sockets: 1, Cores: n, Threads: 1} }

// Count is the schedulable number of vCPUs: the topology product.
func (v VCPU) Count() int { return v.Sockets * v.Cores * v.Threads }

// Validate rejects non-positive dimensions. A zero struct is not a valid
// flavor; use VCPUCount for the legacy count shape.
func (v VCPU) Validate() error {
	if v.Sockets <= 0 || v.Cores <= 0 || v.Threads <= 0 {
		return fmt.Errorf("vcpu needs positive sockets, cores and threads: %w", ErrInvalidRequest)
	}
	return nil
}

// UnmarshalJSON accepts an object or a legacy bare count.
//
// An all-zero object decodes without validation: encoding/json's omitempty
// does not omit structs, so envelopes like agent.Request always carry a zero
// VCPU on ops that have no flavor. "No flavor given" must decode; the stores
// reject a zero VCPU where a real flavor is required.
func (v *VCPU) UnmarshalJSON(raw []byte) error {
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		if n <= 0 {
			return fmt.Errorf("vcpu count must be positive: %w", ErrInvalidRequest)
		}
		*v = VCPUCount(n)
		return nil
	}
	type plain VCPU
	var p plain
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("vcpu: want {sockets,cores,threads} or a count: %w", err)
	}
	*v = VCPU(p)
	if p == (plain{}) {
		return nil
	}
	return v.Validate()
}

// UnmarshalYAML accepts a mapping or a legacy bare count (yaml.v3 calls this).
// Like UnmarshalJSON, an all-zero mapping is "no flavor given", not an error.
func (v *VCPU) UnmarshalYAML(unmarshal func(any) error) error {
	var n int
	if err := unmarshal(&n); err == nil {
		if n <= 0 {
			return fmt.Errorf("vcpu count must be positive: %w", ErrInvalidRequest)
		}
		*v = VCPUCount(n)
		return nil
	}
	type plain VCPU
	var p plain
	if err := unmarshal(&p); err != nil {
		return fmt.Errorf("vcpu: want {sockets,cores,threads} or a count: %w", err)
	}
	*v = VCPU(p)
	if p == (plain{}) {
		return nil
	}
	return v.Validate()
}
