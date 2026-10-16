package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func testNode(t *testing.T, s *Store, total core.Resources) core.Node {
	t.Helper()
	n, err := s.Register(context.Background(), core.Node{
		ID: "n-1", Name: "n1", Enabled: true, Total: total,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Pinning is sticky: the first node wins, re-pinning there is a no-op, and
// pinning elsewhere conflicts instead of moving files that do not move.
func TestPinVolumeSticky(t *testing.T) {
	s := New()
	ctx := context.Background()
	v, err := s.CreateVolume(ctx, core.Volume{ID: "vol-1", Name: "d", Namespace: "default", SizeMiB: 100})
	if err != nil {
		t.Fatal(err)
	}
	if v.NodeID != "" {
		t.Fatalf("fresh volume pinned to %q, want unpinned", v.NodeID)
	}
	if _, err := s.PinVolume(ctx, "vol-1", "n-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PinVolume(ctx, "vol-1", "n-1"); err != nil {
		t.Fatalf("re-pin to the same node must be a no-op: %v", err)
	}
	if _, err := s.PinVolume(ctx, "vol-1", "n-2"); err == nil {
		t.Fatal("pinning elsewhere must conflict, not move")
	}
	if _, err := s.PinVolume(ctx, "vol-ghost", "n-1"); err == nil {
		t.Fatal("pinning a missing volume must fail")
	}
}

// Dedicated claims are exclusive: the second claim skips held CPUs, and a
// claim larger than the free set fails without mutating.
func TestClaimCPUsExclusive(t *testing.T) {
	s := New()
	testNode(t, s, core.Resources{CPU: 4, RAM: 16, Disk: 100})
	ctx := context.Background()

	first, err := s.ClaimCPUs(ctx, "n-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0] != 0 || first[1] != 1 {
		t.Fatalf("first claim = %v, want [0 1]", first)
	}
	second, err := s.ClaimCPUs(ctx, "n-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range second {
		for _, h := range first {
			if c == h {
				t.Fatalf("overlap: %v and %v share cpu %d", first, second, c)
			}
		}
	}
	if _, err := s.ClaimCPUs(ctx, "n-1", 1); !errors.Is(err, core.ErrCapacityConflict) {
		t.Fatalf("over-claim err = %v, want capacity conflict", err)
	}
	n, _ := s.GetNode(ctx, "n-1")
	if n.Available.CPU != 0 || len(n.PinnedCPUs) != 4 {
		t.Fatalf("node = %+v, want 0 available and 4 pinned", n)
	}
}

// Releasing restores the count and frees the set; repeating is a no-op that
// must not invent capacity past the total.
func TestReleaseCPUsRestores(t *testing.T) {
	s := New()
	testNode(t, s, core.Resources{CPU: 4, RAM: 16, Disk: 100})
	ctx := context.Background()

	held, err := s.ClaimCPUs(ctx, "n-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseCPUs(ctx, "n-1", held); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseCPUs(ctx, "n-1", held); err != nil {
		t.Fatal(err)
	}
	n, _ := s.GetNode(ctx, "n-1")
	if n.Available.CPU != 4 || len(n.PinnedCPUs) != 0 {
		t.Fatalf("node = %+v, want 4 available and none pinned", n)
	}
}

// Tearing down an instance frees its recorded pins along with its count.
func TestReleaseReservationsFreesPins(t *testing.T) {
	s := New()
	testNode(t, s, core.Resources{CPU: 4, RAM: 16, Disk: 100})
	ctx := context.Background()

	pins, err := s.ClaimCPUs(ctx, "n-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := s.Create(ctx, core.Instance{
		ID: "i-1", Name: "vm1",
		Flavor:     core.Flavor{Name: "d", VCPU: core.VCPU{Sockets: 1, Cores: 2, Threads: 1, Dedicated: true}},
		PinnedCPUs: pins,
		State:      core.StateBuilding,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseReservations(ctx, inst.ID, "n-1"); err != nil {
		t.Fatal(err)
	}
	n, _ := s.GetNode(ctx, "n-1")
	if n.Available.CPU != 4 || len(n.PinnedCPUs) != 0 {
		t.Fatalf("node = %+v, want 4 available and none pinned", n)
	}
}
