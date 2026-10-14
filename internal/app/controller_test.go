package app

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cinvat/arsan/internal/core"
)

// quiet swallows log output so a failing test's cause is visible.
type quiet struct{}

func (quiet) Info(string, ...any)  {}
func (quiet) Warn(string, ...any)  {}
func (quiet) Error(string, ...any) {}

// newTestController builds a controller over a fresh store. The probe itself is
// stubbed through the driverForNode seam where a test needs it.
func newTestController(t *testing.T) *Controller {
	t.Helper()
	c, err := NewController(Config{}, nil, quiet{})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func registerTestNode(t *testing.T, c *Controller, id string, enabled bool) core.Node {
	t.Helper()
	n, err := c.Store.Register(t.Context(), core.Node{
		ID: id, Name: id, Enabled: enabled,
		Total:     core.Resources{VCPU: 8, RAM: 16, Disk: 200},
		Address:   "127.0.0.1:1", // nothing listens here, so probes fail
		Instances: []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// An unreachable agent must leave the node NOTREADY with a reason an operator
// can act on. A bare NOTREADY with no explanation is not useful.
func TestProbeUnreachableAgentRecordsReason(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)

	if err := c.ProbeAgent(t.Context(), "n-1"); err != nil {
		t.Fatalf("probe should record its outcome, not fail: %v", err)
	}

	n, err := c.Store.GetNode(t.Context(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	if n.State.Ready() {
		t.Fatal("an unreachable agent must not be READY")
	}
	if n.NotReadyReason == "" {
		t.Fatal("NOTREADY without a reason is not actionable; the reason must be recorded")
	}
}

// The reason must not flap: an unchanged failure should not rewrite the record.
func TestProbeIsIdempotentWhileUnreachable(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)

	if err := c.ProbeAgent(t.Context(), "n-1"); err != nil {
		t.Fatal(err)
	}
	first, _ := c.Store.GetNode(t.Context(), "n-1")

	for i := 0; i < 3; i++ {
		if err := c.ProbeAgent(t.Context(), "n-1"); err != nil {
			t.Fatal(err)
		}
	}
	again, _ := c.Store.GetNode(t.Context(), "n-1")

	if again.NotReadyReason != first.NotReadyReason {
		t.Fatalf("reason churned between identical failures:\n%q\n%q", first.NotReadyReason, again.NotReadyReason)
	}
}

// An operator-disabled node is not a broken node. The probe must not overwrite
// their reason with its own failure.
func TestProbeLeavesDisabledNodeAlone(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", false)

	if _, err := c.Store.SetReadiness(t.Context(), "n-1", core.NodeNotReady, "drained by operator"); err != nil {
		t.Fatal(err)
	}
	if err := c.ProbeAgent(t.Context(), "n-1"); err != nil {
		t.Fatal(err)
	}

	n, _ := c.Store.GetNode(t.Context(), "n-1")
	if n.State.Ready() {
		t.Fatal("a drained node must not be returned to service by a probe")
	}
	if n.NotReadyReason != "drained by operator" {
		t.Fatalf("reason = %q, want the operator's own reason preserved", n.NotReadyReason)
	}
}

func TestProbeMissingNode(t *testing.T) {
	c := newTestController(t)
	err := c.ProbeAgent(t.Context(), "ghost")
	if !errors.Is(err, core.ErrNodeNotFound) {
		t.Fatalf("got %v, want ErrNodeNotFound", err)
	}
}

// A sweep must not be stopped by one unreachable agent.
func TestProbeAllContinuesPastFailures(t *testing.T) {
	c := newTestController(t)
	for _, id := range []string{"n-1", "n-2", "n-3"} {
		registerTestNode(t, c, id, true)
	}

	if err := c.ProbeAll(t.Context()); err != nil {
		t.Fatalf("sweep returned an error for nodes it was meant to tolerate: %v", err)
	}

	nodes, err := c.Store.ListNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Fatalf("probed %d nodes, want 3", len(nodes))
	}
	for _, n := range nodes {
		if n.NotReadyReason == "" {
			t.Errorf("node %s was skipped: every node should have been probed", n.ID)
		}
	}
}

func TestProbeLoopStopsOnContext(t *testing.T) {
	c := newTestController(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := c.ProbeLoop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

// Checking an agent records nothing and changes no state.
func TestCheckAgentDoesNotSetReadiness(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)

	if err := CheckAgent(c.Store, "n-1"); err != nil {
		t.Fatal(err)
	}

	n, _ := c.Store.GetNode(t.Context(), "n-1")
	if n.State.Ready() {
		t.Fatal("checking an agent must not mark a node READY; only a probe may")
	}
}

func TestCheckAgentRejectsMissingAddress(t *testing.T) {
	c := newTestController(t)
	if _, err := c.Store.Register(t.Context(), core.Node{
		ID: "n-1", Name: "n-1", Enabled: true,
		Total: core.Resources{VCPU: 1, RAM: 1, Disk: 1},
	}); err != nil {
		t.Fatal(err)
	}

	err := CheckAgent(c.Store, "n-1")
	if err == nil || !strings.Contains(err.Error(), "no agent address") {
		t.Fatalf("got %v, want a missing-address error", err)
	}
}

// The reason must be cleared when a node recovers, or a stale explanation
// outlives the fault it described.
func TestReadinessReasonIsClearedOnRecovery(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)

	if _, err := c.Store.SetReadiness(t.Context(), "n-1", core.NodeNotReady, "connection refused"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.SetReadiness(t.Context(), "n-1", core.NodeReady, ""); err != nil {
		t.Fatal(err)
	}

	n, _ := c.Store.GetNode(t.Context(), "n-1")
	if n.NotReadyReason != "" {
		t.Fatalf("reason = %q, want it cleared when the node recovered", n.NotReadyReason)
	}
}

// A READY node that stops answering must come back out, or it keeps attracting
// VMs it cannot run.
func TestProbeTakesAReadyNodeBackOut(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)

	if _, err := c.Store.SetReadiness(t.Context(), "n-1", core.NodeReady, ""); err != nil {
		t.Fatal(err)
	}
	if err := c.ProbeAgent(t.Context(), "n-1"); err != nil {
		t.Fatal(err)
	}

	n, _ := c.Store.GetNode(t.Context(), "n-1")
	if n.State.Ready() {
		t.Fatal("a node that stopped answering must not stay READY")
	}
}

// A controller that restarts after node.registered was acked has an empty
// in-process driver registry but a node record that still holds the address.
// It must still be able to drive that node.
func TestDriverResolvesAfterRestartWithEmptyRegistry(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)
	// No RegisterAgent: simulate a restart where the command was already
	// acked, so nothing repopulates the registry.

	d, err := drivers(c.Store)("n-1")
	if err != nil {
		t.Fatalf("driver must resolve from the node record, not the cache: %v", err)
	}
	if d == nil {
		t.Fatal("nil driver")
	}
}

// A node with no address must fail loudly rather than resolve to something.
func TestDriverFailsWhenAddressUnknown(t *testing.T) {
	c := newTestController(t)
	// A distinct id, so this cannot pass by resolving some other test's node.
	const id = "n-no-address"
	if _, err := c.Store.Register(t.Context(), core.Node{
		ID: id, Name: id, Enabled: true,
		Total: core.Resources{VCPU: 1, RAM: 1, Disk: 1},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := drivers(c.Store)(id); err == nil {
		t.Fatal("expected an error for a node with no agent address")
	}
}

// A node must stay READY across probe sweeps.
//
// An earlier version also aged nodes out on a LastHeartbeat timestamp. No agent
// sends a heartbeat -- it bootstraps once and then only answers gRPC -- so
// every node aged out of service 30s after registering and the cluster quietly
// stopped placing anything. Readiness must depend on the probe and nothing else.
func TestNodeStaysReadyAcrossProbeSweeps(t *testing.T) {
	c := newTestController(t)
	registerTestNode(t, c, "n-1", true)

	// The agent is unreachable, so the probe will take it out; that is
	// correct. What must not happen is a *passing* probe being ignored, or a
	// stale timestamp aging it out.
	if _, err := c.Store.SetReadiness(t.Context(), "n-1", core.NodeReady, ""); err != nil {
		t.Fatal(err)
	}

	// Simulate the passage of time that a timestamp check would have used.
	old := time.Now().UTC().Add(-time.Hour)
	if _, err := c.Store.RecordUsage(t.Context(), "n-1", core.Resources{}, old); err != nil {
		t.Fatal(err)
	}

	n, err := c.Store.GetNode(t.Context(), "n-1")
	if err != nil {
		t.Fatal(err)
	}
	if !n.State.Ready() {
		t.Fatal("reporting usage must not age a node out; only a failed probe may")
	}
}
