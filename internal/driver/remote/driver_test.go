package remote

import (
	"context"
	"testing"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/fake"
)

// loopback routes requests straight into a worker: the full protocol, no
// transport at all. Transport is NATS' job and NATS tests live in
// internal/bus; what matters here is that both ends agree on the envelope.
type loopback struct {
	worker *agent.Worker
}

func (l *loopback) Publish(context.Context, bus.Command) error      { return nil }
func (l *loopback) PublishAsync(context.Context, bus.Command) error { return nil }
func (l *loopback) Subscribe(ctx context.Context, topic string, h bus.Handler) (func(), error) {
	return func() {}, nil
}
func (l *loopback) SubscribeAs(ctx context.Context, topic, _ string, h bus.Handler) (func(), error) {
	return func() {}, nil
}
func (l *loopback) Request(ctx context.Context, _ string, payload []byte) ([]byte, error) {
	return l.worker.Handle(ctx, payload), nil
}
func (l *loopback) Close() error { return nil }

func dial(t *testing.T) *Driver {
	t.Helper()
	return New("n-1", &loopback{worker: agent.NewWorker("n-1", fake.New())})
}

// The full engine path the controller uses: remote driver -> bus -> engine.
func TestRemoteDriverRoundTrip(t *testing.T) {
	d := dial(t)
	ctx := t.Context()
	flavor := core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 2, Disk: 10}

	if err := d.Validate(ctx, flavor); err != nil {
		t.Fatalf("validate: %v", err)
	}
	info, err := d.Create(ctx, core.CreateRequest{
		InstanceID: "i-1", NodeID: "n-1", Name: "vm1", Flavor: flavor,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.ID != "i-1" || info.NodeID != "n-1" {
		t.Fatalf("info = %+v", info)
	}
	if err := d.Start(ctx, "i-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := d.Usage(ctx); err != nil {
		t.Fatalf("usage: %v", err)
	}
	if err := d.Stop(ctx, "i-1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := d.Delete(ctx, "i-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// A retried delete is a no-op: the engine, not the transport, owns
// idempotency, and this proves the round trip preserves it.
func TestRemoteDeleteIsIdempotent(t *testing.T) {
	d := dial(t)
	if err := d.Delete(t.Context(), "never-existed"); err != nil {
		t.Fatalf("delete of a missing instance must succeed: %v", err)
	}
}

// Info is how the controller learns the engine and capacity after the probe.
func TestRemoteInfo(t *testing.T) {
	d := dial(t)
	engine, _, err := d.Info(t.Context())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if engine != "fake" {
		t.Fatalf("engine = %q, want fake", engine)
	}
}
