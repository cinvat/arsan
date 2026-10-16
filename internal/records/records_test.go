package records

import (
	"context"
	"testing"

	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/store/memory"
)

// stubBus records published commands instead of delivering them.
type stubBus struct {
	published []bus.Command
}

func (b *stubBus) Publish(_ context.Context, cmd bus.Command) error {
	b.published = append(b.published, cmd)
	return nil
}

func (b *stubBus) PublishAsync(ctx context.Context, cmd bus.Command) error {
	return b.Publish(ctx, cmd)
}

func (b *stubBus) Subscribe(context.Context, string, bus.Handler) (func(), error) {
	return func() {}, nil
}

func (b *stubBus) SubscribeAs(context.Context, string, string, bus.Handler) (func(), error) {
	return func() {}, nil
}

func (b *stubBus) Request(context.Context, string, []byte) ([]byte, error) {
	return nil, nil
}

func (b *stubBus) Close() error { return nil }

// Delete marks TERMINATING instead of removing the row: the record must show
// the intent even when the agent that would carry it out is unreachable.
func TestDeleteMarksTerminating(t *testing.T) {
	store := memory.New()
	commands := &stubBus{}
	svc := &Instances{Store: store, Bus: commands}
	ctx := context.Background()

	created, err := store.Create(ctx, core.Instance{
		ID: "i-1", Name: "vm1", Flavor: core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 2}, State: core.StateRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, created.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, err := store.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateTerminating {
		t.Fatalf("state = %s, want TERMINATING", got.State)
	}
	if len(commands.published) != 1 || commands.published[0].Topic != bus.TopicInstanceDelete {
		t.Fatalf("published = %+v, want one instance.delete command", commands.published)
	}
}

// Re-deleting a TERMINATING instance re-publishes rather than erroring, so a
// stuck delete is retryable without pretending the row is gone.
func TestDeleteTerminatingIsIdempotent(t *testing.T) {
	store := memory.New()
	commands := &stubBus{}
	svc := &Instances{Store: store, Bus: commands}
	ctx := context.Background()

	created, err := store.Create(ctx, core.Instance{
		ID: "i-2", Name: "vm2", Flavor: core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 2}, State: core.StateRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, created.ID); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := svc.Delete(ctx, created.ID); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	got, _ := store.Get(ctx, created.ID)
	if got.State != core.StateTerminating {
		t.Fatalf("state = %s, want TERMINATING", got.State)
	}
	if len(commands.published) != 2 {
		t.Fatalf("published %d commands, want 2 (one per delete)", len(commands.published))
	}
}

// Deleting a missing instance is still a 404, not a silent success: the api
// maps it before anything is queued.
func TestDeleteMissingIsNotFound(t *testing.T) {
	store := memory.New()
	svc := &Instances{Store: store, Bus: &stubBus{}}
	if err := svc.Delete(context.Background(), "ghost"); err == nil {
		t.Fatal("expected an error for a missing instance")
	}
}
