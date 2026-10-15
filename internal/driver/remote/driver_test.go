package remote

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver/fake"
)

// serve starts a real agent server over a real socket. The structs cross it as
// JSON (see agent/codec.go), so this is the test that proves both ends agree.
func serve(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	agent.RegisterServer(srv, agent.NewAgentServer(fake.New()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// The full engine path the controller uses: remote driver -> gRPC -> engine.
func TestRemoteDriverRoundTrip(t *testing.T) {
	d, err := New("n-1", serve(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	flavor := core.Flavor{Name: "m1", VCPU: 1, RAM: 2, Disk: 10}

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
	d, err := New("n-1", serve(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := d.Delete(ctx, "never-existed"); err != nil {
		t.Fatalf("delete of a missing instance must succeed: %v", err)
	}
}
