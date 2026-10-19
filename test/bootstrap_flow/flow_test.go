// Package bootstrap_flow_test wires the real pieces together: an agent using its
// REST client against the real api engine, with a real NATS bus underneath.
//
// The unit tests on each side pass even when the two have drifted — a renamed
// JSON field, a changed path. This test is what catches that.
//
//	nats-server -js -p 4222
//	NATS_URL=nats://127.0.0.1:4222 go test ./test/bootstrap_flow/...
package bootstrap_flow_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cinvat/arsan/api/v1alpha1"
	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/records"
	"github.com/cinvat/arsan/internal/store/memory"
	"github.com/gin-gonic/gin"
)

func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run: nats-server -js -p 4222")
	}
	return url
}

// The agent's client against the api's real routes.
func TestAgentRegistersWithRealAPI(t *testing.T) {
	commands, err := bus.ConnectNATS(natsURL(t), 5*time.Second)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(func() { commands.Close() })

	store := memory.New()
	nodes := &records.Nodes{Store: store}

	// Stand in for the api's HTTP listener.
	engine := gin.New()
	v1alpha1.Register(engine, v1alpha1.Config{
		Nodes:          nodes,
		Bus:            commands,
		BootstrapToken: "shared-secret",
	})
	srv := newTestServer(t, engine)

	notices := make(chan bus.Command, 4)
	stop, err := commands.Subscribe(t.Context(), bus.TopicNodeRegistered, func(_ context.Context, c bus.Command) error {
		select {
		case notices <- c:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(stop)

	// The agent side, exactly as cmd/agent.go calls it.
	node, err := agent.Register(t.Context(), srv.URL, strings.TrimPrefix(v1alpha1.GroupVersionPath, "/"), "shared-secret", agent.BootstrapRequest{
		Name: "compute-1",
	})
	if err != nil {
		t.Fatalf("agent bootstrap: %v", err)
	}

	if node.ID != "n-compute-1" {
		t.Errorf("node ID = %q, want n-compute-1", node.ID)
	}
	if node.State != core.NodeNotReady {
		t.Errorf("state = %q, want NOTREADY until the node answers", node.State)
	}

	select {
	case cmd := <-notices:
		if cmd.Target != node.ID {
			t.Errorf("notice target = %q, want %q", cmd.Target, node.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("controller was never notified")
	}
}

// A mismatched token must stop the agent, not let it start unregistered.
func TestAgentWithWrongTokenCannotJoin(t *testing.T) {
	commands, err := bus.ConnectNATS(natsURL(t), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { commands.Close() })

	store := memory.New()
	engine := gin.New()
	v1alpha1.Register(engine, v1alpha1.Config{
		Nodes:          &records.Nodes{Store: store},
		Bus:            commands,
		BootstrapToken: "the-real-one",
	})
	srv := newTestServer(t, engine)

	_, err = agent.Register(t.Context(), srv.URL, strings.TrimPrefix(v1alpha1.GroupVersionPath, "/"), "a-guess", agent.BootstrapRequest{
		Name: "compute-9",
	})
	if err == nil {
		t.Fatal("an agent with the wrong token was allowed to join")
	}

	nodes, listErr := store.ListNodes(t.Context())
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(nodes) != 0 {
		t.Fatalf("a rejected agent left %d node records behind", len(nodes))
	}
}

// newTestServer runs the real gin engine over a real listener, because the
// agent's client builds a full URL and an in-memory recorder would not catch a
// wrong path.
func newTestServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}
