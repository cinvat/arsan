// Package bootstrap_test exercises the agent registration endpoint over the
// real REST surface, against a real NATS JetStream.
//
// These are the tests that matter for bootstrap, because the failure they guard
// is silent: a rejected agent that still registers, or an accepted one that
// never reaches the controller, both look like success from the api's side.
//
//	nats-server -js -p 4222
//	NATS_URL=nats://127.0.0.1:4222 go test ./test/bootstrap/...
package bootstrap_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cinvat/arsan/api/v1alpha1"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/records"
	"github.com/cinvat/arsan/internal/store/memory"
	"github.com/gin-gonic/gin"
)

const version = "v1alpha1"

func natsURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("set NATS_URL to run: nats-server -js -p 4222")
	}
	return url
}

type harness struct {
	router  *gin.Engine
	store   *memory.Store
	notices chan bus.Command
}

// new builds the api with a real bus and a real store, and subscribes to the
// notice topic so a test can assert what the controller would have been told.
func new(t *testing.T, token string) *harness {
	t.Helper()

	commands, err := bus.ConnectNATS(natsURL(t), 5*time.Second)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(func() { commands.Close() })

	store := memory.New()
	nodes := &records.Nodes{Store: store}

	// Tests in this package run sequentially, so subscribing to the real
	// topic is safe. Drain first: the durable consumer may still hold a
	// message from an earlier test in the same run.
	notices := make(chan bus.Command, 8)
	// A per-test subscriber name: JetStream delivers each message to one
	// consumer per name, so a shared name would make this test resume the
	// previous one's position and miss its own message.
	stop, err := commands.SubscribeAs(t.Context(), bus.TopicNodeRegistered,
		strings.ReplaceAll(t.Name(), "/", "-"), func(_ context.Context, c bus.Command) error {
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

	// The durable consumer may redeliver a message a previous test's
	// consumer had not finished acking. Let that settle, then drain, so a
	// test only sees what its own requests produced.
	time.Sleep(200 * time.Millisecond)
	drain(notices)

	// The version package mounts onto an engine rather than owning one, so
	// api.Register can add future versions to the same router.
	engine := gin.New()
	v1alpha1.Register(engine, v1alpha1.Config{
		Nodes:          nodes,
		Bus:            commands,
		BootstrapToken: token,
	})

	return &harness{
		router:  engine,
		store:   store,
		notices: notices,
	}
}

// drain discards anything already queued.
func drain(ch chan bus.Command) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// body is a valid registration for the given token: identity and nothing
// else. Capacity and engine arrive later, when the controller asks after the
// probe succeeds.
func body(token string) map[string]any {
	return map[string]any{
		"token": token,
		"name":  "compute-1",
	}
}

func (h *harness) post(t *testing.T, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	drain(h.notices)
	blob, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+version+"/nodes", bytes.NewReader(blob))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.router.ServeHTTP(w, req)
	return w
}

// waitNotice blocks briefly for the controller notification.
func (h *harness) waitNotice(d time.Duration) (bus.Command, bool) {
	select {
	case c := <-h.notices:
		return c, true
	case <-time.After(d):
		return bus.Command{}, false
	}
}

func TestValidTokenRegistersAndNotifies(t *testing.T) {
	h := new(t, "s3cret")

	w := h.post(t, body("s3cret"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body)
	}

	var node core.Node
	if err := json.Unmarshal(w.Body.Bytes(), &node); err != nil {
		t.Fatalf("decode node: %v", err)
	}
	if node.ID != "n-compute-1" {
		t.Errorf("id = %q, want n-compute-1", node.ID)
	}
	if node.State != core.NodeNotReady {
		t.Errorf("state = %q, want NOTREADY", node.State)
	}

	cmd, ok := h.waitNotice(2 * time.Second)
	if !ok {
		t.Fatal("controller was never notified; the node would sit unmanaged")
	}
	if cmd.Topic != bus.TopicNodeRegistered {
		t.Errorf("topic = %q, want %q", cmd.Topic, bus.TopicNodeRegistered)
	}
	if cmd.Target != "n-compute-1" {
		t.Errorf("target = %q, want n-compute-1", cmd.Target)
	}
}

func TestWrongTokenIsRejectedAndSilent(t *testing.T) {
	h := new(t, "s3cret")

	// Near-misses included on purpose: a prefix, a trailing space and a
	// case change must all fail. A comparison that trims or folds case is
	// not comparing.
	for _, bad := range []string{"wrong", "", "s3cre", "s3cret ", " S3CRET"} {
		w := h.post(t, body(bad))
		if w.Code != http.StatusUnauthorized {
			t.Errorf("token %q: status = %d, want 401", bad, w.Code)
		}
	}

	if cmd, ok := h.waitNotice(300 * time.Millisecond); ok {
		t.Fatalf("a rejected agent was announced: %+v", cmd)
	}

	// And nothing was persisted.
	if _, err := h.store.GetNode(t.Context(), "n-compute-1"); err == nil {
		t.Fatal("a rejected bootstrap left a node record behind")
	}
}

// With no token configured, registration is closed rather than open.
func TestBootstrapClosedWhenUnconfigured(t *testing.T) {
	h := new(t, "")

	if w := h.post(t, body("anything")); w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 when bootstrap is unconfigured", w.Code)
	}
}

// An agent restarting must update its endpoint, not register a second node.
func TestBootstrapIsAnUpsert(t *testing.T) {
	h := new(t, "s3cret")

	if w := h.post(t, body("s3cret")); w.Code != http.StatusOK {
		t.Fatalf("first: %d %s", w.Code, w.Body)
	}

	second := h.post(t, body("s3cret"))
	if second.Code != http.StatusOK {
		t.Fatalf("second: %d %s", second.Code, second.Body)
	}

	var node core.Node
	if err := json.Unmarshal(second.Body.Bytes(), &node); err != nil {
		t.Fatal(err)
	}
	if node.ID != "n-compute-1" {
		t.Fatalf("id = %q; re-bootstrap created a second node", node.ID)
	}
	all, err := h.store.ListNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("%d nodes after re-bootstrap, want 1", len(all))
	}
}

func TestIncompleteRequestIsRejected(t *testing.T) {
	h := new(t, "s3cret")

	// A nameless request is malformed; a tokenless one is unauthenticated.
	// Either way the node must not be created.
	if w := h.post(t, map[string]any{"token": "s3cret"}); w.Code != http.StatusBadRequest {
		t.Errorf("no name: status = %d, want 400", w.Code)
	}
	if w := h.post(t, map[string]any{"name": "compute-1"}); w.Code != http.StatusUnauthorized {
		t.Errorf("no token: status = %d, want 401", w.Code)
	}
}

// The response body must not echo the token back.
func TestResponseDoesNotLeakToken(t *testing.T) {
	h := new(t, "s3cret")

	w := h.post(t, body("s3cret"))
	if bytes.Contains(w.Body.Bytes(), []byte("s3cret")) {
		t.Fatalf("response body contains the token: %s", w.Body)
	}
}
