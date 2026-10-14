package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cinvat/arsan/internal/core"
)

func TestRegisterPostsEverythingTheControllerNeeds(t *testing.T) {
	var got BootstrapRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1alpha1/nodes" {
			t.Errorf("path = %q, want /v1alpha1/nodes", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		decodeBody(t, r, &got)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ID":"n-compute-1","Name":"compute-1","State":"NOTREADY","Address":"10.0.0.11:9090"}`))
	}))
	defer srv.Close()

	node, err := Register(t.Context(), srv.URL, "v1alpha1", "s3cret", BootstrapRequest{
		Name:    "compute-1",
		Arch:    "amd64",
		Driver:  "kvm",
		Address: "10.0.0.11:9090",
		Total:   core.Resources{VCPU: 8, RAM: 16, Disk: 200},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if node.ID != "n-compute-1" {
		t.Errorf("node ID = %q, want n-compute-1", node.ID)
	}
	if got.Token != "s3cret" {
		t.Errorf("token = %q, want the configured one", got.Token)
	}
	// The address is the whole point: it is how the controller dials back.
	if got.Address != "10.0.0.11:9090" {
		t.Errorf("address = %q, want the agent's endpoint", got.Address)
	}
	if got.Total.VCPU != 8 || got.Total.RAM != 16 || got.Total.Disk != 200 {
		t.Errorf("total = %+v, want the declared capacity", got.Total)
	}
}

func TestRegisterReportsRejectedToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"invalid token"}`))
	}))
	defer srv.Close()

	_, err := Register(t.Context(), srv.URL, "v1alpha1", "wrong", BootstrapRequest{Name: "n"})
	if err == nil {
		t.Fatal("a 401 must be an error; the agent must not proceed")
	}

	var be *BootstrapError
	if !errors.As(err, &be) {
		t.Fatalf("error type = %T, want *BootstrapError", err)
	}
	if !be.Unauthenticated() {
		t.Error("a 401 must be reported as an authentication failure")
	}
}

func TestRegisterTrimsTrailingSlashOnAPIURL(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	// A trailing slash must not produce a doubled separator in the path.
	if _, err := Register(t.Context(), srv.URL+"/", "v1alpha1", "t", BootstrapRequest{}); err != nil {
		t.Fatal(err)
	}
	if path != "/v1alpha1/nodes" {
		t.Fatalf("path = %q, want /v1alpha1/nodes", path)
	}
}

// A refused token must not be retried: waiting will not make it valid, and an
// agent looping on a 401 looks alive while being permanently unable to join.
func TestRetryBootstrapDoesNotRetryOnUnauthorized(t *testing.T) {
	var calls atomic.Int32
	fn := func(context.Context) (core.Node, error) {
		calls.Add(1)
		return core.Node{}, &BootstrapError{StatusCode: http.StatusUnauthorized}
	}

	_, err := RetryBootstrap(t.Context(), fn, 5, time.Millisecond)
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("made %d attempts, want 1: a refused token must not be retried", got)
	}
}

// An unreachable api is a startup ordering problem, so it is retried.
func TestRetryBootstrapRetriesUntilItWorks(t *testing.T) {
	var calls atomic.Int32
	fn := func(context.Context) (core.Node, error) {
		if calls.Add(1) < 3 {
			return core.Node{}, errors.New("connection refused")
		}
		return core.Node{ID: "n-1"}, nil
	}

	node, err := RetryBootstrap(t.Context(), fn, 5, time.Millisecond)
	if err != nil {
		t.Fatalf("expected success on the third attempt: %v", err)
	}
	if node.ID != "n-1" {
		t.Fatalf("node = %+v", node)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("made %d attempts, want 3", got)
	}
}

func TestRetryBootstrapGivesUp(t *testing.T) {
	var calls atomic.Int32
	fn := func(context.Context) (core.Node, error) {
		calls.Add(1)
		return core.Node{}, errors.New("connection refused")
	}

	if _, err := RetryBootstrap(t.Context(), fn, 3, time.Millisecond); err == nil {
		t.Fatal("expected failure after exhausting attempts")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("made %d attempts, want 3", got)
	}
}

func TestRetryBootstrapStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	fn := func(context.Context) (core.Node, error) { return core.Node{}, errors.New("down") }
	if _, err := RetryBootstrap(ctx, fn, 10, time.Second); err == nil {
		t.Fatal("expected an error once the context is cancelled")
	}
}

// decodeBody reads a JSON request body into dst.
func decodeBody(t *testing.T, r *http.Request, dst any) {
	t.Helper()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
}
