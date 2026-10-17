package v1alpha1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/records"
	"github.com/cinvat/arsan/internal/store/memory"
	"github.com/gin-gonic/gin"
)

// newTestEngine mounts the whole version over an in-memory store, so a test
// exercises the real route table, not a hand-built subset of it.
func newTestEngine(t *testing.T) (*gin.Engine, *memory.Store) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	st := memory.New()
	r := gin.New()
	Register(r, Config{
		Instances: &records.Instances{Store: st},
		Nodes:     &records.Nodes{Store: st},
		Flavors:   &records.Flavors{Store: st},
		Volumes:   &records.Volumes{Store: st},
	})
	return r, st
}

// The groupVersion path is the discovery contract an aggregator fetches; it
// must name every kind this version routes, or a client cannot find it.
func TestDiscoveryNamesEveryKind(t *testing.T) {
	r, _ := newTestEngine(t)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1alpha1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1alpha1 = %d, want 200", rec.Code)
	}
	var doc struct {
		GroupVersion string `json:"groupVersion"`
		Resources    []struct {
			Name       string `json:"name"`
			Kind       string `json:"kind"`
			Namespaced bool   `json:"namespaced"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("decode discovery: %v", err)
	}
	if doc.GroupVersion != "v1alpha1" {
		t.Fatalf("groupVersion = %q, want v1alpha1", doc.GroupVersion)
	}
	want := map[string]bool{
		"Node": false, "Flavor": false, "VirtualMachine": true, "Volume": true,
	}
	for _, res := range doc.Resources {
		ns, ok := want[res.Kind]
		if !ok {
			t.Errorf("discovery names unrouted kind %q", res.Kind)
			continue
		}
		if res.Namespaced != ns {
			t.Errorf("%s namespaced = %v, want %v", res.Kind, res.Namespaced, ns)
		}
		delete(want, res.Kind)
	}
	for kind := range want {
		t.Errorf("discovery omits kind %q", kind)
	}
}

// Discovery advertises `get` for nodes, so the verb has to exist.
func TestGetNodeIsRouted(t *testing.T) {
	r, st := newTestEngine(t)
	n, err := (&records.Nodes{Store: st}).Register(context.Background(), core.RegisterNodeRequest{Name: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1alpha1/nodes/"+n.ID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1alpha1/nodes/%s = %d, want 200", n.ID, rec.Code)
	}
	var got core.Node
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode node: %v", err)
	}
	if got.ID != n.ID {
		t.Fatalf("node id = %q, want %q", got.ID, n.ID)
	}
}
