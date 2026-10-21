package alvand

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func portResponse() string {
	return `{"status":{"id":"port-1","state":"READY","mac":"02:00:00:00:00:01","ip":"10.0.0.5","ovnLogicalPort":"port-1","binding":{"vifType":"ovs","bridge":"br-int","ifaceId":"port-1"}}}`
}

func TestAttachSendsBindingHostAndResolvesDetails(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(portResponse()))
	}))
	defer srv.Close()

	got, err := New(srv.URL).Attach(context.Background(), "port-1", "i-1", "eth0", "compute-1")
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/apis/alvand/v1alpha1/logicalports/port-1/attach" {
		t.Fatalf("path = %s", gotPath)
	}
	if gotBody["bindingHost"] != "compute-1" || gotBody["instanceRef"] != "i-1" || gotBody["device"] != "eth0" {
		t.Fatalf("body = %v", gotBody)
	}
	if got.PortID != "port-1" || got.MAC != "02:00:00:00:00:01" || got.IP != "10.0.0.5" {
		t.Fatalf("resolved = %+v", got)
	}
	if got.OVNLogicalPort != "port-1" || got.VIFType != "ovs" || got.Bridge != "br-int" {
		t.Fatalf("vif = %+v", got)
	}
}

func TestAttachDefaultsWhenBindingMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":{"id":"port-2","mac":"02:00:00:00:00:02"}}`))
	}))
	defer srv.Close()

	got, err := New(srv.URL).Attach(context.Background(), "port-2", "i-1", "eth0", "compute-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.OVNLogicalPort != "port-2" || got.VIFType != "ovs" || got.Bridge != "br-int" {
		t.Fatalf("defaults = %+v", got)
	}
}

func TestDetachTreatsNotFoundAsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"code":"not_found"}`, http.StatusNotFound)
	}))
	defer srv.Close()

	if err := New(srv.URL).Detach(context.Background(), "gone"); err != nil {
		t.Fatalf("detach missing port: %v", err)
	}
}

func TestAttachSurfacesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := New(srv.URL).Attach(context.Background(), "port-1", "i-1", "eth0", "compute-1"); err == nil {
		t.Fatal("expected an error")
	}
}
