package kvm

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestIsRemote(t *testing.T) {
	for url, want := range map[string]bool{
		"https://example.com/base.qcow2": true,
		"http://192.168.1.1/img.qcow2":   true,
		"/var/lib/libvirt/base.qcow2":    false,
		"base.qcow2":                     false,
		"":                               false,
	} {
		if got := isRemote(url); got != want {
			t.Errorf("isRemote(%q) = %v, want %v", url, got, want)
		}
	}
}

func TestResolveLocalPath(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if got, err := resolveImage(t.Context(), f.Name()); err != nil || got != f.Name() {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := resolveImage(t.Context(), filepath.Join(t.TempDir(), "missing.qcow2")); err == nil {
		t.Fatal("expected an error for a missing local image")
	}
}

// A URL downloads once; the second build reuses the file without touching
// the network again.
func TestFetchCachesByURL(t *testing.T) {
	imageCacheDir = t.TempDir()
	t.Cleanup(func() { imageCacheDir = "/var/lib/arsan/images" })

	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		_, _ = w.Write([]byte("fake-qcow2-bytes"))
	}))
	defer srv.Close()

	first, err := resolveImage(t.Context(), srv.URL+"/base.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolveImage(t.Context(), srv.URL+"/base.qcow2")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("cache paths differ: %q vs %q", first, second)
	}
	if hits != 1 {
		t.Fatalf("server hit %d times, want exactly 1", hits)
	}
	raw, err := os.ReadFile(first)
	if err != nil || string(raw) != "fake-qcow2-bytes" {
		t.Fatalf("cached content = %q, %v", raw, err)
	}
}

func TestFetchRejectsBadStatus(t *testing.T) {
	imageCacheDir = t.TempDir()
	t.Cleanup(func() { imageCacheDir = "/var/lib/arsan/images" })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	if _, err := resolveImage(t.Context(), srv.URL+"/nope.qcow2"); err == nil {
		t.Fatal("expected an error for a 404 image")
	}
}
