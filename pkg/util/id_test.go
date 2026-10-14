package util

import "testing"

func TestNewID(t *testing.T) {
	a, b := NewID("i"), NewID("i")
	if a == b {
		t.Fatal("NewID returned the same value twice")
	}
	if got := a[:2]; got != "i-" {
		t.Fatalf("prefix = %q, want %q", got, "i-")
	}
	if bare := NewID(""); bare[0] == '-' {
		t.Fatalf("empty prefix produced a leading dash: %q", bare)
	}
}

func TestSlug(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"compute-1", "compute-1"},
		{"Compute Node 1", "compute-node-1"},
		{"  COMPUTE   NODE   1  ", "compute-node-1"},
		{"compute__1", "compute-1"},
		{"a/b/c", "a-b-c"},
		{"--trim--", "trim"},
		{"", ""},
		{"!!!", ""},
		{"node.vcpu8", "node-vcpu8"},
	}
	for _, tt := range tests {
		if got := Slug(tt.in); got != tt.want {
			t.Errorf("Slug(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// The property that actually matters: one host spelled two ways must not
// register as two nodes.
func TestSlugIsStableAcrossSpellings(t *testing.T) {
	a := Slug("Compute Node 1")
	b := Slug("compute-node-1")
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
}
