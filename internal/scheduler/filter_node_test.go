package scheduler

import (
	"context"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func nodeWith(id string) core.Node {
	return core.Node{ID: id, State: core.NodeReady, Enabled: true}
}

func TestExcludeNodesFilter_DropsExcluded(t *testing.T) {
	nodes := []core.Node{nodeWith("n1"), nodeWith("n2"), nodeWith("n3")}

	got, err := ExcludeNodesFilter{}.FilterNodes(context.Background(),
		core.PlacementRequest{ExcludeNodeIDs: []string{"n1", "n3"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "n2" {
		t.Fatalf("got %v, want [n2]", ids(got))
	}
}

func TestExcludeNodesFilter_NoExclusions(t *testing.T) {
	nodes := []core.Node{nodeWith("n1"), nodeWith("n2")}

	got, err := ExcludeNodesFilter{}.FilterNodes(context.Background(),
		core.PlacementRequest{}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %v, want both nodes", ids(got))
	}
}

// An excluded node that does not exist must not disturb the rest.
func TestExcludeNodesFilter_UnknownIDIsHarmless(t *testing.T) {
	nodes := []core.Node{nodeWith("n1"), nodeWith("n2")}

	got, err := ExcludeNodesFilter{}.FilterNodes(context.Background(),
		core.PlacementRequest{ExcludeNodeIDs: []string{"ghost"}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %v, want both nodes", ids(got))
	}
}

func ids(nodes []core.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}
