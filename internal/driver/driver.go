package driver

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cinvat/arsan/internal/core"
)

// Factory builds a compute engine.
type Factory func() (core.Driver, error)

var (
	mu       sync.Mutex
	registry = map[string]Factory{}
)

// Register makes an engine available by name.
//
// Engines are only ever run on an agent. The controller does not host VMs, so it
// never constructs one of these: it dials the agent, and the agent uses whatever
// engine it was started with.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	registry[name] = f
}

// Names lists the registered engine names.
func Names() []string {
	mu.Lock()
	defer mu.Unlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// New builds the named engine.
func New(name string) (core.Driver, error) {
	mu.Lock()
	f, ok := registry[name]
	mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown driver %q, available %v: %w", name, Names(), core.ErrRequestRejected)
	}
	d, err := f()
	if err != nil {
		return nil, fmt.Errorf("driver %q: %w", name, err)
	}
	return d, nil
}

// Engines register themselves from build-tagged files (register_fake.go,
// register_kvm.go), so the set of available engines follows the build.
