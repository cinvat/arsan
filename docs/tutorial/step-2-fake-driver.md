# Step 2: Fake Driver

## Goal
Implement an in-memory `core.Driver` for testing and platforms without a hypervisor. Runs everywhere (macOS, Linux, Windows).

---

## File: `internal/driver/fake/driver.go`

```go
//go:build !kvm

package fake

import (
	"context"
	"fmt"
	"sync"

	"github.com/cinvat/arsan/internal/core"
)

type Driver struct {
	mu     sync.Mutex
	name   string
	vms    map[string]core.InstanceInfo
	usage  core.Resources
	failNext map[string]error // one-shot failures for testing
}

func New() *Driver {
	return &Driver{
		name:     "fake",
		vms:      make(map[string]core.InstanceInfo),
		failNext: make(map[string]error),
	}
}

func (d *Driver) FailNext(op string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failNext[op] = err
}

func (d *Driver) take(op string) error {
	if err, ok := d.failNext[op]; ok {
		delete(d.failNext, op)
		return err
	}
	return nil
}

func (d *Driver) Name() string { return d.name }

func (d *Driver) Validate(ctx context.Context, f core.Flavor) error {
	return d.take("Validate")
}

func (d *Driver) Create(ctx context.Context, req core.CreateRequest) (core.InstanceInfo, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.take("Create"); err != nil {
		return core.InstanceInfo{}, err
	}
	if _, exists := d.vms[req.InstanceID]; exists {
		return core.InstanceInfo{}, fmt.Errorf("fake: instance already exists")
	}
	info := core.InstanceInfo{ID: req.InstanceID, NodeID: req.NodeID, Driver: d.name, Started: false}
	d.vms[req.InstanceID] = info
	return info, nil
}

func (d *Driver) Start(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.take("Start"); err != nil {
		return err
	}
	info, ok := d.vms[id]
	if !ok {
		return core.ErrInstanceNotFound
	}
	info.Started = true
	d.vms[id] = info
	return nil
}

func (d *Driver) Stop(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, ok := d.vms[id]
	if !ok {
		return core.ErrInstanceNotFound
	}
	info.Started = false
	d.vms[id] = info
	return nil
}

// Delete is IDEMPOTENT: deleting unknown instance returns nil, NOT ErrInstanceNotFound.
// Compute retries Delete after crashes. A driver that errors on "already gone"
// turns recovery into a failure loop.
func (d *Driver) Delete(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.take("Delete"); err != nil {
		return err
	}
	delete(d.vms, id)
	return nil
}

func (d *Driver) Reboot(ctx context.Context, id string) error {
	if err := d.Stop(ctx, id); err != nil {
		return err
	}
	return d.Start(ctx, id)
}

func (d *Driver) Migrate(ctx context.Context, id, targetNodeID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	info, ok := d.vms[id]
	if !ok {
		return core.ErrInstanceNotFound
	}
	info.NodeID = targetNodeID
	d.vms[id] = info
	return nil
}

func (d *Driver) Usage(ctx context.Context) (core.Resources, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.usage, nil
}
```

---

## Key Design Decisions

### Idempotent Delete
```go
func (d *Driver) Delete(ctx context.Context, id string) error {
    delete(d.vms, id)
    return nil  // Never returns ErrInstanceNotFound
}
```
**Why**: Compute retries Delete after partial failures. If the driver errors on "already gone", recovery loops forever.

### One-Shot Failure Injection
```go
func (d *Driver) FailNext(op string, err error) { ... }

func (d *Driver) take(op string) error {
    if err, ok := d.failNext[op]; ok {
        delete(d.failNext, op)
        return err
    }
    return nil
}
```
Allows tests to inject failures: `driver.FailNext("Create", errors.New("disk full"))`

### Build Tag
```go
//go:build !kvm
```
Excludes this file when building with `-tags kvm`, so the real KVM driver can register under the same name.

---

## Test: `internal/driver/fake/driver_test.go`

```go
package fake

import (
	"context"
	"errors"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func TestDriver_CreateStartStopDelete(t *testing.T) {
	d := New()
	ctx := context.Background()

	info, err := d.Create(ctx, core.CreateRequest{
		InstanceID: "i-1", NodeID: "n-1", Name: "test",
		Flavor: core.Flavor{VCPU: 1, RAM: 2, Disk: 10}, ImageRef: "cirros",
	})
	if err != nil { t.Fatal(err) }
	if info.ID != "i-1" || info.Started { t.Fatal(info) }

	if err := d.Start(ctx, "i-1"); err != nil { t.Fatal(err) }
	if !d.vms["i-1"].Started { t.Fatal("not started") }

	if err := d.Stop(ctx, "i-1"); err != nil { t.Fatal(err) }
	if d.vms["i-1"].Started { t.Fatal("not stopped") }

	// Idempotent delete
	if err := d.Delete(ctx, "i-1"); err != nil { t.Fatal(err) }
	if err := d.Delete(ctx, "i-1"); err != nil { t.Fatal("second delete failed") }
}

func TestDriver_FailNext(t *testing.T) {
	d := New()
	ctx := context.Background()

	d.FailNext("Create", errors.New("injected failure"))
	_, err := d.Create(ctx, core.CreateRequest{InstanceID: "i-1"})
	if err == nil || err.Error() != "injected failure" { t.Fatal(err) }

	// One-shot: second call succeeds
	_, err = d.Create(ctx, core.CreateRequest{InstanceID: "i-2"})
	if err != nil { t.Fatal(err) }
}

func TestDriver_DeleteIdempotent(t *testing.T) {
	d := New()
	ctx := context.Background()
	if err := d.Delete(ctx, "unknown"); err != nil { t.Fatal(err) }
}
```

---

## Run Tests

```bash
go test ./internal/driver/fake/...
```

---

## Verify Driver Registry

```bash
go run . drivers   # one binary: api | controller | agent
# fake       ready
```

---

## Next

Step 3: In-Memory Store