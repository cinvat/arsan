//go:build !kvm

package fake

import (
	"context"
	"fmt"
	"sync"

	"github.com/cinvat/arsan/internal/core"
)

type Driver struct {
	mu       sync.Mutex
	name     string
	vms      map[string]core.InstanceInfo
	usage    core.Resources
	failNext map[string]error
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

	info := core.InstanceInfo{
		ID:      req.InstanceID,
		NodeID:  req.NodeID,
		Driver:  d.name,
		Started: false,
	}
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
