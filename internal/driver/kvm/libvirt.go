//go:build kvm

package kvm

import (
	"context"
	"errors"
	"fmt"
	"sync"

	libvirt "libvirt.org/go/libvirt"

	"github.com/cinvat/arsan/internal/core"
)

// uri names the daemon explicitly. An empty URI asks libvirt to guess a
// socket, which fails on hosts where the daemon was never started — an
// explicit URI fails instead with "cannot connect", which names the cure.
const uri = "qemu:///system"

// Driver runs VMs through the libvirt daemon directly.
//
// A libvirt connection serialises its calls internally, but a mutex keeps
// concurrent engine operations from interleaving at this layer too. Blocking C
// calls run in a goroutine so the caller's context still bounds them; on
// timeout the call keeps running server-side and the goroutine is abandoned,
// which is the documented cost of bounding a C call.
type Driver struct {
	mu   sync.Mutex
	conn *libvirt.Connect
	name string
}

// New connects to the local daemon. A missing daemon or missing headers at
// build time is a clear startup error, not a failure on first use.
func New() (*Driver, error) {
	conn, err := libvirt.NewConnect(uri)
	if err != nil {
		return nil, fmt.Errorf("kvm: connect to libvirt: %w", err)
	}
	return &Driver{conn: conn, name: "kvm"}, nil
}

func (d *Driver) Name() string { return d.name }

// Close releases the connection. The agent calls it on shutdown.
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.conn.Close()
	return err
}

// do runs fn with the mutex held and bounded by ctx.
func (d *Driver) do(ctx context.Context, fn func() error) error {
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		done <- result{fn()}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-done:
		return r.err
	}
}

// lookup returns the domain handle, or nil when it is not defined. A lookup
// miss is not an error here because every operation treats "already gone" as
// success; callers that need existence check for nil themselves.
//
// The handle must be freed by the caller.
func (d *Driver) lookup(name string) (*libvirt.Domain, error) {
	dom, err := d.conn.LookupDomainByName(name)
	if err != nil {
		if errors.Is(err, libvirt.ERR_NO_DOMAIN) {
			return nil, nil
		}
		return nil, err
	}
	return dom, nil
}

// pinsCurrent reports whether dom's definition already matches req's pinning.
// Shared flavors have no pinning to drift, so they are always current. A
// running domain counts as current however stale it looks: replacing its
// definition under a live guest is worse than the drift, and live pin updates
// are out of scope.
func pinsCurrent(dom *libvirt.Domain, req core.CreateRequest) (bool, error) {
	if !req.Flavor.VCPU.Dedicated {
		return true, nil
	}
	desc, err := dom.GetXMLDesc(0)
	if err != nil {
		return false, err
	}
	if pinsMatchDesc(desc, req) {
		return true, nil
	}
	state, _, err := dom.GetState()
	if err != nil {
		return false, err
	}
	return state == libvirt.DOMAIN_RUNNING, nil
}

// Validate checks the daemon answers and the flavor asks for something real.
// It does not reserve anything; placement already claimed capacity.
func (d *Driver) Validate(ctx context.Context, f core.Flavor) error {
	if err := f.VCPU.Validate(); err != nil {
		return fmt.Errorf("kvm: flavor %q has no resources: %w", f.Name, core.ErrRequestRejected)
	}
	if f.RAM <= 0 || f.Disk <= 0 {
		return fmt.Errorf("kvm: flavor %q has no resources: %w", f.Name, core.ErrRequestRejected)
	}
	return d.do(ctx, func() error {
		_, err := d.conn.GetNodeInfo()
		if err != nil {
			return fmt.Errorf("kvm: libvirt unreachable: %w", err)
		}
		return nil
	})
}

// Create defines the domain. It does not start it; Start does that. Defining
// an already-defined domain is success, so a retried command is a no-op —
// unless a dedicated flavor's pins drifted, in which case the stale
// definition is replaced (never under a running guest).
func (d *Driver) Create(ctx context.Context, req core.CreateRequest) (core.InstanceInfo, error) {
	info := core.InstanceInfo{ID: req.InstanceID, NodeID: req.NodeID, Driver: d.name}
	if req.ImageRef == "" {
		return core.InstanceInfo{}, fmt.Errorf("kvm: image_ref is required: %w", core.ErrRequestRejected)
	}
	// A dedicated flavor without exactly its vCPUs' worth of pins is a
	// scheduler bug, not a placement to attempt: booting it floating would
	// silently break the exclusivity the flavor promises.
	if req.Flavor.VCPU.Dedicated && len(req.PinnedCPUs) != req.Flavor.VCPU.Count() {
		return core.InstanceInfo{}, fmt.Errorf("kvm: dedicated flavor needs %d pins, got %d: %w",
			req.Flavor.VCPU.Count(), len(req.PinnedCPUs), core.ErrRequestRejected)
	}
	disk, err := resolveImage(ctx, req.ImageRef)
	if err != nil {
		return core.InstanceInfo{}, err
	}
	req.ImageRef = disk
	err = d.do(ctx, func() error {
		dom, err := d.lookup(domainName(req.InstanceID))
		if err != nil {
			return err
		}
		if dom != nil {
			defer dom.Free()
			// A retry is a no-op only when the definition is current. A
			// rebuilt dedicated VM can arrive with different pins than the
			// stale domain holds; keeping it would double-book CPUs the
			// node record calls exclusive.
			if current, err := pinsCurrent(dom, req); err != nil {
				return err
			} else if current {
				return nil
			}
			if err := dom.Undefine(); err != nil {
				return err
			}
		}
		dom, err = d.conn.DomainDefineXML(domainXML(domainName(req.InstanceID), req))
		if err != nil {
			return err
		}
		defer dom.Free()
		return nil
	})
	if err != nil {
		return core.InstanceInfo{}, err
	}
	return info, nil
}

// running reports whether the domain is up. A missing domain is not an error;
// callers decide what absence means for their operation.
func (d *Driver) running(name string) (exists, running bool, err error) {
	dom, err := d.lookup(name)
	if err != nil || dom == nil {
		return dom != nil, false, err
	}
	defer dom.Free()
	state, _, err := dom.GetState()
	if err != nil {
		return true, false, err
	}
	return true, state == libvirt.DOMAIN_RUNNING, nil
}

// Start boots the domain. An already-running domain is success.
func (d *Driver) Start(ctx context.Context, id string) error {
	return d.do(ctx, func() error {
		exists, running, err := d.running(domainName(id))
		if err != nil {
			return err
		}
		if !exists {
			return core.ErrInstanceNotFound
		}
		if running {
			return nil
		}
		dom, err := d.lookup(domainName(id))
		if err != nil || dom == nil {
			return core.ErrInstanceNotFound
		}
		defer dom.Free()
		return dom.Create()
	})
}

// Stop shuts the domain down, destroying it if it refuses. A stopped or
// missing domain is success.
func (d *Driver) Stop(ctx context.Context, id string) error {
	return d.do(ctx, func() error {
		exists, running, err := d.running(domainName(id))
		if err != nil {
			return err
		}
		if !exists || !running {
			return nil
		}
		dom, err := d.lookup(domainName(id))
		if err != nil || dom == nil {
			return nil
		}
		defer dom.Free()
		if err := dom.Shutdown(); err != nil {
			return dom.Destroy()
		}
		return nil
	})
}

// Delete undefines the domain, stopping it first if needed. Storage is never
// removed: the disk may be a shared base image. A missing domain is success.
func (d *Driver) Delete(ctx context.Context, id string) error {
	return d.do(ctx, func() error {
		dom, err := d.lookup(domainName(id))
		if err != nil || dom == nil {
			return nil
		}
		defer dom.Free()
		state, _, err := dom.GetState()
		if err != nil {
			return err
		}
		if state == libvirt.DOMAIN_RUNNING {
			if err := dom.Destroy(); err != nil {
				return err
			}
		}
		return dom.Undefine()
	})
}

// Reboot restarts the domain, falling back to stop+start when the guest path
// fails.
func (d *Driver) Reboot(ctx context.Context, id string) error {
	err := d.do(ctx, func() error {
		dom, err := d.lookup(domainName(id))
		if err != nil {
			return err
		}
		if dom == nil {
			return core.ErrInstanceNotFound
		}
		defer dom.Free()
		return dom.Reboot(libvirt.DOMAIN_REBOOT_DEFAULT)
	})
	if err != nil {
		if errors.Is(err, core.ErrInstanceNotFound) {
			return err
		}
		if serr := d.Stop(ctx, id); serr != nil {
			return serr
		}
		return d.Start(ctx, id)
	}
	return nil
}

// Migrate is not implemented: live migration needs the target's libvirt URI
// and shared storage, and this driver only knows a node id. Returning a clear
// error beats attempting a half-migration.
func (d *Driver) Migrate(_ context.Context, _, _ string) error {
	return fmt.Errorf("kvm: live migration is not implemented")
}

// HostCapacity reports what the host actually has: CPUs and memory from the
// node, free disk summed over the active storage pools. No pools, or none
// active, means zero disk — placement still accounts disk through
// reservations, and a zero here only means "unknown", never "none".
func (d *Driver) HostCapacity(ctx context.Context) (core.Resources, error) {
	var out core.Resources
	err := d.do(ctx, func() error {
		info, err := d.conn.GetNodeInfo()
		if err != nil {
			return err
		}
		out = core.Resources{CPU: int(info.Cpus), RAM: int(info.Memory / 1024)}
		pools, err := d.conn.ListAllStoragePools(0)
		if err != nil {
			return err
		}
		var diskBytes uint64
		for i := range pools {
			pool := &pools[i]
			active, err := pool.IsActive()
			if err != nil {
				pool.Free()
				continue
			}
			if !active {
				pool.Free()
				continue
			}
			pinfo, err := pool.GetInfo()
			pool.Free()
			if err != nil {
				continue
			}
			diskBytes += pinfo.Available
		}
		out.Disk = int(diskBytes / (1024 * 1024))
		return nil
	})
	return out, err
}

// Usage sums what running domains hold. Placement decisions come from
// reservations, not from here; this is what the controller records and an
// operator reads.
func (d *Driver) Usage(ctx context.Context) (core.Resources, error) {
	var out core.Resources
	err := d.do(ctx, func() error {
		doms, err := d.conn.ListAllDomains(0)
		if err != nil {
			return err
		}
		for i := range doms {
			dom := &doms[i]
			info, err := dom.GetInfo()
			dom.Free()
			if err != nil {
				continue
			}
			if info.State != libvirt.DOMAIN_RUNNING {
				continue
			}
			out.CPU += int(info.NrVirtCpu)
			out.RAM += int(info.Memory / 1024)
		}
		return nil
	})
	return out, err
}
