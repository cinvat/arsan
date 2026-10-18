//go:build kvm

package kvm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
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

// lookup returns the domain handle for an instance id, or nil when it is not
// defined. A lookup miss is not an error here because every operation treats
// "already gone" as success; callers that need existence check for nil
// themselves.
//
// The fast path is the legacy "arsan-<id>" name. Namespaced domains are
// found by scanning for our metadata id, which id-only operations cannot
// derive from the id alone. The scan costs one list plus XML reads, paid
// only on lifecycle operations, never on the scheduling hot path.
//
// The handle must be freed by the caller.
func (d *Driver) lookup(id string) (*libvirt.Domain, error) {
	dom, err := d.conn.LookupDomainByName("arsan-" + id)
	if err == nil {
		return dom, nil
	}
	if !errors.Is(err, libvirt.ERR_NO_DOMAIN) {
		return nil, err
	}
	doms, err := d.conn.ListAllDomains(0)
	if err != nil {
		return nil, err
	}
	for i := range doms {
		dom := &doms[i]
		desc, err := dom.GetXMLDesc(0)
		if err != nil {
			dom.Free()
			continue
		}
		if got, ok := parseDomainInstanceID(desc); ok && got == id {
			return dom, nil
		}
		dom.Free()
	}
	return nil, nil
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
	if f.RAM <= 0 {
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
	// A dedicated flavor without exactly its vCPUs' worth of pins is a
	// scheduler bug, not a placement to attempt: booting it floating would
	// silently break the exclusivity the flavor promises.
	if req.Flavor.VCPU.Dedicated && len(req.PinnedCPUs) != req.Flavor.VCPU.Count() {
		return core.InstanceInfo{}, fmt.Errorf("kvm: dedicated flavor needs %d pins, got %d: %w",
			req.Flavor.VCPU.Count(), len(req.PinnedCPUs), core.ErrRequestRejected)
	}
	// Resolve bases and provision owned files before defining anything:
	// missing qemu-img or an unwritable dir fails here, before any domain
	// exists to clean up.
	files, bases, err := d.resolveStorage(ctx, req)
	if err != nil {
		return core.InstanceInfo{}, err
	}
	if err := d.provisionAll(ctx, req, files, bases); err != nil {
		return core.InstanceInfo{}, err
	}
	var vncPort int
	err = d.do(ctx, func() error {
		dom, err := d.lookup(req.InstanceID)
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
				if desc, err := dom.GetXMLDesc(0); err == nil {
					vncPort = parseVNCPort(desc)
				}
				return nil
			}
			if err := dom.Undefine(); err != nil {
				return err
			}
		}
		dom, err = d.conn.DomainDefineXML(domainXML(domainName(req), req, files, bases))
		if err != nil {
			return err
		}
		defer dom.Free()
		if desc, err := dom.GetXMLDesc(0); err == nil {
			vncPort = parseVNCPort(desc)
		}
		return nil
	})
	if err != nil {
		return core.InstanceInfo{}, err
	}
	info.VNCPort = vncPort
	return info, nil
}

// VNCPort returns the auto-assigned VNC port for a defined domain. Libvirt
// assigns the port when the domain starts, so callers should invoke this
// after Start.
func (d *Driver) VNCPort(ctx context.Context, id string) (int, error) {
	var port int
	err := d.do(ctx, func() error {
		dom, err := d.lookup(id)
		if err != nil {
			return err
		}
		if dom == nil {
			return core.ErrInstanceNotFound
		}
		defer dom.Free()
		desc, err := dom.GetXMLDesc(0)
		if err != nil {
			return err
		}
		port = parseVNCPort(desc)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return port, nil
}

// running reports whether the domain is up. A missing domain is not an error;
// callers decide what absence means for their operation.
func (d *Driver) running(id string) (exists, running bool, err error) {
	dom, err := d.lookup(id)
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
		exists, running, err := d.running(id)
		if err != nil {
			return err
		}
		if !exists {
			return core.ErrInstanceNotFound
		}
		if running {
			return nil
		}
		dom, err := d.lookup(id)
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
		exists, running, err := d.running(id)
		if err != nil {
			return err
		}
		if !exists || !running {
			return nil
		}
		dom, err := d.lookup(id)
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

// Delete undefines the domain, stopping it first if needed, then removes the
// volumes this driver provisioned (root overlay plus extras, read off the
// domain's own metadata). Bases and isos are never touched: they are shared.
// A missing domain is success.
func (d *Driver) Delete(ctx context.Context, id string) error {
	return d.do(ctx, func() error {
		dom, err := d.lookup(id)
		if err != nil || dom == nil {
			return nil
		}
		defer dom.Free()
		desc, err := dom.GetXMLDesc(0)
		if err != nil {
			return err
		}
		state, _, err := dom.GetState()
		if err != nil {
			return err
		}
		if state == libvirt.DOMAIN_RUNNING {
			if err := dom.Destroy(); err != nil {
				return err
			}
		}
		if err := dom.Undefine(); err != nil {
			return err
		}
		// Only files the record marked delete-on-terminate go: shared
		// volumes outlive any one VM, and isos were never listed at all.
		for _, vol := range parseDomainVolumes(desc) {
			if !vol.delete {
				continue
			}
			if err := os.Remove(vol.file); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		return nil
	})
}

// UpdateVolumes redefines a stopped domain with a new volume set: attach and
// detach on SHUTOFF, and boot moves, converge here. Provision first so a
// failed qemu-img leaves the old definition alone.
func (d *Driver) UpdateVolumes(ctx context.Context, req core.CreateRequest) error {
	return d.do(ctx, func() error {
		files, bases, err := d.resolveStorage(ctx, req)
		if err != nil {
			return err
		}
		if err := d.provisionAll(ctx, req, files, bases); err != nil {
			return err
		}
		dom, err := d.lookup(req.InstanceID)
		if err != nil {
			return err
		}
		if dom == nil {
			return core.ErrInstanceNotFound
		}
		defer dom.Free()
		state, _, err := dom.GetState()
		if err != nil {
			return err
		}
		if state == libvirt.DOMAIN_RUNNING {
			return fmt.Errorf("kvm: update volumes of a running domain: stop it first: %w", core.ErrInvalidTransition)
		}
		if err := dom.Undefine(); err != nil {
			return err
		}
		dom, err = d.conn.DomainDefineXML(domainXML(domainName(req), req, files, bases))
		if err != nil {
			return err
		}
		defer dom.Free()
		return nil
	})
}

// AttachVolume provisions the volume's file and hotplugs it: live plus
// config when the guest runs, redefine when it does not. The record already
// carries the attachment; the engine only converges the hypervisor to it.
// Isos need no file: they attach straight from the shared image.
func (d *Driver) AttachVolume(ctx context.Context, req core.CreateRequest, att core.AttachedVolume) error {
	return d.do(ctx, func() error {
		files, bases, err := d.resolveStorage(ctx, req)
		if err != nil {
			return err
		}
		if err := d.provisionVolume(ctx, att, files, bases); err != nil {
			return err
		}
		dom, err := d.lookup(req.InstanceID)
		if err != nil {
			return err
		}
		if dom == nil {
			return core.ErrInstanceNotFound
		}
		defer dom.Free()
		state, _, err := dom.GetState()
		if err != nil {
			return err
		}
		if state != libvirt.DOMAIN_RUNNING {
			return d.redefine(ctx, req, files, bases)
		}
		return dom.AttachDeviceFlags(
			d.deviceXML(att, files, bases, ""),
			libvirt.DOMAIN_DEVICE_MODIFY_LIVE|libvirt.DOMAIN_DEVICE_MODIFY_CONFIG,
		)
	})
}

// DetachVolume unplugs the volume and deletes its file when the volume says
// DeleteOnTermination (shared volumes and isos are never removed). The
// record already dropped the attachment; the file delete always happens for
// owned files, so a shutoff detach cannot strand storage.
func (d *Driver) DetachVolume(ctx context.Context, req core.CreateRequest, att core.AttachedVolume) error {
	return d.do(ctx, func() error {
		files, bases, err := d.resolveStorage(ctx, req)
		if err != nil {
			return err
		}
		dom, err := d.lookup(req.InstanceID)
		if err != nil {
			return err
		}
		if dom == nil {
			// No domain left: still delete the file below, then done.
		} else {
			defer dom.Free()
			state, _, err := dom.GetState()
			if err != nil {
				return err
			}
			if state == libvirt.DOMAIN_RUNNING {
				if err := dom.DetachDeviceFlags(
					d.deviceXML(att, files, bases, ""),
					libvirt.DOMAIN_DEVICE_MODIFY_LIVE|libvirt.DOMAIN_DEVICE_MODIFY_CONFIG,
				); err != nil {
					return err
				}
			} else if err := d.redefine(ctx, req, files, bases); err != nil {
				return err
			}
		}
		if !att.Volume.DeleteOnTermination {
			return nil // shared volume or iso: the record, not the file, goes
		}
		if f, ok := files[att.Volume.ID]; ok {
			if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		return nil
	})
}

// deviceXML renders one attach/detach snippet, identical to what the full
// domain carries: DetachDevice matches on the XML it is given, so live ops
// and defines must agree byte for byte.
func (d *Driver) deviceXML(att core.AttachedVolume, files, bases map[string]string, bootOrder string) string {
	if att.Volume.Cdrom {
		return cdromDeviceXML(bases[att.Volume.ID], bootOrder)
	}
	return diskDeviceXML(att.Device, files[att.Volume.ID], bootOrder)
}

// redefine swaps the definition for a freshly rendered one. The caller holds
// the do-mutex and has checked the domain is not running.
func (d *Driver) redefine(ctx context.Context, req core.CreateRequest, files, bases map[string]string) error {
	dom, err := d.lookup(req.InstanceID)
	if err != nil {
		return err
	}
	if dom == nil {
		return core.ErrInstanceNotFound
	}
	defer dom.Free()
	state, _, err := dom.GetState()
	if err != nil {
		return err
	}
	if state == libvirt.DOMAIN_RUNNING {
		return fmt.Errorf("kvm: redefine a running domain: stop it first: %w", core.ErrInvalidTransition)
	}
	if err := dom.Undefine(); err != nil {
		return err
	}
	dom, err = d.conn.DomainDefineXML(domainXML(domainName(req), req, files, bases))
	if err != nil {
		return err
	}
	defer dom.Free()
	return nil
}

// resolveStorage maps every attached volume to its local file and base:
// overlays resolve their base (URLs download once into the image cache),
// blank disks need no base, isos point at the resolved image directly. Files
// land next to the first base on hand; blank-only sets land in ".". Both maps
// are keyed by volume id and fully deterministic, so attach, define and
// detach agree without consulting the record.
func (d *Driver) resolveStorage(ctx context.Context, req core.CreateRequest) (files, bases map[string]string, err error) {
	files = make(map[string]string, len(req.Volumes))
	bases = make(map[string]string, len(req.Volumes))
	dir := "."
	for _, att := range req.Volumes {
		if att.Volume.Cdrom || att.Volume.Image == "" {
			continue
		}
		p, rerr := resolveImage(ctx, att.Volume.Image)
		if rerr != nil {
			return nil, nil, rerr
		}
		bases[att.Volume.ID] = p
		dir = volumeDir(p, dir)
	}
	for _, att := range req.Volumes {
		if att.Volume.Cdrom {
			continue
		}
		files[att.Volume.ID] = volumeFile(dir, att.Volume)
	}
	return files, bases, nil
}

// provisionAll creates every owned file for a fresh build. Existing files
// are left alone, so a retried build is a no-op here too.
func (d *Driver) provisionAll(ctx context.Context, req core.CreateRequest, files, bases map[string]string) error {
	for _, att := range req.Volumes {
		if err := d.provisionVolume(ctx, att, files, bases); err != nil {
			return err
		}
	}
	return nil
}

// provisionVolume creates one qcow2 if absent: an overlay of its base, or a
// blank disk of its size. A blank disk without a size is a request bug,
// reported rather than guessed at.
func (d *Driver) provisionVolume(ctx context.Context, att core.AttachedVolume, files, bases map[string]string) error {
	if att.Volume.Cdrom {
		return nil // isos are shared, never provisioned
	}
	path, ok := files[att.Volume.ID]
	if !ok || path == "" {
		return fmt.Errorf("kvm: no file for volume %s: %w", att.Volume.ID, core.ErrRequestRejected)
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if _, err := exec.LookPath("qemu-img"); err != nil {
		return fmt.Errorf("kvm: qemu-img not found in PATH: %w", err)
	}
	args := []string{"create", "-f", "qcow2"}
	if base := bases[att.Volume.ID]; base != "" {
		args = append(args, "-F", "qcow2", "-b", base)
	} else {
		if att.Volume.SizeMiB <= 0 {
			return fmt.Errorf("kvm: blank volume %s needs a size: %w", att.Volume.ID, core.ErrRequestRejected)
		}
		args = append(args, qemuImgSize(att.Volume.SizeMiB))
	}
	args = append(args, path)
	if out, err := exec.CommandContext(ctx, "qemu-img", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("kvm: qemu-img create %s: %s: %w", path, strings.TrimSpace(string(out)), err)
	}
	return nil
}

// qemuImgSize renders MiB in qemu-img's K/M/G units, preferring whole GB.
func qemuImgSize(mib int) string {
	if mib%1024 == 0 {
		return fmt.Sprintf("%dG", mib/1024)
	}
	return fmt.Sprintf("%dM", mib)
}

// Reboot restarts the domain, falling back to stop+start when the guest path
// fails.
func (d *Driver) Reboot(ctx context.Context, id string) error {
	err := d.do(ctx, func() error {
		dom, err := d.lookup(id)
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

// Domains reports observed guest states for reconcile. Only domains this
// driver defined (the arsan- prefix) are reported, and only steady guest
// states: BLOCKED folds into RUNNING, PMSUSPENDED into PAUSED, and NOSTATE
// is skipped rather than stored.
func (d *Driver) Domains(ctx context.Context) ([]core.DomainStatus, error) {
	var out []core.DomainStatus
	err := d.do(ctx, func() error {
		doms, err := d.conn.ListAllDomains(0)
		if err != nil {
			return err
		}
		for i := range doms {
			dom := &doms[i]
			desc, err := dom.GetXMLDesc(0)
			if err != nil {
				dom.Free()
				continue
			}
			id, ok := parseDomainInstanceID(desc)
			if !ok {
				// Domains defined before metadata existed carry only the
				// legacy "arsan-<id>" name; read the id back off it so an
				// upgrade does not report them missing.
				name, err := dom.GetName()
				if err != nil {
					dom.Free()
					continue
				}
				if id, ok = strings.CutPrefix(name, "arsan-"); !ok {
					dom.Free()
					continue
				}
			}
			state, _, err := dom.GetState()
			dom.Free()
			if err != nil {
				continue
			}
			s, ok := guestState(state)
			if !ok {
				continue
			}
			out = append(out, core.DomainStatus{InstanceID: id, State: s})
		}
		return nil
	})
	return out, err
}

// guestState maps a libvirt domain state onto a stored guest state. The
// second return is false for NOSTATE, which is never recorded: it means
// libvirt itself does not know, and churning the record on it helps nothing.
func guestState(state libvirt.DomainState) (core.InstanceState, bool) {
	switch state {
	case libvirt.DOMAIN_RUNNING, libvirt.DOMAIN_BLOCKED:
		return core.StateRunning, true
	case libvirt.DOMAIN_SHUTOFF:
		return core.StateShutoff, true
	case libvirt.DOMAIN_SHUTDOWN:
		return core.StateShutdown, true
	case libvirt.DOMAIN_PAUSED, libvirt.DOMAIN_PMSUSPENDED:
		return core.StatePaused, true
	case libvirt.DOMAIN_CRASHED:
		return core.StateCrashed, true
	default:
		return "", false
	}
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
