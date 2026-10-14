// Package kvm is the libvirt compute engine.
//
// It drives the host's libvirt daemon through the virsh CLI rather than CGO
// bindings, so it compiles everywhere and needs only the libvirt client where
// it runs. virsh honours VIRSH_URI, which defaults to qemu:///system.
//
// Image handling is deliberately minimal: ImageRef names a qcow2 file already
// present on the host (a shared base image). The domain gets its own
// copy-on-write overlay? No — v1 uses the file directly and never deletes
// storage on undefine, so a shared base image is safe. Provisioning images is
// out of scope for this engine.
package kvm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cinvat/arsan/internal/core"
)

// Driver runs VMs through virsh.
type Driver struct {
	name string
}

// New fails fast when virsh is missing, so --driver=kvm on a host without
// libvirt is a clear startup error instead of a failure on first use.
func New() (*Driver, error) {
	if _, err := exec.LookPath("virsh"); err != nil {
		return nil, fmt.Errorf("kvm engine needs the virsh binary on PATH: %w", err)
	}
	return &Driver{name: "kvm"}, nil
}

func (d *Driver) Name() string { return d.name }

// run executes virsh with the caller's context, so a hung daemon cannot hang
// the controller's operation past its deadline.
func run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "virsh", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("virsh %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// Validate checks the daemon answers and the flavor asks for something real.
// It does not reserve anything; placement already claimed capacity.
func (d *Driver) Validate(ctx context.Context, f core.Flavor) error {
	if f.VCPU <= 0 || f.RAM <= 0 || f.Disk <= 0 {
		return fmt.Errorf("kvm: flavor %q has no resources: %w", f.Name, core.ErrRequestRejected)
	}
	if _, err := run(ctx, "version"); err != nil {
		return fmt.Errorf("kvm: libvirt unreachable: %w", err)
	}
	return nil
}

// domainName maps an instance id to a libvirt domain name. Libvirt is lenient
// about names, but keeping them prefixed avoids collisions with hand-made
// domains on the same host.
func domainName(id string) string { return "arsan-" + id }

// Create defines the domain. It does not start it; Start does that, mirroring
// the fake engine's define-then-start split. Defining an already-defined
// domain is success, so a retried command is a no-op.
func (d *Driver) Create(ctx context.Context, req core.CreateRequest) (core.InstanceInfo, error) {
	info := core.InstanceInfo{ID: req.InstanceID, NodeID: req.NodeID, Driver: d.name}
	if _, err := run(ctx, "dominfo", domainName(req.InstanceID)); err == nil {
		return info, nil
	}
	if strings.TrimSpace(req.ImageRef) == "" {
		return core.InstanceInfo{}, fmt.Errorf("kvm: image_ref is required and must name a qcow2 file on the host: %w", core.ErrRequestRejected)
	}
	xml := domainXML(domainName(req.InstanceID), req)
	path := filepath.Join(os.TempDir(), domainName(req.InstanceID)+".xml")
	if err := os.WriteFile(path, []byte(xml), 0o600); err != nil {
		return core.InstanceInfo{}, fmt.Errorf("kvm: write domain xml: %w", err)
	}
	defer os.Remove(path)
	if _, err := run(ctx, "define", path); err != nil {
		return core.InstanceInfo{}, err
	}
	return info, nil
}

// domainXML renders a minimal KVM guest: virtio disk and NIC, no graphics.
// RAM is GiB in the api and KiB in libvirt.
func domainXML(name string, req core.CreateRequest) string {
	return fmt.Sprintf(`<domain type='kvm'>
  <name>%s</name>
  <memory unit='KiB'>%d</memory>
  <vcpu>%d</vcpu>
  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
    <boot dev='hd'/>
  </os>
  <devices>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='%s'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <interface type='network'>
      <source network='default'/>
      <model type='virtio'/>
    </interface>
  </devices>
</domain>
`, name, req.Flavor.RAM*1024*1024, req.Flavor.VCPU, req.ImageRef)
}

// state reports the domain's state, or "" when it is not defined.
func state(ctx context.Context, id string) string {
	out, err := run(ctx, "domstate", domainName(id))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// Start boots the domain. An already-running domain is success.
func (d *Driver) Start(ctx context.Context, id string) error {
	switch state(ctx, id) {
	case "running":
		return nil
	case "":
		return core.ErrInstanceNotFound
	}
	_, err := run(ctx, "start", domainName(id))
	return err
}

// Stop shuts the domain down, destroying it if it refuses. An already-stopped
// domain is success.
func (d *Driver) Stop(ctx context.Context, id string) error {
	switch state(ctx, id) {
	case "shut off", "":
		return nil
	}
	if _, err := run(ctx, "shutdown", domainName(id)); err != nil {
		_, derr := run(ctx, "destroy", domainName(id))
		return derr
	}
	return nil
}

// Delete undefines the domain, stopping it first if needed. Storage is never
// removed: the disk may be a shared base image. An already-undefined domain
// is success.
func (d *Driver) Delete(ctx context.Context, id string) error {
	if state(ctx, id) == "" {
		return nil
	}
	if err := d.Stop(ctx, id); err != nil {
		return err
	}
	_, err := run(ctx, "undefine", domainName(id))
	return err
}

// Reboot restarts the domain, falling back to stop+start when the guest agent
// path fails.
func (d *Driver) Reboot(ctx context.Context, id string) error {
	if _, err := run(ctx, "reboot", domainName(id)); err == nil {
		return nil
	}
	if err := d.Stop(ctx, id); err != nil {
		return err
	}
	return d.Start(ctx, id)
}

// Migrate is not implemented: live migration needs the target's libvirt URI
// and shared storage, and this driver only knows a node id. Returning a clear
// error beats attempting a half-migration.
func (d *Driver) Migrate(_ context.Context, _, _ string) error {
	return fmt.Errorf("kvm: live migration is not implemented")
}

// HostCapacity reports what the host actually has: CPUs and memory from
// nodeinfo, free disk where guests live. Declaring capacity by hand would be a
// second copy of facts libvirt already knows, drifting the first time the
// host changes.
func (d *Driver) HostCapacity(ctx context.Context) (core.Resources, error) {
	info, err := run(ctx, "nodeinfo")
	if err != nil {
		return core.Resources{}, err
	}
	return core.Resources{
		VCPU: parseNodeinfoCPUs(info),
		RAM:  parseNodeinfoMemoryKiB(info) / (1024 * 1024),
		Disk: guestDiskFreeGiB(ctx),
	}, nil
}

// parseNodeinfoCPUs reads the "CPU(s): 8" line of `virsh nodeinfo`.
func parseNodeinfoCPUs(out string) int {
	for _, line := range strings.Split(out, "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(name) != "CPU(s)" {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// guestDiskFreeGiB is the free space where guest disks land. Unknown is zero,
// not an error — capacity detection must not fail a host whose disk layout is
// merely unusual.
func guestDiskFreeGiB(ctx context.Context) int {
	for _, dir := range []string{"/var/lib/libvirt/images", "/var/lib/libvirt", "/"} {
		out, err := dfAvailGiB(ctx, dir)
		if err == nil {
			return out
		}
	}
	return 0
}

func dfAvailGiB(ctx context.Context, dir string) (int, error) {
	cmd := exec.CommandContext(ctx, "df", "-BG", "--output=avail", dir)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return 0, err
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) < 2 {
		return 0, fmt.Errorf("df: unexpected output")
	}
	v := strings.TrimSuffix(strings.TrimSpace(lines[1]), "G")
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("df: unexpected output %q", lines[1])
	}
	return n, nil
}

// freecell. CPU and disk usage are not tracked by this engine yet; the probe
// only needs the call to succeed, and placement decisions come from
// reservations, not from here.
func (d *Driver) Usage(ctx context.Context) (core.Resources, error) {
	info, err := run(ctx, "nodeinfo")
	if err != nil {
		return core.Resources{}, err
	}
	free, err := run(ctx, "freecell", "--all")
	if err != nil {
		return core.Resources{}, err
	}
	totalKiB := parseNodeinfoMemoryKiB(info)
	freeKiB := parseKiBFirstField(lastLine(free))
	usedKiB := totalKiB - freeKiB
	if usedKiB < 0 {
		usedKiB = 0
	}
	return core.Resources{RAM: usedKiB / (1024 * 1024)}, nil
}

func lastLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		return ""
	}
	return lines[len(lines)-1]
}

// parseKiBFirstField reads a "123456 KiB" line. Unknown input is zero, not an
// error — usage is informational and must never fail a probe.
func parseKiBFirstField(line string) int {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return 0
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// parseNodeinfoMemoryKiB reads the "Memory size: 123456 KiB" line of
// `virsh nodeinfo`.
func parseNodeinfoMemoryKiB(out string) int {
	for _, line := range strings.Split(out, "\n") {
		name, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(name) != "Memory size" {
			continue
		}
		return parseKiBFirstField(value)
	}
	return 0
}
