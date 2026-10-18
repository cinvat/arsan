// Package kvm is the libvirt compute engine, built on the native
// libvirt.org/go/libvirt bindings.
//
// It compiles only with the kvm build tag, because the bindings are CGO and
// need libvirt headers where they build:
//
//	go build -tags kvm ./...
//
// Without the tag the engine is unregistered and --driver=kvm fails with a
// clear "unknown driver" error listing what is available. There is no virsh
// subprocess anywhere in this path.
//
// Image handling is deliberately minimal: ImageRef names a qcow2 file already
// present on the host (a shared base image). Delete never removes storage, so
// sharing that file is safe. Provisioning images is out of scope.
package kvm

import (
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/cinvat/arsan/internal/core"
	"github.com/samber/lo"
)

// domainName maps an instance to a libvirt domain name: "<namespace>-<name>".
// Rows written before namespaces existed keep their "arsan-<id>" name, so an
// upgrade never orphans (or duplicates) a running domain by renaming it.
func domainName(req core.CreateRequest) string {
	if req.Namespace == "" {
		return "arsan-" + req.InstanceID
	}
	return core.DomainName(req.Namespace, req.Name)
}

// domainXML renders a minimal KVM guest: virtio disks and NIC, no graphics.
// RAM is MiB in the api and KiB in libvirt.
//
// vCPU topology is always rendered: a bare count leaves the guest layout to
// QEMU's default, which silently differs from what the flavor promised.
// Dedicated flavors additionally pin each vCPU to its exclusive host CPU and
// pin the emulator threads alongside, so nothing else is scheduled there.
//
// files maps volume id to the provisioned qcow2 to use as its source, bases
// to the resolved base the file overlays (absent for blank disks). Isos are
// never provisioned: the cdrom points at the resolved base directly.
func domainXML(name string, req core.CreateRequest, files, bases map[string]string) string {
	v := req.Flavor.VCPU
	// The instance id rides in metadata because id-only ops (start, stop,
	// delete) cannot derive "<namespace>-<name>" from the id alone. Name
	// stays human-readable; metadata stays the lookup key.
	//
	// Volumes ride along too: every qcow2 this driver provisioned is listed
	// file by file, so Delete removes exactly what Create made, even across
	// restarts. Bases and isos stay out: shared storage is never removed.
	meta := fmt.Sprintf(`<metadata><arsan:instance xmlns:arsan='https://cinvat.github.io/arsan' id='%s'/>%s</metadata>`,
		req.InstanceID, volumesMeta(req, files))
	topology := fmt.Sprintf(`<cpu><topology sockets='%d' cores='%d' threads='%d'/></cpu>`,
		v.Sockets, v.Cores, v.Threads)
	pinning := ""
	if len(req.PinnedCPUs) > 0 {
		var pins strings.Builder
		for i, cpu := range req.PinnedCPUs {
			fmt.Fprintf(&pins, "\n    <vcpupin vcpu='%d' cpuset='%d'/>", i, cpu)
		}
		fmt.Fprintf(&pins, "\n    <emulatorpin cpuset='%s'/>", cpuset(req.PinnedCPUs))
		pinning = fmt.Sprintf(`<cputune>%s
  </cputune>
  `, pins.String())
	}
	return fmt.Sprintf(`<domain type='kvm'>
  <name>%s</name>
%s
  <memory unit='KiB'>%d</memory>
  <vcpu>%d</vcpu>
%s  %s
  <os>
    <type arch='x86_64' machine='pc'>hvm</type>
  </os>
  <devices>
%s
    <interface type='network'>
      <source network='default'/>
      <model type='virtio'/>
    </interface>
    <serial type='pty'>
      <target port='0'/>
    </serial>
    <console type='pty'>
      <target type='serial' port='0'/>
    </console>
  </devices>
</domain>
`, name, meta, req.Flavor.RAM*1024, v.Count(), pinning, topology, storageXML(req, files, bases))
}

// volumeDir places a provisioned file next to the base it derives from:
// same filesystem, so overlays stay cheap, and no new path to configure.
// Blank disks land next to the first base on hand, else the work directory.
func volumeDir(base, fallback string) string {
	if base != "" {
		if dir := filepath.Dir(base); dir != "" {
			return dir
		}
	}
	if fallback != "" {
		if dir := filepath.Dir(fallback); dir != "" {
			return dir
		}
	}
	return "."
}

// volumeFile names the qcow2 this driver owns for one volume: namespace,
// volume name and an id fragment. Stable across detaches (it keys off the
// volume, never its position), unique across renames (the id never repeats),
// and traceable in ls. Bases and isos are never renamed or moved.
func volumeFile(dir string, vol core.Volume) string {
	id := vol.ID
	if len(id) > 12 {
		id = id[len(id)-12:]
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%s.qcow2", vol.Name, id))
}

// bootOrder returns the boot element for a device: order 1 on the volume
// carrying boot index 0, nothing otherwise. At most one volume boots;
// records enforce that, the renderer trusts it.
func bootOrder(idx *int) string {
	if idx != nil && *idx == 0 {
		return "\n      <boot order='1'/>"
	}
	return ""
}

// volumesMeta lists every owned file for Delete: one entry per provisioned
// qcow2. Bases and isos stay out, so deleting a VM never removes shared
// storage even though the metadata travels in the domain.
func volumesMeta(req core.CreateRequest, files map[string]string) string {
	byID := make(map[string]core.AttachedVolume, len(req.Volumes))
	for _, att := range req.Volumes {
		byID[att.Volume.ID] = att
	}
	ids := slices.Sorted(maps.Keys(files))
	var b strings.Builder
	for _, id := range ids {
		del := "false"
		if byID[id].Volume.DeleteOnTermination {
			del = "true"
		}
		b.WriteString(fmt.Sprintf(`<arsan:volume file='%s' delete='%s'/>`, files[id], del))
	}
	return b.String()
}

// ownedVolume is one provisioned file plus whether VM delete removes it.
type ownedVolume struct {
	file   string
	delete bool
}

// parseDomainVolumes reads back what volumesMeta wrote. The split's first
// segment precedes any marker (it holds the instance element), so only the
// segments after a marker carry a file path.
func parseDomainVolumes(desc string) []ownedVolume {
	segs := strings.Split(desc, "<arsan:volume file='")
	var out []ownedVolume
	for _, seg := range segs[1:] {
		i := strings.IndexByte(seg, '\'')
		if i <= 0 {
			continue
		}
		out = append(out, ownedVolume{file: seg[:i], delete: strings.Contains(seg, "delete='true'")})
	}
	return out
}

// storageXML renders every attached volume in attach order: qcow2 disks on
// virtio, isos as read-only sata cdroms. The volume carrying boot index 0
// gets boot order 1; the rest boot in attach order after it. Device names
// come from the attachment record, never recomputed, so they survive
// detaches.
func storageXML(req core.CreateRequest, files, bases map[string]string) string {
	var b strings.Builder
	for i, att := range req.Volumes {
		var dev string
		if att.Volume.Cdrom {
			// Prefer the resolved base, fall back to the raw image: an iso
			// needs no provisioning map entry to render.
			src := bases[att.Volume.ID]
			if src == "" {
				src = att.Volume.Image
			}
			dev = cdromDeviceXML(src, bootOrderOf(att.Volume.BootIndex))
		} else {
			dev = diskDeviceXML(att.Device, files[att.Volume.ID], bootOrderOf(att.Volume.BootIndex))
		}
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(dev)
	}
	return b.String()
}

// bootOrderOf returns the boot element for an attachment: order 1 on boot
// index 0, nothing otherwise. Records keep at most one bootable, set-boot
// moves it, so the renderer never arbitrates.
func bootOrderOf(idx *int) string {
	if idx != nil && *idx == 0 {
		return "\n      <boot order='1'/>"
	}
	return ""
}

// diskDeviceXML renders one virtio data disk. Live attach and detach build
// the identical snippet from the same helper, because DetachDevice matches
// on the XML it is given.
func diskDeviceXML(target, source string, bootOrder string) string {
	return fmt.Sprintf(`<disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='%s'/>%s
      <target dev='%s' bus='virtio'/>
    </disk>`, source, bootOrder, target)
}

// cdromDeviceXML renders the read-only sata cdrom.
func cdromDeviceXML(source string, bootOrder string) string {
	return fmt.Sprintf(`<disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='%s'/>%s
      <target dev='sda' bus='sata'/>
      <readonly/>
    </disk>`, source, bootOrder)
}

// parseDomainInstanceID extracts the instance id our renderer stored in
// domain metadata. It matches our own rendering byte-for-byte, which is
// exactly the set of domains this driver defined.
// arsanInstanceIDRe matches the instance id whether libvirt preserved our
// attribute order and quoting or normalised them. Attribute order and quote
// style are not part of the contract; only the id value is.
var arsanInstanceIDRe = regexp.MustCompile(`<arsan:instance\b[^>]*?\bid\s*=\s*['"]([^'"]+)['"]`)

func parseDomainInstanceID(desc string) (string, bool) {
	m := arsanInstanceIDRe.FindStringSubmatch(desc)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// pinsMatchDesc reports whether a domain XML description carries exactly
// req's pinning, one vcpupin line per vCPU. Pure string matching against our
// own rendering: no libvirt handle needed, so it is unit-testable.
func pinsMatchDesc(desc string, req core.CreateRequest) bool {
	if !req.Flavor.VCPU.Dedicated {
		return true
	}
	if len(req.PinnedCPUs) != req.Flavor.VCPU.Count() {
		return false
	}
	present := lo.Map(req.PinnedCPUs, func(cpu int, i int) bool {
		return strings.Contains(desc, fmt.Sprintf("<vcpupin vcpu='%d' cpuset='%d'/>", i, cpu))
	})
	return !lo.Contains(present, false)
}

// cpuset renders pins as a libvirt cpuset: "0-3,7".
func cpuset(pins []int) string {
	sorted := slices.Clone(pins)
	slices.Sort(sorted)
	var parts []string
	start, prev := sorted[0], sorted[0]
	flush := func() {
		if start == prev {
			parts = append(parts, strconv.Itoa(start))
		} else {
			parts = append(parts, strconv.Itoa(start)+"-"+strconv.Itoa(prev))
		}
	}
	for _, c := range sorted[1:] {
		if c == prev+1 {
			prev = c
			continue
		}
		flush()
		start, prev = c, c
	}
	flush()
	return strings.Join(parts, ",")
}
