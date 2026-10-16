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
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/cinvat/arsan/internal/core"
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

// domainXML renders a minimal KVM guest: virtio disk and NIC, no graphics.
// RAM is MiB in the api and KiB in libvirt.
//
// vCPU topology is always rendered: a bare count leaves the guest layout to
// QEMU's default, which silently differs from what the flavor promised.
// Dedicated flavors additionally pin each vCPU to its exclusive host CPU and
// pin the emulator threads alongside, so nothing else is scheduled there.
func domainXML(name string, req core.CreateRequest) string {
	v := req.Flavor.VCPU
	// The instance id rides in metadata because id-only ops (start, stop,
	// delete) cannot derive "<namespace>-<name>" from the id alone. Name
	// stays human-readable; metadata stays the lookup key.
	//
	// Volumes ride along too: every qcow2 this driver provisioned (root
	// overlay plus extras, never bases or isos) is listed file by file, so
	// Delete removes exactly what Create made, even across restarts.
	meta := fmt.Sprintf(`<metadata><arsan:instance xmlns:arsan='https://cinvat.github.io/arsan' id='%s'/>%s</metadata>`,
		req.InstanceID, volumesMeta(req))
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
    <boot dev='hd'/>
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
`, name, meta, req.Flavor.RAM*1024, v.Count(), pinning, topology, storageXML(req))
}

// dirFor places provisioned files next to the base image they derive from:
// same filesystem, so overlays stay cheap, and no new path to configure.
func dirFor(req core.CreateRequest) string {
	if dir := filepath.Dir(req.ImageRef); dir != "" {
		return dir
	}
	return "."
}

// volumeBase is the file stem every owned volume shares. Legacy rows without
// a namespace keep the arsan-<id> stem, matching their domain name.
func volumeBase(req core.CreateRequest) string {
	if req.Namespace == "" {
		return "arsan-" + req.InstanceID
	}
	return core.DomainName(req.Namespace, req.Name)
}

// volumePath names the qcow2 this driver owns for one disk: base plus stable
// file index. Bases and isos are never renamed or moved.
func volumePath(dir string, req core.CreateRequest, index int) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%d.qcow2", volumeBase(req), index))
}

// rootVolume is the thin overlay of the base image, always index 0.
func rootVolume(req core.CreateRequest) string {
	return volumePath(dirFor(req), req, 0)
}

// dataVolume names extra d, indexed from 1 in creation order. Indexes are
// never reused, so detaching disk 1 does not rename disk 2's file.
func dataVolume(req core.CreateRequest, d core.Disk) string {
	return volumePath(dirFor(req), req, d.Index)
}

// targetDev maps a file index onto a virtio letter: 0 is vda, 1 is vdb and
// so on. Index-derived, never slice-derived, so the letter survives detaches.
func targetDev(index int) string {
	return fmt.Sprintf("vd%c", 'a'+byte(index))
}

// volumesMeta lists every owned file for Delete. Only overlays and blank
// provisioned disks appear here; the base and any iso stay out.
func volumesMeta(req core.CreateRequest) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf(`<arsan:volume file='%s'/>`, rootVolume(req)))
	for _, d := range req.Disks {
		b.WriteString(fmt.Sprintf(`<arsan:volume file='%s'/>`, dataVolume(req, d)))
	}
	return b.String()
}

// parseDomainVolumes reads back what volumesMeta wrote. The split's first
// segment precedes any marker (it holds the instance element), so only the
// segments after a marker carry a file path.
func parseDomainVolumes(desc string) []string {
	segs := strings.Split(desc, "<arsan:volume file='")
	var out []string
	for _, seg := range segs[1:] {
		if i := strings.IndexByte(seg, '\''); i > 0 {
			out = append(out, seg[:i])
		}
	}
	return out
}

// storageXML renders the root overlay, every extra disk and the optional
// cdrom. Exactly one device carries boot order 1: the cdrom when boot says
// so, else the root. Declaration order otherwise decides, KubeVirt-style.
func storageXML(req core.CreateRequest) string {
	var b strings.Builder
	bootCD := req.BootDevice == core.BootCDROM && req.CdromImage != ""
	boot := func(wantCD bool) string {
		if (wantCD && bootCD) || (!wantCD && !bootCD) {
			return "\n      <boot order='1'/>"
		}
		return ""
	}
	b.WriteString(diskDeviceXML("vda", rootVolume(req), boot(false)))
	for _, d := range req.Disks {
		b.WriteString("\n" + diskDeviceXML(targetDev(d.Index), dataVolume(req, d), ""))
	}
	if req.CdromImage != "" {
		b.WriteString("\n" + cdromDeviceXML(req.CdromImage, boot(true)))
	}
	return b.String()
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
func parseDomainInstanceID(desc string) (string, bool) {
	const open = `<arsan:instance xmlns:arsan='https://cinvat.github.io/arsan' id='`
	i := strings.Index(desc, open)
	if i < 0 {
		return "", false
	}
	rest := desc[i+len(open):]
	j := strings.IndexByte(rest, '\'')
	if j < 0 {
		return "", false
	}
	return rest[:j], true
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
	for i, cpu := range req.PinnedCPUs {
		if !strings.Contains(desc, fmt.Sprintf("<vcpupin vcpu='%d' cpuset='%d'/>", i, cpu)) {
			return false
		}
	}
	return true
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
