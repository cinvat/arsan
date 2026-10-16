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
	"slices"
	"strconv"
	"strings"

	"github.com/cinvat/arsan/internal/core"
)

// domainName maps an instance id to a libvirt domain name. Libvirt is lenient
// about names, but keeping them prefixed avoids collisions with hand-made
// domains on the same host.
func domainName(id string) string { return "arsan-" + id }

// domainXML renders a minimal KVM guest: virtio disk and NIC, no graphics.
// RAM is MiB in the api and KiB in libvirt.
//
// vCPU topology is always rendered: a bare count leaves the guest layout to
// QEMU's default, which silently differs from what the flavor promised.
// Dedicated flavors additionally pin each vCPU to its exclusive host CPU and
// pin the emulator threads alongside, so nothing else is scheduled there.
func domainXML(name string, req core.CreateRequest) string {
	v := req.Flavor.VCPU
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
  <memory unit='KiB'>%d</memory>
  <vcpu>%d</vcpu>
%s  %s
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
    <serial type='pty'>
      <target port='0'/>
    </serial>
    <console type='pty'>
      <target type='serial' port='0'/>
    </console>
  </devices>
</domain>
`, name, req.Flavor.RAM*1024, v.Count(), pinning, topology, req.ImageRef)
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
