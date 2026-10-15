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

	"github.com/cinvat/arsan/internal/core"
)

// domainName maps an instance id to a libvirt domain name. Libvirt is lenient
// about names, but keeping them prefixed avoids collisions with hand-made
// domains on the same host.
func domainName(id string) string { return "arsan-" + id }

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
