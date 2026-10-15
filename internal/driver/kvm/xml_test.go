package kvm

import (
	"strings"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func TestDomainXML(t *testing.T) {
	xml := domainXML("arsan-i-1", core.CreateRequest{
		InstanceID: "i-1",
		Flavor:     core.Flavor{Name: "m1", VCPU: core.VCPUCount(2), RAM: 4},
		ImageRef:   "/var/lib/libvirt/images/base.qcow2",
	})
	for _, want := range []string{
		"<name>arsan-i-1</name>",
		"<memory unit='KiB'>4096</memory>", // 4 MiB in KiB
		"<vcpu>2</vcpu>",
		"<topology sockets='1' cores='2' threads='1'/>",
		"<source file='/var/lib/libvirt/images/base.qcow2'/>",
		"type='kvm'",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("domain xml missing %q:\n%s", want, xml)
		}
	}
	if strings.Contains(xml, "vcpupin") {
		t.Errorf("shared flavor must not pin:\n%s", xml)
	}
}

func TestDomainXMLDedicated(t *testing.T) {
	xml := domainXML("arsan-i-2", core.CreateRequest{
		InstanceID: "i-2",
		Flavor: core.Flavor{Name: "m1.dedicated",
			VCPU: core.VCPU{Sockets: 1, Cores: 2, Threads: 2, Dedicated: true}},
		PinnedCPUs: []int{4, 5, 6, 7},
		ImageRef:   "/var/lib/libvirt/images/base.qcow2",
	})
	for _, want := range []string{
		"<vcpu>4</vcpu>",
		"<topology sockets='1' cores='2' threads='2'/>",
		"<vcpupin vcpu='0' cpuset='4'/>",
		"<vcpupin vcpu='3' cpuset='7'/>",
		"<emulatorpin cpuset='4-7'/>",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("domain xml missing %q:\n%s", want, xml)
		}
	}
}

func TestPinsMatchDesc(t *testing.T) {
	req := core.CreateRequest{
		InstanceID: "i-2",
		Flavor: core.Flavor{Name: "m1.dedicated",
			VCPU: core.VCPU{Sockets: 1, Cores: 2, Threads: 1, Dedicated: true}},
		PinnedCPUs: []int{4, 5},
		ImageRef:   "/var/lib/libvirt/images/base.qcow2",
	}
	if !pinsMatchDesc(domainXML("arsan-i-2", req), req) {
		t.Fatal("own rendering must match")
	}
	stale := req
	stale.PinnedCPUs = []int{0, 1}
	if pinsMatchDesc(domainXML("arsan-i-2", req), stale) {
		t.Fatal("different pins must not match")
	}
	shared := core.CreateRequest{Flavor: core.Flavor{VCPU: core.VCPUCount(2)}}
	if !pinsMatchDesc("<domain/>", shared) {
		t.Fatal("shared flavors are always current")
	}
}

func TestDomainNamePrefixes(t *testing.T) {
	if got := domainName("i-1"); got != "arsan-i-1" {
		t.Fatalf("got %q", got)
	}
}
