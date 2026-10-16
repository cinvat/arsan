package kvm

import (
	"strings"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func TestDomainXML(t *testing.T) {
	req := core.CreateRequest{
		InstanceID: "i-1", Name: "web1", Namespace: "team-a",
		Flavor:   core.Flavor{Name: "m1", VCPU: core.VCPUCount(2), RAM: 4},
		ImageRef: "/var/lib/libvirt/images/base.qcow2",
	}
	xml := domainXML(domainName(req), req)
	for _, want := range []string{
		"<name>team-a-web1</name>",
		"<arsan:instance xmlns:arsan='https://cinvat.github.io/arsan' id='i-1'/>",
		"<memory unit='KiB'>4096</memory>", // 4 MiB in KiB
		"<vcpu>2</vcpu>",
		"<topology sockets='1' cores='2' threads='1'/>",
		"<source file='/var/lib/libvirt/images/team-a-web1-0.qcow2'/>",
		"type='kvm'",
		"<serial type='pty'>",
		"<console type='pty'>",
		"<target type='serial' port='0'/>",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("domain xml missing %q:\n%s", want, xml)
		}
	}
	if strings.Contains(xml, "vcpupin") {
		t.Errorf("shared flavor must not pin:\n%s", xml)
	}
	id, ok := parseDomainInstanceID(xml)
	if !ok || id != "i-1" {
		t.Fatalf("parseDomainInstanceID = %q,%v, want i-1,true", id, ok)
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

func TestDomainXMLStorage(t *testing.T) {
	req := core.CreateRequest{
		InstanceID: "i-9", Name: "web1", Namespace: "team-a",
		Flavor:   core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 32},
		Disks:    []core.Disk{{Name: "data", Index: 1, SizeMiB: 50 * 1024}},
		ImageRef: "/var/lib/libvirt/images/base.qcow2",
	}
	xml := domainXML(domainName(req), req)
	for _, want := range []string{
		"<name>team-a-web1</name>",
		"<source file='/var/lib/libvirt/images/team-a-web1-0.qcow2'/>",
		"<target dev='vda' bus='virtio'/>",
		"<source file='/var/lib/libvirt/images/team-a-web1-1.qcow2'/>",
		"<target dev='vdb' bus='virtio'/>",
		"<boot order='1'/>",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("domain xml missing %q:\n%s", want, xml)
		}
	}
	if strings.Contains(xml, "cdrom") {
		t.Errorf("no cdrom requested, none must render:\n%s", xml)
	}
	files := parseDomainVolumes(xml)
	if len(files) != 2 {
		t.Fatalf("volumes = %v, want root + data", files)
	}

	cd := req
	cd.CdromImage = "/var/lib/libvirt/images/rescue.iso"
	cd.BootDevice = core.BootCDROM
	cdx := domainXML(domainName(cd), cd)
	for _, want := range []string{
		"<source file='/var/lib/libvirt/images/rescue.iso'/>",
		"<target dev='sda' bus='sata'/>",
		"<readonly/>",
	} {
		if !strings.Contains(cdx, want) {
			t.Errorf("cdrom xml missing %q:\n%s", want, cdx)
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
	if got := domainName(core.CreateRequest{InstanceID: "i-1"}); got != "arsan-i-1" {
		t.Fatalf("legacy row without namespace: got %q", got)
	}
	req := core.CreateRequest{InstanceID: "i-1", Name: "web1", Namespace: "team-a"}
	if got := domainName(req); got != "team-a-web1" {
		t.Fatalf("got %q, want team-a-web1", got)
	}
	if got := core.DomainName("team-a", "web1"); got != "team-a-web1" {
		t.Fatalf("got %q", got)
	}
}
