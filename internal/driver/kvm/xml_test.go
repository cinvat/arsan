package kvm

import (
	"strings"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func testVolume(id, name string, boot *int) core.AttachedVolume {
	return core.AttachedVolume{Volume: core.Volume{
		ID: id, Name: name, Namespace: "team-a",
		SizeMiB: 10240, Image: "/var/lib/libvirt/images/base.qcow2",
		BootIndex: boot, DeleteOnTermination: true,
	}}
}

func TestDomainXML(t *testing.T) {
	zero := 0
	req := core.CreateRequest{
		InstanceID: "i-1", Name: "web1", Namespace: "team-a",
		Flavor: core.Flavor{Name: "m1", VCPU: core.VCPUCount(2), RAM: 4},
		Volumes: []core.AttachedVolume{
			{Volume: core.Volume{ID: "vol-root", Name: "root", Namespace: "team-a", SizeMiB: 10240, Image: "/var/lib/libvirt/images/base.qcow2", BootIndex: &zero, DeleteOnTermination: true}, Device: "vda"},
		},
	}
	files := map[string]string{"vol-root": "/var/lib/libvirt/images/team-a-root-xyz.qcow2"}
	bases := map[string]string{"vol-root": "/var/lib/libvirt/images/base.qcow2"}
	xml := domainXML(domainName(req), req, files, bases)
	for _, want := range []string{
		"<name>team-a-web1</name>",
		"<arsan:instance xmlns:arsan='https://cinvat.github.io/arsan' id='i-1'/>",
		"<arsan:volume file='/var/lib/libvirt/images/team-a-root-xyz.qcow2' delete='true'/>",
		"<memory unit='KiB'>4096</memory>", // 4 MiB in KiB
		"<vcpu>2</vcpu>",
		"<topology sockets='1' cores='2' threads='1'/>",
		"<source file='/var/lib/libvirt/images/team-a-root-xyz.qcow2'/>",
		"<target dev='vda' bus='virtio'/>",
		"<boot order='1'/>",
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
	got := parseDomainVolumes(xml)
	if len(got) != 1 || got[0].file != "/var/lib/libvirt/images/team-a-root-xyz.qcow2" || !got[0].delete {
		t.Fatalf("volumes = %+v, want the root with delete", got)
	}
}

func TestDomainXMLInterfaces(t *testing.T) {
	req := core.CreateRequest{
		InstanceID: "i-3", Name: "web1", Namespace: "team-a",
		Flavor: core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 1},
		Interfaces: []core.LogicalSwitchInterface{
			{PortID: "port-1", MAC: "02:00:00:00:00:01", OVNLogicalPort: "port-1", VIFType: "ovs", Bridge: "br-int"},
			{PortID: "port-2", MAC: "02:00:00:00:00:02", VIFType: "bridge", Bridge: "br-ex"},
		},
	}
	xml := domainXML(domainName(req), req, nil, nil)
	for _, want := range []string{
		"<interface type='bridge'>",
		"<mac address='02:00:00:00:00:01'/>",
		"<source bridge='br-int'/>",
		"<virtualport type='openvswitch'>",
		"<parameters interfaceid='port-1'/>",
		"<source bridge='br-ex'/>",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("domain xml missing %q:\n%s", want, xml)
		}
	}
	if strings.Contains(xml, "network='default'") {
		t.Errorf("interfaces must replace the default network:\n%s", xml)
	}
	if strings.Count(xml, "type='openvswitch'") != 1 {
		t.Errorf("only the ovs interface gets a virtualport:\n%s", xml)
	}
}

func TestDomainXMLDefaultNetworkWithoutInterfaces(t *testing.T) {
	req := core.CreateRequest{
		InstanceID: "i-4",
		Flavor:     core.Flavor{Name: "m1", VCPU: core.VCPUCount(1)},
	}
	xml := domainXML("arsan-i-4", req, nil, nil)
	if !strings.Contains(xml, "<source network='default'/>") {
		t.Fatalf("pre-OVN VMs must keep the default network:\n%s", xml)
	}
}

func TestDomainXMLDedicated(t *testing.T) {
	zero := 0
	req := core.CreateRequest{
		InstanceID: "i-2",
		Flavor: core.Flavor{Name: "m1.dedicated",
			VCPU: core.VCPU{Sockets: 1, Cores: 2, Threads: 2, Dedicated: true}},
		PinnedCPUs: []int{4, 5, 6, 7},
		Volumes: []core.AttachedVolume{
			{Volume: core.Volume{ID: "vol-r", Name: "root", SizeMiB: 10240, Image: "base.qcow2", BootIndex: &zero}, Device: "vda"},
		},
	}
	xml := domainXML("arsan-i-2", req, map[string]string{"vol-r": "r.qcow2"}, map[string]string{"vol-r": "base.qcow2"})
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
	zero := 0
	req := core.CreateRequest{
		InstanceID: "i-9", Name: "web1", Namespace: "team-a",
		Flavor: core.Flavor{Name: "m1", VCPU: core.VCPUCount(1), RAM: 32},
		Volumes: []core.AttachedVolume{
			{Volume: core.Volume{ID: "vol-root", Name: "root", SizeMiB: 10240, Image: "base.qcow2", BootIndex: &zero, DeleteOnTermination: true}, Device: "vda"},
			{Volume: core.Volume{ID: "vol-data", Name: "data", SizeMiB: 50 * 1024, DeleteOnTermination: true}, Device: "vdb"},
		},
	}
	files := map[string]string{"vol-root": "/v/root.qcow2", "vol-data": "/v/data.qcow2"}
	bases := map[string]string{"vol-root": "/var/lib/libvirt/images/base.qcow2"}
	xml := domainXML(domainName(req), req, files, bases)
	for _, want := range []string{
		"<name>team-a-web1</name>",
		"<source file='/v/root.qcow2'/>",
		"<target dev='vda' bus='virtio'/>",
		"<source file='/v/data.qcow2'/>",
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
	got := parseDomainVolumes(xml)
	if len(got) != 2 {
		t.Fatalf("volumes = %v, want root + data", got)
	}

	cd := req
	cd.Volumes = append(append([]core.AttachedVolume{}, req.Volumes...),
		core.AttachedVolume{Volume: core.Volume{ID: "vol-cd", Name: "rescue", Cdrom: true, Image: "https://example.com/rescue.iso"}, Device: "vdd"})
	cdx := domainXML(domainName(cd), cd, files, bases)
	for _, want := range []string{
		"<source file='https://example.com/rescue.iso'/>",
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
	if !pinsMatchDesc(domainXML("arsan-i-2", req, nil, nil), req) {
		t.Fatal("own rendering must match")
	}
	stale := req
	stale.PinnedCPUs = []int{0, 1}
	if pinsMatchDesc(domainXML("arsan-i-2", req, nil, nil), stale) {
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
