package kvm

import (
	"strings"
	"testing"

	"github.com/cinvat/arsan/internal/core"
)

func TestDomainXML(t *testing.T) {
	xml := domainXML("arsan-i-1", core.CreateRequest{
		InstanceID: "i-1",
		Flavor:     core.Flavor{Name: "m1", VCPU: 2, RAM: 4},
		ImageRef:   "/var/lib/libvirt/images/base.qcow2",
	})
	for _, want := range []string{
		"<name>arsan-i-1</name>",
		"<memory unit='KiB'>4194304</memory>", // 4 GiB in KiB
		"<vcpu>2</vcpu>",
		"<source file='/var/lib/libvirt/images/base.qcow2'/>",
		"type='kvm'",
	} {
		if !strings.Contains(xml, want) {
			t.Errorf("domain xml missing %q:\n%s", want, xml)
		}
	}
}

func TestDomainNamePrefixes(t *testing.T) {
	if got := domainName("i-1"); got != "arsan-i-1" {
		t.Fatalf("got %q", got)
	}
}
