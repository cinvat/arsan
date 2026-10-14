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

func TestParseNodeinfoMemory(t *testing.T) {
	out := `CPU model:           x86_64
CPU(s):              8
Memory size:         16777216 KiB
`
	if got := parseNodeinfoMemoryKiB(out); got != 16777216 {
		t.Fatalf("got %d", got)
	}
	if got := parseNodeinfoMemoryKiB("garbage"); got != 0 {
		t.Fatalf("unknown input must be zero, got %d", got)
	}
}

func TestParseKiBFirstField(t *testing.T) {
	if got := parseKiBFirstField("123456 KiB"); got != 123456 {
		t.Fatalf("got %d", got)
	}
	for _, bad := range []string{"", "garbage", "-5 KiB"} {
		if got := parseKiBFirstField(bad); got != 0 {
			t.Fatalf("input %q: got %d, want 0", bad, got)
		}
	}
}

func TestUsageMath(t *testing.T) {
	// 16 GiB total, 12 GiB free => 4 GiB used.
	total := parseNodeinfoMemoryKiB("Memory size:         16777216 KiB\n")
	free := parseKiBFirstField(lastLine("0 KiB\n12582912 KiB"))
	if used := (total - free) / (1024 * 1024); used != 4 {
		t.Fatalf("used = %d GiB, want 4", used)
	}
}
