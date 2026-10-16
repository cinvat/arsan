package core

import "testing"

func TestValidateName(t *testing.T) {
	for _, ok := range []string{"web1", "cirros1", "a", "team-a-1"} {
		if err := ValidateName(ok); err != nil {
			t.Errorf("ValidateName(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "Web1", "web_1", "web 1", "a/b", "-lead", "trail-", string(make([]byte, 64))} {
		if err := ValidateName(bad); err == nil {
			t.Errorf("ValidateName(%q) = nil, want an error", bad)
		}
	}
}

func TestDomainName(t *testing.T) {
	if got := DomainName("team-a", "web1"); got != "team-a-web1" {
		t.Fatalf("got %q", got)
	}
}
