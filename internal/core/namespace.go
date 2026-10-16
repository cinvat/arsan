package core

import (
	"fmt"
	"regexp"
)

// dnsLabel is the Kubernetes DNS_LABEL rule, shared by VM names and
// namespaces: lowercase alphanumerics and '-', 1-63 chars. The hypervisor
// domain is "<namespace>-<name>", so both halves must survive libvirt and
// shell intact — no slashes, spaces or uppercase.
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// DefaultNamespace is used when the request names none, kubectl-style.
const DefaultNamespace = "default"

// ValidateName rejects VM names that cannot appear in a domain name.
func ValidateName(name string) error {
	if len(name) == 0 || len(name) > 63 || !dnsLabel.MatchString(name) {
		return fmt.Errorf("name %q: want 1-63 lowercase alphanumeric/- characters: %w", name, ErrInvalidRequest)
	}
	return nil
}

// ValidateNamespace rejects namespace names for the same reason.
func ValidateNamespace(ns string) error {
	if len(ns) == 0 || len(ns) > 63 || !dnsLabel.MatchString(ns) {
		return fmt.Errorf("namespace %q: want 1-63 lowercase alphanumeric/- characters: %w", ns, ErrInvalidRequest)
	}
	return nil
}

// DomainName is the hypervisor-visible identity: namespace first, like a
// pod's namespace/name pair flattened for libvirt.
func DomainName(namespace, name string) string { return namespace + "-" + name }
