package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// manifest is the file shape arsanctl accepts:
//
//	apiVersion: v1alpha1
//	kind: Flavor
//	metadata:
//	  name: m1
//	spec:
//	  vcpu: {sockets: 1, cores: 2, threads: 1}  # or vcpu: 2 (legacy count)
//	  memory: 32MB  # or 2GB; only MB and GB
//	  disk: 10GB
//	---
//	apiVersion: v1alpha1
//	kind: VirtualMachine
//	metadata:
//	  name: web1
//	  namespace: team-a   # optional, defaults to "default"
//	spec:
//	  image: https://example.com/base.qcow2   # or a host-local qcow2 path
//	  flavorRef: m1
type manifest struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Image string `yaml:"image"`
		// FlavorRef names a stored flavor (preferred).
		FlavorRef string `yaml:"flavorRef"`
		// Flavor is the legacy inline sizing, kept for back-compat.
		Flavor Flavor `yaml:"flavor"`
		VCPU   VCPU   `yaml:"vcpu"`
		Memory string `yaml:"memory"`
		// Disks are extra data volumes, each {name, size[, image]}.
		Disks []DiskRequest `yaml:"disks"`
		// Cdrom attaches an iso read-only. Boot selects disk or cdrom.
		Cdrom string `yaml:"cdrom"`
		Boot  string `yaml:"boot"`
	} `yaml:"spec"`
}

// loadManifests parses one file holding one or more --- separated documents.
// Unknown kinds fail fast: silently skipping a document would apply half a
// file.
func loadManifests(path string) ([]manifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []manifest
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	for {
		var m manifest
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if m.Kind == "" {
			continue // trailing --- leaves an empty document
		}
		if m.Kind != "VirtualMachine" && m.Kind != "Flavor" {
			return nil, fmt.Errorf("%s: unknown kind %q (only Flavor, VirtualMachine)", path, m.Kind)
		}
		if m.Metadata.Name == "" {
			return nil, fmt.Errorf("%s: metadata.name is required", path)
		}
		if m.Kind == "VirtualMachine" && m.Spec.Image == "" {
			return nil, fmt.Errorf("%s: spec.image is required", path)
		}
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no documents", path)
	}
	return out, nil
}

// manifestNamespace defaults an empty metadata.namespace, kubectl-style.
func manifestNamespace(m manifest) string {
	if m.Metadata.Namespace == "" {
		return "default"
	}
	return m.Metadata.Namespace
}

// findVM resolves a VM id or name to its record. Ids match in any namespace;
// names match within the given one, because names repeat across namespaces
// while the api keys rows by id.
func (c *Client) findVM(namespace, nameOrID string) (VirtualMachine, error) {
	vms, err := c.GetVirtualMachines()
	if err != nil {
		return VirtualMachine{}, err
	}
	for _, vm := range vms {
		if vm.ID == nameOrID {
			return vm, nil
		}
	}
	for _, vm := range vms {
		// Rows written before namespaces existed carry "": they read as
		// the default namespace so an upgrade does not duplicate them.
		ns := vm.Namespace
		if ns == "" {
			ns = "default"
		}
		if ns == namespace && vm.Name == nameOrID {
			return vm, nil
		}
	}
	return VirtualMachine{}, fmt.Errorf("virtualmachine %q not found in namespace %q", nameOrID, namespace)
}

// sameSpec reports whether the live VM already matches the manifest, so apply
// can leave it alone instead of churning it.
func sameSpec(vm VirtualMachine, m manifest) bool {
	if vm.ImageRef != m.Spec.Image {
		return false
	}
	if m.Spec.FlavorRef != "" {
		return vm.Flavor.Name == m.Spec.FlavorRef
	}
	return vm.Flavor.VCPU == m.Spec.Flavor.VCPU &&
		vm.Flavor.RAM == m.Spec.Flavor.RAM
}

// flavorOf resolves a VM manifest to its flavor reference and legacy inline
// fallback. FlavorRef wins; inline is back-compat only.
func flavorOf(m manifest) (string, Flavor) { return m.Spec.FlavorRef, m.Spec.Flavor }

// flavorArgs converts a Flavor-kind manifest into create arguments.
// Vcpu and memory pass through as written: the api parses and validates
// them, so arsanctl never duplicates the sizing logic.
func flavorArgs(m manifest) (vcpu VCPU, memory string) {
	return m.Spec.VCPU, m.Spec.Memory
}
