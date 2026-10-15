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
//	kind: VirtualMachine
//	metadata:
//	  name: web1
//	spec:
//	  image: https://example.com/base.qcow2   # or a host-local qcow2 path
//	  flavor:
//	    name: m1
//	    vcpu: 1
//	    ram: 2
//	    disk: 10
type manifest struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Image  string `yaml:"image"`
		Flavor Flavor `yaml:"flavor"`
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
		if m.Kind != "VirtualMachine" {
			return nil, fmt.Errorf("%s: unknown kind %q (only VirtualMachine)", path, m.Kind)
		}
		if m.Metadata.Name == "" {
			return nil, fmt.Errorf("%s: metadata.name is required", path)
		}
		if m.Spec.Image == "" {
			return nil, fmt.Errorf("%s: spec.image is required", path)
		}
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no documents", path)
	}
	return out, nil
}

// findByName resolves a VM name to its record. Names are unique per
// registration in practice; the api keys rows by id, so this lists and
// matches client-side.
func (c *Client) findByName(name string) (VirtualMachine, error) {
	vms, err := c.GetVirtualMachines()
	if err != nil {
		return VirtualMachine{}, err
	}
	for _, vm := range vms {
		if vm.Name == name {
			return vm, nil
		}
	}
	return VirtualMachine{}, fmt.Errorf("virtualmachine %q not found", name)
}

// sameSpec reports whether the live VM already matches the manifest, so apply
// can leave it alone instead of churning it.
func sameSpec(vm VirtualMachine, m manifest) bool {
	return vm.ImageRef == m.Spec.Image &&
		vm.Flavor.VCPU == m.Spec.Flavor.VCPU &&
		vm.Flavor.RAM == m.Spec.Flavor.RAM &&
		vm.Flavor.Disk == m.Spec.Flavor.Disk
}
