package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	// A VM has too many parameters for flags (flavor, image, disks,
	// networks): files only, like kubectl.
	Use:   "create -f FILE",
	Short: "Create virtual machines from a file",
	RunE: func(cmd *cobra.Command, args []string) error {
		file, _ := cmd.Flags().GetString("filename")
		if file == "" {
			return fmt.Errorf("usage: create -f FILE")
		}
		c := NewClient(apiURL(cmd))
		manifests, err := loadManifests(file)
		if err != nil {
			return err
		}
		for _, m := range manifests {
			if m.Kind == "Flavor" {
				vcpu, memory := flavorArgs(m)
				f, err := c.CreateFlavor(m.Metadata.Name, vcpu, memory)
				if err != nil {
					return fmt.Errorf("%s: %w", m.Metadata.Name, err)
				}
				fmt.Printf("flavor/%s created\n", f.Name)
				continue
			}
			ref, inline := flavorOf(m)
			vm, err := c.CreateVirtualMachine(manifestNamespace(m), m.Metadata.Name, m.Spec.Image, ref, inline, m.Spec.Disks, m.Spec.Cdrom, m.Spec.Boot)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", manifestNamespace(m), m.Metadata.Name, err)
			}
			// Accepted, not running: poll `arsanctl get vm` until RUNNING.
			fmt.Printf("virtualmachine/%s/%s created (%s)\n", vm.Namespace, vm.Name, vm.State)
		}
		return nil
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete (vm ID|NAME | -f FILE)",
	Short: "Delete virtual machines",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		if file, _ := cmd.Flags().GetString("filename"); file != "" {
			manifests, err := loadManifests(file)
			if err != nil {
				return err
			}
			for _, m := range manifests {
				vm, err := c.findVM(manifestNamespace(m), m.Metadata.Name)
				if err != nil {
					return err
				}
				if err := c.DeleteVirtualMachine(vm.ID); err != nil {
					return fmt.Errorf("%s/%s: %w", vm.Namespace, m.Metadata.Name, err)
				}
				fmt.Printf("virtualmachine/%s/%s deleted\n", vm.Namespace, m.Metadata.Name)
			}
			return nil
		}
		if len(args) != 2 || (args[0] != "vm" && args[0] != "virtualmachine") {
			return fmt.Errorf("usage: delete (vm ID|NAME | -f FILE)")
		}
		ns, _ := cmd.Flags().GetString("namespace")
		id := args[1]
		// A name works too: resolve it within the namespace, then delete by id.
		vm, err := c.findVM(ns, id)
		if err == nil {
			id = vm.ID
			ns = vm.Namespace
		}
		if err := c.DeleteVirtualMachine(id); err != nil {
			return err
		}
		fmt.Printf("virtualmachine/%s/%s deleted\n", ns, args[1])
		return nil
	},
}

// applyCmd reconciles files with the cluster: missing VMs are created,
// matching ones are left alone, and changed ones are deleted and recreated.
// There is no in-place update — a VM's flavor and image are immutable once
// defined, so a changed spec means a new VM, said out loud.
var applyCmd = &cobra.Command{
	Use:   "apply -f FILE",
	Short: "Create or recreate virtual machines from a file",
	RunE: func(cmd *cobra.Command, args []string) error {
		file, _ := cmd.Flags().GetString("filename")
		if file == "" {
			return fmt.Errorf("usage: apply -f FILE")
		}
		c := NewClient(apiURL(cmd))
		manifests, err := loadManifests(file)
		if err != nil {
			return err
		}
		for _, m := range manifests {
			if m.Kind == "Flavor" {
				vcpu, memory := flavorArgs(m)
				f, err := c.CreateFlavor(m.Metadata.Name, vcpu, memory)
				if err != nil {
					return fmt.Errorf("%s: %w", m.Metadata.Name, err)
				}
				fmt.Printf("flavor/%s created\n", f.Name)
				continue
			}
			ref, inline := flavorOf(m)
			ns := manifestNamespace(m)
			vm, err := c.findVM(ns, m.Metadata.Name)
			if err != nil {
				created, cerr := c.CreateVirtualMachine(ns, m.Metadata.Name, m.Spec.Image, ref, inline, m.Spec.Disks, m.Spec.Cdrom, m.Spec.Boot)
				if cerr != nil {
					return fmt.Errorf("%s/%s: %w", ns, m.Metadata.Name, cerr)
				}
				fmt.Printf("virtualmachine/%s/%s created (%s)\n", created.Namespace, created.Name, created.State)
				continue
			}
			if sameSpec(vm, m) {
				fmt.Printf("virtualmachine/%s/%s unchanged\n", vm.Namespace, vm.Name)
				continue
			}
			if err := c.DeleteVirtualMachine(vm.ID); err != nil {
				return fmt.Errorf("%s/%s: %w", vm.Namespace, m.Metadata.Name, err)
			}
			created, err := c.CreateVirtualMachine(ns, m.Metadata.Name, m.Spec.Image, ref, inline, m.Spec.Disks, m.Spec.Cdrom, m.Spec.Boot)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", ns, m.Metadata.Name, err)
			}
			fmt.Printf("virtualmachine/%s/%s recreated (%s): spec changed\n", created.Namespace, created.Name, created.State)
		}
		return nil
	},
}

func init() {
	createCmd.Flags().StringP("filename", "f", "", "manifest file (--- separated)")
	_ = createCmd.MarkFlagRequired("filename")
	rootCmd.AddCommand(createCmd)

	deleteCmd.Flags().StringP("filename", "f", "", "manifest file (--- separated)")
	deleteCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	rootCmd.AddCommand(deleteCmd)

	applyCmd.Flags().StringP("filename", "f", "", "manifest file (--- separated)")
	_ = applyCmd.MarkFlagRequired("filename")
	rootCmd.AddCommand(applyCmd)
}
