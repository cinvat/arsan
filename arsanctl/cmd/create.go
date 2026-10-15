package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create (vm NAME | -f FILE)",
	Short: "Create virtual machines, imperatively or from a file",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		if file, _ := cmd.Flags().GetString("filename"); file != "" {
			manifests, err := loadManifests(file)
			if err != nil {
				return err
			}
			for _, m := range manifests {
				vm, err := c.CreateVirtualMachine(m.Metadata.Name, m.Spec.Image, m.Spec.Flavor)
				if err != nil {
					return fmt.Errorf("%s: %w", m.Metadata.Name, err)
				}
				// Accepted, not running: poll `arsanctl get vm` until ACTIVE.
				fmt.Printf("virtualmachine/%s created (%s %s)\n", vm.Name, vm.ID, vm.State)
			}
			return nil
		}
		if len(args) != 2 || (args[0] != "vm" && args[0] != "virtualmachine") {
			return fmt.Errorf("usage: create (vm NAME --image IMAGE | -f FILE)")
		}
		name := args[1]
		image, _ := cmd.Flags().GetString("image")
		vcpu, _ := cmd.Flags().GetInt("vcpu")
		ram, _ := cmd.Flags().GetInt("ram")
		disk, _ := cmd.Flags().GetInt("disk")
		if image == "" {
			return fmt.Errorf("--image is required: local qcow2 path or http(s) URL")
		}
		vm, err := c.CreateVirtualMachine(name, image, Flavor{
			Name: "cli", VCPU: vcpu, RAM: ram, Disk: disk,
		})
		if err != nil {
			return err
		}
		fmt.Printf("virtualmachine/%s created (%s %s)\n", vm.Name, vm.ID, vm.State)
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
				vm, err := c.findByName(m.Metadata.Name)
				if err != nil {
					return err
				}
				if err := c.DeleteVirtualMachine(vm.ID); err != nil {
					return fmt.Errorf("%s: %w", m.Metadata.Name, err)
				}
				fmt.Printf("virtualmachine/%s deleted\n", m.Metadata.Name)
			}
			return nil
		}
		if len(args) != 2 || (args[0] != "vm" && args[0] != "virtualmachine") {
			return fmt.Errorf("usage: delete (vm ID|NAME | -f FILE)")
		}
		id := args[1]
		// A name works too: resolve it, then delete by id.
		if vm, err := c.findByName(id); err == nil {
			id = vm.ID
		}
		if err := c.DeleteVirtualMachine(id); err != nil {
			return err
		}
		fmt.Printf("virtualmachine/%s deleted\n", args[1])
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
			vm, err := c.findByName(m.Metadata.Name)
			if err != nil {
				created, cerr := c.CreateVirtualMachine(m.Metadata.Name, m.Spec.Image, m.Spec.Flavor)
				if cerr != nil {
					return fmt.Errorf("%s: %w", m.Metadata.Name, cerr)
				}
				fmt.Printf("virtualmachine/%s created (%s %s)\n", created.Name, created.ID, created.State)
				continue
			}
			if sameSpec(vm, m) {
				fmt.Printf("virtualmachine/%s unchanged\n", vm.Name)
				continue
			}
			if err := c.DeleteVirtualMachine(vm.ID); err != nil {
				return fmt.Errorf("%s: %w", m.Metadata.Name, err)
			}
			created, err := c.CreateVirtualMachine(m.Metadata.Name, m.Spec.Image, m.Spec.Flavor)
			if err != nil {
				return fmt.Errorf("%s: %w", m.Metadata.Name, err)
			}
			fmt.Printf("virtualmachine/%s recreated (%s %s): spec changed\n", created.Name, created.ID, created.State)
		}
		return nil
	},
}

func init() {
	createCmd.Flags().String("image", "", "qcow2 path on the agent host, or http(s) URL to fetch")
	createCmd.Flags().Int("vcpu", 1, "vCPUs")
	createCmd.Flags().Int("ram", 2, "RAM in GiB")
	createCmd.Flags().Int("disk", 10, "disk in GiB")
	createCmd.Flags().StringP("filename", "f", "", "manifest file (--- separated)")
	rootCmd.AddCommand(createCmd)

	deleteCmd.Flags().StringP("filename", "f", "", "manifest file (--- separated)")
	rootCmd.AddCommand(deleteCmd)

	applyCmd.Flags().StringP("filename", "f", "", "manifest file (--- separated)")
	_ = applyCmd.MarkFlagRequired("filename")
	rootCmd.AddCommand(applyCmd)
}
