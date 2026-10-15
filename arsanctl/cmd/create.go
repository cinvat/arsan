package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var createCmd = &cobra.Command{
	Use:   "create vm NAME --image IMAGE",
	Short: "Create a virtual machine (returns before it is running)",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "vm" && args[0] != "virtualmachine" {
			return fmt.Errorf("only 'vm' can be created, got %q", args[0])
		}
		name := args[1]
		image, _ := cmd.Flags().GetString("image")
		vcpu, _ := cmd.Flags().GetInt("vcpu")
		ram, _ := cmd.Flags().GetInt("ram")
		disk, _ := cmd.Flags().GetInt("disk")
		if image == "" {
			return fmt.Errorf("--image is required: local qcow2 path or http(s) URL")
		}
		vm, err := NewClient(apiURL(cmd)).CreateVirtualMachine(name, image, Flavor{
			Name: "cli", VCPU: vcpu, RAM: ram, Disk: disk,
		})
		if err != nil {
			return err
		}
		// Accepted, not running: poll `arsanctl get vm` until ACTIVE.
		fmt.Printf("%s %s\n", vm.ID, vm.State)
		return nil
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete vm ID",
	Short: "Delete a virtual machine",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "vm" && args[0] != "virtualmachine" {
			return fmt.Errorf("only 'vm' can be deleted, got %q", args[0])
		}
		return NewClient(apiURL(cmd)).DeleteVirtualMachine(args[1])
	},
}

func init() {
	createCmd.Flags().String("image", "", "qcow2 path on the agent host, or http(s) URL to fetch")
	createCmd.Flags().Int("vcpu", 1, "vCPUs")
	createCmd.Flags().Int("ram", 2, "RAM in GiB")
	createCmd.Flags().Int("disk", 10, "disk in GiB")
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(deleteCmd)
}
