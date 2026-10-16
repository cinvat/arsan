package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Storage commands mutate one VM's disks. Attach and detach work on a
// running guest (live hotplug) or a stopped one (redefine); the boot device
// only ever changes stopped, because boot order lives in the definition.
var attachDiskCmd = &cobra.Command{
	Use:   "disk VM NAME --size 50GB [--image base]",
	Short: "Attach an extra data disk",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		size, _ := cmd.Flags().GetString("size")
		image, _ := cmd.Flags().GetString("image")
		if size == "" {
			return fmt.Errorf("usage: attach disk VM NAME --size 50GB [--image base]")
		}
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.AttachDisk(vm.ID, DiskRequest{Name: args[1], Size: size, Image: image}); err != nil {
			return err
		}
		fmt.Printf("disk/%s attached to virtualmachine/%s/%s\n", args[1], vm.Namespace, vm.Name)
		return nil
	},
}

var detachDiskCmd = &cobra.Command{
	Use:   "disk VM NAME",
	Short: "Detach an extra data disk and delete its file",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.DetachDisk(vm.ID, args[1]); err != nil {
			return err
		}
		fmt.Printf("disk/%s detached from virtualmachine/%s/%s\n", args[1], vm.Namespace, vm.Name)
		return nil
	},
}

var attachCdromCmd = &cobra.Command{
	Use:   "cdrom VM --image rescue.iso",
	Short: "Attach an iso read-only",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		image, _ := cmd.Flags().GetString("image")
		if image == "" {
			return fmt.Errorf("usage: attach cdrom VM --image rescue.iso")
		}
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.AttachCdrom(vm.ID, image); err != nil {
			return err
		}
		fmt.Printf("cdrom attached to virtualmachine/%s/%s\n", vm.Namespace, vm.Name)
		return nil
	},
}

var detachCdromCmd = &cobra.Command{
	Use:   "cdrom VM",
	Short: "Detach the iso (the file is kept)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.DetachCdrom(vm.ID); err != nil {
			return err
		}
		fmt.Printf("cdrom detached from virtualmachine/%s/%s\n", vm.Namespace, vm.Name)
		return nil
	},
}

var bootCmd = &cobra.Command{
	// Rescue is stop, attach cdrom, boot cdrom, start — this command is the
	// middle step, and it requires the VM stopped.
	Use:   "boot VM (disk|cdrom)",
	Short: "Select what a stopped VM boots next",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if args[1] != "disk" && args[1] != "cdrom" {
			return fmt.Errorf("usage: boot VM (disk|cdrom)")
		}
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.SetBootDevice(vm.ID, args[1]); err != nil {
			return err
		}
		fmt.Printf("virtualmachine/%s/%s boots %s next\n", vm.Namespace, vm.Name, args[1])
		return nil
	},
}

func init() {
	attachDiskCmd.Flags().String("size", "", "disk size, e.g. 50GB (required)")
	attachDiskCmd.Flags().String("image", "", "base image to overlay instead of a blank disk")
	attachDiskCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	attachCdromCmd.Flags().String("image", "", "iso image to attach (required)")
	attachCdromCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	detachDiskCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	detachCdromCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	bootCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")

	attachCmd := &cobra.Command{Use: "attach (disk|cdrom)", Short: "Attach a disk or iso to a VM"}
	attachCmd.AddCommand(attachDiskCmd, attachCdromCmd)
	detachCmd := &cobra.Command{Use: "detach (disk|cdrom)", Short: "Detach a disk or iso from a VM"}
	detachCmd.AddCommand(detachDiskCmd, detachCdromCmd)
	rootCmd.AddCommand(attachCmd, detachCmd, bootCmd)
}
