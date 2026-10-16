package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Storage commands converge one VM's volumes to its record. Attach and
// detach work on a running guest (live hotplug) or a stopped one (redefine);
// only the boot move needs the VM stopped. Rescue is attach a cdrom volume,
// set boot to it, start — then detach and delete it when done.
var attachCmd = &cobra.Command{
	Use:   "attach vm VOLUME [--boot]",
	Short: "Attach a prepared volume to a VM",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		boot, _ := cmd.Flags().GetBool("boot")
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.AttachVolume(vm.ID, args[1], bootFlag(boot)); err != nil {
			return err
		}
		fmt.Printf("volume/%s attached to virtualmachine/%s/%s\n", args[1], vm.Namespace, vm.Name)
		return nil
	},
}

var detachCmd = &cobra.Command{
	Use:   "detach vm VOLUME",
	Short: "Detach a volume back to available storage",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.DetachVolume(vm.ID, args[1]); err != nil {
			return err
		}
		fmt.Printf("volume/%s detached from virtualmachine/%s/%s\n", args[1], vm.Namespace, vm.Name)
		return nil
	},
}

var bootCmd = &cobra.Command{
	// Rescue is attach, boot, start: this command is the middle step, and
	// the VM must be stopped — boot order lives in the defined XML.
	Use:   "boot vm VOLUME",
	Short: "Move the single boot position to an attached volume",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		vm, err := c.findVM(ns, args[0])
		if err != nil {
			return err
		}
		if err := c.SetBootVolume(vm.ID, args[1]); err != nil {
			return err
		}
		fmt.Printf("virtualmachine/%s/%s boots %s next\n", vm.Namespace, vm.Name, args[1])
		return nil
	},
}

// bootFlag turns --boot into boot index 0; no flag means no boot position,
// and the controller rejects a second bootable rather than guessing.
func bootFlag(boot bool) *int {
	if !boot {
		return nil
	}
	i := 0
	return &i
}

func init() {
	attachCmd.Flags().Bool("boot", false, "make this the boot volume (fails if one exists)")
	attachCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	detachCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	bootCmd.Flags().StringP("namespace", "n", "default", "namespace of the VM")
	rootCmd.AddCommand(attachCmd, detachCmd, bootCmd)
}
