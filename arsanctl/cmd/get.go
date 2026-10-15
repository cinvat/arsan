package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get (nodes|virtualmachines)",
	Short: "List nodes or virtual machines",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		switch args[0] {
		case "nodes", "node", "no":
			nodes, err := c.GetNodes()
			if err != nil {
				return err
			}
			fmt.Printf("%-32s %-10s %-8s %-8s %s\n", "ID", "STATE", "ENGINE", "VCPU", "REASON")
			for _, n := range nodes {
				fmt.Printf("%-32s %-10s %-8s %-8d %s\n",
					n.ID, n.State, n.Driver, n.Total.VCPU, n.NotReadyReason)
			}
			return nil
		case "virtualmachines", "virtualmachine", "vm", "vms":
			vms, err := c.GetVirtualMachines()
			if err != nil {
				return err
			}
			fmt.Printf("%-16s %-10s %-10s %-32s %s\n", "ID", "STATE", "NODE", "IMAGE", "NAME")
			for _, vm := range vms {
				fmt.Printf("%-16s %-10s %-10s %-32s %s\n",
					vm.ID, vm.State, vm.NodeID, vm.ImageRef, vm.Name)
			}
			return nil
		default:
			return fmt.Errorf("unknown resource %q: nodes or virtualmachines", args[0])
		}
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}
