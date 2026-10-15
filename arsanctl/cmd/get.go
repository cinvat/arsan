package cmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get (nodes|virtualmachines)",
	Short: "List nodes or virtual machines",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		watch, _ := cmd.Flags().GetBool("watch")
		c := NewClient(apiURL(cmd))
		switch args[0] {
		case "nodes", "node", "no":
			if watch {
				return watchNodes(c)
			}
			nodes, err := c.GetNodes()
			if err != nil {
				return err
			}
			printNodes(nodes)
			return nil
		case "virtualmachines", "virtualmachine", "vm", "vms":
			if watch {
				return watchVMs(c)
			}
			vms, err := c.GetVirtualMachines()
			if err != nil {
				return err
			}
			printVMs(vms)
			return nil
		default:
			return fmt.Errorf("unknown resource %q: nodes or virtualmachines", args[0])
		}
	},
}

func printNodes(nodes []Node) {
	fmt.Printf("%-32s %-10s %-8s %-8s %s\n", "ID", "STATE", "ENGINE", "CPU", "REASON")
	for _, n := range nodes {
		fmt.Printf("%-32s %-10s %-8s %-8d %s\n",
			n.ID, n.State, n.Driver, n.Total.CPU, n.NotReadyReason)
	}
}

func printVMs(vms []VirtualMachine) {
	fmt.Printf("%-16s %-10s %-10s %-32s %s\n", "ID", "STATE", "NODE", "IMAGE", "NAME")
	for _, vm := range vms {
		fmt.Printf("%-16s %-10s %-10s %-32s %s\n",
			vm.ID, vm.State, vm.NodeID, vm.ImageRef, vm.Name)
	}
}

// watchVMs polls until interrupted. Nodes have a real stream
// (arsanctl watch nodes); VMs do not, so this re-lists on a tick and
// reprints only when something changed.
func watchVMs(c *Client) error {
	var last []byte
	for {
		vms, err := c.GetVirtualMachines()
		if err != nil {
			return err
		}
		raw, err := json.Marshal(vms)
		if err != nil {
			return err
		}
		if string(raw) != string(last) {
			last = raw
			printVMs(vms)
			fmt.Println("---")
		}
		time.Sleep(2 * time.Second)
	}
}

func init() {
	getCmd.Flags().BoolP("watch", "w", false, "reprint when something changes")
	rootCmd.AddCommand(getCmd)
}
