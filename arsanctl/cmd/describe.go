package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var describeCmd = &cobra.Command{
	// describe is the long view: everything get deliberately omits (image
	// urls, NOTREADY reasons, pin lists, failure text) lives here.
	Use:   "describe (node|vm|flavor) NAME",
	Short: "Show details of one node, VM or flavor",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		switch args[0] {
		case "nodes", "node", "no":
			n, err := c.findNode(args[1])
			if err != nil {
				return err
			}
			describeNode(n)
			return nil
		case "virtualmachines", "virtualmachine", "vm", "vms":
			vm, err := c.findByName(args[1])
			if err != nil {
				// A name miss may be an id: try it directly.
				vms, lerr := c.GetVirtualMachines()
				if lerr != nil {
					return err
				}
				found := false
				for _, v := range vms {
					if v.ID == args[1] {
						vm, found = v, true
						break
					}
				}
				if !found {
					return err
				}
			}
			describeVM(vm)
			return nil
		case "flavors", "flavor", "fl":
			f, err := c.findFlavor(args[1])
			if err != nil {
				return err
			}
			describeFlavor(f)
			return nil
		default:
			return fmt.Errorf("unknown resource %q: node, vm or flavor", args[0])
		}
	},
}

// findNode resolves a node id or name, like findByName does for VMs.
func (c *Client) findNode(idOrName string) (Node, error) {
	nodes, err := c.GetNodes()
	if err != nil {
		return Node{}, err
	}
	for _, n := range nodes {
		if n.ID == idOrName || n.Name == idOrName {
			return n, nil
		}
	}
	return Node{}, fmt.Errorf("node %q not found", idOrName)
}

func (c *Client) findFlavor(name string) (Flavor, error) {
	flavors, err := c.GetFlavors()
	if err != nil {
		return Flavor{}, err
	}
	for _, f := range flavors {
		if f.Name == name {
			return f, nil
		}
	}
	return Flavor{}, fmt.Errorf("flavor %q not found", name)
}

func describeNode(n Node) {
	fmt.Printf("Name:     %s\n", n.Name)
	fmt.Printf("ID:       %s\n", n.ID)
	fmt.Printf("State:    %s\n", n.State)
	fmt.Printf("Enabled:  %t\n", n.Enabled)
	fmt.Printf("IP:       %s\n", dash(n.IP))
	fmt.Printf("Engine:   %s\n", dash(n.Driver))
	fmt.Printf("CPU:      total %d, available %d\n", n.Total.CPU, n.Available.CPU)
	fmt.Printf("Memory:   total %s, available %s\n", mibShort(n.Total.RAM), mibShort(n.Available.RAM))
	fmt.Printf("Disk:     total %s, available %s\n", mibShort(n.Total.Disk), mibShort(n.Available.Disk))
	if len(n.PinnedCPUs) > 0 {
		fmt.Printf("Pinned:   %v\n", n.PinnedCPUs)
	}
	if n.NotReadyReason != "" {
		fmt.Printf("Reason:   %s\n", n.NotReadyReason)
	}
}

func describeVM(vm VirtualMachine) {
	fmt.Printf("Name:     %s\n", vm.Name)
	fmt.Printf("ID:       %s\n", vm.ID)
	fmt.Printf("State:    %s\n", vm.State)
	fmt.Printf("Node:     %s\n", dash(vm.NodeID))
	fmt.Printf("Image:    %s\n", vm.ImageRef)
	fmt.Printf("Flavor:   %s (%s, memory %s, disk %s)\n",
		vm.Flavor.Name, vcpuLong(vm.Flavor.VCPU), mibShort(vm.Flavor.RAM), mibShort(vm.Flavor.Disk))
	if len(vm.PinnedCPUs) > 0 {
		fmt.Printf("Pinned:   %v\n", vm.PinnedCPUs)
	}
	if vm.PendingReason != "" {
		fmt.Printf("Waiting:  %s\n", vm.PendingReason)
	}
	if vm.FailureReason != "" {
		fmt.Printf("Failure:  %s\n", vm.FailureReason)
	}
}

func describeFlavor(f Flavor) {
	fmt.Printf("Name:     %s\n", f.Name)
	fmt.Printf("vCPU:     %s\n", vcpuLong(f.VCPU))
	fmt.Printf("Memory:   %s\n", mibShort(f.RAM))
	fmt.Printf("Disk:     %s\n", mibShort(f.Disk))
}

// vcpuLong spells topology out: 1 socket, 2 cores, 1 thread, shared.
func vcpuLong(v VCPU) string {
	mode := "shared"
	if v.Dedicated {
		mode = "dedicated"
	}
	return fmt.Sprintf("%d sockets, %d cores, %d threads, %s", v.Sockets, v.Cores, v.Threads, mode)
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func init() {
	rootCmd.AddCommand(describeCmd)
}
