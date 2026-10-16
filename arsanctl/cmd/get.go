package cmd

import (
	"fmt"

	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	// get stays short: one line per object, no long fields. IMAGE urls,
	// NOTREADY reasons and pin lists live in describe.
	Use:   "get (nodes|virtualmachines|flavors|volumes)",
	Short: "List nodes, virtual machines, flavors or volumes",
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
			ns, _ := cmd.Flags().GetString("namespace")
			all, _ := cmd.Flags().GetBool("all-namespaces")
			if watch {
				return watchVMs(c, ns, all)
			}
			vms, err := c.GetVirtualMachines()
			if err != nil {
				return err
			}
			printVMs(filterVMs(vms, ns, all))
			return nil
		case "flavors", "flavor", "fl":
			flavors, err := c.GetFlavors()
			if err != nil {
				return err
			}
			printFlavors(flavors)
			return nil
		case "volumes", "volume", "vol", "vols":
			ns, _ := cmd.Flags().GetString("namespace")
			all, _ := cmd.Flags().GetBool("all-namespaces")
			volumes, err := c.GetVolumes()
			if err != nil {
				return err
			}
			printVolumes(filterVolumes(volumes, ns, all))
			return nil
		default:
			return fmt.Errorf("unknown resource %q: nodes, virtualmachines, flavors or volumes", args[0])
		}
	},
}

func printNodes(nodes []Node) {
	fmt.Printf("%-16s %-10s %-10s %-15s\n", "NAME", "STATE", "ENGINE", "IP")
	for _, n := range nodes {
		fmt.Printf("%-16s %-10s %-10s %-15s\n",
			n.Name, n.State, n.Driver, n.IP)
	}
}

func printVMs(vms []VirtualMachine) {
	fmt.Printf("%-16s %-16s %-10s %-10s %-10s\n", "NAMESPACE", "NAME", "STATE", "NODE", "FLAVOR")
	for _, vm := range vms {
		fmt.Printf("%-16s %-16s %-10s %-10s %-10s\n",
			vm.Namespace, vm.Name, vm.State, vm.NodeID, vm.Flavor.Name)
	}
}

// filterVMs scopes the list to one namespace, kubectl-style. Empty namespace
// rows (pre-namespace records) read as default.
func filterVMs(vms []VirtualMachine, ns string, all bool) []VirtualMachine {
	if all {
		return vms
	}
	ns = orDefault(ns)
	return lo.Filter(vms, func(vm VirtualMachine, _ int) bool {
		return orDefault(vm.Namespace) == ns
	})
}

func printVolumes(volumes []Volume) {
	fmt.Printf("%-16s %-16s %-10s %-8s %-10s %s\n", "NAMESPACE", "NAME", "SIZE", "STATE", "ATTACHED", "IMAGE")
	for _, v := range volumes {
		attached := dash(v.AttachedTo)
		fmt.Printf("%-16s %-16s %-10s %-8s %-10s %s\n",
			v.Namespace, v.Name, mibShort(v.SizeMiB), v.State, attached, dash(v.Image))
	}
}

// filterVolumes scopes the list to one namespace, like filterVMs.
func filterVolumes(volumes []Volume, ns string, all bool) []Volume {
	if all {
		return volumes
	}
	ns = orDefault(ns)
	return lo.Filter(volumes, func(v Volume, _ int) bool {
		return orDefault(v.Namespace) == ns
	})
}

func printFlavors(flavors []Flavor) {
	fmt.Printf("%-16s %-14s %-10s\n", "NAME", "VCPU", "MEMORY")
	for _, f := range flavors {
		fmt.Printf("%-16s %-14s %-10s\n",
			f.Name, vcpuShort(f.VCPU), mibShort(f.RAM))
	}
}

// vcpuShort renders topology compactly: 1s/2c/1t, with ! when dedicated.
func vcpuShort(v VCPU) string {
	s := fmt.Sprintf("%ds/%dc/%dt", v.Sockets, v.Cores, v.Threads)
	if v.Dedicated {
		s += "!"
	}
	return s
}

// mibShort renders MiB as MB or GB, whichever is whole.
func mibShort(mib int) string {
	if mib%1024 == 0 {
		return fmt.Sprintf("%dGB", mib/1024)
	}
	return fmt.Sprintf("%dMB", mib)
}

func init() {
	getCmd.Flags().BoolP("watch", "w", false, "reprint when something changes")
	getCmd.Flags().StringP("namespace", "n", "default", "namespace to list")
	getCmd.Flags().BoolP("all-namespaces", "A", false, "list all namespaces")
	rootCmd.AddCommand(getCmd)
}
