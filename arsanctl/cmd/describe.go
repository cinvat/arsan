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
			ns, _ := cmd.Flags().GetString("namespace")
			vm, err := c.findVM(ns, args[1])
			if err != nil {
				return err
			}
			volumes, err := c.GetVolumes()
			if err != nil {
				return err
			}
			describeVM(vm, volumes)
			return nil
		case "flavors", "flavor", "fl":
			f, err := c.findFlavor(args[1])
			if err != nil {
				return err
			}
			describeFlavor(f)
			return nil
		case "volumes", "volume", "vol", "vols":
			ns, _ := cmd.Flags().GetString("namespace")
			v, err := c.findVolume(ns, args[1])
			if err != nil {
				return err
			}
			describeVolume(v)
			return nil
		default:
			return fmt.Errorf("unknown resource %q: node, vm, flavor or volume", args[0])
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

// describedVolume joins an attachment with its row for display.
type describedVolume struct {
	att VolumeAttachment
	vol Volume
}

func describeVM(vm VirtualMachine, all []Volume) {
	fmt.Printf("Name:     %s\n", vm.Name)
	fmt.Printf("Namespace: %s\n", vm.Namespace)
	fmt.Printf("ID:       %s\n", vm.ID)
	fmt.Printf("State:    %s\n", vm.State)
	fmt.Printf("Node:     %s\n", dash(vm.NodeID))
	fmt.Printf("Image:    %s\n", vm.ImageRef)
	fmt.Printf("Flavor:   %s (%s, memory %s)\n",
		vm.Flavor.Name, vcpuLong(vm.Flavor.VCPU), mibShort(vm.Flavor.RAM))
	byID := make(map[string]Volume, len(all))
	for _, v := range all {
		byID[v.ID] = v
	}
	var vols []describedVolume
	for _, att := range vm.Volumes {
		v, ok := byID[att.VolumeID]
		if !ok {
			continue // record deleted out from under the attachment
		}
		vols = append(vols, describedVolume{att: att, vol: v})
	}
	for _, wd := range vols {
		boot := ""
		if wd.att.BootIndex != nil {
			boot = fmt.Sprintf(", boots %d", *wd.att.BootIndex)
		}
		kind := "disk"
		if wd.vol.Cdrom {
			kind = "cdrom"
		}
		detail := fmt.Sprintf("%s %s %s", wd.att.Device, kind, wd.vol.Name)
		if wd.vol.Image != "" {
			detail = fmt.Sprintf("%s from %s", detail, wd.vol.Image)
		}
		if wd.vol.SizeMiB > 0 {
			detail = fmt.Sprintf("%s, %s", detail, mibShort(wd.vol.SizeMiB))
		}
		if !wd.vol.DeleteOnTermination {
			detail += ", shared"
		}
		fmt.Printf("Volume:   %s%s\n", detail, boot)
	}
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

func describeVolume(v Volume) {
	fmt.Printf("Name:     %s\n", v.Name)
	fmt.Printf("Namespace: %s\n", v.Namespace)
	fmt.Printf("ID:       %s\n", v.ID)
	fmt.Printf("Size:     %s\n", mibShort(v.SizeMiB))
	fmt.Printf("Image:    %s\n", dash(v.Image))
	fmt.Printf("Cdrom:    %t\n", v.Cdrom)
	state := string(v.State)
	if v.State == "" {
		state = "available"
	}
	fmt.Printf("State:    %s\n", state)
	fmt.Printf("Attached: %s\n", dash(v.AttachedTo))
	fmt.Printf("DeleteOnTermination: %t\n", v.DeleteOnTermination)
}

func describeFlavor(f Flavor) {
	fmt.Printf("Name:     %s\n", f.Name)
	fmt.Printf("vCPU:     %s\n", vcpuLong(f.VCPU))
	fmt.Printf("Memory:   %s\n", mibShort(f.RAM))
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
	describeCmd.Flags().StringP("namespace", "n", "default", "namespace of the resource")
	rootCmd.AddCommand(describeCmd)
}
