package cmd

import (
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

// Completion is best-effort and never errors visibly: a completion that fails
// (api down, bad flags) completes nothing rather than printing into the
// shell's completion stream.
var (
	nodeKinds   = []string{"node", "nodes"}
	vmKinds     = []string{"vm", "vms", "virtualmachine", "virtualmachines"}
	flavorKinds = []string{"flavor", "flavors", "fl"}
	volumeKinds = []string{"volume", "volumes", "vol", "vols"}
)

func noFiles() ([]string, cobra.ShellCompDirective) {
	return nil, cobra.ShellCompDirectiveNoFileComp
}

// completeKinds completes the resource word: get/describe/watch's first arg.
func completeKinds(kinds ...[]string) func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) != 0 {
			return noFiles()
		}
		var out []string
		for _, ks := range kinds {
			out = append(out, ks...)
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	}
}

// completeNames completes the second arg from the api: node, vm and flavor
// names (ids work too, but names are what get shows now).
func completeNames(cmd *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 1 {
		return noFiles()
	}
	c := NewClient(apiURL(cmd))
	ns, _ := cmd.Flags().GetString("namespace")
	ns = orDefault(ns)
	switch args[0] {
	case "node", "nodes", "no":
		nodes, err := c.GetNodes()
		if err != nil {
			return noFiles()
		}
		return lo.Map(nodes, func(n Node, _ int) string { return n.Name }), cobra.ShellCompDirectiveNoFileComp
	case "vm", "vms", "virtualmachine", "virtualmachines":
		vms, err := c.GetVirtualMachines()
		if err != nil {
			return noFiles()
		}
		return lo.Map(lo.Filter(vms, func(vm VirtualMachine, _ int) bool {
			return orDefault(vm.Namespace) == ns
		}), func(vm VirtualMachine, _ int) string { return vm.Name }), cobra.ShellCompDirectiveNoFileComp
	case "flavor", "flavors", "fl":
		flavors, err := c.GetFlavors()
		if err != nil {
			return noFiles()
		}
		return lo.Map(flavors, func(f Flavor, _ int) string { return f.Name }), cobra.ShellCompDirectiveNoFileComp
	case "volume", "volumes", "vol", "vols":
		volumes, err := c.GetVolumes()
		if err != nil {
			return noFiles()
		}
		out := lo.Map(lo.Filter(volumes, func(v Volume, _ int) bool {
			return orDefault(v.Namespace) == ns
		}), func(v Volume, _ int) string { return v.Name })
		return out, cobra.ShellCompDirectiveNoFileComp
	default:
		return noFiles()
	}
}

// completeKindThenNames completes a kind first, then names of that kind:
// describe's and delete's two-arg shape.
func completeKindThenNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return completeKinds(nodeKinds, vmKinds, flavorKinds, volumeKinds)(cmd, args, toComplete)
	}
	return completeNames(cmd, args, toComplete)
}

func init() {
	getCmd.ValidArgsFunction = completeKinds(nodeKinds, vmKinds, flavorKinds, volumeKinds)

	// attach/detach/boot take a VM first, then a volume name.
	vmThenVolume := func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		c := NewClient(apiURL(cmd))
		ns, _ := cmd.Flags().GetString("namespace")
		ns = orDefault(ns)
		if len(args) == 0 {
			vms, err := c.GetVirtualMachines()
			if err != nil {
				return noFiles()
			}
			out := lo.Map(lo.Filter(vms, func(vm VirtualMachine, _ int) bool {
				return orDefault(vm.Namespace) == ns
			}), func(vm VirtualMachine, _ int) string { return vm.Name })
			return out, cobra.ShellCompDirectiveNoFileComp
		}
		if len(args) == 1 {
			volumes, err := c.GetVolumes()
			if err != nil {
				return noFiles()
			}
			out := lo.Map(lo.Filter(volumes, func(v Volume, _ int) bool {
				return orDefault(v.Namespace) == ns
			}), func(v Volume, _ int) string { return v.Name })
			return out, cobra.ShellCompDirectiveNoFileComp
		}
		return noFiles()
	}
	attachCmd.ValidArgsFunction = vmThenVolume
	detachCmd.ValidArgsFunction = vmThenVolume
	bootCmd.ValidArgsFunction = vmThenVolume
	watchCmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) != 0 {
			return noFiles()
		}
		return append(append([]string{}, nodeKinds...), "vms", "vm"), cobra.ShellCompDirectiveNoFileComp
	}
	describeCmd.ValidArgsFunction = completeKindThenNames
	deleteCmd.ValidArgsFunction = func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		// delete takes (vm ID|NAME | -f FILE): complete the kind, then names.
		if len(args) == 0 {
			return []string{"vm", "virtualmachine"}, cobra.ShellCompDirectiveNoFileComp
		}
		c := NewClient(apiURL(cmd))
		vms, err := c.GetVirtualMachines()
		if err != nil {
			return noFiles()
		}
		out := make([]string, 0, len(vms))
		for _, vm := range vms {
			out = append(out, vm.Name, vm.ID)
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	}
	// Manifest flags complete yaml files, not every file on disk.
	_ = createCmd.MarkFlagFilename("filename", "yaml", "yml")
	_ = applyCmd.MarkFlagFilename("filename", "yaml", "yml")
	_ = deleteCmd.MarkFlagFilename("filename", "yaml", "yml")
}
