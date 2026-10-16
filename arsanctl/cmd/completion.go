package cmd

import (
	"github.com/spf13/cobra"
)

// Completion is best-effort and never errors visibly: a completion that fails
// (api down, bad flags) completes nothing rather than printing into the
// shell's completion stream.
var (
	nodeKinds   = []string{"node", "nodes"}
	vmKinds     = []string{"vm", "vms", "virtualmachine", "virtualmachines"}
	flavorKinds = []string{"flavor", "flavors", "fl"}
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
	switch args[0] {
	case "node", "nodes", "no":
		nodes, err := c.GetNodes()
		if err != nil {
			return noFiles()
		}
		out := make([]string, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, n.Name)
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	case "vm", "vms", "virtualmachine", "virtualmachines":
		vms, err := c.GetVirtualMachines()
		if err != nil {
			return noFiles()
		}
		out := make([]string, 0, len(vms))
		for _, vm := range vms {
			out = append(out, vm.Name)
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	case "flavor", "flavors", "fl":
		flavors, err := c.GetFlavors()
		if err != nil {
			return noFiles()
		}
		out := make([]string, 0, len(flavors))
		for _, f := range flavors {
			out = append(out, f.Name)
		}
		return out, cobra.ShellCompDirectiveNoFileComp
	default:
		return noFiles()
	}
}

// completeKindThenNames completes a kind first, then names of that kind:
// describe's and delete's two-arg shape.
func completeKindThenNames(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return completeKinds(nodeKinds, vmKinds, flavorKinds)(cmd, args, toComplete)
	}
	return completeNames(cmd, args, toComplete)
}

func init() {
	getCmd.ValidArgsFunction = completeKinds(nodeKinds, vmKinds, flavorKinds)
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
