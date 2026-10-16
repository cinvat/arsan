package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

var watchCmd = &cobra.Command{
	// Both watches stream NDJSON snapshots over one h2c connection: the
	// client reads, never polls. Level-triggered server-side, so an idle
	// cluster costs one store read per tick and zero bandwidth.
	Use:   "watch (nodes|vms)",
	Short: "Stream node or VM states until interrupted",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := NewClient(apiURL(cmd))
		switch args[0] {
		case "nodes", "node", "no":
			return watchNodes(c)
		case "virtualmachines", "virtualmachine", "vm", "vms":
			return watchVMs(c)
		default:
			return fmt.Errorf("only nodes or vms can be watched, got %q", args[0])
		}
	},
}

// stream opens an NDJSON watch and decodes one snapshot per line until the
// server closes or the client is interrupted. No polling: a quiet server
// sends nothing, and the scanner blocks until the next newline lands.
func stream(c *Client, path string, handle func(snap snapshot)) error {
	resp, err := c.stream.Get(c.base + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s", resp.Status)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var snap snapshot
		if err := json.Unmarshal(sc.Bytes(), &snap); err != nil {
			return err
		}
		handle(snap)
	}
	return sc.Err()
}

// watchNodes streams the server's node snapshots, printing each exactly like
// get nodes. Reasons stay in describe: watch is the short view that updates.
func watchNodes(c *Client) error {
	return stream(c, "/nodes?watch=true", func(snap snapshot) {
		printNodes(snap.Nodes)
		fmt.Println("---")
	})
}

// watchVMs streams the server's VM snapshots the same way. It replaces the
// old 2s re-list loop: same output shape, zero request overhead while idle.
func watchVMs(c *Client) error {
	return stream(c, "/virtualmachines?watch=true", func(snap snapshot) {
		printVMs(snap.VirtualMachines)
		fmt.Println("---")
	})
}

func init() {
	rootCmd.AddCommand(watchCmd)
}
