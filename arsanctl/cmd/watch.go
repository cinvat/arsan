package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
)

var watchCmd = &cobra.Command{
	Use:   "watch nodes",
	Short: "Stream node states until interrupted",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "nodes" && args[0] != "node" {
			return fmt.Errorf("only 'nodes' can be watched, got %q", args[0])
		}
		return watchNodes(NewClient(apiURL(cmd)))
	},
}

// watchNodes streams the server's node snapshots: one JSON object per line,
// each a full snapshot, until the server closes or the client is interrupted.
func watchNodes(c *Client) error {
	resp, err := c.http.Get(c.base + "/nodes/watch")
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
		for _, n := range snap.Nodes {
			reason := ""
			if n.NotReadyReason != "" {
				reason = " (" + n.NotReadyReason + ")"
			}
			fmt.Printf("%-32s %-10s%s\n", n.ID, n.State, reason)
		}
		fmt.Println("---")
	}
	return sc.Err()
}

func init() {
	rootCmd.AddCommand(watchCmd)
}
