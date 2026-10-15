package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "arsanctl",
	Short: "Control Arsan IaaS from the terminal",
	Long: `arsanctl talks to the arsan api: list nodes and virtual machines,
create and delete them, and watch node states stream in.

Every command takes --api-url (default http://127.0.0.1:8080).`,
}

// apiURL reads the persistent flag the subcommands share.
func apiURL(cmd *cobra.Command) string {
	url, _ := cmd.Flags().GetString("api-url")
	if url == "" {
		url, _ = cmd.PersistentFlags().GetString("api-url")
	}
	if url == "" {
		url = "http://127.0.0.1:8080"
	}
	return url
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().String("api-url", "http://127.0.0.1:8080", "arsan api base URL")
}
