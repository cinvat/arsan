package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run the compute node agent",
	PreRunE: func(cmd *cobra.Command, _ []string) error {
		return viper.BindPFlags(cmd.Flags())
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		// Default to the hostname, like a kubelet: the node's identity is the
		// machine it runs on, so an operator does not have to invent a name and
		// keep it consistent across restarts.
		name := viper.GetString("node.name")
		if name == "" {
			host, err := os.Hostname()
			if err != nil {
				return fmt.Errorf("node.name is required and the hostname is unavailable: %w", err)
			}
			name = host
		}
		driverName := viper.GetString("driver")

		// The token is the only thing the agent must know to join. Capacity,
		// engine and usage are fetched by the controller after the probe
		// succeeds, so nothing here describes the host.
		token := viper.GetString("bootstrap-token")
		if token == "" {
			return fmt.Errorf("a bootstrap token is required to join; set -t/--bootstrap-token")
		}

		drv, err := driver.New(driverName)
		if err != nil {
			return err
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		commands, err := bus.ConnectNATS(viper.GetString("bus.url"), 10*time.Second)
		if err != nil {
			return err
		}
		defer commands.Close()

		// Register over REST, like a kubelet registering its Node object.
		// The request carries nothing but identity: the controller learns
		// everything else by asking, after the probe says the agent answers.
		node, err := agent.RetryBootstrap(ctx, func(c context.Context) (core.Node, error) {
			return agent.Register(c, viper.GetString("api.url"), "v1alpha1", token,
				agent.BootstrapRequest{Name: name})
		}, bootstrapAttempts, bootstrapBackoff)
		if err != nil {
			return fmt.Errorf("agent: %w", err)
		}
		nodeID := node.ID

		log.Printf("agent serving as %s (%s)", nodeID, name)
		return agent.Serve(ctx, commands, nodeID, drv)
	},
}

func init() {
	agentCmd.Flags().String("node.name", "", "Node name; defaults to the hostname")
	agentCmd.Flags().String("driver", "fake", "compute engine (fake, kvm)")
	agentCmd.Flags().String("bus.url", "nats://127.0.0.1:4222", "NATS URL for the command bus")
	agentCmd.Flags().String("api.url", "http://127.0.0.1:8080", "control plane REST API to bootstrap against")
	// Default is empty on purpose: a built-in token value would be a
	// standing credential that silently works on a misconfigured deployment.
	agentCmd.Flags().StringP("bootstrap-token", "t", "", "bootstrap token")

	rootCmd.AddCommand(agentCmd)
}

const (
	// bootstrapAttempts bounds registration retries. An unreachable api is
	// usually a startup ordering problem and resolves in seconds; a refused
	// token is never retried at all.
	bootstrapAttempts = 10
	bootstrapBackoff  = 2 * time.Second
)
