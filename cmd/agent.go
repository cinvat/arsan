package cmd

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cinvat/arsan/internal/agent"
	"github.com/cinvat/arsan/internal/core"
	"github.com/cinvat/arsan/internal/driver"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run the compute node gRPC agent",
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
		arch := viper.GetString("node.arch")
		vcpu := viper.GetInt("node.vcpu")
		ram := viper.GetInt("node.ram")
		disk := viper.GetInt("node.disk")
		driverName := viper.GetString("driver")
		listen := viper.GetString("agent.addr")

		if secret := bootstrapSecret(); secret == "" {
			return fmt.Errorf("a bootstrap token is required to join; set -t/--bootstrap-token or --bootstrap-token-file")
		}

		total := core.Resources{VCPU: vcpu, RAM: ram, Disk: disk}
		drv, err := driver.New(driverName)
		if err != nil {
			return err
		}
		// An engine that knows its host reports capacity itself; hand-declared
		// numbers would be a second copy of facts libvirt already holds. The
		// flags remain for engines with nothing to detect (fake).
		if capable, ok := drv.(interface {
			HostCapacity(context.Context) (core.Resources, error)
		}); ok {
			capCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			detected, cerr := capable.HostCapacity(capCtx)
			cancel()
			if cerr != nil {
				return fmt.Errorf("detect host capacity: %w", cerr)
			}
			total = detected
		}

		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		agentSrv := agent.NewAgentServer(drv)
		grpcServer := grpc.NewServer()
		agent.RegisterServer(grpcServer, agentSrv)

		// Listen BEFORE bootstrapping. net.Listen binds the port, so a
		// connection arriving between here and Serve queues in the backlog
		// instead of being refused. Bootstrapping first would open a window
		// where the controller is told this agent exists but cannot reach it.
		lis, err := net.Listen("tcp", listen)
		if err != nil {
			return err
		}

		// Register over REST, like a kubelet registering its Node object.
		// The address sent is where we are already listening, which is what
		// the controller will dial for engine calls.
		advertised := viper.GetString("node.advertise")
		if advertised == "" {
			advertised = listen
		}
		if _, err := agent.RetryBootstrap(ctx, func(c context.Context) (core.Node, error) {
			return agent.Register(c, viper.GetString("api.url"), "v1alpha1", bootstrapSecret(),
				agent.BootstrapRequest{
					Name:    name,
					Arch:    arch,
					Driver:  driverName,
					Address: advertised,
					Total:   total,
				})
		}, bootstrapAttempts, bootstrapBackoff); err != nil {
			return fmt.Errorf("agent: %w", err)
		}

		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
			agentSrv.StopAgent()
		}()

		log.Printf("agent serving on %s as %s", listen, name)
		return grpcServer.Serve(lis)
	},
}

func init() {
	agentCmd.Flags().String("node.name", "", "Node name; defaults to the hostname")
	agentCmd.Flags().String("node.arch", "amd64", "Node architecture")
	agentCmd.Flags().Int("node.vcpu", 8, "Total vCPUs (ignored when the engine reports its host)")
	agentCmd.Flags().Int("node.ram", 16, "Total RAM in GiB (ignored when the engine reports its host)")
	agentCmd.Flags().Int("node.disk", 200, "Total disk in GiB (ignored when the engine reports its host)")
	agentCmd.Flags().String("driver", "fake", "compute engine (fake, kvm, esxi)")
	agentCmd.Flags().String("agent.addr", ":9090", "gRPC listen address")
	agentCmd.Flags().String("node.advertise", "", "address the controller should dial (default: agent.addr)")
	agentCmd.Flags().String("api.url", "http://127.0.0.1:8080", "control plane REST API to bootstrap against")
	// Default is empty on purpose: a built-in token value would be a
	// standing credential that silently works on a misconfigured deployment.
	agentCmd.Flags().StringP("bootstrap-token", "t", "", "bootstrap token (prefer --bootstrap-token-file: flags are world-readable via ps)")
	agentCmd.Flags().String("bootstrap-token-file", "", "file holding the bootstrap token (mode 0600)")

	rootCmd.AddCommand(agentCmd)
}

// bootstrapSecret reads the shared bootstrap token.
//
// The file variant is preferred: a flag value is world-readable through ps, and
// this secret authorises a host to join the cluster. The flag stays available
// for development.
// bootstrapSecret reads the shared bootstrap token.
//
// The agent names its flag with a hyphen (--bootstrap-token-file) while the api
// uses a dotted key (--bootstrap.token-file), so the shared secret helper is
// given both explicitly. Deriving one from the other silently ignored the
// agent's file flag, and the agent then refused to start with "a bootstrap
// token is required" even though one had been configured.
func bootstrapSecret() string {
	return secretFile("bootstrap.token", "bootstrap-token-file", "bootstrap-token")
}

const (
	// bootstrapAttempts bounds registration retries. An unreachable api is
	// usually a startup ordering problem and resolves in seconds; a refused
	// token is never retried at all.
	bootstrapAttempts = 10
	bootstrapBackoff  = 2 * time.Second
)
