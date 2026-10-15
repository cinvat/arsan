package cmd

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/cinvat/arsan/api"
	"github.com/cinvat/arsan/internal/bus"
	"github.com/cinvat/arsan/internal/records"
	"github.com/cinvat/arsan/internal/store/memory"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "Run the REST API",
	Long: `Serve the versioned REST API.

The api validates and persists records, and authenticates compute nodes at
bootstrap. It never places a VM and never calls the controller's libraries:
anything that needs a decision is published to the command bus, and the
controller acts on it.

Routes are mounted under their API version group, so v1alpha1 lives at
/v1alpha1/... .`,
	PreRunE: func(cmd *cobra.Command, _ []string) error {
		return viper.BindPFlags(cmd.Flags())
	},
	RunE: func(cmd *cobra.Command, _ []string) error {
		log := newLogger()

		commands, err := bus.ConnectNATS(viper.GetString("bus.url"), 10*time.Second)
		if err != nil {
			return err
		}
		defer commands.Close()

		// The api assembles a store and a CRUD layer. It deliberately does
		// NOT assemble a scheduler, a compute service or a node watcher:
		// those are the controller's, and importing them here is exactly the
		// coupling this separation exists to prevent.
		store := memory.New()
		nodes := &records.Nodes{Store: store}
		instances := &records.Instances{Store: store, Bus: commands}

		ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		srv := &http.Server{
			Addr: viper.GetString("addr"),
			Handler: api.Register(api.Config{
				Instances:      instances,
				Nodes:          nodes,
				Bus:            commands,
				BootstrapToken: bootstrapToken(),
			}),
			ReadHeaderTimeout: 10 * time.Second,
		}

		errCh := make(chan error, 1)
		go func() {
			log.Info("api listening", "addr", srv.Addr, "version", "v1alpha1",
				"bus", viper.GetString("bus.url"))
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
			close(errCh)
		}()

		select {
		case err := <-errCh:
			return err
		case <-ctx.Done():
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	},
}

// bootstrapToken is the shared secret an agent presents to register.
func bootstrapToken() string {
	return viper.GetString("bootstrap.token")
}

func init() {
	apiCmd.Flags().String("addr", ":8080", "HTTP listen address")
	apiCmd.Flags().String("bus.url", "nats://127.0.0.1:4222", "NATS URL for the command bus")
	apiCmd.Flags().String("bootstrap.token", "", "shared bootstrap token agents present to register")

	rootCmd.AddCommand(apiCmd)
}
