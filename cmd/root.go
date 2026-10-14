package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cinvat/arsan/internal/app"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "arsan",
	Short: "Arsan IaaS control plane",
	Long:  `Arsan schedules and supervises virtual machines across physical nodes.`,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		return initConfig(cmd)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.arsan.yaml)")

	viper.SetEnvPrefix("ARSAN")
	viper.AutomaticEnv()
}

func initConfig(cmd *cobra.Command) error {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("arsan")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.config/arsan")
		viper.AddConfigPath("/etc/arsan")
	}
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
	return nil
}

// newLogger is the single place klog-free structured logging is adapted to
// the narrow Logger interface each package declares.
func newLogger() app.Logger { return stdLogger{} }

type stdLogger struct{}

func (stdLogger) Info(msg string, kv ...any)  { log.Printf("INFO  %s %s", msg, join(kv)) }
func (stdLogger) Warn(msg string, kv ...any)  { log.Printf("WARN  %s %s", msg, join(kv)) }
func (stdLogger) Error(msg string, kv ...any) { log.Printf("ERROR %s %s", msg, join(kv)) }

func join(kv []any) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i+1 < len(kv); i += 2 {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%v=%v", kv[i], kv[i+1])
	}
	return b.String()
}
