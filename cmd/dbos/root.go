package main

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	rootCmd = &cobra.Command{
		Use:          "dbos",
		Short:        "DBOS CLI",
		Long:         `DBOS CLI is a command-line interface for managing DBOS workflows`,
		SilenceUsage: true,
	}

	dbUrl      string
	configFile string
	verbose    bool
	schema     string

	config *Config
	logger *slog.Logger
)

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVarP(&dbUrl, "db-url", "D", "", "Your DBOS system database URL")
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "Config file (default is dbos-config.yaml)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Enable verbose mode (DEBUG level logging)")
	rootCmd.PersistentFlags().StringVar(&schema, "schema", "", "Database schema name (defaults to \"dbos\")")

	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(postgresCmd)
	rootCmd.AddCommand(workflowCmd)
}

func initConfig() {

	logger = initLogger(slog.LevelInfo)

	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("dbos-config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err == nil {

		expandEnvVarsInConfig()

		var cfg Config
		if err := viper.Unmarshal(&cfg); err == nil {
			config = &cfg
		}
	}
}

func initLogger(logLevel slog.Level) *slog.Logger {
	if verbose {
		logLevel = slog.LevelDebug
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	}))
}

func expandEnvVarsInConfig() {
	for _, key := range viper.AllKeys() {
		value := viper.Get(key)
		if strValue, ok := value.(string); ok {
			expandedValue := os.ExpandEnv(strValue)
			viper.Set(key, expandedValue)
		}
	}
}
