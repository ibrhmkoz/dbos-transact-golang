package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset the DBOS system database",
	RunE:  runReset,
}

var (
	skipConfirmation bool
)

func init() {
	resetCmd.Flags().BoolVarP(&skipConfirmation, "yes", "y", false, "Skip confirmation prompt")
}

func runReset(cmd *cobra.Command, args []string) error {

	if !skipConfirmation {
		prompt := "This command resets your DBOS system database, deleting metadata about past workflows and steps. Are you sure you want to proceed?"
		if !confirmAction(prompt) {
			logger.Info("Operation cancelled.")
			return nil
		}
	}

	dbUrl, err := getDBUrl()
	if err != nil {
		return err
	}

	ctx := context.Background()

	config, err := pgxpool.ParseConfig(dbUrl)
	if err != nil {
		return fmt.Errorf("failed to parse database URL: %w", err)
	}

	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in connection string")
	}

	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}
	defer conn.Close(ctx)

	logger.Info("Resetting system database", "database", dbName)
	err = dropDatabaseIfExists(ctx, conn, dbName)
	if err != nil {
		return fmt.Errorf("failed to drop system database: %w", err)
	}

	createSql := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
	_, err = conn.Exec(ctx, createSql)
	if err != nil {
		return fmt.Errorf("failed to create system database: %w", err)
	}

	logger.Info("System database has been reset successfully", "database", dbName)
	return nil
}
