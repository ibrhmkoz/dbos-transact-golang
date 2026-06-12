package main

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Create DBOS system tables",
	RunE:  runMigrate,
}

var (
	applicationRole string
)

func init() {
	migrateCmd.Flags().StringVarP(&applicationRole, "app-role", "r", "", "The role with which you will run your DBOS application")
}

func runMigrate(cmd *cobra.Command, args []string) error {

	dbUrl, err := getDBUrl()
	if err != nil {
		return err
	}

	ctx := context.Background()

	_, err = createDbosContext(ctx, dbUrl)
	if err != nil {
		return err
	}

	dbSchema := "dbos"
	if schema != "" {
		dbSchema = schema
	}

	if applicationRole != "" {
		if err := grantDbosSchemaPermissions(dbUrl, applicationRole, dbSchema); err != nil {
			return err
		}
	}

	if config != nil && len(config.Database.Migrate) > 0 {
		logger.Info("Executing migration commands from 'dbos-config.yaml'")
		for _, command := range config.Database.Migrate {
			logger.Info("Executing migration command", "command", command)

			var process *exec.Cmd
			if runtime.GOOS == "windows" {
				process = exec.Command("cmd", "/C", command)
			} else {
				process = exec.Command("sh", "-c", command)
			}
			output, err := process.CombinedOutput()
			if err != nil {
				return fmt.Errorf("migration command failed: %s\nOutput: %s", err, output)
			}
			if len(output) > 0 {
				logger.Info("Migration output", "output", string(output))
			}
		}
	}

	logger.Info("DBOS migrations completed successfully")
	return nil
}

func grantDbosSchemaPermissions(databaseUrl, roleName, schemaName string) error {
	logger.Info("Granting permissions for schema", "role", roleName, "schema", schemaName)

	db, err := sql.Open("pgx", databaseUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	schemaSql := pgx.Identifier{schemaName}.Sanitize()
	roleSql := pgx.Identifier{roleName}.Sanitize()

	queries := []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schemaSql, roleSql),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s`, schemaSql, roleSql),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO %s`, schemaSql, roleSql),
		fmt.Sprintf(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s`, schemaSql, roleSql),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON TABLES TO %s`, schemaSql, roleSql),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON SEQUENCES TO %s`, schemaSql, roleSql),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s`, schemaSql, roleSql),
	}

	for _, query := range queries {
		logger.Debug("Executing grant query", "query", query)
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to execute grant: %w", err)
		}
	}

	return nil
}
