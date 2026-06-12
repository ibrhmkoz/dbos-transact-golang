package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/viper"
)

func maskPassword(dbUrl string) (string, error) {
	parsedUrl, err := url.Parse(dbUrl)
	if err == nil && parsedUrl.Scheme != "" {

		if parsedUrl.User != nil {
			username := parsedUrl.User.Username()
			_, hasPassword := parsedUrl.User.Password()
			if hasPassword {
				// Manually construct the URL with masked password to avoid encoding
				maskedUrl := parsedUrl.Scheme + "://" + username + ":***@" + parsedUrl.Host + parsedUrl.Path
				if parsedUrl.RawQuery != "" {
					maskedUrl += "?" + parsedUrl.RawQuery
				}
				if parsedUrl.Fragment != "" {
					maskedUrl += "#" + parsedUrl.Fragment
				}
				return maskedUrl, nil
			}
		}

		return parsedUrl.String(), nil
	}

	return maskPasswordInKeyValueFormat(dbUrl), nil
}

func createDbosAdmin(ctx context.Context, dbUrl string) (dbos.DbosAdmin, error) {
	return dbos.NewDbosAdmin(ctx, dbos.DbosAdminConfig{
		DatabaseUrl: dbUrl,
		Logger:      logger,
	})
}

func maskPasswordInKeyValueFormat(connStr string) string {

	re := regexp.MustCompile(`(?i)password\s*=\s*[^\s]+`)
	return re.ReplaceAllString(connStr, "password=***")
}

func getDBUrl() (string, error) {
	var resolvedUrl string
	var source string

	if dbUrl != "" {
		resolvedUrl = dbUrl
		source = "flag"
	} else if viper.IsSet("database_url") {

		resolvedUrl = viper.GetString("database_url")
		source = "DBOS config file"
	} else if envUrl := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); envUrl != "" {

		resolvedUrl = envUrl
		source = "environment variable"
	} else {
		return "", fmt.Errorf("missing database URL: please set it using the --db-url flag, your dbos-config.yaml file, or the DBOS_SYSTEM_DATABASE_URL environment variable")
	}

	maskedUrl, err := maskPassword(resolvedUrl)
	if err != nil {
		logger.Debug("Failed to mask database URL", "error", err)
		maskedUrl = resolvedUrl
	}
	logger.Debug("Using database URL", "source", source, "url", maskedUrl)

	return resolvedUrl, nil
}

func createDbosContext(ctx context.Context, dbUrl string) (dbos.DbosContext, error) {
	appName := "dbos-cli"

	config := dbos.Config{
		DatabaseUrl: dbUrl,
		AppName:     appName,
		Logger:      initLogger(slog.LevelError),
	}

	if schema != "" {
		config.DatabaseSchema = schema
		logger.Debug("Using database schema", "schema", schema)
	}

	dbosCtx, err := dbos.NewDbosContext(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create DBOS context: %w", err)
	}
	return dbosCtx, nil
}

func outputJson(data any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func confirmAction(prompt string) bool {
	fmt.Printf("%s (y/N): ", prompt)
	var response string
	fmt.Scanln(&response)
	return response == "y" || response == "Y" || response == "yes" || response == "Yes"
}

func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()
	dropSql := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
	if _, err := conn.Exec(ctx, dropSql); err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}
	return nil
}
