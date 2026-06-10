package dbos

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ExportedWorkflow contains all data for a single workflow, in a portable format suitable for
// exporting from one environment and importing into another.
type ExportedWorkflow struct {
	WorkflowStatus        map[string]any   `json:"workflow_status"`
	OperationOutputs      []map[string]any `json:"operation_outputs"`
	WorkflowEvents        []map[string]any `json:"workflow_events"`
	WorkflowEventsHistory []map[string]any `json:"workflow_events_history"`
	Streams               []map[string]any `json:"streams"`
}

type Kernel struct {
	pool                          Pool
	queries                       *db.Queries
	notificationLoopDone          chan struct{}
	workflowNotificationsMap      *sync.Map
	workflowNotificationRepollMap *sync.Map
	workflowEventsMap             *sync.Map
	workflowEventsRepollMap       *sync.Map
	logger                        *slog.Logger
	schema                        string
	launched                      bool
}

// KernelConfig configures the shared DBOS system database dependency.
type KernelConfig struct {
	DatabaseURL     string
	DatabaseSchema  string
	SystemDBPool    *pgxpool.Pool
	Logger          *slog.Logger
	ApplicationName string
}

var errDeduplicationCollision = errors.New("deduplication ID collision")

/*******************************/
/******* INITIALIZATION ********/
/*******************************/

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	// Get the database name from the pool config
	poolConfig := pool.Config()
	dbName := poolConfig.ConnConfig.Database
	if dbName == "" {
		return errors.New("database name not found in pool configuration")
	}

	// Create a connection to the postgres database to create the target database
	serverConfig := poolConfig.ConnConfig.Copy()
	serverConfig.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, serverConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %v", err)
	}
	defer conn.Close(ctx)

	// Create the system database if it doesn't exist
	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if database exists: %v", err)
	}
	if !exists {
		createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(ctx, createSQL)
		if err != nil {
			return fmt.Errorf("failed to create database %s: %v", dbName, err)
		}
		logger.Debug("Database created", "name", dbName)
	}

	return nil
}

//go:embed migrations/1_initial_dbos_schema.sql
var migration1SQL string

//go:embed migrations/1_initial_dbos_schema_listen_notify.sql
var migration1ListenNotifySQL string

//go:embed migrations/2_add_queue_partition_key.sql
var migration2SQL string

//go:embed migrations/3_add_workflow_status_index.sql
var migration3SQL string

//go:embed migrations/4_add_forked_from.sql
var migration4SQL string

//go:embed migrations/5_add_step_timestamps.sql
var migration5SQL string

//go:embed migrations/6_add_workflow_events_history.sql
var migration6SQL string

//go:embed migrations/7_add_owner_xid.sql
var migration7SQL string

//go:embed migrations/8_add_parent_workflow_id.sql
var migration8SQL string

//go:embed migrations/9_add_workflow_schedules.sql
var migration9SQL string

//go:embed migrations/10_add_notifications_pkey.sql
var migration10SQL string

//go:embed migrations/11_add_serialization_columns.sql
var migration11SQL string

//go:embed migrations/12_add_notifications_consumed.sql
var migration12SQL string

//go:embed migrations/13_add_application_versions.sql
var migration13SQL string

//go:embed migrations/14_add_pgsql_client_functions.sql
var migration14SQL string

//go:embed migrations/15_add_workflow_schedule_columns.sql
var migration15SQL string

//go:embed migrations/16_add_delay_until.sql
var migration16SQL string

//go:embed migrations/17_add_workflow_schedule_queue_name.sql
var migration17SQL string

//go:embed migrations/18_add_was_forked_from.sql
var migration18SQL string

//go:embed migrations/19_add_operation_outputs_completed_at_index.sql
var migration19SQL string

//go:embed migrations/20_set_function_search_path.sql
var migration20SQL string

//go:embed migrations/21_create_queues_table.sql
var migration21SQL string

//go:embed migrations/22_drop_forked_from_index.sql
var migration22SQL string

//go:embed migrations/23_create_partial_forked_from_index.sql
var migration23SQL string

//go:embed migrations/24_drop_parent_workflow_id_index.sql
var migration24SQL string

//go:embed migrations/25_create_partial_parent_workflow_id_index.sql
var migration25SQL string

//go:embed migrations/26_drop_executor_id_index.sql
var migration26SQL string

//go:embed migrations/27_create_partial_dedup_id_index.sql
var migration27SQL string

//go:embed migrations/28_drop_dedup_id_constraint.sql
var migration28SQL string

//go:embed migrations/29_create_pending_index.sql
var migration29SQL string

//go:embed migrations/30_create_failed_index.sql
var migration30SQL string

//go:embed migrations/31_drop_status_index.sql
var migration31SQL string

//go:embed migrations/32_create_in_flight_index.sql
var migration32SQL string

//go:embed migrations/33_add_rate_limited.sql
var migration33SQL string

//go:embed migrations/34_create_rate_limited_index.sql
var migration34SQL string

//go:embed migrations/35_drop_queue_status_started_index.sql
var migration35SQL string

//go:embed migrations/36_add_completed_at.sql
var migration36SQL string

//go:embed migrations/37_create_started_at_index.sql
var migration37SQL string

//go:embed migrations/38_create_workflow_definitions.sql
var migration38SQL string

//go:embed migrations/39_add_workflow_retention.sql
var migration39SQL string

//go:embed migrations/40_drop_child_workflow_id.sql
var migration40SQL string

type migrationFile struct {
	version int64
	sql     string
	online  bool
}

const (
	_DBOS_MIGRATION_TABLE = "dbos_migrations"

	// Notification channels
	_DBOS_NOTIFICATIONS_CHANNEL   = "dbos_notifications_channel"
	_DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel"

	// Stream sentinel value for closure
	_DBOS_STREAM_CLOSED_SENTINEL = "__DBOS_STREAM_CLOSED__"

	// Database retry timeouts
	_DB_CONNECTION_RETRY_BASE_DELAY  = 1 * time.Second
	_DB_CONNECTION_RETRY_FACTOR      = 2
	_DB_CONNECTION_RETRY_MAX_RETRIES = 10
	_DB_CONNECTION_MAX_DELAY         = 120 * time.Second
	_DB_RETRY_INTERVAL               = 1 * time.Second
)

// buildMigrations renders the full list of migrations against the target schema.
func buildMigrations(schema string) []migrationFile {
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()

	migration1SQLProcessed := fmt.Sprintf(migration1SQL,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	migration1ListenNotifySQLProcessed := fmt.Sprintf(migration1ListenNotifySQL,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	migration1SQLProcessed = migration1SQLProcessed + "\n" + migration1ListenNotifySQLProcessed
	c := "CONCURRENTLY"
	migration20SQLProcessed := fmt.Sprintf(migration20SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	migration28SQLProcessed := fmt.Sprintf(migration28SQL, sanitizedSchema)

	return []migrationFile{
		{version: 1, sql: migration1SQLProcessed},
		{version: 2, sql: fmt.Sprintf(migration2SQL, sanitizedSchema)},
		{version: 3, sql: fmt.Sprintf(migration3SQL, sanitizedSchema)},
		{version: 4, sql: fmt.Sprintf(migration4SQL, sanitizedSchema, sanitizedSchema)},
		{version: 5, sql: fmt.Sprintf(migration5SQL, sanitizedSchema)},
		{version: 6, sql: fmt.Sprintf(migration6SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 7, sql: fmt.Sprintf(migration7SQL, sanitizedSchema)},
		{version: 8, sql: fmt.Sprintf(migration8SQL, sanitizedSchema, sanitizedSchema)},
		{version: 9, sql: fmt.Sprintf(migration9SQL, sanitizedSchema)},
		{version: 10, sql: fmt.Sprintf(migration10SQL, schema, sanitizedSchema)},
		{version: 11, sql: fmt.Sprintf(migration11SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 12, sql: fmt.Sprintf(migration12SQL, sanitizedSchema, sanitizedSchema)},
		{version: 13, sql: fmt.Sprintf(migration13SQL, sanitizedSchema)},
		{version: 14, sql: fmt.Sprintf(migration14SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 15, sql: fmt.Sprintf(migration15SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 16, sql: fmt.Sprintf(migration16SQL, sanitizedSchema, sanitizedSchema)},
		{version: 17, sql: fmt.Sprintf(migration17SQL, sanitizedSchema)},
		{version: 18, sql: fmt.Sprintf(migration18SQL, sanitizedSchema)},
		{version: 19, sql: fmt.Sprintf(migration19SQL, sanitizedSchema)},
		{version: 20, sql: migration20SQLProcessed},
		{version: 21, sql: fmt.Sprintf(migration21SQL, sanitizedSchema)},
		{version: 22, sql: fmt.Sprintf(migration22SQL, c, sanitizedSchema), online: true},
		{version: 23, sql: fmt.Sprintf(migration23SQL, c, sanitizedSchema), online: true},
		{version: 24, sql: fmt.Sprintf(migration24SQL, c, sanitizedSchema), online: true},
		{version: 25, sql: fmt.Sprintf(migration25SQL, c, sanitizedSchema), online: true},
		{version: 26, sql: fmt.Sprintf(migration26SQL, c, sanitizedSchema), online: true},
		{version: 27, sql: fmt.Sprintf(migration27SQL, c, sanitizedSchema), online: true},
		{version: 28, sql: migration28SQLProcessed},
		{version: 29, sql: fmt.Sprintf(migration29SQL, c, sanitizedSchema), online: true},
		{version: 30, sql: fmt.Sprintf(migration30SQL, c, sanitizedSchema), online: true},
		{version: 31, sql: fmt.Sprintf(migration31SQL, c, sanitizedSchema), online: true},
		{version: 32, sql: fmt.Sprintf(migration32SQL, c, sanitizedSchema), online: true},
		{version: 33, sql: fmt.Sprintf(migration33SQL, sanitizedSchema)},
		{version: 34, sql: fmt.Sprintf(migration34SQL, c, sanitizedSchema), online: true},
		{version: 35, sql: fmt.Sprintf(migration35SQL, c, sanitizedSchema), online: true},
		{version: 36, sql: fmt.Sprintf(migration36SQL, sanitizedSchema, sanitizedSchema)},
		{version: 37, sql: fmt.Sprintf(migration37SQL, c, sanitizedSchema), online: true},
		{version: 38, sql: fmt.Sprintf(migration38SQL, sanitizedSchema, c, sanitizedSchema, c, sanitizedSchema), online: true},
		{version: 39, sql: fmt.Sprintf(migration39SQL, sanitizedSchema)},
		{version: 40, sql: fmt.Sprintf(migration40SQL, sanitizedSchema)},
	}
}

// shouldMigrate reports whether any migration work remains for the schema.
// Returns true if the schema is missing, the dbos_migrations table is missing,
// or the recorded version is behind the latest.
func shouldMigrate(ctx context.Context, pool *pgxpool.Pool, schema string) (bool, error) {
	var schemaExists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schema).Scan(&schemaExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if schema %s exists: %v", schema, err)
	}
	if !schemaExists {
		return true, nil
	}

	var tableExists bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, _DBOS_MIGRATION_TABLE).Scan(&tableExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !tableExists {
		return true, nil
	}

	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", pgx.Identifier{schema}.Sanitize(), _DBOS_MIGRATION_TABLE)
	err = pool.QueryRow(ctx, q).Scan(&currentVersion)
	if err != nil && err != pgx.ErrNoRows {
		return false, fmt.Errorf("failed to get current migration version: %v", err)
	}
	migrations := buildMigrations(schema)
	return currentVersion < migrations[len(migrations)-1].version, nil
}

// cleanupInvalidIndexes drops indexes left in an INVALID state by a prior
// failed CREATE INDEX CONCURRENTLY. Such indexes are not used by the planner
// but block recreating an index of the same name. Must be called before
// retrying an online migration.
func cleanupInvalidIndexes(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) error {
	q := `SELECT i.relname FROM pg_index ix
	      JOIN pg_class i ON i.oid = ix.indexrelid
	      JOIN pg_class t ON t.oid = ix.indrelid
	      JOIN pg_namespace n ON n.oid = t.relnamespace
	      WHERE NOT ix.indisvalid AND n.nspname = $1`
	rows, err := pool.Query(ctx, q, schema)
	if err != nil {
		return fmt.Errorf("failed to list invalid indexes: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan invalid index name: %v", err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate invalid indexes: %v", err)
	}
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	for _, name := range names {
		if logger != nil {
			logger.Warn("dropping invalid index left by a prior failed migration", "schema", schema, "index", name)
		}
		dropQ := fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s.%s`, sanitizedSchema, pgx.Identifier{name}.Sanitize())
		if _, err := pool.Exec(ctx, dropQ); err != nil {
			return fmt.Errorf("failed to drop invalid index %s.%s: %v", schema, name, err)
		}
	}
	return nil
}

func writeMigrationVersion(ctx context.Context, exec interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, schema string, version int64, lastApplied int64) error {
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	if lastApplied == 0 {
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (version) VALUES ($1)", sanitizedSchema, _DBOS_MIGRATION_TABLE)
		if _, err := exec.Exec(ctx, insertQuery, version); err != nil {
			return fmt.Errorf("failed to insert migration version %d: %v", version, err)
		}
	} else {
		updateQuery := fmt.Sprintf("UPDATE %s.%s SET version = $1", sanitizedSchema, _DBOS_MIGRATION_TABLE)
		if _, err := exec.Exec(ctx, updateQuery, version); err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", version, err)
		}
	}
	return nil
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) error {
	migrations := buildMigrations(schema)
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()

	// Schema + migrations table setup in a single short transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	var schemaExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schema).Scan(&schemaExists); err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %v", schema, err)
	}
	if !schemaExists {
		createSchemaQuery := fmt.Sprintf("CREATE SCHEMA %s", sanitizedSchema)
		if _, err := tx.Exec(ctx, createSchemaQuery); err != nil {
			return fmt.Errorf("failed to create schema %s: %v", schema, err)
		}
	}
	var migrationTableExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, _DBOS_MIGRATION_TABLE).Scan(&migrationTableExists); err != nil {
		return fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !migrationTableExists {
		createTableQuery := fmt.Sprintf(`CREATE TABLE %s.%s (version BIGINT NOT NULL PRIMARY KEY)`,
			sanitizedSchema, _DBOS_MIGRATION_TABLE)
		if _, err := tx.Exec(ctx, createTableQuery); err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}
	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", sanitizedSchema, _DBOS_MIGRATION_TABLE)
	if err := tx.QueryRow(ctx, q).Scan(&currentVersion); err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to get current migration version: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration setup transaction: %v", err)
	}

	// Apply pending migrations one at a time.
	invalidIndexesCleaned := false
	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}

		if migration.online {
			// Online migrations must run outside a transaction so PostgreSQL will accept CREATE/DROP INDEX CONCURRENTLY.
			// Before the first online migration, sweep up any indexes left INVALID by a prior crashed run.
			// The version bump is necessarily a second, non-atomic round-trip. If it fails and must re-run, re-executing the migration has to be safe.
			if !invalidIndexesCleaned {
				if err := cleanupInvalidIndexes(ctx, pool, schema, logger); err != nil {
					return err
				}
				invalidIndexesCleaned = true
			}
			// Execute each statement separately: CREATE/DROP INDEX CONCURRENTLY
			// cannot run in a transaction block, and pgx sends a multi-statement
			// string as an implicit transaction. A migration may mix a catalog
			// statement (e.g. CREATE TABLE) with concurrent index builds, so we
			// split on statement boundaries and run them one at a time.
			for _, stmt := range splitSQLStatements(migration.sql) {
				if _, err := pool.Exec(ctx, stmt); err != nil {
					return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
				}
			}
			if err := writeMigrationVersion(ctx, pool, schema, migration.version, currentVersion); err != nil {
				return err
			}
			currentVersion = migration.version
			continue
		}

		if err := applyCatalogMigration(ctx, pool, schema, migration, currentVersion); err != nil {
			return err
		}
		currentVersion = migration.version
	}

	return nil
}

// q returns the sqlc query set bound to tx when one is supplied, otherwise the
// pool-bound set. Lets optional-transaction methods share a single code path.
func (k *Kernel) q(tx Tx) *db.Queries {
	if tx != nil {
		return k.queries.WithTx(PgxTx(tx))
	}
	return k.queries
}

// splitSQLStatements splits a migration script into individual statements on
// `;` boundaries, stripping `--` line comments and dropping empty fragments.
// It is intentionally simple: online migrations contain only DDL (CREATE/DROP
// TABLE/INDEX) with no semicolons or `--` inside string literals, so naive
// splitting is safe for that set.
func splitSQLStatements(script string) []string {
	var b strings.Builder
	for _, line := range strings.Split(script, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	var stmts []string
	for _, raw := range strings.Split(b.String(), ";") {
		if stmt := strings.TrimSpace(raw); stmt != "" {
			stmts = append(stmts, stmt)
		}
	}
	return stmts
}

// applyCatalogMigration runs a single non-online migration and its version bump in one transaction.
func applyCatalogMigration(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema string,
	migration migrationFile,
	currentVersion int64,
) error {
	mtx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for migration %d: %v", migration.version, err)
	}
	defer mtx.Rollback(ctx)

	if _, err := mtx.Exec(ctx, migration.sql); err != nil {
		return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
	}

	if err := writeMigrationVersion(ctx, mtx, schema, migration.version, currentVersion); err != nil {
		return err
	}
	if err := mtx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration %d: %v", migration.version, err)
	}
	return nil
}

type newKernelInput struct {
	databaseURL     string
	databaseSchema  string
	customPool      *pgxpool.Pool
	logger          *slog.Logger
	applicationName string
}

func newKernel(ctx context.Context, inputs newKernelInput) (*Kernel, error) {
	// Dereference fields from inputs
	databaseURL := inputs.databaseURL
	databaseSchema := inputs.databaseSchema
	customPool := inputs.customPool
	logger := inputs.logger

	// Validate that schema is provided
	if databaseSchema == "" {
		return nil, fmt.Errorf("database schema cannot be empty")
	}
	if customPool == nil {
		if err := validateDatabaseURL(databaseURL); err != nil {
			return nil, err
		}
	}

	// Configure a connection pool
	var pool *pgxpool.Pool
	if customPool != nil {
		logger.Info("Using custom database connection pool")
		// Verify the pool is valid
		poolConn, err := customPool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to validate custom pool: %v", err)
		}
		defer poolConn.Release()
		err = poolConn.Ping(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to validate custom pool: %v", err)
		}
		pool = customPool
	} else {
		// Parse the connection string to get a config
		config, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database URL: %v", err)
		}

		// Set pool configuration
		config.MaxConns = 20
		config.MinConns = 0
		config.MaxConnLifetime = time.Hour
		config.MaxConnIdleTime = time.Minute * 5

		// Add acquire timeout to prevent indefinite blocking
		config.ConnConfig.ConnectTimeout = 10 * time.Second

		if config.ConnConfig.RuntimeParams == nil {
			config.ConnConfig.RuntimeParams = make(map[string]string)
		}
		// Route all unqualified table references to the configured schema so
		// queries do not need to embed the schema name. Set on every pooled
		// connection via the startup parameter.
		config.ConnConfig.RuntimeParams["search_path"] = databaseSchema

		// Set application_name parameter if provided
		if inputs.applicationName != "" {
			config.ConnConfig.RuntimeParams["application_name"] = inputs.applicationName
		}

		// Create pool with configuration
		newPool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pool: %v", err)
		}
		pool = newPool
	}

	// Displaying Masked Database URL
	maskedDatabaseURL, err := maskPassword(pool.Config().ConnString())
	if err != nil {
		logger.Error("Failed to parse database URL", "error", err)
		return nil, fmt.Errorf("failed to parse database URL: %v", err)
	}
	logger.Info("Connecting to system database", "database_url", maskedDatabaseURL, "schema", databaseSchema)

	if customPool == nil {
		// Create the database if it doesn't exist
		if err := retry(ctx, func() error {
			return createDatabaseIfNotExists(ctx, pool, logger)
		}, withRetrierLogger(logger)); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create database: %v", err)
		}
	}

	needsMigration, smErr := shouldMigrate(ctx, pool, databaseSchema)
	if smErr != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to determine migration status: %v", smErr)
	}
	if needsMigration {
		if err := retry(ctx, func() error {
			return runMigrations(ctx, pool, databaseSchema, logger)
		}, withRetrierLogger(logger)); err != nil {
			if customPool == nil {
				pool.Close()
			}
			return nil, fmt.Errorf("failed to run migrations: %v", err)
		}
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	// Create a map of notification payloads to channels
	workflowNotificationsMap := &sync.Map{}
	workflowNotificationRepollMap := &sync.Map{}
	workflowEventsMap := &sync.Map{}
	workflowEventsRepollMap := &sync.Map{}

	return &Kernel{
		pool:                          newPgxPool(pool),
		queries:                       db.New(pool),
		workflowNotificationsMap:      workflowNotificationsMap,
		workflowNotificationRepollMap: workflowNotificationRepollMap,
		workflowEventsMap:             workflowEventsMap,
		workflowEventsRepollMap:       workflowEventsRepollMap,
		notificationLoopDone:          make(chan struct{}),
		logger:                        logger.With("service", "system_database"),
		schema:                        databaseSchema,
	}, nil
}

// NewKernel creates the shared system database and runs its migrations.
func NewKernel(ctx context.Context, config KernelConfig) (*Kernel, error) {
	if config.DatabaseSchema == "" {
		config.DatabaseSchema = "dbos"
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return newKernel(ctx, newKernelInput{
		databaseURL:     config.DatabaseURL,
		databaseSchema:  config.DatabaseSchema,
		customPool:      config.SystemDBPool,
		logger:          config.Logger,
		applicationName: config.ApplicationName,
	})
}

// Launch starts the system database notification loops.
func (k *Kernel) Launch(ctx context.Context) {
	k.launch(ctx)
}

// Shutdown stops the system database and closes its connection pool.
func (k *Kernel) Shutdown(ctx context.Context, timeout time.Duration) {
	k.shutdown(ctx, timeout)
}

func (k *Kernel) listenNotifyPool() *pgxpool.Pool {
	return PgxPool(k.pool)
}

func (k *Kernel) launch(ctx context.Context) {
	go k.notificationListenerLoop(ctx)
	k.launched = true
}

func (k *Kernel) shutdown(ctx context.Context, timeout time.Duration) {
	k.logger.Debug("Closing system database connection pool")

	if k.launched {
		// Wait for the notification loop to exit
		// The context should be cancelled prior to calling shutdown
		select {
		case <-k.notificationLoopDone:
		case <-time.After(timeout):
			k.logger.Warn("Notification listener loop did not finish in time", "timeout", timeout)
		}
	}

	if k.pool != nil {
		poolClose := make(chan struct{})
		go func() {
			// Will block until every acquired connection is released
			k.pool.Close()
			close(poolClose)
		}()
		select {
		case <-poolClose:
		case <-time.After(timeout):
			k.logger.Warn("System database connection pool did not close in time", "timeout", timeout)
		}
	}

	k.workflowNotificationsMap.Clear()
	k.workflowEventsMap.Clear()

	k.launched = false
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type insertWorkflowResult struct {
	attempts          int
	status            WorkflowStatusType
	name              string
	queueName         *string
	queuePartitionKey *string
	timeout           time.Duration
	workflowDeadline  time.Time
	ownerXID          string
}

type insertWorkflowStatusDBInput struct {
	status            WorkflowStatus
	maxRetries        int
	tx                Tx
	ownerXID          *string
	incrementAttempts bool
}

func (k *Kernel) insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error) {
	if input.tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

	// Set default values
	attempts := 1
	if input.status.Status == WorkflowStatusEnqueued || input.status.Status == WorkflowStatusDelayed {
		attempts = 0
	}

	var delayUntilEpochMs *int64
	if !input.status.DelayUntil.IsZero() {
		millis := input.status.DelayUntil.UnixMilli()
		delayUntilEpochMs = &millis
	}

	updatedAt := time.Now()
	if !input.status.UpdatedAt.IsZero() {
		updatedAt = input.status.UpdatedAt
	}

	var deadline *int64 = nil
	if !input.status.Deadline.IsZero() {
		millis := input.status.Deadline.UnixMilli()
		deadline = &millis
	}

	var timeoutMs *int64 = nil
	if input.status.Timeout > 0 {
		millis := input.status.Timeout.Round(time.Millisecond).Milliseconds()
		timeoutMs = &millis
	}

	// Our DB works with NULL values
	var applicationVersion *string
	if len(input.status.ApplicationVersion) > 0 {
		applicationVersion = &input.status.ApplicationVersion
	}

	var deduplicationID *string
	if len(input.status.DeduplicationID) > 0 {
		deduplicationID = &input.status.DeduplicationID
	}

	var queuePartitionKey *string
	if len(input.status.QueuePartitionKey) > 0 {
		queuePartitionKey = &input.status.QueuePartitionKey
	}

	var parentWorkflowID *string
	if len(input.status.ParentWorkflowID) > 0 {
		parentWorkflowID = &input.status.ParentWorkflowID
	}

	var className *string
	if len(input.status.ClassName) > 0 {
		className = &input.status.ClassName
	}

	// Marshal authenticated roles (slice of strings) to JSON for TEXT column
	authenticatedRoles, err := json.Marshal(input.status.AuthenticatedRoles)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	recoveryIncrement := int64(0)
	if input.incrementAttempts {
		recoveryIncrement = 1
	}

	// input.status.Input holds the encoded input (a *string).
	var inputs *string
	switch v := input.status.Input.(type) {
	case *string:
		inputs = v
	case string:
		inputs = &v
	}

	row, err := k.queries.WithTx(PgxTx(input.tx)).InsertWorkflowStatus(ctx, db.InsertWorkflowStatusParams{
		WorkflowUuid:            input.status.ID,
		Status:                  string(input.status.Status),
		Name:                    input.status.Name,
		QueueName:               input.status.QueueName,
		AuthenticatedUser:       input.status.AuthenticatedUser,
		AssumedRole:             input.status.AssumedRole,
		AuthenticatedRoles:      string(authenticatedRoles),
		ExecutorID:              input.status.ExecutorID,
		ApplicationVersion:      applicationVersion,
		ApplicationID:           input.status.ApplicationID,
		CreatedAt:               input.status.CreatedAt.Round(time.Millisecond).UnixMilli(),
		RecoveryAttempts:        int64(attempts),
		UpdatedAt:               updatedAt.UnixMilli(),
		WorkflowTimeoutMs:       timeoutMs,
		WorkflowDeadlineEpochMs: deadline,
		Inputs:                  inputs,
		DeduplicationID:         deduplicationID,
		Priority:                int32(input.status.Priority),
		QueuePartitionKey:       queuePartitionKey,
		OwnerXid:                input.ownerXID,
		ParentWorkflowID:        parentWorkflowID,
		ClassName:               className,
		ConfigName:              input.status.ConfigName,
		Serialization:           input.status.Serialization,
		DelayUntilEpochMs:       delayUntilEpochMs,
		EnqueuedStatus:          string(WorkflowStatusEnqueued),
		DelayedStatus:           string(WorkflowStatusDelayed),
		RecoveryIncrement:       recoveryIncrement,
	})
	if err != nil {
		// Deduplication collisions are resolved by attaching to the existing workflow.
		if isUniqueViolation(err) {
			return nil, errDeduplicationCollision
		}
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	var result insertWorkflowResult
	if row.RecoveryAttempts != nil {
		result.attempts = int(*row.RecoveryAttempts)
	}
	if row.Status != nil {
		result.status = WorkflowStatusType(*row.Status)
	}
	if row.Name != nil {
		result.name = *row.Name
	}
	result.queueName = row.QueueName
	result.queuePartitionKey = row.QueuePartitionKey
	if row.OwnerXid != nil {
		result.ownerXID = *row.OwnerXid
	}

	// Convert timeout milliseconds to time.Duration
	if row.WorkflowTimeoutMs != nil && *row.WorkflowTimeoutMs > 0 {
		result.timeout = time.Duration(*row.WorkflowTimeoutMs) * time.Millisecond
	}

	// Convert deadline milliseconds to time.Time
	if row.WorkflowDeadlineEpochMs != nil {
		result.workflowDeadline = time.Unix(0, *row.WorkflowDeadlineEpochMs*int64(time.Millisecond))
	}

	if len(input.status.Name) > 0 && result.name != input.status.Name {
		return nil, newConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.name, input.status.Name))
	}
	if len(input.status.QueueName) > 0 && result.queueName != nil && input.status.QueueName != *result.queueName {
		return nil, newConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists in a different queue: %s, but the provided queue is: %s", *result.queueName, input.status.QueueName))
	}

	// Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
	// When this number becomes equal to `maxRetries + 1`, we mark the workflow as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
	if result.status != WorkflowStatusSuccess && result.status != WorkflowStatusError &&
		input.maxRetries > 0 && result.attempts > input.maxRetries+1 {

		// Update workflow status to MAX_RECOVERY_ATTEMPTS_EXCEEDED and clear execution fields.
		if err := k.queries.WithTx(PgxTx(input.tx)).MarkWorkflowMaxRecoveryExceeded(ctx, db.MarkWorkflowMaxRecoveryExceededParams{
			NewStatus:     string(WorkflowStatusMaxRecoveryAttemptsExceeded),
			WorkflowUuid:  input.status.ID,
			PendingStatus: string(WorkflowStatusPending),
		}); err != nil {
			return nil, fmt.Errorf("failed to update workflow to %s: %w", WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		// Commit the transaction before throwing the error
		if err := input.tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction after marking workflow as %s: %w", WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		return nil, newDeadLetterQueueError(input.status.ID, input.maxRetries)
	}

	return &result, nil
}

// listWorkflowsDBInput represents the input parameters for listing workflows.
type listWorkflowsDBInput struct {
	workflowName       []string
	queueName          []string
	queuesOnly         bool
	workflowIDPrefix   []string
	workflowIDs        []string
	authenticatedUser  []string
	startTime          time.Time
	endTime            time.Time
	status             []WorkflowStatusType
	applicationVersion []string
	executorIDs        []string
	forkedFrom         []string
	parentWorkflowID   []string
	deduplicationID    []string
	completedAfter     time.Time
	completedBefore    time.Time
	dequeuedAfter      time.Time
	dequeuedBefore     time.Time
	wasForkedFrom      *bool
	hasParent          *bool
	limit              *int
	offset             *int
	sortDesc           bool
	loadInput          bool
	loadOutput         bool
	tx                 Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (k *Kernel) listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error) {
	idPrefixes := make([]string, len(input.workflowIDPrefix))
	for i, p := range input.workflowIDPrefix {
		idPrefixes[i] = p + "%"
	}
	statuses := make([]string, len(input.status))
	for i, st := range input.status {
		statuses[i] = string(st)
	}

	lim := int64(-1) // NULLIF(-1) disables the limit
	if input.limit != nil {
		lim = int64(*input.limit)
	}
	var off int64
	if input.offset != nil {
		off = int64(*input.offset)
	}

	params := db.ListWorkflowsParams{
		FilterName:            len(input.workflowName) > 0,
		Names:                 input.workflowName,
		FilterQueue:           len(input.queueName) > 0,
		QueueNames:            input.queueName,
		QueuesOnly:            input.queuesOnly,
		FilterIDPrefix:        len(idPrefixes) > 0,
		IDPrefixes:            idPrefixes,
		FilterIds:             len(input.workflowIDs) > 0,
		Ids:                   input.workflowIDs,
		FilterAuthUser:        len(input.authenticatedUser) > 0,
		AuthUsers:             input.authenticatedUser,
		FilterStart:           !input.startTime.IsZero(),
		StartMs:               input.startTime.UnixMilli(),
		FilterEnd:             !input.endTime.IsZero(),
		EndMs:                 input.endTime.UnixMilli(),
		FilterStatus:          len(statuses) > 0,
		Statuses:              statuses,
		FilterAppVersion:      len(input.applicationVersion) > 0,
		AppVersions:           input.applicationVersion,
		FilterExecutor:        len(input.executorIDs) > 0,
		ExecutorIds:           input.executorIDs,
		FilterForked:          len(input.forkedFrom) > 0,
		ForkedFroms:           input.forkedFrom,
		FilterParent:          len(input.parentWorkflowID) > 0,
		ParentIds:             input.parentWorkflowID,
		FilterDedup:           len(input.deduplicationID) > 0,
		DedupIds:              input.deduplicationID,
		FilterCompletedAfter:  !input.completedAfter.IsZero(),
		CompletedAfter:        input.completedAfter.UnixMilli(),
		FilterCompletedBefore: !input.completedBefore.IsZero(),
		CompletedBefore:       input.completedBefore.UnixMilli(),
		FilterDequeuedAfter:   !input.dequeuedAfter.IsZero(),
		DequeuedAfter:         input.dequeuedAfter.UnixMilli(),
		FilterDequeuedBefore:  !input.dequeuedBefore.IsZero(),
		DequeuedBefore:        input.dequeuedBefore.UnixMilli(),
		FilterWasForked:       input.wasForkedFrom != nil,
		WasForked:             input.wasForkedFrom != nil && *input.wasForkedFrom,
		FilterHasParent:       input.hasParent != nil,
		HasParent:             input.hasParent != nil && *input.hasParent,
		SortDesc:              input.sortDesc,
		Off:                   off,
		Lim:                   lim,
	}

	rows, err := k.q(input.tx).ListWorkflows(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to execute ListWorkflows query: %w", err)
	}

	workflows := make([]WorkflowStatus, 0, len(rows))
	for _, r := range rows {
		wf := WorkflowStatus{
			ID:            r.WorkflowUuid,
			Priority:      int(r.Priority),
			WasForkedFrom: r.WasForkedFrom,
		}
		if r.Status != nil {
			wf.Status = WorkflowStatusType(*r.Status)
		}
		if r.Name != nil {
			wf.Name = *r.Name
		}
		if r.RecoveryAttempts != nil {
			wf.Attempts = int(*r.RecoveryAttempts)
		}
		if r.AuthenticatedUser != nil {
			wf.AuthenticatedUser = *r.AuthenticatedUser
		}
		if r.AssumedRole != nil {
			wf.AssumedRole = *r.AssumedRole
		}
		if r.ApplicationID != nil {
			wf.ApplicationID = *r.ApplicationID
		}
		if r.AuthenticatedRoles != nil && *r.AuthenticatedRoles != "" {
			if err := json.Unmarshal([]byte(*r.AuthenticatedRoles), &wf.AuthenticatedRoles); err != nil {
				return nil, fmt.Errorf("failed to unmarshal authenticated_roles: %w", err)
			}
		}
		if r.QueueName != nil && len(*r.QueueName) > 0 {
			wf.QueueName = *r.QueueName
		}
		if r.ExecutorID != nil && len(*r.ExecutorID) > 0 {
			wf.ExecutorID = *r.ExecutorID
		}
		if r.ApplicationVersion != nil && len(*r.ApplicationVersion) > 0 {
			wf.ApplicationVersion = *r.ApplicationVersion
		}
		if r.DeduplicationID != nil && len(*r.DeduplicationID) > 0 {
			wf.DeduplicationID = *r.DeduplicationID
		}
		if r.QueuePartitionKey != nil && len(*r.QueuePartitionKey) > 0 {
			wf.QueuePartitionKey = *r.QueuePartitionKey
		}
		if r.ForkedFrom != nil && len(*r.ForkedFrom) > 0 {
			wf.ForkedFrom = *r.ForkedFrom
		}
		if r.ParentWorkflowID != nil && len(*r.ParentWorkflowID) > 0 {
			wf.ParentWorkflowID = *r.ParentWorkflowID
		}
		if r.Serialization != nil && len(*r.Serialization) > 0 {
			wf.Serialization = *r.Serialization
		}

		wf.CreatedAt = time.Unix(0, r.CreatedAt*int64(time.Millisecond))
		wf.UpdatedAt = time.Unix(0, r.UpdatedAt*int64(time.Millisecond))
		if r.WorkflowTimeoutMs != nil && *r.WorkflowTimeoutMs > 0 {
			wf.Timeout = time.Duration(*r.WorkflowTimeoutMs) * time.Millisecond
		}
		if r.WorkflowDeadlineEpochMs != nil {
			wf.Deadline = time.Unix(0, *r.WorkflowDeadlineEpochMs*int64(time.Millisecond))
		}
		if r.StartedAtEpochMs != nil {
			wf.StartedAt = time.Unix(0, *r.StartedAtEpochMs*int64(time.Millisecond))
		}
		if r.DelayUntilEpochMs != nil {
			wf.DelayUntil = time.Unix(0, *r.DelayUntilEpochMs*int64(time.Millisecond))
		}
		if r.CompletedAt != nil {
			wf.CompletedAt = time.Unix(0, *r.CompletedAt*int64(time.Millisecond))
		}

		// Output/error/inputs are always selected; only surface them when requested.
		if input.loadOutput {
			if r.Error != nil && *r.Error != "" {
				wf.Error = errors.New(*r.Error)
			}
			wf.Output = r.Output
		}
		if input.loadInput {
			wf.Input = r.Inputs
		}

		workflows = append(workflows, wf)
	}

	return workflows, nil
}

type updateWorkflowOutcomeDBInput struct {
	workflowID string
	status     WorkflowStatusType
	output     *string
	errStr     string
	tx         Tx
}

// updateWorkflowOutcome updates the status, output, and error of a workflow
// Note that transitions from CANCELLED to SUCCESS or ERROR are forbidden
func (k *Kernel) updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error {
	// input.output is already a *string from the database layer
	if err := k.q(input.tx).UpdateWorkflowOutcome(ctx, db.UpdateWorkflowOutcomeParams{
		Status:          string(input.status),
		Output:          input.output,
		Error:           input.errStr,
		NowMs:           time.Now().UnixMilli(),
		WorkflowUuid:    input.workflowID,
		CancelledStatus: string(WorkflowStatusCancelled),
		SuccessStatus:   string(WorkflowStatusSuccess),
		ErrorStatus:     string(WorkflowStatusError),
	}); err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

type cancelWorkflowsDBInput struct {
	workflowIDs []string
	tx          Tx
}

// cancelWorkflows cancels the given workflows in a single round-trip. Workflows that
// are already in a terminal state (SUCCESS, ERROR, CANCELLED) are left untouched.
// Returns the subset of input IDs that existed in workflow_status (including terminal
// ones, which are considered existing even though they are not updated).
func (k *Kernel) cancelWorkflows(ctx context.Context, input cancelWorkflowsDBInput) ([]string, error) {
	if len(input.workflowIDs) == 0 {
		return nil, nil
	}

	found, err := k.q(input.tx).CancelWorkflows(ctx, db.CancelWorkflowsParams{
		WorkflowIds:     input.workflowIDs,
		CancelledStatus: string(WorkflowStatusCancelled),
		NowMs:           time.Now().UnixMilli(),
		SuccessStatus:   string(WorkflowStatusSuccess),
		ErrorStatus:     string(WorkflowStatusError),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to cancel workflows: %w", err)
	}
	return found, nil
}

type deleteWorkflowsDBInput struct {
	workflowIDs    []string
	deleteChildren bool
	tx             Tx
}

func (k *Kernel) deleteWorkflows(ctx context.Context, input deleteWorkflowsDBInput) error {
	// If no transaction is provided, create one so the entire operation is atomic
	tx := input.tx
	if tx == nil {
		var err error
		tx, err = k.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction for deleteWorkflows: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	// Collect all workflow IDs to delete
	workflowIDs := make([]string, len(input.workflowIDs))
	copy(workflowIDs, input.workflowIDs)

	if input.deleteChildren {
		for _, wfID := range input.workflowIDs {
			children, err := k.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
				workflowID: wfID,
				tx:         tx,
			})
			if err != nil {
				return err
			}
			for _, child := range children {
				workflowIDs = append(workflowIDs, child.ID)
			}
		}
	}

	// Delete all matching workflows regardless of their state
	if err := k.queries.WithTx(PgxTx(tx)).DeleteWorkflows(ctx, workflowIDs); err != nil {
		return fmt.Errorf("failed to delete workflow(s): %w", err)
	}

	// If we created the transaction internally, commit it
	if input.tx == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit deleteWorkflows transaction: %w", err)
		}
	}

	return nil
}

type getWorkflowChildrenDBInput struct {
	workflowID string
	tx         Tx
}

// getWorkflowChildren retrieves all descendant workflows of the given parent workflow
// (breadth-first) within the same transaction.
func (k *Kernel) getWorkflowChildren(ctx context.Context, input getWorkflowChildrenDBInput) ([]WorkflowStatus, error) {

	children, err := k.listWorkflows(ctx, listWorkflowsDBInput{
		parentWorkflowID: []string{input.workflowID},
		tx:               input.tx,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get children of workflow %s: %w", input.workflowID, err)
	}

	queue := make([]string, 0, len(children))
	for _, child := range children {
		queue = append(queue, child.ID)
	}
	for len(queue) > 0 {
		parentID := queue[0]
		queue = queue[1:]

		grandchildren, err := k.listWorkflows(ctx, listWorkflowsDBInput{
			parentWorkflowID: []string{parentID},
			tx:               input.tx,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get children of workflow %s: %w", parentID, err)
		}
		for _, gc := range grandchildren {
			children = append(children, gc)
			queue = append(queue, gc.ID)
		}
	}

	return children, nil
}

func (k *Kernel) cancelAllBefore(ctx context.Context, cutoffTime time.Time) error {
	// List all workflows in PENDING, ENQUEUED, or DELAYED state ending at cutoffTime
	listInput := listWorkflowsDBInput{
		endTime: cutoffTime,
		status:  []WorkflowStatusType{WorkflowStatusPending, WorkflowStatusEnqueued, WorkflowStatusDelayed},
	}

	workflows, err := k.listWorkflows(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list workflows for cancellation: %w", err)
	}

	if len(workflows) == 0 {
		return nil
	}

	ids := make([]string, len(workflows))
	for i, workflow := range workflows {
		ids[i] = workflow.ID
	}
	if _, err := k.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIDs: ids}); err != nil {
		return fmt.Errorf("failed to cancel workflows during cancelAllBefore: %w", err)
	}
	return nil
}

type garbageCollectWorkflowsInput struct {
	cutoffEpochTimestampMs *int64
	rowsThreshold          *int
}

func (k *Kernel) garbageCollectWorkflows(ctx context.Context, input garbageCollectWorkflowsInput) error {
	// Validate input parameters
	if input.rowsThreshold != nil && *input.rowsThreshold <= 0 {
		return fmt.Errorf("rowsThreshold must be greater than 0, got %d", *input.rowsThreshold)
	}

	cutoffTimestamp := input.cutoffEpochTimestampMs

	// If rowsThreshold is provided, get the timestamp of the Nth newest workflow
	if input.rowsThreshold != nil {
		rowsBasedCutoff, err := k.queries.GetNthNewestCreatedAt(ctx, int32(*input.rowsThreshold-1))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("failed to query cutoff timestamp by rows threshold: %w", err)
		}
		// If we don't have a provided cutoffTimestamp and found one in the database
		// Or if the found cutoffTimestamp is more restrictive (higher timestamp = more recent = less deletion)
		// Use the cutoff timestamp found in the database
		if rowsBasedCutoff > 0 && cutoffTimestamp == nil || (cutoffTimestamp != nil && rowsBasedCutoff > *cutoffTimestamp) {
			cutoffTimestamp = &rowsBasedCutoff
		}
	}

	// Without an administrative cutoff, enforce each workflow definition's retention policy.
	if cutoffTimestamp == nil {
		deletedCount, err := k.queries.GarbageCollectByRetention(ctx, time.Now().UnixMilli())
		if err != nil {
			return fmt.Errorf("failed to garbage collect workflows by definition retention: %w", err)
		}
		k.logger.Info("Garbage collected workflows by definition retention", "deleted_count", deletedCount)
		return nil
	}

	// Delete all workflows older than cutoff that are NOT PENDING, ENQUEUED, or DELAYED
	deletedCount, err := k.queries.GarbageCollectByCutoff(ctx, db.GarbageCollectByCutoffParams{
		Cutoff:         *cutoffTimestamp,
		PendingStatus:  string(WorkflowStatusPending),
		EnqueuedStatus: string(WorkflowStatusEnqueued),
		DelayedStatus:  string(WorkflowStatusDelayed),
	})
	if err != nil {
		return fmt.Errorf("failed to garbage collect workflows: %w", err)
	}

	k.logger.Info("Garbage collected workflows",
		"cutoff_timestamp", *cutoffTimestamp,
		"deleted_count", deletedCount)

	return nil
}

type resumeWorkflowsDBInput struct {
	workflowIDs []string
	queueName   string
	tx          Tx
}

// resumeWorkflows re-enqueues the given workflows onto the specified queue (or the internal
// queue if unset). It returns the subset of IDs that existed in workflow_status; IDs in
// terminal states are considered existing even though they are not updated.
func (k *Kernel) resumeWorkflows(ctx context.Context, input resumeWorkflowsDBInput) ([]string, error) {
	if len(input.workflowIDs) == 0 {
		return nil, nil
	}

	queueName := input.queueName
	if queueName == "" {
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	found, err := k.q(input.tx).ResumeWorkflows(ctx, db.ResumeWorkflowsParams{
		WorkflowIds:    input.workflowIDs,
		EnqueuedStatus: string(WorkflowStatusEnqueued),
		QueueName:      queueName,
		NowMs:          time.Now().UnixMilli(),
		SuccessStatus:  string(WorkflowStatusSuccess),
		ErrorStatus:    string(WorkflowStatusError),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to resume workflows: %w", err)
	}
	return found, nil
}

type forkWorkflowDBInput struct {
	originalWorkflowID string
	forkedWorkflowID   string
	startStep          int
	applicationVersion string
	queueName          string
	queuePartitionKey  string
	tx                 Tx
}

func (k *Kernel) forkWorkflow(ctx context.Context, input forkWorkflowDBInput) (string, error) {
	// Generate new workflow ID if not provided
	forkedWorkflowID := input.forkedWorkflowID
	if forkedWorkflowID == "" {
		forkedWorkflowID = uuid.New().String()
	}

	// Validate startStep
	if input.startStep < 0 {
		return "", fmt.Errorf("startStep must be >= 0, got %d", input.startStep)
	}

	tx := input.tx
	ownTx := tx == nil
	if ownTx {
		var err error
		tx, err = k.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to begin fork transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	}
	txq := k.queries.WithTx(PgxTx(tx))

	// Get the original workflow status. Use the same tx so the read sees the
	// pre-fork state consistently with the writes below.
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{input.originalWorkflowID},
		loadInput:   true,
		tx:          tx,
	}
	wfs, err := k.listWorkflows(ctx, listInput)
	if err != nil {
		return "", fmt.Errorf("failed to list workflows: %w", err)
	}
	if len(wfs) == 0 {
		return "", newNonExistentWorkflowError(input.originalWorkflowID)
	}

	originalWorkflow := wfs[0]

	// Determine the application version to use
	appVersion := originalWorkflow.ApplicationVersion
	if input.applicationVersion != "" {
		appVersion = input.applicationVersion
	}

	// Determine the queue to place the forked workflow on
	queueName := input.queueName
	if queueName == "" {
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	// Marshal authenticated roles (slice of strings) to JSON for TEXT column
	authenticatedRoles, err := json.Marshal(originalWorkflow.AuthenticatedRoles)
	if err != nil {
		return "", fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	var queuePartitionKey *string
	if input.queuePartitionKey != "" {
		queuePartitionKey = &input.queuePartitionKey
	}

	// originalWorkflow.Input holds the loaded encoded input (a *string).
	var inputs *string
	switch v := originalWorkflow.Input.(type) {
	case *string:
		inputs = v
	case string:
		inputs = &v
	}

	now := time.Now().UnixMilli()
	// Create an entry for the forked workflow with the same initial values as the original
	if err := txq.ForkInsertWorkflowStatus(ctx, db.ForkInsertWorkflowStatusParams{
		WorkflowUuid:       forkedWorkflowID,
		Status:             string(WorkflowStatusEnqueued),
		Name:               originalWorkflow.Name,
		AuthenticatedUser:  originalWorkflow.AuthenticatedUser,
		AssumedRole:        originalWorkflow.AssumedRole,
		AuthenticatedRoles: string(authenticatedRoles),
		ApplicationVersion: appVersion,
		ApplicationID:      originalWorkflow.ApplicationID,
		QueueName:          queueName,
		QueuePartitionKey:  queuePartitionKey,
		Inputs:             inputs,
		CreatedAt:          now,
		UpdatedAt:          now,
		RecoveryAttempts:   0,
		ForkedFrom:         input.originalWorkflowID,
		Serialization:      originalWorkflow.Serialization,
	}); err != nil {
		return "", fmt.Errorf("failed to insert forked workflow status: %w", err)
	}

	// Mark the original workflow as having been forked from.
	if err := txq.MarkWorkflowForked(ctx, input.originalWorkflowID); err != nil {
		return "", fmt.Errorf("failed to mark original workflow as forked: %w", err)
	}

	// If startStep > 0, copy the original workflow's state up to startStep into the fork.
	if input.startStep > 0 {
		startStep := int32(input.startStep)
		if err := txq.ForkCopyOperationOutputs(ctx, db.ForkCopyOperationOutputsParams{
			ForkedID:   forkedWorkflowID,
			OriginalID: input.originalWorkflowID,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy operation outputs: %w", err)
		}
		if err := txq.ForkCopyEventsHistory(ctx, db.ForkCopyEventsHistoryParams{
			ForkedID:   forkedWorkflowID,
			OriginalID: input.originalWorkflowID,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy workflow events history: %w", err)
		}
		if err := txq.ForkCopyLatestEvents(ctx, db.ForkCopyLatestEventsParams{
			ForkedID:   forkedWorkflowID,
			OriginalID: input.originalWorkflowID,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy latest workflow events: %w", err)
		}
		if err := txq.ForkCopyStreams(ctx, db.ForkCopyStreamsParams{
			ForkedID:   forkedWorkflowID,
			OriginalID: input.originalWorkflowID,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy streams: %w", err)
		}
	}

	if ownTx {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit fork transaction: %w", err)
		}
	}
	return forkedWorkflowID, nil
}

type awaitWorkflowResultOutput struct {
	output        *string
	serialization string
	errStr        *string
}

func (k *Kernel) awaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*awaitWorkflowResultOutput, error) {
	if pollInterval <= 0 {
		pollInterval = _DB_RETRY_INTERVAL
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row, err := k.queries.GetWorkflowOutcome(ctx, workflowID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				time.Sleep(pollInterval)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		var storedSerialization string
		if row.Serialization != nil {
			storedSerialization = *row.Serialization
		}
		result := &awaitWorkflowResultOutput{output: row.Output, serialization: storedSerialization}

		var status WorkflowStatusType
		if row.Status != nil {
			status = WorkflowStatusType(*row.Status)
		}
		var attempts int64
		if row.RecoveryAttempts != nil {
			attempts = *row.RecoveryAttempts
		}

		switch status {
		case WorkflowStatusSuccess, WorkflowStatusError:
			if row.Error != nil && len(*row.Error) > 0 {
				result.errStr = row.Error
			}
			return result, nil
		case WorkflowStatusCancelled:
			return result, newAwaitedWorkflowCancelledError(workflowID)
		case WorkflowStatusMaxRecoveryAttemptsExceeded:
			return result, newDeadLetterQueueError(workflowID, int(attempts)-2)
		default:
			time.Sleep(pollInterval)
		}
	}
}

type recordOperationResultDBInput struct {
	workflowID    string
	stepID        int
	stepName      string
	output        *string
	errStr        *string
	tx            Tx
	startedAt     time.Time
	completedAt   time.Time
	serialization string
}

func (k *Kernel) recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	startedAtMs := input.startedAt.UnixMilli()
	completedAtMs := input.completedAt.UnixMilli()

	err := k.q(input.tx).RecordOperationResult(ctx, db.RecordOperationResultParams{
		WorkflowUuid:       input.workflowID,
		FunctionID:         int32(input.stepID),
		Output:             input.output,
		Error:              input.errStr,
		FunctionName:       input.stepName,
		StartedAtEpochMs:   &startedAtMs,
		CompletedAtEpochMs: &completedAtMs,
		Serialization:      &input.serialization,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return newWorkflowConflictIDError(input.workflowID)
		}
		return err
	}

	return nil
}

// getDeduplicatedWorkflow returns the ID of the workflow currently holding the
// deduplication slot for (workflowName, deduplicationID), or nil if the slot is free.
func (k *Kernel) getDeduplicatedWorkflow(ctx context.Context, workflowName, deduplicationID string) (*string, error) {
	id, err := k.queries.GetDeduplicatedWorkflow(ctx, db.GetDeduplicatedWorkflowParams{
		Name:            &workflowName,
		DeduplicationID: &deduplicationID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get deduplicated workflow: %w", err)
	}

	return &id, nil
}

/*******************************/
/******* STEPS ********/
/*******************************/

type recordedResult struct {
	output        *string
	errStr        *string
	serialization string
}

type checkOperationExecutionDBInput struct {
	workflowID string
	stepID     int
	stepName   string
	tx         Tx
}

func (k *Kernel) checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error) {
	// Use provided transaction or create a new one. We don't commit -- it is
	// just useful for having READ COMMITTED across the two reads.
	tx := input.tx
	if tx == nil {
		var err error
		tx, err = k.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	}
	q := k.queries.WithTx(PgxTx(tx))

	// Retrieve the workflow status
	status, err := q.GetWorkflowStatusOnly(ctx, input.workflowID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, newNonExistentWorkflowError(input.workflowID)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if status != nil && WorkflowStatusType(*status) == WorkflowStatusCancelled {
		return nil, newWorkflowCancelledError(input.workflowID)
	}

	// Retrieve operation outputs if they exist
	out, err := q.GetOperationOutput(ctx, db.GetOperationOutputParams{
		WorkflowUuid: input.workflowID,
		FunctionID:   int32(input.stepID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	// If the provided and recorded function name are different, return an error
	if input.stepName != out.FunctionName {
		return nil, newUnexpectedStepError(input.workflowID, input.stepID, input.stepName, out.FunctionName)
	}

	var storedSerialization string
	if out.Serialization != nil {
		storedSerialization = *out.Serialization
	}
	var recordedErrStr *string
	if out.Error != nil && *out.Error != "" {
		recordedErrStr = out.Error
	}
	result := &recordedResult{
		output:        out.Output,
		errStr:        recordedErrStr,
		serialization: storedSerialization,
	}
	return result, nil
}

// StepInfo contains information about a workflow step execution.
type stepInfo struct {
	StepID        int       // The sequential ID of the step within the workflow
	StepName      string    // The name of the step function
	Output        *string   // The output returned by the step (if any)
	Error         error     // The error returned by the step (if any)
	StartedAt     time.Time // When the step execution started
	CompletedAt   time.Time // When the step execution completed
	Serialization string    // The serialization format used for this step
}

type getWorkflowStepsInput struct {
	workflowID string
	loadOutput bool
}

func (k *Kernel) getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error) {
	rows, err := k.queries.GetWorkflowSteps(ctx, input.workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}

	steps := make([]stepInfo, 0, len(rows))
	for _, r := range rows {
		step := stepInfo{
			StepID:   int(r.FunctionID),
			StepName: r.FunctionName,
		}

		// Convert timestamps from milliseconds to time.Time
		if r.StartedAtEpochMs != nil {
			step.StartedAt = time.Unix(0, *r.StartedAtEpochMs*int64(time.Millisecond))
		}
		if r.CompletedAtEpochMs != nil {
			step.CompletedAt = time.Unix(0, *r.CompletedAtEpochMs*int64(time.Millisecond))
		}

		// Return output as encoded string if loadOutput is true
		if input.loadOutput {
			step.Output = r.Output
		}

		if r.Serialization != nil {
			step.Serialization = *r.Serialization
		}
		// Convert error string to error if present
		if r.Error != nil && *r.Error != "" {
			step.Error = errors.New(*r.Error)
		}

		steps = append(steps, step)
	}

	return steps, nil
}

// WorkflowAggregateRow is a single row of a workflow aggregate query result.
// Group maps each grouping column name (e.g. "status", "name", "time_bucket") to its
// stringified value, with nil entries for grouping columns that were NULL for that row.
// Count is the number of workflows in this group.
type WorkflowAggregateRow struct {
	Group map[string]*string `json:"group"`
	Count int64              `json:"count"`
}

// _DEFAULT_AGGREGATES_LIMIT caps the number of group rows returned by getWorkflowAggregates
// when the caller does not provide an override.
const _DEFAULT_AGGREGATES_LIMIT = 10_000_000

// getWorkflowAggregatesDBInput represents the input parameters for getting workflow aggregates.
type getWorkflowAggregatesDBInput struct {
	groupByStatus             bool
	groupByName               bool
	groupByQueueName          bool
	groupByExecutorID         bool
	groupByApplicationVersion bool
	timeBucketSizeMs          int64 // 0 disables time bucketing
	status                    []WorkflowStatusType
	startTime                 time.Time
	endTime                   time.Time
	workflowName              []string
	applicationVersion        []string
	executorID                []string
	queueName                 []string
	workflowIDPrefix          []string
	limit                     int64 // 0 means use _DEFAULT_AGGREGATES_LIMIT
	tx                        Tx
}

// aggGroupString normalizes an aggregate group column (scanned as interface{},
// holding string/int64/nil) to a *string for the group map.
func aggGroupString(v any) *string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return &t
	case int64:
		s := strconv.FormatInt(t, 10)
		return &s
	default:
		s := fmt.Sprintf("%v", t)
		return &s
	}
}

// impStr coerces an exported map value (which may be *string, string, or nil —
// JSON round-trips strings as string) to a nullable *string for import.
func impStr(v any) *string {
	switch t := v.(type) {
	case nil:
		return nil
	case *string:
		return t
	case string:
		return &t
	default:
		s := fmt.Sprintf("%v", t)
		return &s
	}
}

// impStrNonNull coerces to a non-null string (for NOT NULL columns / keys).
func impStrNonNull(v any) string {
	if p := impStr(v); p != nil {
		return *p
	}
	return ""
}

// impInt64Ptr coerces an exported map value to *int64. JSON round-trips numbers
// as float64 (or json.Number), so all numeric forms are handled.
func impInt64Ptr(v any) *int64 {
	switch t := v.(type) {
	case nil:
		return nil
	case *int64:
		return t
	case int64:
		return &t
	case int32:
		x := int64(t)
		return &x
	case *int32:
		if t == nil {
			return nil
		}
		x := int64(*t)
		return &x
	case int:
		x := int64(t)
		return &x
	case *int:
		if t == nil {
			return nil
		}
		x := int64(*t)
		return &x
	case float64:
		x := int64(t)
		return &x
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return &n
		}
		return nil
	default:
		return nil
	}
}

func impInt64NonNull(v any) int64 {
	if p := impInt64Ptr(v); p != nil {
		return *p
	}
	return 0
}

func impInt32(v any) int32 {
	if p := impInt64Ptr(v); p != nil {
		return int32(*p)
	}
	return 0
}

// aggInt64 normalizes an aggregate value column (interface{}, holding int64/nil) to *int64.
func aggInt64(v any) *int64 {
	switch t := v.(type) {
	case int64:
		return &t
	case int32:
		x := int64(t)
		return &x
	default:
		return nil
	}
}

func (k *Kernel) getWorkflowAggregates(ctx context.Context, input getWorkflowAggregatesDBInput) ([]WorkflowAggregateRow, error) {
	if input.timeBucketSizeMs < 0 {
		return nil, errors.New("timeBucketSizeMs must be > 0")
	}
	groupByTimeBucket := input.timeBucketSizeMs > 0
	if !input.groupByStatus && !input.groupByName && !input.groupByQueueName &&
		!input.groupByExecutorID && !input.groupByApplicationVersion && !groupByTimeBucket {
		return nil, errors.New("at least one group_by flag must be set, or a time bucket size provided")
	}

	statuses := make([]string, len(input.status))
	for i, st := range input.status {
		statuses[i] = string(st)
	}
	idPrefixes := make([]string, len(input.workflowIDPrefix))
	for i, p := range input.workflowIDPrefix {
		idPrefixes[i] = p + "%"
	}
	limit := input.limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	bucketSize := input.timeBucketSizeMs
	if bucketSize <= 0 {
		bucketSize = 1 // unused when groupByTimeBucket is false; avoids divide-by-zero
	}

	rows, err := k.q(input.tx).GetWorkflowAggregates(ctx, db.GetWorkflowAggregatesParams{
		GroupStatus:      input.groupByStatus,
		GroupName:        input.groupByName,
		GroupQueue:       input.groupByQueueName,
		GroupExecutor:    input.groupByExecutorID,
		GroupAppVersion:  input.groupByApplicationVersion,
		GroupTimeBucket:  groupByTimeBucket,
		BucketSize:       bucketSize,
		FilterStatus:     len(statuses) > 0,
		Statuses:         statuses,
		FilterStart:      !input.startTime.IsZero(),
		StartMs:          input.startTime.UnixMilli(),
		FilterEnd:        !input.endTime.IsZero(),
		EndMs:            input.endTime.UnixMilli(),
		FilterName:       len(input.workflowName) > 0,
		Names:            input.workflowName,
		FilterAppVersion: len(input.applicationVersion) > 0,
		AppVersions:      input.applicationVersion,
		FilterExecutor:   len(input.executorID) > 0,
		ExecutorIds:      input.executorID,
		FilterQueue:      len(input.queueName) > 0,
		QueueNames:       input.queueName,
		FilterIDPrefix:   len(idPrefixes) > 0,
		IDPrefixes:       idPrefixes,
		Lim:              limit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute getWorkflowAggregates query: %w", err)
	}

	results := make([]WorkflowAggregateRow, 0, len(rows))
	for _, r := range rows {
		groupMap := make(map[string]*string)
		if input.groupByStatus {
			groupMap["status"] = aggGroupString(r.GStatus)
		}
		if input.groupByName {
			groupMap["name"] = aggGroupString(r.GName)
		}
		if input.groupByQueueName {
			groupMap["queue_name"] = aggGroupString(r.GQueueName)
		}
		if input.groupByExecutorID {
			groupMap["executor_id"] = aggGroupString(r.GExecutorID)
		}
		if input.groupByApplicationVersion {
			groupMap["application_version"] = aggGroupString(r.GAppVersion)
		}
		if groupByTimeBucket {
			groupMap["time_bucket"] = aggGroupString(r.GTimeBucket)
		}
		results = append(results, WorkflowAggregateRow{Group: groupMap, Count: r.Cnt})
	}
	return results, nil
}

// StepAggregateRow is a single row of a step aggregate query result.
// Group maps each grouping column name (e.g. "function_name", "status", "time_bucket") to its
// stringified value, with nil entries for grouping columns that were NULL for that row.
// Count and MaxDurationMs are pointers because the caller selects which aggregates to compute;
// an unselected aggregate is nil (serialized as null, matching the other SDKs).
type StepAggregateRow struct {
	Group         map[string]*string `json:"group"`
	Count         *int64             `json:"count"`
	MaxDurationMs *int64             `json:"max_duration_ms"`
}

// getStepAggregatesDBInput represents the input parameters for getting step aggregates.
type getStepAggregatesDBInput struct {
	groupByFunctionName bool
	groupByStatus       bool
	selectCount         bool
	selectMaxDurationMs bool
	timeBucketSizeMs    int64 // 0 disables time bucketing
	status              []string
	functionName        []string
	workflowIDPrefix    []string
	completedAfter      time.Time
	completedBefore     time.Time
	limit               int64 // 0 means use _DEFAULT_AGGREGATES_LIMIT
	tx                  Tx
}

// statusExpr derives a step's status from operation_outputs: rows with a NULL error are
// SUCCESS, otherwise ERROR. operation_outputs has no explicit status column.
const stepStatusExpr = "(CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END)"

func (k *Kernel) getStepAggregates(ctx context.Context, input getStepAggregatesDBInput) ([]StepAggregateRow, error) {
	if input.timeBucketSizeMs < 0 {
		return nil, errors.New("timeBucketSizeMs must be > 0")
	}

	groupByTimeBucket := input.timeBucketSizeMs > 0
	if !input.groupByFunctionName && !input.groupByStatus && !groupByTimeBucket {
		return nil, errors.New("at least one group_by flag must be set, or a time bucket size provided")
	}
	if !input.selectCount && !input.selectMaxDurationMs {
		return nil, errors.New("at least one select_ flag must be set")
	}

	idPrefixes := make([]string, len(input.workflowIDPrefix))
	for i, p := range input.workflowIDPrefix {
		idPrefixes[i] = p + "%"
	}
	limit := input.limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	bucketSize := input.timeBucketSizeMs
	if bucketSize <= 0 {
		bucketSize = 1 // unused when groupByTimeBucket is false; avoids divide-by-zero
	}

	rows, err := k.q(input.tx).GetStepAggregates(ctx, db.GetStepAggregatesParams{
		GroupFunctionName:     input.groupByFunctionName,
		GroupStatus:           input.groupByStatus,
		GroupTimeBucket:       groupByTimeBucket,
		BucketSize:            bucketSize,
		FilterStatus:          len(input.status) > 0,
		Statuses:              input.status,
		FilterFunctionName:    len(input.functionName) > 0,
		FunctionNames:         input.functionName,
		FilterIDPrefix:        len(idPrefixes) > 0,
		IDPrefixes:            idPrefixes,
		FilterCompletedAfter:  !input.completedAfter.IsZero(),
		CompletedAfter:        input.completedAfter.UnixMilli(),
		FilterCompletedBefore: !input.completedBefore.IsZero(),
		CompletedBefore:       input.completedBefore.UnixMilli(),
		Lim:                   limit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute getStepAggregates query: %w", err)
	}

	results := make([]StepAggregateRow, 0, len(rows))
	for _, r := range rows {
		groupMap := make(map[string]*string)
		if input.groupByFunctionName {
			groupMap["function_name"] = aggGroupString(r.GFunctionName)
		}
		if input.groupByStatus {
			groupMap["status"] = aggGroupString(r.GStatus)
		}
		if groupByTimeBucket {
			groupMap["time_bucket"] = aggGroupString(r.GTimeBucket)
		}
		row := StepAggregateRow{Group: groupMap}
		if input.selectCount {
			c := r.Cnt
			row.Count = &c
		}
		if input.selectMaxDurationMs {
			row.MaxDurationMs = aggInt64(r.MaxDurationMs)
		}
		results = append(results, row)
	}
	return results, nil
}

type sleepInput struct {
	duration  time.Duration // Duration to sleep
	skipSleep bool          // If true, the function will not actually sleep and just return the remaining sleep duration
	stepID    *int          // Optional step ID to use instead of generating a new one (for internal use)
}

// Sleep is a special type of step that sleeps for a specified duration
// A wakeup time is computed and recorded in the database
// If we sleep is re-executed, it will only sleep for the remaining duration until the wakeup time
// sleep can be called within other special steps (e.g., getEvent, recv) to provide durable sleep

func (k *Kernel) sleep(ctx context.Context, input sleepInput) (time.Duration, error) {
	functionName := "DBOS.sleep"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, newStepExecutionError("", functionName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	// Determine step ID
	var stepID int
	if input.stepID != nil && *input.stepID >= 0 {
		stepID = *input.stepID
	} else {
		stepID = wfState.nextStepID()
	}

	startTime := time.Now()

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   functionName,
	}
	recordedResult, err := k.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return 0, fmt.Errorf("failed to check operation execution: %w", err)
	}

	var endTime time.Time

	if recordedResult != nil {
		if recordedResult.output == nil { // This should never happen
			return 0, fmt.Errorf("no recorded end time for recorded sleep operation")
		}

		// Decode the recorded end time directly into time.Time
		// recordedResult.output is an encoded *string
		serializer := newJSONSerializer[time.Time]()
		endTime, err = serializer.Decode(recordedResult.output)
		if err != nil {
			return 0, fmt.Errorf("failed to decode sleep end time: %w", err)
		}

		if recordedResult.errStr != nil { // This should never happen
			return 0, errors.New(*recordedResult.errStr)
		}
	} else {
		// First execution: calculate and record the end time
		endTime = time.Now().Add(input.duration)

		// Serialize the end time before recording
		serializer := newJSONSerializer[time.Time]()
		encodedEndTime, serErr := serializer.Encode(endTime)
		if serErr != nil {
			return 0, fmt.Errorf("failed to serialize sleep end time: %w", serErr)
		}

		// Record the operation result with the calculated end time
		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowID:    wfState.workflowID,
			stepID:        stepID,
			stepName:      functionName,
			output:        encodedEndTime,
			startedAt:     startTime,
			completedAt:   completedTime,
			serialization: "DBOS_JSON",
		}

		err = k.recordOperationResult(ctx, recordInput)
		if err != nil {
			// Check if this is a ConflictingWorkflowError (operation already recorded by another process)
			if dbosErr, ok := err.(*DBOSError); ok && dbosErr.Code == ConflictingIDError {
			} else {
				return 0, fmt.Errorf("failed to record sleep operation result: %w", err)
			}
		}
	}

	// Calculate remaining duration until wake up time
	remainingDuration := max(0, time.Until(endTime))

	if !input.skipSleep {
		// Actually sleep for the remaining duration
		time.Sleep(remainingDuration)
	}

	return remainingDuration, nil
}

/****************************************/
/******* PATCHES ********/
/****************************************/

type patchDBInput struct {
	workflowID string
	stepID     int
	patchName  string
}

func (k *Kernel) doesPatchExists(ctx context.Context, input patchDBInput) (string, error) {
	return k.queries.DoesPatchExist(ctx, db.DoesPatchExistParams{
		WorkflowUuid: input.workflowID,
		FunctionID:   int32(input.stepID),
	})
}

func (k *Kernel) patch(ctx context.Context, input patchDBInput) (bool, error) {
	functionName, err := k.doesPatchExists(ctx, input)
	if err != nil {
		// No result means this is a new workflow, or an existing workflow that has not reached this step yet
		// Insert the patch marker and return true
		if errors.Is(err, pgx.ErrNoRows) {
			if err := k.queries.InsertPatchMarker(ctx, db.InsertPatchMarkerParams{
				WorkflowUuid: input.workflowID,
				FunctionID:   int32(input.stepID),
				FunctionName: input.patchName,
			}); err != nil {
				return false, fmt.Errorf("failed to insert patch marker: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("failed to check for patch: %w", err)
	}

	// If functionName != patchName, this is a workflow that existed before the patch was applied
	// Else this a new (patched) workflow that is being re-executed (e.g., recovery, or forked at a later step)
	return functionName == input.patchName, nil
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

func (k *Kernel) notificationListenerLoop(ctx context.Context) {
	defer func() {
		k.logger.Debug("Notification listener loop exiting")
		k.notificationLoopDone <- struct{}{}
	}()

	pgxPool := k.listenNotifyPool()
	if pgxPool == nil {
		k.logger.Error("Notification listener loop started without a pgx-backed pool; aborting")
		return
	}

	acquire := func(ctx context.Context) (*pgxpool.Conn, error) {
		// Acquire a connection from the pool and set up LISTEN on the notifications channels
		pc, err := pgxPool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := pc.Begin(ctx)
		if err != nil {
			pc.Release()
			return nil, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", _DBOS_NOTIFICATIONS_CHANNEL)); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				k.logger.Error("Failed to rollback transaction after LISTEN error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", _DBOS_WORKFLOW_EVENTS_CHANNEL)); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				k.logger.Error("Failed to rollback transaction after LISTEN error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				k.logger.Error("Failed to rollback transaction after COMMIT error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		return pc, nil
	}

	k.logger.Debug("DBOS: Starting notification listener loop")

	poolConn, err := retryWithResult(ctx, func() (*pgxpool.Conn, error) {
		return acquire(ctx)
	}, withRetrierLogger(k.logger))
	if err != nil {
		k.logger.Error("Failed to acquire listener connection", "error", err)
		return
	}
	defer poolConn.Release()

	retryAttempt := 0
	for {
		// Block until a notification is received. OnNotification will be called when a notification is received.
		// WaitForNotification handles context cancellation: https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgconn/pgconn.go#L1050
		n, err := poolConn.Conn().WaitForNotification(ctx)
		if err != nil {
			// Context cancellation -> graceful exit
			if ctx.Err() != nil {
				k.logger.Debug("Notification listener exiting (context canceled", "cause", context.Cause(ctx), "error", err)
				poolConn.Release()
				return
			}
			// If the underlying connection is closed, attempt to re-acquire a new one
			if poolConn.Conn().IsClosed() {
				k.logger.Debug("Notification listener connection closed. re-acquiring")
				poolConn.Release()
				for {
					if ctx.Err() != nil {
						k.logger.Debug("Notification listener exiting (context canceled)", "cause", context.Cause(ctx), "error", err)
						return
					}
					poolConn, err = acquire(ctx)
					if err == nil {
						retryAttempt = 0
						break
					}
					k.logger.Debug("failed to re-acquire connection for notification listener", "error", err)
					time.Sleep(backoffWithJitter(retryAttempt))
					retryAttempt++
				}
				// The connection is re-aquired. Signal to all waiters they should poll the database for a potentially missed value.
				k.workflowNotificationRepollMap.Range(func(key, value any) bool {
					repollChannel := value.(chan struct{})
					repollChannel <- struct{}{}
					return true
				})
				k.workflowEventsRepollMap.Range(func(key, value any) bool {
					repollChannel := value.(chan struct{})
					repollChannel <- struct{}{}
					return true
				})
				continue
			}
			// Other transient errors. Backoff and continue on same conn
			k.logger.Error("Error waiting for notification", "error", err)
			time.Sleep(backoffWithJitter(retryAttempt))
			retryAttempt++
			continue
		}

		// Success: reduce backoff pressure
		if retryAttempt > 0 {
			retryAttempt--
		}

		switch n.Channel {
		case _DBOS_NOTIFICATIONS_CHANNEL:
			if cond, ok := k.workflowNotificationsMap.Load(n.Payload); ok {
				cond.(*sync.Cond).L.Lock()
				cond.(*sync.Cond).Broadcast()
				cond.(*sync.Cond).L.Unlock()
			}
		case _DBOS_WORKFLOW_EVENTS_CHANNEL:
			if cond, ok := k.workflowEventsMap.Load(n.Payload); ok {
				cond.(*sync.Cond).L.Lock()
				cond.(*sync.Cond).Broadcast()
				cond.(*sync.Cond).L.Unlock()
			}
		}
	}
}

func (k *Kernel) notificationPollerLoop(ctx context.Context) {
	defer func() {
		k.logger.Debug("Notification poller loop exiting")
		k.notificationLoopDone <- struct{}{}
	}()

	k.logger.Debug("DBOS: Starting notification poller loop")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			k.logger.Debug("Notification poller exiting (context canceled)", "cause", context.Cause(ctx))
			return
		case <-ticker.C:
			k.pollNotifications(ctx)
			k.pollEvents(ctx)
		}
	}
}

func (k *Kernel) pollNotifications(ctx context.Context) {
	// Iterate through all registered notification payloads
	k.workflowNotificationsMap.Range(func(key, value any) bool {
		payload, ok := key.(string)
		if !ok {
			return true // Continue to next item
		}

		// Parse payload: format is "destinationID::topic"
		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			k.logger.Warn("Invalid notification payload format", "payload", payload)
			return true // Continue to next item
		}

		destinationID := parts[0]
		topic := parts[1]

		// Query database to check if an unconsumed notification exists
		exists, err := k.queries.HasUnconsumedMessage(ctx, db.HasUnconsumedMessageParams{
			DestinationUuid: destinationID,
			Topic:           topic,
		})
		if err != nil {
			k.logger.Warn("Failed to poll notification", "payload", payload, "error", err)
			return true // Continue to next item
		}

		// If notification exists, signal the condition variable
		if exists {
			if cond, ok := value.(*sync.Cond); ok {
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
			}
		}

		return true // Continue to next item
	})
}

func (k *Kernel) pollEvents(ctx context.Context) {
	// Iterate through all registered event payloads
	k.workflowEventsMap.Range(func(key, value any) bool {
		payload, ok := key.(string)
		if !ok {
			return true // Continue to next item
		}

		// Parse payload: format is "targetWorkflowID::key"
		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			k.logger.Warn("Invalid event payload format", "payload", payload)
			return true // Continue to next item
		}

		targetWorkflowID := parts[0]
		eventKey := parts[1]

		// Query database to check if event exists
		exists, err := k.queries.HasWorkflowEvent(ctx, db.HasWorkflowEventParams{
			WorkflowUuid: targetWorkflowID,
			Key:          eventKey,
		})
		if err != nil {
			k.logger.Warn("Failed to poll event", "payload", payload, "error", err)
			return true // Continue to next item
		}

		// If event exists, signal the condition variable
		if exists {
			if cond, ok := value.(*sync.Cond); ok {
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
			}
		}

		return true // Continue to next item
	})
}

const _DBOS_NULL_TOPIC = "__null__topic__"

type WorkflowSendInput struct {
	DestinationID string
	Message       any
	Topic         string
	tx            Tx
	serialization string
}

// Send is a special type of step that sends a message to another workflow.
// Can be called both within a workflow (as a step) or outside a workflow (directly).
// When called within a workflow: durability and the function run in the same transaction, and we forbid nested step execution
func (k *Kernel) send(ctx context.Context, input WorkflowSendInput) error {
	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// Set default topic if not provided
	topic := _DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	err := k.q(input.tx).InsertNotification(ctx, db.InsertNotificationParams{
		DestinationUuid:  input.DestinationID,
		Topic:            topic,
		Message:          *(input.Message.(*string)),
		Serialization:    input.serialization,
		MessageUuid:      uuid.NewString(),
		CreatedAtEpochMs: time.Now().UnixMilli(),
	})
	if err != nil {
		k.logger.Error("failed to insert notification", "error", err, "destination_id", input.DestinationID, "topic", topic, "message", input.Message)
		// Check for foreign key violation (destination workflow doesn't exist)
		if isForeignKeyViolation(err) {
			return newNonExistentWorkflowError(input.DestinationID)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}
	return nil
}

// Recv is a special type of step that receives a message destined for a given workflow
func (k *Kernel) recv(ctx context.Context, input recvInput) (*recvResult, error) {
	functionName := "DBOS.recv"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", functionName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	stepID := wfState.nextStepID()
	sleepStepID := wfState.nextStepID() // We will use a sleep step to implement the timeout
	destinationID := wfState.workflowID

	// Set default topic if not provided
	topic := _DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: destinationID,
		stepID:     stepID,
		stepName:   functionName,
	}
	recordedResult, err := k.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return nil, err
	}
	if recordedResult != nil {
		var recvErr error
		if recordedResult.errStr != nil {
			recvErr = errors.New(*recordedResult.errStr)
		}
		return &recvResult{message: recordedResult.output, serialization: recordedResult.serialization}, recvErr
	}

	// First check if there's already a receiver for this workflow/topic to avoid unnecessary database load
	payload := fmt.Sprintf("%s::%s", destinationID, topic)
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	_, loaded := k.workflowNotificationsMap.LoadOrStore(payload, cond)
	if loaded {
		cond.L.Unlock()
		k.logger.Error("Receive already called for workflow", "destination_id", destinationID)
		return nil, newWorkflowConflictIDError(destinationID)
	}
	repollChannel := make(chan struct{}, 1)
	k.workflowNotificationRepollMap.LoadOrStore(payload, repollChannel)
	defer func() {
		// Clean up the condition variable after we're done and broadcast to wake up any waiting goroutines
		cond.Broadcast()
		k.workflowNotificationsMap.Delete(payload)
		k.workflowNotificationRepollMap.Delete(payload)
	}()

	// Now check if there is already an unconsumed message available in the database.
	// If not, we'll wait for a notification and timeout
	hasMsgParams := db.HasUnconsumedMessageParams{DestinationUuid: destinationID, Topic: topic}
	exists, err := k.queries.HasUnconsumedMessage(ctx, hasMsgParams)
	if err != nil {
		cond.L.Unlock()
		return nil, fmt.Errorf("failed to check message: %w", err)
	}
	var timeoutOccurred bool

	// Create the waiting goroutine once (only if !exists, so we don't attempt to unlock twice)
	done := make(chan struct{})
	if !exists {
		go func() {
			// This is the only place we unlock the condition variable if the value did not exist
			// Because of the deferred Broadcast, we'll eventually hit this before returning
			defer cond.L.Unlock()
			cond.Wait()
			close(done)
		}()
	} else {
		cond.L.Unlock()
	}

loop:
	for !exists {
		timeout, err := k.sleep(ctx, sleepInput{
			duration:  input.Timeout,
			skipSleep: true,
			stepID:    &sleepStepID,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to sleep before recv timeout: %w", err)
		}

		select {
		case <-done:
			break loop
		case <-time.After(timeout):
			timeoutOccurred = true
			k.logger.Warn("Recv() timeout reached", "payload", payload, "timeout", input.Timeout)
			break loop
		case <-repollChannel:
			k.logger.Warn("Receive polling after repoll channel signal", "payload", payload)
			// We were instructed to poll again because the connection was disconnected
			exists, err = k.queries.HasUnconsumedMessage(ctx, hasMsgParams)
			if err != nil {
				return nil, fmt.Errorf("failed to check message: %w", err)
			}
			// Restart at the beginning of the loop. If the value was found, we'll exit the loop and process to consuming the value.
			continue
		case <-ctx.Done():
			k.logger.Warn("Recv() context cancelled", "payload", payload, "cause", context.Cause(ctx))
			return nil, ctx.Err()
		}
	}

	// Capture start time before finding and consuming the message
	startTime := time.Now()

	// Find the oldest unconsumed message and atomically mark it consumed.
	// Notifications are retained (consumed=true) so they remain visible for observability;
	// rows are eventually cleaned up by FK cascade when the parent workflow is garbage-collected.
	tx, err := k.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	// Use message_uuid so we update exactly one row; created_at_epoch_ms can match multiple rows when inserts occur in the same millisecond.
	var messageString *string
	var msgSerialization *string
	consumed, err := k.queries.WithTx(PgxTx(tx)).ConsumeOldestMessage(ctx, db.ConsumeOldestMessageParams{
		DestinationUuid: destinationID,
		Topic:           topic,
	})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to consume message: %w", err)
		}
	} else {
		messageString = &consumed.Message
		msgSerialization = consumed.Serialization
	}

	// Use the sender's serialization from the notification; fall back to receiver's format for timeout/no-message case
	serialization := input.serialization
	if msgSerialization != nil && len(*msgSerialization) > 0 {
		serialization = *msgSerialization
	}

	// Record the operation result (with encoded message string)
	completedTime := time.Now()
	recordInput := recordOperationResultDBInput{
		workflowID:    destinationID,
		stepID:        stepID,
		stepName:      functionName,
		output:        messageString,
		tx:            tx,
		startedAt:     startTime,
		completedAt:   completedTime,
		serialization: serialization,
	}

	// Record an error if no message found and timeout occurred
	var timeoutErr error
	if timeoutOccurred && messageString == nil {
		timeoutErr = newTimeoutError(destinationID, functionName, fmt.Sprintf("no message received within %v", input.Timeout))
		s := timeoutErr.Error()
		recordInput.errStr = &s
	}

	err = k.recordOperationResult(ctx, recordInput)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return the message and its serialization format
	return &recvResult{message: messageString, serialization: serialization}, timeoutErr
}

type WorkflowSetEventInput struct {
	Key           string
	Message       any
	tx            Tx
	serialization string
}

func (k *Kernel) setEvent(ctx context.Context, input WorkflowSetEventInput) error {
	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return newStepExecutionError("", "DBOS.setEvent", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// input.Message is already encoded *string from the typed layer
	value := *(input.Message.(*string))
	q := k.q(input.tx)

	// Insert or update the event using UPSERT
	if err := q.UpsertWorkflowEvent(ctx, db.UpsertWorkflowEventParams{
		WorkflowUuid:  wfState.workflowID,
		Key:           input.Key,
		Value:         value,
		Serialization: input.serialization,
	}); err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Record event in workflow_events_history
	return q.InsertWorkflowEventHistory(ctx, db.InsertWorkflowEventHistoryParams{
		WorkflowUuid:  wfState.workflowID,
		FunctionID:    int32(wfState.stepID),
		Key:           input.Key,
		Value:         value,
		Serialization: input.serialization,
	})
}

func (k *Kernel) getEvent(ctx context.Context, input getEventInput) (*getEventResult, error) {
	functionName := "DBOS.getEvent"

	// Get workflow state from context (optional for GetEvent as we can get an event from outside a workflow)
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	var stepID int
	var sleepStepID int
	var isInWorkflow bool

	startTime := time.Now()
	if ok && wfState != nil {
		isInWorkflow = true
		if wfState.isWithinStep {
			return nil, newStepExecutionError(wfState.workflowID, functionName, fmt.Errorf("cannot call GetEvent within a step"))
		}
		stepID = wfState.nextStepID()
		sleepStepID = wfState.nextStepID() // We will use a sleep step to implement the timeout

		// Check if operation was already executed (only if in workflow)
		checkInput := checkOperationExecutionDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
		}
		recordedResult, err := k.checkOperationExecution(ctx, checkInput)
		if err != nil {
			return nil, err
		}
		if recordedResult != nil {
			var evtErr error
			if recordedResult.errStr != nil {
				evtErr = errors.New(*recordedResult.errStr)
			}
			return &getEventResult{value: recordedResult.output, serialization: recordedResult.serialization}, evtErr
		}
	}

	// Create notification payload and condition variable
	payload := fmt.Sprintf("%s::%s", input.TargetWorkflowID, input.Key)
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	existingCond, loaded := k.workflowEventsMap.LoadOrStore(payload, cond)
	if loaded {
		cond.L.Unlock()
		// Reuse the existing condition variable
		cond = existingCond.(*sync.Cond)
	}
	repollChannel := make(chan struct{}, 1)
	k.workflowEventsRepollMap.LoadOrStore(payload, repollChannel)

	// Defer broadcast to ensure any waiting goroutines eventually unlock
	defer func() {
		cond.Broadcast()
		// Clean up the condition variable after we're done (Delete is a no-op if the key doesn't exist)
		k.workflowEventsMap.Delete(payload)
		k.workflowEventsRepollMap.Delete(payload)
	}()

	// Check if the event already exists in the database
	var valueString *string
	var evtSerialization *string
	var err error

	// Helper function to query the event and handle errors
	queryEvent := func() error {
		row, qerr := k.queries.GetWorkflowEvent(ctx, db.GetWorkflowEventParams{
			WorkflowUuid: input.TargetWorkflowID,
			Key:          input.Key,
		})
		if qerr != nil {
			if !errors.Is(qerr, pgx.ErrNoRows) {
				if !loaded {
					cond.L.Unlock()
				}
				return fmt.Errorf("failed to query workflow event: %w", qerr)
			}
			valueString = nil
			return nil
		}
		valueString = &row.Value
		evtSerialization = row.Serialization
		return nil
	}

	if err := queryEvent(); err != nil {
		return nil, err
	}

	var timeoutOccurred bool
	if valueString == nil {
		// Start a goroutine to wait for the event to be set
		// This goroutine is responsible for unlocking the CV, which will happen whenever the event is set (through either the deferred Broadcast or from the notification listener)
		done := make(chan struct{})
		go func() {
			if !loaded {
				defer cond.L.Unlock()
			}
			cond.Wait()
			close(done)
		}()

	loop:
		for valueString == nil {
			// Wait for notification with timeout using condition variable
			timeout := input.Timeout
			if isInWorkflow {
				timeout, err = k.sleep(ctx, sleepInput{
					duration:  input.Timeout,
					skipSleep: true,
					stepID:    &sleepStepID,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to sleep before getEvent timeout: %w", err)
				}
			}

			select {
			case <-done:
				// Received notification
				if err := queryEvent(); err != nil {
					return nil, err
				}
				break loop
			case <-time.After(timeout):
				timeoutOccurred = true
				k.logger.Warn("GetEvent() timeout reached", "target_workflow_id", input.TargetWorkflowID, "key", input.Key, "timeout", input.Timeout)
				// Check if the event exists in the database -- we never know
				if err := queryEvent(); err != nil {
					return nil, err
				}
				break loop
			case <-repollChannel:
				// We were instructed to poll again because the connection was disconnected
				if err := queryEvent(); err != nil {
					return nil, err
				}
				// Restart at the beginning of the loop.
				// If the value was found, we'll exit the loop
				continue
			case <-ctx.Done():
				k.logger.Warn("GetEvent() context cancelled", "target_workflow_id", input.TargetWorkflowID, "key", input.Key, "cause", context.Cause(ctx))
				if !loaded {
					cond.L.Unlock()
				}
				return nil, ctx.Err()
			}
		}
	} else {
		if !loaded {
			cond.L.Unlock()
		}
	}

	// Use the event's serialization from the DB; fall back to caller's format for timeout/no-event case
	serialization := input.serialization
	if evtSerialization != nil && len(*evtSerialization) > 0 {
		serialization = *evtSerialization
	}

	// Record the operation result if this is called within a workflow
	var timeoutErr error
	if isInWorkflow {
		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowID:    wfState.workflowID,
			stepID:        stepID,
			stepName:      functionName,
			output:        valueString,
			startedAt:     startTime,
			completedAt:   completedTime,
			serialization: serialization,
		}

		// Record an error if no event found and timeout occurred
		if timeoutOccurred && valueString == nil {
			timeoutErr = newTimeoutError(wfState.workflowID, functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
			s := timeoutErr.Error()
			recordInput.errStr = &s
		}

		err = k.recordOperationResult(ctx, recordInput)
		if err != nil {
			return nil, err
		}
	} else {
		// If not in workflow and timeout occurred with no event found, return error
		if timeoutOccurred && valueString == nil {
			timeoutErr = newTimeoutError("", functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
		}
	}

	// Return the event value and its serialization format
	return &getEventResult{value: valueString, serialization: serialization}, timeoutErr
}

/*******************************/
/******* STREAMS ********/
/*******************************/

type writeStreamDBInput struct {
	Key           string
	Value         *string // Already serialized
	tx            Tx
	serialization string
}

type readStreamDBInput struct {
	WorkflowID string
	Key        string
	FromOffset int
}

type streamEntry struct {
	Value         string
	Offset        int
	Serialization string
}

func (k *Kernel) writeStream(ctx context.Context, input writeStreamDBInput) error {
	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return fmt.Errorf("workflow state not found in context: are you running this within a workflow?")
	}

	q := k.q(input.tx)

	exists, err := q.CheckStreamClosed(ctx, db.CheckStreamClosedParams{
		WorkflowUuid: wfState.workflowID,
		Key:          input.Key,
		Value:        _DBOS_STREAM_CLOSED_SENTINEL,
	})
	if err == nil && exists == 1 {
		return fmt.Errorf("stream '%s' is already closed", input.Key)
	} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("failed to check stream status: %w", err)
	}

	if err := q.InsertStreamEntry(ctx, db.InsertStreamEntryParams{
		WorkflowUuid:  wfState.workflowID,
		Key:           input.Key,
		Value:         *input.Value,
		FunctionID:    int32(wfState.stepID),
		Serialization: input.serialization,
	}); err != nil {
		return fmt.Errorf("failed to insert stream entry: %w", err)
	}

	return nil
}

// readStream reads stream entries starting from a given offset.
// Returns the entries, whether the stream is closed, and any error.
func (k *Kernel) readStream(ctx context.Context, input readStreamDBInput) ([]streamEntry, bool, error) {
	rows, err := k.queries.ReadStream(ctx, db.ReadStreamParams{
		WorkflowUuid: input.WorkflowID,
		Key:          input.Key,
		Offset:       int32(input.FromOffset),
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to query stream: %w", err)
	}

	var entries []streamEntry
	closed := false
	for _, r := range rows {
		if r.Value == _DBOS_STREAM_CLOSED_SENTINEL {
			closed = true
			break
		}
		var ser string
		if r.Serialization != nil {
			ser = *r.Serialization
		}
		entries = append(entries, streamEntry{
			Value:         r.Value,
			Offset:        int(r.Offset),
			Serialization: ser,
		})
	}

	return entries, closed, nil
}

// eventRecord is one row from the workflow_events table.
type eventRecord struct {
	Key           string
	Value         string
	Serialization string
}

// getAllEvents returns every event row currently set on the workflow.
func (k *Kernel) getAllEvents(ctx context.Context, workflowID string) ([]eventRecord, error) {
	rows, err := k.queries.GetAllEvents(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow events: %w", err)
	}
	events := make([]eventRecord, 0, len(rows))
	for _, r := range rows {
		rec := eventRecord{Key: r.Key, Value: r.Value}
		if r.Serialization != nil {
			rec.Serialization = *r.Serialization
		}
		events = append(events, rec)
	}
	return events, nil
}

// notificationRecord is one row from the notifications table.
// Topic is nil when the row stored the __null__topic__ sentinel.
type notificationRecord struct {
	Topic            *string
	Message          string
	Serialization    string
	CreatedAtEpochMs int64
	Consumed         bool
}

// getAllNotifications returns every notification sent to the workflow, ordered by arrival time.
// The __null__topic__ sentinel is normalized back to a nil Topic.
func (k *Kernel) getAllNotifications(ctx context.Context, workflowID string) ([]notificationRecord, error) {
	rows, err := k.queries.GetAllNotifications(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query notifications: %w", err)
	}
	results := make([]notificationRecord, 0, len(rows))
	for _, r := range rows {
		rec := notificationRecord{
			Topic:            r.Topic,
			Message:          r.Message,
			CreatedAtEpochMs: r.CreatedAtEpochMs,
			Consumed:         r.Consumed,
		}
		if rec.Topic != nil && *rec.Topic == _DBOS_NULL_TOPIC {
			rec.Topic = nil
		}
		if r.Serialization != nil {
			rec.Serialization = *r.Serialization
		}
		results = append(results, rec)
	}
	return results, nil
}

// streamRecord is one row from the streams table; rows holding the closed sentinel are filtered out.
type streamRecord struct {
	Key           string
	Value         string
	Serialization string
}

// getAllStreamEntries returns every stream entry for the workflow, ordered by (key, offset).
// Rows holding the stream-closed sentinel are filtered out; callers may group by Key.
func (k *Kernel) getAllStreamEntries(ctx context.Context, workflowID string) ([]streamRecord, error) {
	rows, err := k.queries.GetAllStreamEntries(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query streams: %w", err)
	}
	records := make([]streamRecord, 0, len(rows))
	for _, r := range rows {
		if r.Value == _DBOS_STREAM_CLOSED_SENTINEL {
			continue
		}
		rec := streamRecord{Key: r.Key, Value: r.Value}
		if r.Serialization != nil {
			rec.Serialization = *r.Serialization
		}
		records = append(records, rec)
	}
	return records, nil
}

/*******************************/
/******* QUEUES ********/
/*******************************/

type setWorkflowDelayDBInput struct {
	workflowID string
	delayUntil time.Time
	tx         Tx
}

// setWorkflowDelay updates the delay on a DELAYED workflow.
func (k *Kernel) setWorkflowDelay(ctx context.Context, input setWorkflowDelayDBInput) error {
	if err := k.q(input.tx).SetWorkflowDelay(ctx, db.SetWorkflowDelayParams{
		DelayUntil:   input.delayUntil.UnixMilli(),
		UpdatedAt:    time.Now().UnixMilli(),
		WorkflowUuid: input.workflowID,
		Status:       string(WorkflowStatusDelayed),
	}); err != nil {
		return fmt.Errorf("failed to set workflow delay: %w", err)
	}
	return nil
}

// transitionDelayedWorkflows transitions DELAYED workflows whose delay has expired to ENQUEUED.
func (k *Kernel) transitionDelayedWorkflows(ctx context.Context) error {
	if err := k.queries.TransitionDelayedWorkflows(ctx, db.TransitionDelayedWorkflowsParams{
		NewStatus: string(WorkflowStatusEnqueued),
		OldStatus: string(WorkflowStatusDelayed),
		NowMs:     time.Now().UnixMilli(),
	}); err != nil {
		return fmt.Errorf("failed to transition delayed workflows: %w", err)
	}
	return nil
}

type dequeuedWorkflow struct {
	id            string
	name          string
	input         *string
	serialization string
}

func (k *Kernel) upsertWorkflowDefinition(ctx context.Context, workflowName string, concurrency *int, rl *rateLimiter, retention time.Duration) error {
	var globalConcurrency, rateLimit *int32
	var ratePeriodMs *int64
	if concurrency != nil {
		v := int32(*concurrency)
		globalConcurrency = &v
	}
	if rl != nil {
		lim := int32(rl.limit)
		rateLimit = &lim
		per := rl.period.Milliseconds()
		ratePeriodMs = &per
	}
	return k.queries.UpsertWorkflowDefinition(ctx, db.UpsertWorkflowDefinitionParams{
		WorkflowName:        workflowName,
		GlobalConcurrency:   globalConcurrency,
		RateLimit:           rateLimit,
		RatePeriodMs:        ratePeriodMs,
		WorkflowRetentionMs: retention.Milliseconds(),
	})
}

type dequeueWorkflowsInput struct {
	workflowName       string
	executorID         string
	applicationVersion string
}

func (k *Kernel) dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error) {
	var policyConcurrency *int
	var policyRateLimit *rateLimiter

	def, err := k.queries.GetWorkflowDefinition(ctx, input.workflowName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load workflow definition %s: %w", input.workflowName, err)
	}
	if def.GlobalConcurrency != nil {
		value := int(*def.GlobalConcurrency)
		policyConcurrency = &value
	}
	if def.RateLimit != nil && def.RatePeriodMs != nil {
		policyRateLimit = &rateLimiter{limit: int(*def.RateLimit), period: time.Duration(*def.RatePeriodMs) * time.Millisecond}
	}

	// Snapshot isolation when a concurrency/rate policy applies, else read committed.
	iso := IsoLevelReadCommitted
	if policyConcurrency != nil || policyRateLimit != nil {
		iso = IsoLevelRepeatableRead
	}
	tx, err := k.pool.BeginTx(ctx, TxOptions{IsoLevel: iso})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	txq := k.queries.WithTx(PgxTx(tx))

	// Rate limiter: count workflows started within the limiter period.
	var numRecentQueries int64
	if policyRateLimit != nil {
		cutoffTimeMs := time.Now().Add(-policyRateLimit.period).UnixMilli()
		numRecentQueries, err = txq.CountRateLimitedWorkflows(ctx, db.CountRateLimitedWorkflowsParams{
			Name:           input.workflowName,
			EnqueuedStatus: string(WorkflowStatusEnqueued),
			DelayedStatus:  string(WorkflowStatusDelayed),
			CutoffMs:       cutoffTimeMs,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}
		if int(numRecentQueries) >= policyRateLimit.limit {
			return []dequeuedWorkflow{}, nil
		}
	}

	maxTasks := _DEFAULT_MAX_TASKS_PER_ITERATION
	if policyConcurrency != nil {
		globalCount, err := txq.CountPendingWorkflows(ctx, db.CountPendingWorkflowsParams{
			Name:          input.workflowName,
			PendingStatus: string(WorkflowStatusPending),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to query pending workflows: %w", err)
		}
		concurrency := *policyConcurrency
		if int(globalCount) > concurrency {
			k.logger.Warn("Total pending workflows exceeds global concurrency limit", "total_pending", globalCount, "workflow_name", input.workflowName, "concurrency_limit", concurrency)
		}
		availableTasks := max(concurrency-int(globalCount), 0)
		if availableTasks < maxTasks {
			maxTasks = availableTasks
		}
	}

	if maxTasks <= 0 {
		return nil, nil
	}

	// SKIP LOCKED when no global concurrency is set to avoid blocking, otherwise
	// NOWAIT to ensure a consistent view across processes.
	var dequeuedIDs []string
	if policyConcurrency == nil {
		dequeuedIDs, err = txq.DequeueCandidatesSkipLocked(ctx, db.DequeueCandidatesSkipLockedParams{
			Name:           input.workflowName,
			EnqueuedStatus: string(WorkflowStatusEnqueued),
			AppVersion:     input.applicationVersion,
			MaxTasks:       int32(maxTasks),
		})
	} else {
		dequeuedIDs, err = txq.DequeueCandidatesNoWait(ctx, db.DequeueCandidatesNoWaitParams{
			Name:           input.workflowName,
			EnqueuedStatus: string(WorkflowStatusEnqueued),
			AppVersion:     input.applicationVersion,
			MaxTasks:       int32(maxTasks),
		})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}

	if len(dequeuedIDs) > 0 {
		k.logger.Debug("attempting to claim workflow(s)", "workflow_name", input.workflowName, "numTasks", len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIDs {
		select {
		case <-ctx.Done():
			k.logger.Warn("DequeueWorkflows context cancelled while claiming dequeue results", "cause", context.Cause(ctx))
			return nil, ctx.Err()
		default:
		}
		if policyRateLimit != nil {
			if int64(len(retWorkflows))+numRecentQueries >= int64(policyRateLimit.limit) {
				break
			}
		}
		claimed, err := txq.DequeueClaimWorkflow(ctx, db.DequeueClaimWorkflowParams{
			PendingStatus:  string(WorkflowStatusPending),
			AppVersion:     input.applicationVersion,
			ExecutorID:     input.executorID,
			StartedAt:      time.Now().UnixMilli(),
			RateLimited:    policyRateLimit != nil,
			WorkflowUuid:   id,
			EnqueuedStatus: string(WorkflowStatusEnqueued),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("failed to update workflow %s during dequeue: %w", id, err)
		}
		retWorkflow := dequeuedWorkflow{id: id, input: claimed.Inputs}
		if claimed.Name != nil {
			retWorkflow.name = *claimed.Name
		}
		if claimed.Serialization != nil {
			retWorkflow.serialization = *claimed.Serialization
		}
		retWorkflows = append(retWorkflows, retWorkflow)
	}

	// Commit only if workflows were dequeued. Avoids WAL bloat / XID advance.
	if len(retWorkflows) > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return retWorkflows, nil
}

func (k *Kernel) clearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	n, err := k.queries.ClearQueueAssignment(ctx, db.ClearQueueAssignmentParams{
		EnqueuedStatus: string(WorkflowStatusEnqueued),
		WorkflowUuid:   workflowID,
		PendingStatus:  string(WorkflowStatusPending),
	})
	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment for workflow %s: %w", workflowID, err)
	}
	// If no rows were affected, the workflow is no longer in the queue or was already completed.
	return n > 0, nil
}

/*******************************/
/******* METRICS ********/
/*******************************/

type metricData struct {
	MetricName string  `json:"metric_name"` // step name or workflow name
	MetricType string  `json:"metric_type"` // workflow_count, step_count, etc
	Value      float64 `json:"value"`
}

func (k *Kernel) getMetrics(ctx context.Context, startTime, endTime string) ([]metricData, error) {
	// Parse ISO timestamp strings to time.Time
	startTimeParsed, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		return nil, fmt.Errorf("invalid start_time format: %w", err)
	}
	endTimeParsed, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return nil, fmt.Errorf("invalid end_time format: %w", err)
	}

	// Convert to epoch milliseconds
	startEpochMs := startTimeParsed.UnixMilli()
	endEpochMs := endTimeParsed.UnixMilli()

	var metrics []metricData

	// Query workflow metrics
	workflowMetrics, err := k.getMetricWorkflowCount(ctx, startEpochMs, endEpochMs)
	if err != nil {
		return nil, err
	}
	metrics = append(metrics, workflowMetrics...)

	// Query step metrics
	stepMetrics, err := k.getMetricStepCount(ctx, startEpochMs, endEpochMs)
	if err != nil {
		return nil, err
	}
	metrics = append(metrics, stepMetrics...)

	return metrics, nil
}

func (k *Kernel) getMetricWorkflowCount(ctx context.Context, startEpochMs, endEpochMs int64) ([]metricData, error) {
	rows, err := k.queries.GetMetricWorkflowCount(ctx, db.GetMetricWorkflowCountParams{
		StartMs: startEpochMs,
		EndMs:   endEpochMs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow metrics: %w", err)
	}
	metrics := make([]metricData, 0, len(rows))
	for _, r := range rows {
		var name string
		if r.Name != nil {
			name = *r.Name
		}
		metrics = append(metrics, metricData{
			MetricType: "workflow_count",
			MetricName: name,
			Value:      float64(r.Count),
		})
	}
	return metrics, nil
}

func (k *Kernel) getMetricStepCount(ctx context.Context, startEpochMs, endEpochMs int64) ([]metricData, error) {
	rows, err := k.queries.GetMetricStepCount(ctx, db.GetMetricStepCountParams{
		StartMs: startEpochMs,
		EndMs:   endEpochMs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query step metrics: %w", err)
	}
	metrics := make([]metricData, 0, len(rows))
	for _, r := range rows {
		metrics = append(metrics, metricData{
			MetricType: "step_count",
			MetricName: r.FunctionName,
			Value:      float64(r.Count),
		})
	}
	return metrics, nil
}

/*******************************/
/******* SCHEDULES ********/
/*******************************/

type createScheduleDBInput struct {
	ScheduleID        string
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           string // JSON serialized
	Status            ScheduleStatus
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
	tx                Tx // optional: run inside an existing transaction
}

// scheduleFromRow maps a generated workflow_schedules row to the domain type,
// applying the internal-queue default and decoding the JSON context.
func scheduleFromRow(r db.WorkflowSchedule) WorkflowSchedule {
	sc := WorkflowSchedule{
		ScheduleID:        r.ScheduleID,
		ScheduleName:      r.ScheduleName,
		WorkflowName:      r.WorkflowName,
		Schedule:          r.Schedule,
		Status:            ScheduleStatus(r.Status),
		AutomaticBackfill: r.AutomaticBackfill,
	}
	if r.QueueName != nil {
		sc.QueueName = *r.QueueName
	} else {
		sc.QueueName = _DBOS_INTERNAL_QUEUE_NAME
	}
	if r.WorkflowClassName != nil {
		sc.WorkflowClassName = *r.WorkflowClassName
	}
	if r.CronTimezone != nil {
		sc.CronTimezone = *r.CronTimezone
	}
	if r.LastFiredAt != nil {
		if t, err := time.Parse(time.RFC3339Nano, *r.LastFiredAt); err == nil {
			sc.LastFiredAt = &t
		} else if t, err := time.Parse(time.RFC3339, *r.LastFiredAt); err == nil {
			sc.LastFiredAt = &t
		}
	}
	if err := json.Unmarshal([]byte(r.Context), &sc.Context); err != nil {
		sc.Context = r.Context
	}
	return sc
}

func (k *Kernel) createSchedule(ctx context.Context, input createScheduleDBInput) error {
	var workflowClassName, queueName *string
	if input.WorkflowClassName != "" {
		workflowClassName = &input.WorkflowClassName
	}
	if input.QueueName != "" {
		queueName = &input.QueueName
	}
	if err := k.q(input.tx).CreateSchedule(ctx, db.CreateScheduleParams{
		ScheduleID:        input.ScheduleID,
		ScheduleName:      input.ScheduleName,
		WorkflowName:      input.WorkflowName,
		WorkflowClassName: workflowClassName,
		Schedule:          input.Schedule,
		Context:           input.Context,
		Status:            string(input.Status),
		AutomaticBackfill: input.AutomaticBackfill,
		CronTimezone:      input.CronTimezone,
		QueueName:         queueName,
	}); err != nil {
		return fmt.Errorf("failed to create schedule: %w", err)
	}
	return nil
}

type listSchedulesDBInput struct {
	Statuses             []ScheduleStatus
	WorkflowNames        []string
	ScheduleNamePrefixes []string
	tx                   Tx // optional: run inside an existing transaction
}

func (k *Kernel) listSchedules(ctx context.Context, input listSchedulesDBInput) ([]WorkflowSchedule, error) {
	statuses := make([]string, len(input.Statuses))
	for i, st := range input.Statuses {
		statuses[i] = string(st)
	}
	patterns := make([]string, len(input.ScheduleNamePrefixes))
	for i, p := range input.ScheduleNamePrefixes {
		patterns[i] = p + "%"
	}

	rows, err := k.q(input.tx).ListSchedules(ctx, db.ListSchedulesParams{
		FilterStatuses:         len(statuses) > 0,
		Statuses:               statuses,
		FilterWorkflowNames:    len(input.WorkflowNames) > 0,
		WorkflowNames:          input.WorkflowNames,
		FilterSchedulePrefixes: len(patterns) > 0,
		SchedulePatterns:       patterns,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}

	schedules := make([]WorkflowSchedule, 0, len(rows))
	for _, r := range rows {
		schedules = append(schedules, scheduleFromRow(r))
	}
	return schedules, nil
}

type updateScheduleDBInput struct {
	ScheduleName string
	Status       ScheduleStatus
	LastFiredAt  *time.Time
	tx           Tx // optional: run inside an existing transaction
}

func (k *Kernel) updateSchedule(ctx context.Context, input updateScheduleDBInput) error {
	var lastFiredAt *string
	if input.LastFiredAt != nil {
		v := input.LastFiredAt.Format(time.RFC3339Nano)
		lastFiredAt = &v
	}
	if err := k.q(input.tx).UpdateSchedule(ctx, db.UpdateScheduleParams{
		Status:       string(input.Status),
		LastFiredAt:  lastFiredAt,
		ScheduleName: input.ScheduleName,
	}); err != nil {
		return fmt.Errorf("failed to update schedule: %w", err)
	}
	return nil
}

func (k *Kernel) updateScheduleLastFiredAt(ctx context.Context, scheduleName string, lastFiredAt time.Time) error {
	if err := k.queries.UpdateScheduleLastFiredAt(ctx, db.UpdateScheduleLastFiredAtParams{
		LastFiredAt:  lastFiredAt.Format(time.RFC3339Nano),
		ScheduleName: scheduleName,
	}); err != nil {
		return fmt.Errorf("failed to update schedule last_fired_at: %w", err)
	}
	return nil
}

type deleteScheduleDBInput struct {
	ScheduleName string
	tx           Tx // optional: run inside an existing transaction
}

func (k *Kernel) deleteSchedule(ctx context.Context, input deleteScheduleDBInput) error {
	if err := k.q(input.tx).DeleteSchedule(ctx, input.ScheduleName); err != nil {
		return fmt.Errorf("failed to delete schedule: %w", err)
	}
	return nil
}

type backfillScheduleDBInput struct {
	ScheduleName string
	Schedule     string
	StartTime    time.Time
	EndTime      time.Time
}

func (k *Kernel) backfillSchedule(ctx context.Context, input backfillScheduleDBInput) ([]string, error) {
	schedules, err := k.listSchedules(ctx, listSchedulesDBInput{ScheduleNamePrefixes: []string{input.ScheduleName}})
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}
	var schedule *WorkflowSchedule
	for i := range schedules {
		if schedules[i].ScheduleName == input.ScheduleName {
			schedule = &schedules[i]
			break
		}
	}
	if schedule == nil {
		return nil, fmt.Errorf("schedule not found: %s", input.ScheduleName)
	}

	spec := input.Schedule
	if schedule.CronTimezone != "" {
		spec = "CRON_TZ=" + schedule.CronTimezone + " " + spec
	}

	scheduleEntry, err := newScheduleCronParser().Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cron schedule: %w", err)
	}

	queueName := _DBOS_INTERNAL_QUEUE_NAME
	if schedule.QueueName != "" {
		queueName = schedule.QueueName
	}

	ser := resolveEncoder(ctx)

	// Backfilled workflows always run against the latest registered application
	// version. If lookup fails (e.g. no versions registered yet) leave it unset.
	var backfillAppVersion string
	backfillLatest, err := retryWithResult(ctx, func() (*VersionInfo, error) {
		return k.getLatestApplicationVersion(ctx)
	}, withRetrierLogger(k.logger))
	if err != nil {
		k.logger.Error("failed to fetch latest application version for schedule backfill", "schedule", input.ScheduleName, "error", err)
	} else if backfillLatest != nil {
		backfillAppVersion = backfillLatest.Name
	}

	tx, err := k.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	txq := k.queries.WithTx(PgxTx(tx))

	nextTime := scheduleEntry.Next(input.StartTime)
	now := time.Now()
	var workflowIDs []string

	for nextTime.Before(input.EndTime) {
		workflowID := fmt.Sprintf("sched-%s-%s", input.ScheduleName, nextTime.Format(time.RFC3339))
		workflowIDs = append(workflowIDs, workflowID)

		_, err := txq.WorkflowExists(ctx, workflowID)
		if err == nil {
			nextTime = scheduleEntry.Next(nextTime)
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to check workflow existence for %s: %w", workflowID, err)
		}

		encodedInput, encErr := ser.Encode(ScheduledWorkflowInput{
			ScheduledTime: nextTime,
			Context:       schedule.Context,
		})
		if encErr != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input for %s: %w", workflowID, encErr)
		}

		status := WorkflowStatus{
			ID:                 workflowID,
			Status:             WorkflowStatusEnqueued,
			Name:               schedule.WorkflowName,
			ClassName:          schedule.WorkflowClassName,
			QueueName:          queueName,
			CreatedAt:          now,
			Input:              encodedInput,
			Serialization:      ser.Name(),
			ApplicationVersion: backfillAppVersion,
		}
		if _, err := k.insertWorkflowStatus(ctx, insertWorkflowStatusDBInput{status: status, tx: tx}); err != nil {
			return nil, fmt.Errorf("failed to enqueue backfill workflow %s: %w", workflowID, err)
		}

		nextTime = scheduleEntry.Next(nextTime)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit backfill transaction: %w", err)
	}
	return workflowIDs, nil
}

// triggerSchedule immediately enqueues the named schedule's workflow at the
// current time, using the schedule's queue (or the internal queue by default)
// and preserving its workflow_class_name and context. Returns the workflow ID.
func (k *Kernel) triggerSchedule(ctx context.Context, scheduleName string) (string, error) {
	if scheduleName == "" {
		return "", errors.New("schedule_name is required")
	}

	tx, err := k.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	schedules, err := k.listSchedules(ctx, listSchedulesDBInput{
		ScheduleNamePrefixes: []string{scheduleName},
		tx:                   tx,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get schedule: %w", err)
	}
	var schedule *WorkflowSchedule
	for i := range schedules {
		if schedules[i].ScheduleName == scheduleName {
			schedule = &schedules[i]
			break
		}
	}
	if schedule == nil {
		return "", fmt.Errorf("schedule not found: %s", scheduleName)
	}

	queueName := schedule.QueueName
	if queueName == "" {
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	now := time.Now()
	workflowID := fmt.Sprintf("sched-%s-trigger-%s", scheduleName, now.Format(time.RFC3339Nano))

	ser := resolveEncoder(ctx)
	encodedInput, err := ser.Encode(ScheduledWorkflowInput{
		ScheduledTime: now,
		Context:       schedule.Context,
	})
	if err != nil {
		return "", fmt.Errorf("failed to encode scheduled workflow input: %w", err)
	}

	// Triggered scheduled workflows run against the latest registered application
	// version. If lookup fails (e.g. no versions registered yet) leave it unset.
	var triggerAppVersion string
	triggerLatest, err := retryWithResult(ctx, func() (*VersionInfo, error) {
		return k.getLatestApplicationVersion(ctx)
	}, withRetrierLogger(k.logger))
	if err != nil {
		k.logger.Error("failed to fetch latest application version for schedule trigger", "schedule", scheduleName, "error", err)
	} else if triggerLatest != nil {
		triggerAppVersion = triggerLatest.Name
	}

	status := WorkflowStatus{
		ID:                 workflowID,
		Status:             WorkflowStatusEnqueued,
		Name:               schedule.WorkflowName,
		ClassName:          schedule.WorkflowClassName,
		QueueName:          queueName,
		CreatedAt:          now,
		Input:              encodedInput,
		Serialization:      ser.Name(),
		ApplicationVersion: triggerAppVersion,
	}

	if _, err := k.insertWorkflowStatus(ctx, insertWorkflowStatusDBInput{status: status, tx: tx}); err != nil {
		return "", fmt.Errorf("failed to enqueue triggered workflow: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return workflowID, nil
}

/*******************************/
/******* APPLICATION VERSIONS **/
/*******************************/

// VersionInfo describes a registered application version.
type VersionInfo struct {
	ID        string `json:"version_id"`
	Name      string `json:"version_name"`
	Timestamp int64  `json:"version_timestamp"` // epoch milliseconds
	CreatedAt int64  `json:"created_at"`        // epoch milliseconds
}

func (k *Kernel) createApplicationVersion(ctx context.Context, versionName string) error {
	nowMs := time.Now().UnixMilli()
	if err := k.queries.CreateApplicationVersion(ctx, db.CreateApplicationVersionParams{
		VersionID:        uuid.New().String(),
		VersionName:      versionName,
		VersionTimestamp: nowMs,
		CreatedAt:        nowMs,
	}); err != nil {
		return fmt.Errorf("failed to create application version: %w", err)
	}
	return nil
}

func (k *Kernel) updateApplicationVersionTimestamp(ctx context.Context, versionName string, newTimestamp int64) error {
	if err := k.queries.UpdateApplicationVersionTimestamp(ctx, db.UpdateApplicationVersionTimestampParams{
		VersionTimestamp: newTimestamp,
		VersionName:      versionName,
	}); err != nil {
		return fmt.Errorf("failed to update application version timestamp: %w", err)
	}
	return nil
}

func (k *Kernel) listApplicationVersions(ctx context.Context) ([]VersionInfo, error) {
	rows, err := k.queries.ListApplicationVersions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list application versions: %w", err)
	}
	versions := make([]VersionInfo, 0, len(rows))
	for _, r := range rows {
		versions = append(versions, VersionInfo{
			ID:        r.VersionID,
			Name:      r.VersionName,
			Timestamp: r.VersionTimestamp,
			CreatedAt: r.CreatedAt,
		})
	}
	return versions, nil
}

func (k *Kernel) getLatestApplicationVersion(ctx context.Context) (*VersionInfo, error) {
	r, err := k.queries.GetLatestApplicationVersion(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, newNoApplicationVersionsError()
		}
		return nil, fmt.Errorf("failed to get latest application version: %w", err)
	}
	return &VersionInfo{
		ID:        r.VersionID,
		Name:      r.VersionName,
		Timestamp: r.VersionTimestamp,
		CreatedAt: r.CreatedAt,
	}, nil
}

/*******************************/
/******* UTILS ********/
/*******************************/

// dropDatabaseIfExists force-drops a PostgreSQL database.
func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
	if _, err := conn.Exec(ctx, dropSQL); err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}
	return nil
}

func (k *Kernel) resetSystemDB(ctx context.Context) error {
	// Get the current database configuration from the pool
	config := PgxPool(k.pool).Config()
	if config == nil || config.ConnConfig == nil {
		return fmt.Errorf("failed to get pool configuration")
	}

	// Extract the database name before closing the pool
	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in pool configuration")
	}

	// Close the current pool before dropping the database
	k.pool.Close()

	// Create a new connection configuration pointing to the postgres database
	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	// Connect to the postgres database
	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Drop the database using the helper function
	err = dropDatabaseIfExists(ctx, conn, dbName)
	if err != nil {
		return err
	}

	return nil
}

func backoffWithJitter(retryAttempt int) time.Duration {
	exp := float64(_DB_CONNECTION_RETRY_BASE_DELAY) * math.Pow(_DB_CONNECTION_RETRY_FACTOR, float64(retryAttempt))
	// cap backoff to max number of retries, then do a fixed time delay
	// expected retryAttempt to initially be 0, so >= used
	// cap delay to maximum of _DB_CONNECTION_MAX_DELAY milliseconds
	if retryAttempt >= _DB_CONNECTION_RETRY_MAX_RETRIES || exp > float64(_DB_CONNECTION_MAX_DELAY) {
		exp = float64(_DB_CONNECTION_MAX_DELAY)
	}

	// want randomization between +-25% of exp
	jitter := 0.75 + rand.Float64()*0.5 // #nosec G404 -- trivial use of math/rand
	return time.Duration(exp * jitter)
}

// maskPassword replaces the password in a database URL with asterisks
func maskPassword(dbURL string) (string, error) {
	parsedURL, err := url.Parse(dbURL)
	if err == nil && parsedURL.Scheme != "" {

		// Check if there is user info with a password
		if parsedURL.User != nil {
			username := parsedURL.User.Username()
			_, hasPassword := parsedURL.User.Password()
			if hasPassword {
				// Manually construct the URL with masked password to avoid encoding
				maskedURL := parsedURL.Scheme + "://" + username + ":***@" + parsedURL.Host + parsedURL.Path
				if parsedURL.RawQuery != "" {
					maskedURL += "?" + parsedURL.RawQuery
				}
				if parsedURL.Fragment != "" {
					maskedURL += "#" + parsedURL.Fragment
				}
				return maskedURL, nil
			}
		}

		return parsedURL.String(), nil
	}

	// If URL parsing failed or no scheme, try key-value format (libpq connection string)
	return maskPasswordInKeyValueFormat(dbURL), nil
}

// maskPasswordInKeyValueFormat masks password in libpq-style key-value connection strings
// Format: "user=foo password=bar database=db host=localhost"
// Supports all spacing variations: password=value, password =value, password= value, password = value
func maskPasswordInKeyValueFormat(connStr string) string {
	// Match password=value (case insensitive, handles spaces around =)
	// Pattern matches: password (case insensitive), optional spaces, =, optional spaces, then value until next space or end
	re := regexp.MustCompile(`(?i)password\s*=\s*[^\s]+`)
	return re.ReplaceAllString(connStr, "password=***")
}

/*******************************/
/******* RETRIER ********/
/*******************************/

// retryConfig holds the configuration for a retry operation
type retryConfig struct {
	maxRetries          int // -1 for infinite retries
	baseDelay           time.Duration
	maxDelay            time.Duration
	backoffFactor       float64
	jitterMin           float64
	jitterMax           float64
	retryConditionChain []func(error, *slog.Logger) bool
	logger              *slog.Logger
}

// retryOption is a functional option for configuring retry behavior
type retryOption func(*retryConfig)

// withRetrierLogger sets the logger for the retrier
func withRetrierLogger(logger *slog.Logger) retryOption {
	return func(c *retryConfig) {
		c.logger = logger
	}
}

// withRetryCondition appends the given condition functions to the retry condition chain.
// An error is retryable if any function in the chain returns true.
func withRetryCondition(fns ...func(error, *slog.Logger) bool) retryOption {
	return func(c *retryConfig) {
		c.retryConditionChain = append(c.retryConditionChain, fns...)
	}
}

// retry executes a function with retry logic using functional optionsr
func retry(ctx context.Context, fn func() error, options ...retryOption) error {
	config := &retryConfig{
		maxRetries:    -1,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      30 * time.Second,
		backoffFactor: 2.0,
		jitterMin:     0.95,
		jitterMax:     1.05,
		retryConditionChain: []func(error, *slog.Logger) bool{
			isRetryable,
		},
	}

	// Apply options
	for _, opt := range options {
		opt(config)
	}

	var lastErr error
	delay := config.baseDelay
	attempt := 0

	for {
		lastErr = fn()

		// Success and rollback case
		if lastErr == nil {
			return nil
		}

		// Check if error is retryable (any condition in the chain returns true)
		retryable := false
		for _, cond := range config.retryConditionChain {
			if cond(lastErr, config.logger) {
				retryable = true
				break
			}
		}
		if !retryable {
			if config.logger != nil {
				config.logger.Debug("Non-retryable error encountered", "error", lastErr)
			}
			return lastErr
		}

		// Check if we should continue retrying
		// If maxRetries is -1, retry indefinitely
		if config.maxRetries >= 0 && attempt >= config.maxRetries {
			return lastErr
		}

		// Log retry attempt if logger is provided
		if config.logger != nil {
			config.logger.Debug("Retrying operation",
				"attempt", attempt+1,
				"max_retries", config.maxRetries,
				"delay", delay,
				"error", lastErr)
		}

		// Apply jitter to the delay
		jitterRange := config.jitterMax - config.jitterMin
		jitterFactor := config.jitterMin + rand.Float64()*jitterRange // #nosec G404 -- trivial use of math/rand
		jitteredDelay := time.Duration(float64(delay) * jitterFactor)

		// Wait before retrying with context cancellation support
		select {
		case <-time.After(jitteredDelay):
		case <-ctx.Done():
			if config.logger != nil {
				config.logger.Debug("Retry operation cancelled", "error", ctx.Err())
			}
			return ctx.Err()
		}

		// Calculate next delay with exponential backoff
		delay = min(time.Duration(float64(delay)*config.backoffFactor), config.maxDelay)

		attempt++
	}
}

// retryWithResult executes a function that returns a value with retry logic
// It uses the non-generic retry function under the hood
func retryWithResult[T any](ctx context.Context, fn func() (T, error), options ...retryOption) (T, error) {
	var result T
	var capturedErr error

	// Wrap the generic function to work with the non-generic retry
	wrappedFn := func() error {
		var err error
		result, err = fn()
		capturedErr = err
		return err
	}

	// Use the non-generic retry function
	err := retry(ctx, wrappedFn, options...)

	// Return the last result and error
	if err != nil {
		return result, capturedErr
	}
	return result, nil
}

func (k *Kernel) exportWorkflow(ctx context.Context, workflowID string, exportChildren bool) ([]ExportedWorkflow, error) {
	tx, err := k.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for exportWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	workflowIDs := []string{workflowID}
	if exportChildren {
		children, err := k.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
			workflowID: workflowID,
			tx:         tx,
		})
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			workflowIDs = append(workflowIDs, child.ID)
		}
	}

	txq := k.queries.WithTx(PgxTx(tx))
	exported := make([]ExportedWorkflow, 0, len(workflowIDs))

	for _, wfID := range workflowIDs {
		// Export workflow_status
		st, err := txq.ExportWorkflowStatus(ctx, wfID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, newNonExistentWorkflowError(wfID)
			}
			return nil, fmt.Errorf("failed to export workflow_status for %s: %w", wfID, err)
		}
		workflowStatus := map[string]any{
			"workflow_uuid":              st.WorkflowUuid,
			"status":                     st.Status,
			"name":                       st.Name,
			"authenticated_user":         st.AuthenticatedUser,
			"assumed_role":               st.AssumedRole,
			"authenticated_roles":        st.AuthenticatedRoles,
			"output":                     st.Output,
			"error":                      st.Error,
			"executor_id":                st.ExecutorID,
			"created_at":                 st.CreatedAt,
			"updated_at":                 st.UpdatedAt,
			"application_version":        st.ApplicationVersion,
			"application_id":             st.ApplicationID,
			"class_name":                 st.ClassName,
			"config_name":                st.ConfigName,
			"recovery_attempts":          st.RecoveryAttempts,
			"queue_name":                 st.QueueName,
			"workflow_timeout_ms":        st.WorkflowTimeoutMs,
			"workflow_deadline_epoch_ms": st.WorkflowDeadlineEpochMs,
			"started_at_epoch_ms":        st.StartedAtEpochMs,
			"deduplication_id":           st.DeduplicationID,
			"inputs":                     st.Inputs,
			"priority":                   st.Priority,
			"queue_partition_key":        st.QueuePartitionKey,
			"forked_from":                st.ForkedFrom,
			"parent_workflow_id":         st.ParentWorkflowID,
			"delay_until_epoch_ms":       st.DelayUntilEpochMs,
			"serialization":              st.Serialization,
		}

		// Export operation_outputs
		ops, err := txq.ExportOperationOutputs(ctx, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export operation_outputs for %s: %w", wfID, err)
		}
		var operationOutputs []map[string]any
		for _, op := range ops {
			operationOutputs = append(operationOutputs, map[string]any{
				"workflow_uuid":         op.WorkflowUuid,
				"function_id":           op.FunctionID,
				"function_name":         op.FunctionName,
				"output":                op.Output,
				"error":                 op.Error,
				"started_at_epoch_ms":   op.StartedAtEpochMs,
				"completed_at_epoch_ms": op.CompletedAtEpochMs,
			})
		}

		// Export workflow_events
		evs, err := txq.ExportWorkflowEvents(ctx, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events for %s: %w", wfID, err)
		}
		var workflowEvents []map[string]any
		for _, ev := range evs {
			workflowEvents = append(workflowEvents, map[string]any{
				"workflow_uuid": ev.WorkflowUuid,
				"key":           ev.Key,
				"value":         ev.Value,
			})
		}

		// Export workflow_events_history
		hist, err := txq.ExportWorkflowEventsHistory(ctx, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events_history for %s: %w", wfID, err)
		}
		var workflowEventsHistory []map[string]any
		for _, h := range hist {
			workflowEventsHistory = append(workflowEventsHistory, map[string]any{
				"workflow_uuid": h.WorkflowUuid,
				"function_id":   h.FunctionID,
				"key":           h.Key,
				"value":         h.Value,
			})
		}

		// Export streams
		strms, err := txq.ExportStreams(ctx, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export streams for %s: %w", wfID, err)
		}
		var streams []map[string]any
		for _, st := range strms {
			streams = append(streams, map[string]any{
				"workflow_uuid": st.WorkflowUuid,
				"key":           st.Key,
				"value":         st.Value,
				"offset":        st.Offset,
				"function_id":   st.FunctionID,
			})
		}

		exported = append(exported, ExportedWorkflow{
			WorkflowStatus:        workflowStatus,
			OperationOutputs:      operationOutputs,
			WorkflowEvents:        workflowEvents,
			WorkflowEventsHistory: workflowEventsHistory,
			Streams:               streams,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit exportWorkflow transaction: %w", err)
	}
	return exported, nil
}

func (k *Kernel) importWorkflow(ctx context.Context, workflows []ExportedWorkflow) error {
	tx, err := k.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction for importWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	txq := k.queries.WithTx(PgxTx(tx))

	for _, wf := range workflows {
		status := wf.WorkflowStatus

		// Import workflow_status
		if err := txq.ImportWorkflowStatus(ctx, db.ImportWorkflowStatusParams{
			WorkflowUuid:            impStrNonNull(status["workflow_uuid"]),
			Status:                  impStr(status["status"]),
			Name:                    impStr(status["name"]),
			AuthenticatedUser:       impStr(status["authenticated_user"]),
			AssumedRole:             impStr(status["assumed_role"]),
			AuthenticatedRoles:      impStr(status["authenticated_roles"]),
			Output:                  impStr(status["output"]),
			Error:                   impStr(status["error"]),
			ExecutorID:              impStr(status["executor_id"]),
			CreatedAt:               impInt64NonNull(status["created_at"]),
			UpdatedAt:               impInt64NonNull(status["updated_at"]),
			ApplicationVersion:      impStr(status["application_version"]),
			ApplicationID:           impStr(status["application_id"]),
			ClassName:               impStr(status["class_name"]),
			ConfigName:              impStr(status["config_name"]),
			RecoveryAttempts:        impInt64Ptr(status["recovery_attempts"]),
			QueueName:               impStr(status["queue_name"]),
			WorkflowTimeoutMs:       impInt64Ptr(status["workflow_timeout_ms"]),
			WorkflowDeadlineEpochMs: impInt64Ptr(status["workflow_deadline_epoch_ms"]),
			StartedAtEpochMs:        impInt64Ptr(status["started_at_epoch_ms"]),
			DeduplicationID:         impStr(status["deduplication_id"]),
			Inputs:                  impStr(status["inputs"]),
			Priority:                impInt32(status["priority"]),
			QueuePartitionKey:       impStr(status["queue_partition_key"]),
			ForkedFrom:              impStr(status["forked_from"]),
			ParentWorkflowID:        impStr(status["parent_workflow_id"]),
			DelayUntilEpochMs:       impInt64Ptr(status["delay_until_epoch_ms"]),
			Serialization:           impStr(status["serialization"]),
		}); err != nil {
			return fmt.Errorf("failed to import workflow_status: %w", err)
		}

		// Import operation_outputs
		for _, op := range wf.OperationOutputs {
			if err := txq.ImportOperationOutput(ctx, db.ImportOperationOutputParams{
				WorkflowUuid:       impStrNonNull(op["workflow_uuid"]),
				FunctionID:         impInt32(op["function_id"]),
				FunctionName:       impStrNonNull(op["function_name"]),
				Output:             impStr(op["output"]),
				Error:              impStr(op["error"]),
				StartedAtEpochMs:   impInt64Ptr(op["started_at_epoch_ms"]),
				CompletedAtEpochMs: impInt64Ptr(op["completed_at_epoch_ms"]),
			}); err != nil {
				return fmt.Errorf("failed to import operation_outputs: %w", err)
			}
		}

		// Import workflow_events
		for _, ev := range wf.WorkflowEvents {
			if err := txq.ImportWorkflowEvent(ctx, db.ImportWorkflowEventParams{
				WorkflowUuid: impStrNonNull(ev["workflow_uuid"]),
				Key:          impStrNonNull(ev["key"]),
				Value:        impStrNonNull(ev["value"]),
			}); err != nil {
				return fmt.Errorf("failed to import workflow_events: %w", err)
			}
		}

		// Import workflow_events_history
		for _, h := range wf.WorkflowEventsHistory {
			if err := txq.ImportWorkflowEventHistory(ctx, db.ImportWorkflowEventHistoryParams{
				WorkflowUuid: impStrNonNull(h["workflow_uuid"]),
				FunctionID:   impInt32(h["function_id"]),
				Key:          impStrNonNull(h["key"]),
				Value:        impStrNonNull(h["value"]),
			}); err != nil {
				return fmt.Errorf("failed to import workflow_events_history: %w", err)
			}
		}

		// Import streams
		for _, st := range wf.Streams {
			if err := txq.ImportStream(ctx, db.ImportStreamParams{
				WorkflowUuid: impStrNonNull(st["workflow_uuid"]),
				Key:          impStrNonNull(st["key"]),
				Value:        impStrNonNull(st["value"]),
				StreamOffset: impInt32(st["offset"]),
				FunctionID:   impInt32(st["function_id"]),
			}); err != nil {
				return fmt.Errorf("failed to import streams: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit importWorkflow transaction: %w", err)
	}
	return nil
}
