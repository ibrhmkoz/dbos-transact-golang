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

type ExportedWorkflow struct {
	WorkflowStatus        map[string]any   `json:"workflow_status"`
	OperationOutputs      []map[string]any `json:"operation_outputs"`
	WorkflowEvents        []map[string]any `json:"workflow_events"`
	WorkflowEventsHistory []map[string]any `json:"workflow_events_history"`
	Streams               []map[string]any `json:"streams"`
}

type Kernel struct {
	pool                          *pgxpool.Pool
	queries                       *db.Queries
	workflowNotificationsMap      *sync.Map
	workflowNotificationRepollMap *sync.Map
	workflowEventsMap             *sync.Map
	workflowEventsRepollMap       *sync.Map
	logger                        *slog.Logger
	schema                        string

	// Daemon lifecycle: the kernel owns the context its background loops run
	// under, so callers control it only through Launch/Shutdown.
	lifecycleMu sync.Mutex
	loopCancel  context.CancelFunc
	loopWg      sync.WaitGroup
}

type KernelConfig struct {
	DatabaseUrl     string
	DatabaseSchema  string
	SystemDBPool    *pgxpool.Pool
	Logger          *slog.Logger
	ApplicationName string
}

var errDeduplicationCollision = errors.New("deduplication ID collision")

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {

	poolConfig := pool.Config()
	dbName := poolConfig.ConnConfig.Database
	if dbName == "" {
		return errors.New("database name not found in pool configuration")
	}

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
		createSql := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(ctx, createSql)
		if err != nil {
			return fmt.Errorf("failed to create database %s: %v", dbName, err)
		}
		logger.Debug("Database created", "name", dbName)
	}

	return nil
}

//go:embed migrations/1_initial_dbos_schema.sql
var migration1Sql string

//go:embed migrations/1_initial_dbos_schema_listen_notify.sql
var migration1ListenNotifySql string

//go:embed migrations/2_add_queue_partition_key.sql
var migration2Sql string

//go:embed migrations/3_add_workflow_status_index.sql
var migration3Sql string

//go:embed migrations/4_add_forked_from.sql
var migration4Sql string

//go:embed migrations/5_add_step_timestamps.sql
var migration5Sql string

//go:embed migrations/6_add_workflow_events_history.sql
var migration6Sql string

//go:embed migrations/7_add_owner_xid.sql
var migration7Sql string

//go:embed migrations/8_add_parent_workflow_id.sql
var migration8Sql string

//go:embed migrations/9_add_workflow_schedules.sql
var migration9Sql string

//go:embed migrations/10_add_notifications_pkey.sql
var migration10Sql string

//go:embed migrations/11_add_serialization_columns.sql
var migration11Sql string

//go:embed migrations/12_add_notifications_consumed.sql
var migration12Sql string

//go:embed migrations/13_add_application_versions.sql
var migration13Sql string

//go:embed migrations/14_add_pgsql_client_functions.sql
var migration14Sql string

//go:embed migrations/15_add_workflow_schedule_columns.sql
var migration15Sql string

//go:embed migrations/16_add_delay_until.sql
var migration16Sql string

//go:embed migrations/17_add_workflow_schedule_queue_name.sql
var migration17Sql string

//go:embed migrations/18_add_was_forked_from.sql
var migration18Sql string

//go:embed migrations/19_add_operation_outputs_completed_at_index.sql
var migration19Sql string

//go:embed migrations/20_set_function_search_path.sql
var migration20Sql string

//go:embed migrations/21_create_queues_table.sql
var migration21Sql string

//go:embed migrations/22_drop_forked_from_index.sql
var migration22Sql string

//go:embed migrations/23_create_partial_forked_from_index.sql
var migration23Sql string

//go:embed migrations/24_drop_parent_workflow_id_index.sql
var migration24Sql string

//go:embed migrations/25_create_partial_parent_workflow_id_index.sql
var migration25Sql string

//go:embed migrations/26_drop_executor_id_index.sql
var migration26Sql string

//go:embed migrations/27_create_partial_dedup_id_index.sql
var migration27Sql string

//go:embed migrations/28_drop_dedup_id_constraint.sql
var migration28Sql string

//go:embed migrations/29_create_pending_index.sql
var migration29Sql string

//go:embed migrations/30_create_failed_index.sql
var migration30Sql string

//go:embed migrations/31_drop_status_index.sql
var migration31Sql string

//go:embed migrations/32_create_in_flight_index.sql
var migration32Sql string

//go:embed migrations/33_add_rate_limited.sql
var migration33Sql string

//go:embed migrations/34_create_rate_limited_index.sql
var migration34Sql string

//go:embed migrations/35_drop_queue_status_started_index.sql
var migration35Sql string

//go:embed migrations/36_add_completed_at.sql
var migration36Sql string

//go:embed migrations/37_create_started_at_index.sql
var migration37Sql string

//go:embed migrations/38_create_workflow_definitions.sql
var migration38Sql string

//go:embed migrations/39_add_workflow_retention.sql
var migration39Sql string

//go:embed migrations/40_drop_child_workflow_id.sql
var migration40Sql string

//go:embed migrations/41_add_error_encoded.sql
var migration41Sql string

type migrationFile struct {
	version int64
	sql     string
	online  bool
}

const (
	_dbosMigrationTable = "dbos_migrations"

	_dbosNotificationsChannel  = "dbos_notifications_channel"
	_dbosWorkflowEventsChannel = "dbos_workflow_events_channel"

	_dbosStreamClosedSentinel = "__DBOS_STREAM_CLOSED__"

	_dbConnectionRetryBaseDelay  = 1 * time.Second
	_dbConnectionRetryFactor     = 2
	_dbConnectionRetryMaxRetries = 10
	_dbConnectionMaxDelay        = 120 * time.Second
	_dbRetryInterval             = 1 * time.Second
)

func buildMigrations(schema string) []migrationFile {
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()

	migration1SqlProcessed := fmt.Sprintf(migration1Sql,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	migration1ListenNotifySqlProcessed := fmt.Sprintf(migration1ListenNotifySql,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	migration1SqlProcessed = migration1SqlProcessed + "\n" + migration1ListenNotifySqlProcessed
	c := "CONCURRENTLY"
	migration20SqlProcessed := fmt.Sprintf(migration20Sql, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	migration28SqlProcessed := fmt.Sprintf(migration28Sql, sanitizedSchema)

	return []migrationFile{
		{version: 1, sql: migration1SqlProcessed},
		{version: 2, sql: fmt.Sprintf(migration2Sql, sanitizedSchema)},
		{version: 3, sql: fmt.Sprintf(migration3Sql, sanitizedSchema)},
		{version: 4, sql: fmt.Sprintf(migration4Sql, sanitizedSchema, sanitizedSchema)},
		{version: 5, sql: fmt.Sprintf(migration5Sql, sanitizedSchema)},
		{version: 6, sql: fmt.Sprintf(migration6Sql, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 7, sql: fmt.Sprintf(migration7Sql, sanitizedSchema)},
		{version: 8, sql: fmt.Sprintf(migration8Sql, sanitizedSchema, sanitizedSchema)},
		{version: 9, sql: fmt.Sprintf(migration9Sql, sanitizedSchema)},
		{version: 10, sql: fmt.Sprintf(migration10Sql, schema, sanitizedSchema)},
		{version: 11, sql: fmt.Sprintf(migration11Sql, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 12, sql: fmt.Sprintf(migration12Sql, sanitizedSchema, sanitizedSchema)},
		{version: 13, sql: fmt.Sprintf(migration13Sql, sanitizedSchema)},
		{version: 14, sql: fmt.Sprintf(migration14Sql, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 15, sql: fmt.Sprintf(migration15Sql, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 16, sql: fmt.Sprintf(migration16Sql, sanitizedSchema, sanitizedSchema)},
		{version: 17, sql: fmt.Sprintf(migration17Sql, sanitizedSchema)},
		{version: 18, sql: fmt.Sprintf(migration18Sql, sanitizedSchema)},
		{version: 19, sql: fmt.Sprintf(migration19Sql, sanitizedSchema)},
		{version: 20, sql: migration20SqlProcessed},
		{version: 21, sql: fmt.Sprintf(migration21Sql, sanitizedSchema)},
		{version: 22, sql: fmt.Sprintf(migration22Sql, c, sanitizedSchema), online: true},
		{version: 23, sql: fmt.Sprintf(migration23Sql, c, sanitizedSchema), online: true},
		{version: 24, sql: fmt.Sprintf(migration24Sql, c, sanitizedSchema), online: true},
		{version: 25, sql: fmt.Sprintf(migration25Sql, c, sanitizedSchema), online: true},
		{version: 26, sql: fmt.Sprintf(migration26Sql, c, sanitizedSchema), online: true},
		{version: 27, sql: fmt.Sprintf(migration27Sql, c, sanitizedSchema), online: true},
		{version: 28, sql: migration28SqlProcessed},
		{version: 29, sql: fmt.Sprintf(migration29Sql, c, sanitizedSchema), online: true},
		{version: 30, sql: fmt.Sprintf(migration30Sql, c, sanitizedSchema), online: true},
		{version: 31, sql: fmt.Sprintf(migration31Sql, c, sanitizedSchema), online: true},
		{version: 32, sql: fmt.Sprintf(migration32Sql, c, sanitizedSchema), online: true},
		{version: 33, sql: fmt.Sprintf(migration33Sql, sanitizedSchema)},
		{version: 34, sql: fmt.Sprintf(migration34Sql, c, sanitizedSchema), online: true},
		{version: 35, sql: fmt.Sprintf(migration35Sql, c, sanitizedSchema), online: true},
		{version: 36, sql: fmt.Sprintf(migration36Sql, sanitizedSchema, sanitizedSchema)},
		{version: 37, sql: fmt.Sprintf(migration37Sql, c, sanitizedSchema), online: true},
		{version: 38, sql: fmt.Sprintf(migration38Sql, sanitizedSchema, c, sanitizedSchema, c, sanitizedSchema), online: true},
		{version: 39, sql: fmt.Sprintf(migration39Sql, sanitizedSchema)},
		{version: 40, sql: fmt.Sprintf(migration40Sql, sanitizedSchema)},
		{version: 41, sql: fmt.Sprintf(migration41Sql, sanitizedSchema, sanitizedSchema)},
	}
}

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
		schema, _dbosMigrationTable).Scan(&tableExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !tableExists {
		return true, nil
	}

	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", pgx.Identifier{schema}.Sanitize(), _dbosMigrationTable)
	err = pool.QueryRow(ctx, q).Scan(&currentVersion)
	if err != nil && err != pgx.ErrNoRows {
		return false, fmt.Errorf("failed to get current migration version: %v", err)
	}
	migrations := buildMigrations(schema)
	return currentVersion < migrations[len(migrations)-1].version, nil
}

// but block recreating an index of the same name. Must be called before

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
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (version) VALUES ($1)", sanitizedSchema, _dbosMigrationTable)
		if _, err := exec.Exec(ctx, insertQuery, version); err != nil {
			return fmt.Errorf("failed to insert migration version %d: %v", version, err)
		}
	} else {
		updateQuery := fmt.Sprintf("UPDATE %s.%s SET version = $1", sanitizedSchema, _dbosMigrationTable)
		if _, err := exec.Exec(ctx, updateQuery, version); err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", version, err)
		}
	}
	return nil
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) error {
	migrations := buildMigrations(schema)
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()

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
		schema, _dbosMigrationTable).Scan(&migrationTableExists); err != nil {
		return fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !migrationTableExists {
		createTableQuery := fmt.Sprintf(`CREATE TABLE %s.%s (version BIGINT NOT NULL PRIMARY KEY)`,
			sanitizedSchema, _dbosMigrationTable)
		if _, err := tx.Exec(ctx, createTableQuery); err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}
	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", sanitizedSchema, _dbosMigrationTable)
	if err := tx.QueryRow(ctx, q).Scan(&currentVersion); err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to get current migration version: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration setup transaction: %v", err)
	}

	invalidIndexesCleaned := false
	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}

		if migration.online {
			// Online migrations must run outside a transaction so PostgreSQL will accept CREATE/DROP INDEX CONCURRENTLY.

			// The version bump is necessarily a second, non-atomic round-trip. If it fails and must re-run, re-executing the migration has to be safe.
			if !invalidIndexesCleaned {
				if err := cleanupInvalidIndexes(ctx, pool, schema, logger); err != nil {
					return err
				}
				invalidIndexesCleaned = true
			}

			// cannot run in a transaction block, and pgx sends a multi-statement

			// statement (e.g. CREATE TABLE) with concurrent index builds, so we

			for _, stmt := range splitSqlStatements(migration.sql) {
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

func (k *Kernel) q(tx pgx.Tx) *db.Queries {
	if tx != nil {
		return k.queries.WithTx(tx)
	}
	return k.queries
}

func splitSqlStatements(script string) []string {
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

func NewKernel(ctx context.Context, config KernelConfig) (*Kernel, error) {

	databaseUrl := config.DatabaseUrl
	databaseSchema := config.DatabaseSchema
	customPool := config.SystemDBPool
	logger := config.Logger

	if databaseSchema == "" {
		databaseSchema = "dbos"
	}
	if logger == nil {
		logger = slog.Default()
	}
	if customPool == nil {
		if err := validateDatabaseUrl(databaseUrl); err != nil {
			return nil, err
		}
	}

	var pool *pgxpool.Pool
	if customPool != nil {
		logger.Info("Using custom database connection pool")

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

		poolConfig, err := pgxpool.ParseConfig(databaseUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database URL: %v", err)
		}

		poolConfig.MaxConns = 20
		poolConfig.MinConns = 0
		poolConfig.MaxConnLifetime = time.Hour
		poolConfig.MaxConnIdleTime = time.Minute * 5

		// Add acquire timeout to prevent indefinite blocking
		poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second

		if poolConfig.ConnConfig.RuntimeParams == nil {
			poolConfig.ConnConfig.RuntimeParams = make(map[string]string)
		}

		poolConfig.ConnConfig.RuntimeParams["search_path"] = databaseSchema

		if config.ApplicationName != "" {
			poolConfig.ConnConfig.RuntimeParams["application_name"] = config.ApplicationName
		}

		newPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pool: %v", err)
		}
		pool = newPool
	}

	maskedDatabaseUrl, err := maskPassword(pool.Config().ConnString())
	if err != nil {
		logger.Error("Failed to parse database URL", "error", err)
		return nil, fmt.Errorf("failed to parse database URL: %v", err)
	}
	logger.Info("Connecting to system database", "database_url", maskedDatabaseUrl, "schema", databaseSchema)

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

	if err := pool.Ping(ctx); err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	workflowNotificationsMap := &sync.Map{}
	workflowNotificationRepollMap := &sync.Map{}
	workflowEventsMap := &sync.Map{}
	workflowEventsRepollMap := &sync.Map{}

	return &Kernel{
		pool:                          pool,
		queries:                       db.New(pool),
		workflowNotificationsMap:      workflowNotificationsMap,
		workflowNotificationRepollMap: workflowNotificationRepollMap,
		workflowEventsMap:             workflowEventsMap,
		workflowEventsRepollMap:       workflowEventsRepollMap,
		logger:                        logger.With("service", "system_database"),
		schema:                        databaseSchema,
	}, nil
}

func (k *Kernel) listenNotifyPool() *pgxpool.Pool {
	return k.pool
}

// Launch starts the kernel's background daemons. It is idempotent: calling it
// on an already-launched kernel is a no-op.
func (k *Kernel) Launch() {
	k.lifecycleMu.Lock()
	defer k.lifecycleMu.Unlock()
	if k.loopCancel != nil {
		return
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	k.loopCancel = cancel
	k.loopWg.Go(func() {
		k.notificationListenerLoop(loopCtx)
	})
}

// Shutdown stops the daemons started by Launch and closes the connection pool.
// ctx only bounds how long Shutdown waits for graceful completion; on deadline
// it logs, keeps tearing down, and returns ctx.Err().
func (k *Kernel) Shutdown(ctx context.Context) error {
	k.lifecycleMu.Lock()
	defer k.lifecycleMu.Unlock()

	k.logger.Debug("Closing system database connection pool")

	var err error
	if k.loopCancel != nil {
		k.loopCancel()
		loopsDone := make(chan struct{})
		go func() {
			k.loopWg.Wait()
			close(loopsDone)
		}()
		select {
		case <-loopsDone:
		case <-ctx.Done():
			k.logger.Warn("Notification listener loop did not finish in time", "cause", context.Cause(ctx))
			err = ctx.Err()
		}
		k.loopCancel = nil
	}

	if k.pool != nil {
		poolClose := make(chan struct{})
		go func() {

			k.pool.Close()
			close(poolClose)
		}()
		select {
		case <-poolClose:
		case <-ctx.Done():
			k.logger.Warn("System database connection pool did not close in time", "cause", context.Cause(ctx))
			err = ctx.Err()
		}
	}

	k.workflowNotificationsMap.Clear()
	k.workflowEventsMap.Clear()

	return err
}

type insertWorkflowResult struct {
	attempts          int
	status            WorkflowStatusType
	name              string
	queueName         *string
	queuePartitionKey *string
	timeout           time.Duration
	workflowDeadline  time.Time
	ownerXId          string
}

type insertWorkflowStatusDBInput struct {
	status            WorkflowStatus
	maxRetries        int
	tx                pgx.Tx
	ownerXId          *string
	incrementAttempts bool
}

func (k *Kernel) insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error) {
	if input.tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

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

	var applicationVersion *string
	if len(input.status.ApplicationVersion) > 0 {
		applicationVersion = &input.status.ApplicationVersion
	}

	var deduplicationId *string
	if len(input.status.DeduplicationId) > 0 {
		deduplicationId = &input.status.DeduplicationId
	}

	var queuePartitionKey *string
	if len(input.status.QueuePartitionKey) > 0 {
		queuePartitionKey = &input.status.QueuePartitionKey
	}

	var parentWorkflowId *string
	if len(input.status.ParentWorkflowId) > 0 {
		parentWorkflowId = &input.status.ParentWorkflowId
	}

	var className *string
	if len(input.status.ClassName) > 0 {
		className = &input.status.ClassName
	}

	authenticatedRoles, err := json.Marshal(input.status.AuthenticatedRoles)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	recoveryIncrement := int64(0)
	if input.incrementAttempts {
		recoveryIncrement = 1
	}

	var inputs *string
	switch v := input.status.Input.(type) {
	case *string:
		inputs = v
	case string:
		inputs = &v
	}

	row, err := k.queries.WithTx(input.tx).InsertWorkflowStatus(ctx, db.InsertWorkflowStatusParams{
		WorkflowUuid:            input.status.Id,
		Status:                  string(input.status.Status),
		Name:                    input.status.Name,
		QueueName:               input.status.QueueName,
		AuthenticatedUser:       input.status.AuthenticatedUser,
		AssumedRole:             input.status.AssumedRole,
		AuthenticatedRoles:      string(authenticatedRoles),
		ExecutorID:              input.status.ExecutorId,
		ApplicationVersion:      applicationVersion,
		ApplicationID:           input.status.ApplicationId,
		CreatedAt:               input.status.CreatedAt.Round(time.Millisecond).UnixMilli(),
		RecoveryAttempts:        int64(attempts),
		UpdatedAt:               updatedAt.UnixMilli(),
		WorkflowTimeoutMs:       timeoutMs,
		WorkflowDeadlineEpochMs: deadline,
		Inputs:                  inputs,
		DeduplicationID:         deduplicationId,
		Priority:                int32(input.status.Priority),
		QueuePartitionKey:       queuePartitionKey,
		OwnerXid:                input.ownerXId,
		ParentWorkflowID:        parentWorkflowId,
		ClassName:               className,
		ConfigName:              input.status.ConfigName,
		Serialization:           input.status.Serialization,
		DelayUntilEpochMs:       delayUntilEpochMs,
		EnqueuedStatus:          string(WorkflowStatusEnqueued),
		DelayedStatus:           string(WorkflowStatusDelayed),
		RecoveryIncrement:       recoveryIncrement,
	})
	if err != nil {

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
		result.ownerXId = *row.OwnerXid
	}

	if row.WorkflowTimeoutMs != nil && *row.WorkflowTimeoutMs > 0 {
		result.timeout = time.Duration(*row.WorkflowTimeoutMs) * time.Millisecond
	}

	if row.WorkflowDeadlineEpochMs != nil {
		result.workflowDeadline = time.Unix(0, *row.WorkflowDeadlineEpochMs*int64(time.Millisecond))
	}

	if len(input.status.Name) > 0 && result.name != input.status.Name {
		return nil, newConflictingWorkflowError(input.status.Id, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.name, input.status.Name))
	}
	if len(input.status.QueueName) > 0 && result.queueName != nil && input.status.QueueName != *result.queueName {
		return nil, newConflictingWorkflowError(input.status.Id, fmt.Sprintf("Workflow already exists in a different queue: %s, but the provided queue is: %s", *result.queueName, input.status.QueueName))
	}

	if result.status != WorkflowStatusSuccess && result.status != WorkflowStatusError &&
		input.maxRetries > 0 && result.attempts > input.maxRetries+1 {

		if err := k.queries.WithTx(input.tx).MarkWorkflowMaxRecoveryExceeded(ctx, db.MarkWorkflowMaxRecoveryExceededParams{
			NewStatus:     string(WorkflowStatusMaxRecoveryAttemptsExceeded),
			WorkflowUuid:  input.status.Id,
			PendingStatus: string(WorkflowStatusPending),
		}); err != nil {
			return nil, fmt.Errorf("failed to update workflow to %s: %w", WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		if err := input.tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction after marking workflow as %s: %w", WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		return nil, newDeadLetterQueueError(input.status.Id, input.maxRetries)
	}

	return &result, nil
}

type listWorkflowsDBInput struct {
	workflowName       []string
	queueName          []string
	queuesOnly         bool
	workflowIdPrefix   []string
	workflowIds        []string
	authenticatedUser  []string
	startTime          time.Time
	endTime            time.Time
	status             []WorkflowStatusType
	applicationVersion []string
	executorIds        []string
	forkedFrom         []string
	parentWorkflowId   []string
	deduplicationId    []string
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
	tx                 pgx.Tx
}

func (k *Kernel) listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error) {
	idPrefixes := make([]string, len(input.workflowIdPrefix))
	for i, p := range input.workflowIdPrefix {
		idPrefixes[i] = p + "%"
	}
	statuses := make([]string, len(input.status))
	for i, st := range input.status {
		statuses[i] = string(st)
	}

	lim := int64(-1)
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
		FilterIds:             len(input.workflowIds) > 0,
		Ids:                   input.workflowIds,
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
		FilterExecutor:        len(input.executorIds) > 0,
		ExecutorIds:           input.executorIds,
		FilterForked:          len(input.forkedFrom) > 0,
		ForkedFroms:           input.forkedFrom,
		FilterParent:          len(input.parentWorkflowId) > 0,
		ParentIds:             input.parentWorkflowId,
		FilterDedup:           len(input.deduplicationId) > 0,
		DedupIds:              input.deduplicationId,
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
			Id:            r.WorkflowUuid,
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
			wf.ApplicationId = *r.ApplicationID
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
			wf.ExecutorId = *r.ExecutorID
		}
		if r.ApplicationVersion != nil && len(*r.ApplicationVersion) > 0 {
			wf.ApplicationVersion = *r.ApplicationVersion
		}
		if r.DeduplicationID != nil && len(*r.DeduplicationID) > 0 {
			wf.DeduplicationId = *r.DeduplicationID
		}
		if r.QueuePartitionKey != nil && len(*r.QueuePartitionKey) > 0 {
			wf.QueuePartitionKey = *r.QueuePartitionKey
		}
		if r.ForkedFrom != nil && len(*r.ForkedFrom) > 0 {
			wf.ForkedFrom = *r.ForkedFrom
		}
		if r.ParentWorkflowID != nil && len(*r.ParentWorkflowID) > 0 {
			wf.ParentWorkflowId = *r.ParentWorkflowID
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
	workflowId string
	status     WorkflowStatusType
	output     *string
	errStr     string
	errEncoded *string
	tx         pgx.Tx
}

func (k *Kernel) updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error {

	if err := k.q(input.tx).UpdateWorkflowOutcome(ctx, db.UpdateWorkflowOutcomeParams{
		Status:          string(input.status),
		Output:          input.output,
		Error:           input.errStr,
		ErrorEncoded:    input.errEncoded,
		NowMs:           time.Now().UnixMilli(),
		WorkflowUuid:    input.workflowId,
		CancelledStatus: string(WorkflowStatusCancelled),
		SuccessStatus:   string(WorkflowStatusSuccess),
		ErrorStatus:     string(WorkflowStatusError),
	}); err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

type cancelWorkflowsDBInput struct {
	workflowIds []string
	tx          pgx.Tx
}

func (k *Kernel) cancelWorkflows(ctx context.Context, input cancelWorkflowsDBInput) ([]string, error) {
	if len(input.workflowIds) == 0 {
		return nil, nil
	}

	found, err := k.q(input.tx).CancelWorkflows(ctx, db.CancelWorkflowsParams{
		WorkflowIds:     input.workflowIds,
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
	workflowIds    []string
	deleteChildren bool
	tx             pgx.Tx
}

func (k *Kernel) deleteWorkflows(ctx context.Context, input deleteWorkflowsDBInput) error {

	tx := input.tx
	if tx == nil {
		var err error
		tx, err = k.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction for deleteWorkflows: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	workflowIds := make([]string, len(input.workflowIds))
	copy(workflowIds, input.workflowIds)

	if input.deleteChildren {
		for _, wfId := range input.workflowIds {
			children, err := k.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
				workflowId: wfId,
				tx:         tx,
			})
			if err != nil {
				return err
			}
			for _, child := range children {
				workflowIds = append(workflowIds, child.Id)
			}
		}
	}

	if err := k.queries.WithTx(tx).DeleteWorkflows(ctx, workflowIds); err != nil {
		return fmt.Errorf("failed to delete workflow(s): %w", err)
	}

	if input.tx == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit deleteWorkflows transaction: %w", err)
		}
	}

	return nil
}

type getWorkflowChildrenDBInput struct {
	workflowId string
	tx         pgx.Tx
}

func (k *Kernel) getWorkflowChildren(ctx context.Context, input getWorkflowChildrenDBInput) ([]WorkflowStatus, error) {

	children, err := k.listWorkflows(ctx, listWorkflowsDBInput{
		parentWorkflowId: []string{input.workflowId},
		tx:               input.tx,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get children of workflow %s: %w", input.workflowId, err)
	}

	queue := make([]string, 0, len(children))
	for _, child := range children {
		queue = append(queue, child.Id)
	}
	for len(queue) > 0 {
		parentId := queue[0]
		queue = queue[1:]

		grandchildren, err := k.listWorkflows(ctx, listWorkflowsDBInput{
			parentWorkflowId: []string{parentId},
			tx:               input.tx,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get children of workflow %s: %w", parentId, err)
		}
		for _, gc := range grandchildren {
			children = append(children, gc)
			queue = append(queue, gc.Id)
		}
	}

	return children, nil
}

func (k *Kernel) cancelAllBefore(ctx context.Context, cutoffTime time.Time) error {

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
		ids[i] = workflow.Id
	}
	if _, err := k.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIds: ids}); err != nil {
		return fmt.Errorf("failed to cancel workflows during cancelAllBefore: %w", err)
	}
	return nil
}

type garbageCollectWorkflowsInput struct {
	cutoffEpochTimestampMs *int64
	rowsThreshold          *int
}

func (k *Kernel) garbageCollectWorkflows(ctx context.Context, input garbageCollectWorkflowsInput) error {

	if input.rowsThreshold != nil && *input.rowsThreshold <= 0 {
		return fmt.Errorf("rowsThreshold must be greater than 0, got %d", *input.rowsThreshold)
	}

	cutoffTimestamp := input.cutoffEpochTimestampMs

	if input.rowsThreshold != nil {
		rowsBasedCutoff, err := k.queries.GetNthNewestCreatedAt(ctx, int32(*input.rowsThreshold-1))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("failed to query cutoff timestamp by rows threshold: %w", err)
		}

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
	workflowIds []string
	queueName   string
	tx          pgx.Tx
}

func (k *Kernel) resumeWorkflows(ctx context.Context, input resumeWorkflowsDBInput) ([]string, error) {
	if len(input.workflowIds) == 0 {
		return nil, nil
	}

	queueName := input.queueName
	if queueName == "" {
		queueName = _dbosInternalQueueName
	}

	found, err := k.q(input.tx).ResumeWorkflows(ctx, db.ResumeWorkflowsParams{
		WorkflowIds:    input.workflowIds,
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
	originalWorkflowId string
	forkedWorkflowId   string
	startStep          int
	applicationVersion string
	queueName          string
	queuePartitionKey  string
	tx                 pgx.Tx
}

func (k *Kernel) forkWorkflow(ctx context.Context, input forkWorkflowDBInput) (string, error) {

	forkedWorkflowId := input.forkedWorkflowId
	if forkedWorkflowId == "" {
		forkedWorkflowId = uuid.New().String()
	}

	if input.startStep < 0 {
		return "", fmt.Errorf("startStep must be >= 0, got %d", input.startStep)
	}

	tx := input.tx
	ownTx := tx == nil
	if ownTx {
		var err error
		tx, err = k.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to begin fork transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	}
	txq := k.queries.WithTx(tx)

	listInput := listWorkflowsDBInput{
		workflowIds: []string{input.originalWorkflowId},
		loadInput:   true,
		tx:          tx,
	}
	wfs, err := k.listWorkflows(ctx, listInput)
	if err != nil {
		return "", fmt.Errorf("failed to list workflows: %w", err)
	}
	if len(wfs) == 0 {
		return "", newNonExistentWorkflowError(input.originalWorkflowId)
	}

	originalWorkflow := wfs[0]

	appVersion := originalWorkflow.ApplicationVersion
	if input.applicationVersion != "" {
		appVersion = input.applicationVersion
	}

	queueName := input.queueName
	if queueName == "" {
		queueName = _dbosInternalQueueName
	}

	authenticatedRoles, err := json.Marshal(originalWorkflow.AuthenticatedRoles)
	if err != nil {
		return "", fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	var queuePartitionKey *string
	if input.queuePartitionKey != "" {
		queuePartitionKey = &input.queuePartitionKey
	}

	var inputs *string
	switch v := originalWorkflow.Input.(type) {
	case *string:
		inputs = v
	case string:
		inputs = &v
	}

	now := time.Now().UnixMilli()

	if err := txq.ForkInsertWorkflowStatus(ctx, db.ForkInsertWorkflowStatusParams{
		WorkflowUuid:       forkedWorkflowId,
		Status:             string(WorkflowStatusEnqueued),
		Name:               originalWorkflow.Name,
		AuthenticatedUser:  originalWorkflow.AuthenticatedUser,
		AssumedRole:        originalWorkflow.AssumedRole,
		AuthenticatedRoles: string(authenticatedRoles),
		ApplicationVersion: appVersion,
		ApplicationID:      originalWorkflow.ApplicationId,
		QueueName:          queueName,
		QueuePartitionKey:  queuePartitionKey,
		Inputs:             inputs,
		CreatedAt:          now,
		UpdatedAt:          now,
		RecoveryAttempts:   0,
		ForkedFrom:         input.originalWorkflowId,
		Serialization:      originalWorkflow.Serialization,
	}); err != nil {
		return "", fmt.Errorf("failed to insert forked workflow status: %w", err)
	}

	if err := txq.MarkWorkflowForked(ctx, input.originalWorkflowId); err != nil {
		return "", fmt.Errorf("failed to mark original workflow as forked: %w", err)
	}

	if input.startStep > 0 {
		startStep := int32(input.startStep)
		if err := txq.ForkCopyOperationOutputs(ctx, db.ForkCopyOperationOutputsParams{
			ForkedID:   forkedWorkflowId,
			OriginalID: input.originalWorkflowId,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy operation outputs: %w", err)
		}
		if err := txq.ForkCopyEventsHistory(ctx, db.ForkCopyEventsHistoryParams{
			ForkedID:   forkedWorkflowId,
			OriginalID: input.originalWorkflowId,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy workflow events history: %w", err)
		}
		if err := txq.ForkCopyLatestEvents(ctx, db.ForkCopyLatestEventsParams{
			ForkedID:   forkedWorkflowId,
			OriginalID: input.originalWorkflowId,
			StartStep:  startStep,
		}); err != nil {
			return "", fmt.Errorf("failed to copy latest workflow events: %w", err)
		}
		if err := txq.ForkCopyStreams(ctx, db.ForkCopyStreamsParams{
			ForkedID:   forkedWorkflowId,
			OriginalID: input.originalWorkflowId,
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
	return forkedWorkflowId, nil
}

type awaitWorkflowResultOutput struct {
	output        *string
	serialization string
	errStr        *string
	errEncoded    *string
}

func (k *Kernel) awaitWorkflowResult(ctx context.Context, workflowId string, pollInterval time.Duration) (*awaitWorkflowResultOutput, error) {
	if pollInterval <= 0 {
		pollInterval = _dbRetryInterval
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row, err := k.queries.GetWorkflowOutcome(ctx, workflowId)
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
		result := &awaitWorkflowResultOutput{output: row.Output, serialization: storedSerialization, errEncoded: row.ErrorEncoded}

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

			if row.Error != nil && len(*row.Error) > 0 {
				result.errStr = row.Error
			}
			return result, newAwaitedWorkflowCancelledError(workflowId)
		case WorkflowStatusMaxRecoveryAttemptsExceeded:
			return result, newDeadLetterQueueError(workflowId, int(attempts)-2)
		default:
			time.Sleep(pollInterval)
		}
	}
}

type recordOperationResultDBInput struct {
	workflowId    string
	stepId        int
	stepName      string
	output        *string
	errStr        *string
	errEncoded    *string
	tx            pgx.Tx
	startedAt     time.Time
	completedAt   time.Time
	serialization string
}

func (k *Kernel) recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	startedAtMs := input.startedAt.UnixMilli()
	completedAtMs := input.completedAt.UnixMilli()

	err := k.q(input.tx).RecordOperationResult(ctx, db.RecordOperationResultParams{
		WorkflowUuid:       input.workflowId,
		FunctionID:         int32(input.stepId),
		Output:             input.output,
		Error:              input.errStr,
		ErrorEncoded:       input.errEncoded,
		FunctionName:       input.stepName,
		StartedAtEpochMs:   &startedAtMs,
		CompletedAtEpochMs: &completedAtMs,
		Serialization:      &input.serialization,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return newWorkflowConflictIdError(input.workflowId)
		}
		return err
	}

	return nil
}

func (k *Kernel) getDeduplicatedWorkflow(ctx context.Context, workflowName, deduplicationId string) (*string, error) {
	id, err := k.queries.GetDeduplicatedWorkflow(ctx, db.GetDeduplicatedWorkflowParams{
		Name:            &workflowName,
		DeduplicationID: &deduplicationId,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get deduplicated workflow: %w", err)
	}

	return &id, nil
}

type recordedResult struct {
	output        *string
	errStr        *string
	errEncoded    *string
	serialization string
}

type checkOperationExecutionDBInput struct {
	workflowId string
	stepId     int
	stepName   string
	tx         pgx.Tx
}

func (k *Kernel) checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error) {

	tx := input.tx
	if tx == nil {
		var err error
		tx, err = k.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	}
	q := k.queries.WithTx(tx)

	status, err := q.GetWorkflowStatusOnly(ctx, input.workflowId)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, newNonExistentWorkflowError(input.workflowId)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if status != nil && WorkflowStatusType(*status) == WorkflowStatusCancelled {
		return nil, newWorkflowCancelledError(input.workflowId)
	}

	out, err := q.GetOperationOutput(ctx, db.GetOperationOutputParams{
		WorkflowUuid: input.workflowId,
		FunctionID:   int32(input.stepId),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	if input.stepName != out.FunctionName {
		return nil, newUnexpectedStepError(input.workflowId, input.stepId, input.stepName, out.FunctionName)
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
		errEncoded:    out.ErrorEncoded,
		serialization: storedSerialization,
	}
	return result, nil
}

type stepInfo struct {
	StepId        int
	StepName      string
	Output        *string
	Error         error
	StartedAt     time.Time
	CompletedAt   time.Time
	Serialization string
}

type getWorkflowStepsInput struct {
	workflowId string
	loadOutput bool
}

func (k *Kernel) getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error) {
	rows, err := k.queries.GetWorkflowSteps(ctx, input.workflowId)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}

	steps := make([]stepInfo, 0, len(rows))
	for _, r := range rows {
		step := stepInfo{
			StepId:   int(r.FunctionID),
			StepName: r.FunctionName,
		}

		if r.StartedAtEpochMs != nil {
			step.StartedAt = time.Unix(0, *r.StartedAtEpochMs*int64(time.Millisecond))
		}
		if r.CompletedAtEpochMs != nil {
			step.CompletedAt = time.Unix(0, *r.CompletedAtEpochMs*int64(time.Millisecond))
		}

		if input.loadOutput {
			step.Output = r.Output
		}

		if r.Serialization != nil {
			step.Serialization = *r.Serialization
		}

		if r.Error != nil && *r.Error != "" {
			step.Error = errors.New(*r.Error)
		}

		steps = append(steps, step)
	}

	return steps, nil
}

type WorkflowAggregateRow struct {
	Group map[string]*string `json:"group"`
	Count int64              `json:"count"`
}

// when the caller does not provide an override.
const _DEFAULT_AGGREGATES_LIMIT = 10_000_000

type getWorkflowAggregatesDBInput struct {
	groupByStatus             bool
	groupByName               bool
	groupByQueueName          bool
	groupByExecutorId         bool
	groupByApplicationVersion bool
	timeBucketSizeMs          int64
	status                    []WorkflowStatusType
	startTime                 time.Time
	endTime                   time.Time
	workflowName              []string
	applicationVersion        []string
	executorId                []string
	queueName                 []string
	workflowIdPrefix          []string
	limit                     int64
	tx                        pgx.Tx
}

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

func impStrNonNull(v any) string {
	if p := impStr(v); p != nil {
		return *p
	}
	return ""
}

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
		!input.groupByExecutorId && !input.groupByApplicationVersion && !groupByTimeBucket {
		return nil, errors.New("at least one group_by flag must be set, or a time bucket size provided")
	}

	statuses := make([]string, len(input.status))
	for i, st := range input.status {
		statuses[i] = string(st)
	}
	idPrefixes := make([]string, len(input.workflowIdPrefix))
	for i, p := range input.workflowIdPrefix {
		idPrefixes[i] = p + "%"
	}
	limit := input.limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	bucketSize := input.timeBucketSizeMs
	if bucketSize <= 0 {
		bucketSize = 1
	}

	rows, err := k.q(input.tx).GetWorkflowAggregates(ctx, db.GetWorkflowAggregatesParams{
		GroupStatus:      input.groupByStatus,
		GroupName:        input.groupByName,
		GroupQueue:       input.groupByQueueName,
		GroupExecutor:    input.groupByExecutorId,
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
		FilterExecutor:   len(input.executorId) > 0,
		ExecutorIds:      input.executorId,
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
		if input.groupByExecutorId {
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

// Count and MaxDurationMs are pointers because the caller selects which aggregates to compute;

type StepAggregateRow struct {
	Group         map[string]*string `json:"group"`
	Count         *int64             `json:"count"`
	MaxDurationMs *int64             `json:"max_duration_ms"`
}

type getStepAggregatesDBInput struct {
	groupByFunctionName bool
	groupByStatus       bool
	selectCount         bool
	selectMaxDurationMs bool
	timeBucketSizeMs    int64
	status              []string
	functionName        []string
	workflowIdPrefix    []string
	completedAfter      time.Time
	completedBefore     time.Time
	limit               int64
	tx                  pgx.Tx
}

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

	idPrefixes := make([]string, len(input.workflowIdPrefix))
	for i, p := range input.workflowIdPrefix {
		idPrefixes[i] = p + "%"
	}
	limit := input.limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	bucketSize := input.timeBucketSizeMs
	if bucketSize <= 0 {
		bucketSize = 1
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
	duration  time.Duration
	skipSleep bool
	stepId    *int // Optional step ID to use instead of generating a new one (for internal use)
}

func (k *Kernel) sleep(ctx context.Context, input sleepInput) (time.Duration, error) {
	functionName := "DBOS.sleep"

	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, newStepExecutionError("", functionName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	var stepId int
	if input.stepId != nil && *input.stepId >= 0 {
		stepId = *input.stepId
	} else {
		stepId = wfState.nextStepId()
	}

	startTime := time.Now()

	checkInput := checkOperationExecutionDBInput{
		workflowId: wfState.workflowId,
		stepId:     stepId,
		stepName:   functionName,
	}
	recordedResult, err := k.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return 0, fmt.Errorf("failed to check operation execution: %w", err)
	}

	var endTime time.Time

	if recordedResult != nil {
		if recordedResult.output == nil {
			return 0, fmt.Errorf("no recorded end time for recorded sleep operation")
		}

		serializer := newJsonSerializer[time.Time]()
		endTime, err = serializer.Decode(recordedResult.output)
		if err != nil {
			return 0, fmt.Errorf("failed to decode sleep end time: %w", err)
		}

		if recordedResult.errStr != nil {
			return 0, errors.New(*recordedResult.errStr)
		}
	} else {

		endTime = time.Now().Add(input.duration)

		serializer := newJsonSerializer[time.Time]()
		encodedEndTime, serErr := serializer.Encode(endTime)
		if serErr != nil {
			return 0, fmt.Errorf("failed to serialize sleep end time: %w", serErr)
		}

		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowId:    wfState.workflowId,
			stepId:        stepId,
			stepName:      functionName,
			output:        encodedEndTime,
			startedAt:     startTime,
			completedAt:   completedTime,
			serialization: "DBOS_JSON",
		}

		err = k.recordOperationResult(ctx, recordInput)
		if err != nil {

			if dbosErr, ok := err.(*DbosError); ok && dbosErr.Code == ConflictingIdError {
			} else {
				return 0, fmt.Errorf("failed to record sleep operation result: %w", err)
			}
		}
	}

	remainingDuration := max(0, time.Until(endTime))

	if !input.skipSleep {

		time.Sleep(remainingDuration)
	}

	return remainingDuration, nil
}

type patchDBInput struct {
	workflowId string
	stepId     int
	patchName  string
}

func (k *Kernel) doesPatchExists(ctx context.Context, input patchDBInput) (string, error) {
	return k.queries.DoesPatchExist(ctx, db.DoesPatchExistParams{
		WorkflowUuid: input.workflowId,
		FunctionID:   int32(input.stepId),
	})
}

func (k *Kernel) patch(ctx context.Context, input patchDBInput) (bool, error) {
	functionName, err := k.doesPatchExists(ctx, input)
	if err != nil {

		if errors.Is(err, pgx.ErrNoRows) {
			if err := k.queries.InsertPatchMarker(ctx, db.InsertPatchMarkerParams{
				WorkflowUuid: input.workflowId,
				FunctionID:   int32(input.stepId),
				FunctionName: input.patchName,
			}); err != nil {
				return false, fmt.Errorf("failed to insert patch marker: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("failed to check for patch: %w", err)
	}

	return functionName == input.patchName, nil
}

func (k *Kernel) notificationListenerLoop(ctx context.Context) {
	defer k.logger.Debug("Notification listener loop exiting")

	pgxPool := k.listenNotifyPool()
	if pgxPool == nil {
		k.logger.Error("Notification listener loop started without a pgx-backed pool; aborting")
		return
	}

	acquire := func(ctx context.Context) (*pgxpool.Conn, error) {

		pc, err := pgxPool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := pc.Begin(ctx)
		if err != nil {
			pc.Release()
			return nil, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", _dbosNotificationsChannel)); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				k.logger.Error("Failed to rollback transaction after LISTEN error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", _dbosWorkflowEventsChannel)); err != nil {
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

		n, err := poolConn.Conn().WaitForNotification(ctx)
		if err != nil {

			if ctx.Err() != nil {
				k.logger.Debug("Notification listener exiting (context canceled", "cause", context.Cause(ctx), "error", err)
				poolConn.Release()
				return
			}

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

			k.logger.Error("Error waiting for notification", "error", err)
			time.Sleep(backoffWithJitter(retryAttempt))
			retryAttempt++
			continue
		}

		if retryAttempt > 0 {
			retryAttempt--
		}

		switch n.Channel {
		case _dbosNotificationsChannel:
			if cond, ok := k.workflowNotificationsMap.Load(n.Payload); ok {
				cond.(*sync.Cond).L.Lock()
				cond.(*sync.Cond).Broadcast()
				cond.(*sync.Cond).L.Unlock()
			}
		case _dbosWorkflowEventsChannel:
			if cond, ok := k.workflowEventsMap.Load(n.Payload); ok {
				cond.(*sync.Cond).L.Lock()
				cond.(*sync.Cond).Broadcast()
				cond.(*sync.Cond).L.Unlock()
			}
		}
	}
}

func (k *Kernel) notificationPollerLoop(ctx context.Context) {
	defer k.logger.Debug("Notification poller loop exiting")

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

	k.workflowNotificationsMap.Range(func(key, value any) bool {
		payload, ok := key.(string)
		if !ok {
			return true
		}

		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			k.logger.Warn("Invalid notification payload format", "payload", payload)
			return true
		}

		destinationId := parts[0]
		topic := parts[1]

		exists, err := k.queries.HasUnconsumedMessage(ctx, db.HasUnconsumedMessageParams{
			DestinationUuid: destinationId,
			Topic:           topic,
		})
		if err != nil {
			k.logger.Warn("Failed to poll notification", "payload", payload, "error", err)
			return true
		}

		if exists {
			if cond, ok := value.(*sync.Cond); ok {
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
			}
		}

		return true
	})
}

func (k *Kernel) pollEvents(ctx context.Context) {

	k.workflowEventsMap.Range(func(key, value any) bool {
		payload, ok := key.(string)
		if !ok {
			return true
		}

		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			k.logger.Warn("Invalid event payload format", "payload", payload)
			return true
		}

		targetWorkflowId := parts[0]
		eventKey := parts[1]

		exists, err := k.queries.HasWorkflowEvent(ctx, db.HasWorkflowEventParams{
			WorkflowUuid: targetWorkflowId,
			Key:          eventKey,
		})
		if err != nil {
			k.logger.Warn("Failed to poll event", "payload", payload, "error", err)
			return true
		}

		if exists {
			if cond, ok := value.(*sync.Cond); ok {
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
			}
		}

		return true
	})
}

const _Dbos_NULL_TOPIC = "__null__topic__"

type WorkflowSendInput struct {
	DestinationId string
	Message       any
	Topic         string
	tx            pgx.Tx
	serialization string
}

func (k *Kernel) send(ctx context.Context, input WorkflowSendInput) error {
	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	topic := _Dbos_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	err := k.q(input.tx).InsertNotification(ctx, db.InsertNotificationParams{
		DestinationUuid:  input.DestinationId,
		Topic:            topic,
		Message:          *(input.Message.(*string)),
		Serialization:    input.serialization,
		MessageUuid:      uuid.NewString(),
		CreatedAtEpochMs: time.Now().UnixMilli(),
	})
	if err != nil {
		k.logger.Error("failed to insert notification", "error", err, "destination_id", input.DestinationId, "topic", topic, "message", input.Message)
		// Check for foreign key violation (destination workflow doesn't exist)
		if isForeignKeyViolation(err) {
			return newNonExistentWorkflowError(input.DestinationId)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}
	return nil
}

func (k *Kernel) recv(ctx context.Context, input recvInput) (*recvResult, error) {
	functionName := "DBOS.recv"

	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", functionName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	stepId := wfState.nextStepId()
	sleepStepId := wfState.nextStepId()
	destinationId := wfState.workflowId

	topic := _Dbos_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	checkInput := checkOperationExecutionDBInput{
		workflowId: destinationId,
		stepId:     stepId,
		stepName:   functionName,
	}
	recordedResult, err := k.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return nil, err
	}
	if recordedResult != nil {
		recvErr := deserializeWorkflowError(recordedResult.errStr, recordedResult.errEncoded, recordedResult.serialization)
		return &recvResult{message: recordedResult.output, serialization: recordedResult.serialization}, recvErr
	}

	// First check if there's already a receiver for this workflow/topic to avoid unnecessary database load
	payload := fmt.Sprintf("%s::%s", destinationId, topic)
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	_, loaded := k.workflowNotificationsMap.LoadOrStore(payload, cond)
	if loaded {
		cond.L.Unlock()
		k.logger.Error("Receive already called for workflow", "destination_id", destinationId)
		return nil, newWorkflowConflictIdError(destinationId)
	}
	repollChannel := make(chan struct{}, 1)
	k.workflowNotificationRepollMap.LoadOrStore(payload, repollChannel)
	defer func() {

		cond.Broadcast()
		k.workflowNotificationsMap.Delete(payload)
		k.workflowNotificationRepollMap.Delete(payload)
	}()

	hasMsgParams := db.HasUnconsumedMessageParams{DestinationUuid: destinationId, Topic: topic}
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
			stepId:    &sleepStepId,
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

			continue
		case <-ctx.Done():
			k.logger.Warn("Recv() context cancelled", "payload", payload, "cause", context.Cause(ctx))
			return nil, ctx.Err()
		}
	}

	startTime := time.Now()

	tx, err := k.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	// Use message_uuid so we update exactly one row; created_at_epoch_ms can match multiple rows when inserts occur in the same millisecond.
	var messageString *string
	var msgSerialization *string
	consumed, err := k.queries.WithTx(tx).ConsumeOldestMessage(ctx, db.ConsumeOldestMessageParams{
		DestinationUuid: destinationId,
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

	serialization := input.serialization
	if msgSerialization != nil && len(*msgSerialization) > 0 {
		serialization = *msgSerialization
	}

	completedTime := time.Now()
	recordInput := recordOperationResultDBInput{
		workflowId:    destinationId,
		stepId:        stepId,
		stepName:      functionName,
		output:        messageString,
		tx:            tx,
		startedAt:     startTime,
		completedAt:   completedTime,
		serialization: serialization,
	}

	var timeoutErr error
	if timeoutOccurred && messageString == nil {
		timeoutErr = newTimeoutError(destinationId, functionName, fmt.Sprintf("no message received within %v", input.Timeout))
		s := timeoutErr.Error()
		recordInput.errStr = &s
		recordInput.errEncoded = encodeWorkflowError(timeoutErr)
	}

	err = k.recordOperationResult(ctx, recordInput)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &recvResult{message: messageString, serialization: serialization}, timeoutErr
}

type WorkflowSetEventInput struct {
	Key           string
	Message       any
	tx            pgx.Tx
	serialization string
}

func (k *Kernel) setEvent(ctx context.Context, input WorkflowSetEventInput) error {

	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return newStepExecutionError("", "DBOS.setEvent", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	value := *(input.Message.(*string))
	q := k.q(input.tx)

	if err := q.UpsertWorkflowEvent(ctx, db.UpsertWorkflowEventParams{
		WorkflowUuid:  wfState.workflowId,
		Key:           input.Key,
		Value:         value,
		Serialization: input.serialization,
	}); err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	return q.InsertWorkflowEventHistory(ctx, db.InsertWorkflowEventHistoryParams{
		WorkflowUuid:  wfState.workflowId,
		FunctionID:    int32(wfState.stepId),
		Key:           input.Key,
		Value:         value,
		Serialization: input.serialization,
	})
}

func (k *Kernel) getEvent(ctx context.Context, input getEventInput) (*getEventResult, error) {
	functionName := "DBOS.getEvent"

	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	var stepId int
	var sleepStepId int
	var isInWorkflow bool

	startTime := time.Now()
	if ok && wfState != nil {
		isInWorkflow = true
		if wfState.isWithinStep {
			return nil, newStepExecutionError(wfState.workflowId, functionName, fmt.Errorf("cannot call GetEvent within a step"))
		}
		stepId = wfState.nextStepId()
		sleepStepId = wfState.nextStepId()

		checkInput := checkOperationExecutionDBInput{
			workflowId: wfState.workflowId,
			stepId:     stepId,
			stepName:   functionName,
		}
		recordedResult, err := k.checkOperationExecution(ctx, checkInput)
		if err != nil {
			return nil, err
		}
		if recordedResult != nil {
			evtErr := deserializeWorkflowError(recordedResult.errStr, recordedResult.errEncoded, recordedResult.serialization)
			return &getEventResult{value: recordedResult.output, serialization: recordedResult.serialization}, evtErr
		}
	}

	payload := fmt.Sprintf("%s::%s", input.TargetWorkflowId, input.Key)
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	existingCond, loaded := k.workflowEventsMap.LoadOrStore(payload, cond)
	if loaded {
		cond.L.Unlock()

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

	var valueString *string
	var evtSerialization *string
	var err error

	queryEvent := func() error {
		row, qerr := k.queries.GetWorkflowEvent(ctx, db.GetWorkflowEventParams{
			WorkflowUuid: input.TargetWorkflowId,
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

			timeout := input.Timeout
			if isInWorkflow {
				timeout, err = k.sleep(ctx, sleepInput{
					duration:  input.Timeout,
					skipSleep: true,
					stepId:    &sleepStepId,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to sleep before getEvent timeout: %w", err)
				}
			}

			select {
			case <-done:

				if err := queryEvent(); err != nil {
					return nil, err
				}
				break loop
			case <-time.After(timeout):
				timeoutOccurred = true
				k.logger.Warn("GetEvent() timeout reached", "target_workflow_id", input.TargetWorkflowId, "key", input.Key, "timeout", input.Timeout)

				if err := queryEvent(); err != nil {
					return nil, err
				}
				break loop
			case <-repollChannel:
				// We were instructed to poll again because the connection was disconnected
				if err := queryEvent(); err != nil {
					return nil, err
				}

				continue
			case <-ctx.Done():
				k.logger.Warn("GetEvent() context cancelled", "target_workflow_id", input.TargetWorkflowId, "key", input.Key, "cause", context.Cause(ctx))
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

	serialization := input.serialization
	if evtSerialization != nil && len(*evtSerialization) > 0 {
		serialization = *evtSerialization
	}

	var timeoutErr error
	if isInWorkflow {
		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowId:    wfState.workflowId,
			stepId:        stepId,
			stepName:      functionName,
			output:        valueString,
			startedAt:     startTime,
			completedAt:   completedTime,
			serialization: serialization,
		}

		if timeoutOccurred && valueString == nil {
			timeoutErr = newTimeoutError(wfState.workflowId, functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
			s := timeoutErr.Error()
			recordInput.errStr = &s
			recordInput.errEncoded = encodeWorkflowError(timeoutErr)
		}

		err = k.recordOperationResult(ctx, recordInput)
		if err != nil {
			return nil, err
		}
	} else {

		if timeoutOccurred && valueString == nil {
			timeoutErr = newTimeoutError("", functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
		}
	}

	return &getEventResult{value: valueString, serialization: serialization}, timeoutErr
}

type writeStreamDBInput struct {
	Key           string
	Value         *string
	tx            pgx.Tx
	serialization string
}

type readStreamDBInput struct {
	WorkflowId string
	Key        string
	FromOffset int
}

type streamEntry struct {
	Value         string
	Offset        int
	Serialization string
}

func (k *Kernel) writeStream(ctx context.Context, input writeStreamDBInput) error {

	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return fmt.Errorf("workflow state not found in context: are you running this within a workflow?")
	}

	q := k.q(input.tx)

	exists, err := q.CheckStreamClosed(ctx, db.CheckStreamClosedParams{
		WorkflowUuid: wfState.workflowId,
		Key:          input.Key,
		Value:        _dbosStreamClosedSentinel,
	})
	if err == nil && exists == 1 {
		return fmt.Errorf("stream '%s' is already closed", input.Key)
	} else if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("failed to check stream status: %w", err)
	}

	if err := q.InsertStreamEntry(ctx, db.InsertStreamEntryParams{
		WorkflowUuid:  wfState.workflowId,
		Key:           input.Key,
		Value:         *input.Value,
		FunctionID:    int32(wfState.stepId),
		Serialization: input.serialization,
	}); err != nil {
		return fmt.Errorf("failed to insert stream entry: %w", err)
	}

	return nil
}

func (k *Kernel) readStream(ctx context.Context, input readStreamDBInput) ([]streamEntry, bool, error) {
	rows, err := k.queries.ReadStream(ctx, db.ReadStreamParams{
		WorkflowUuid: input.WorkflowId,
		Key:          input.Key,
		Offset:       int32(input.FromOffset),
	})
	if err != nil {
		return nil, false, fmt.Errorf("failed to query stream: %w", err)
	}

	var entries []streamEntry
	closed := false
	for _, r := range rows {
		if r.Value == _dbosStreamClosedSentinel {
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

type eventRecord struct {
	Key           string
	Value         string
	Serialization string
}

func (k *Kernel) getAllEvents(ctx context.Context, workflowId string) ([]eventRecord, error) {
	rows, err := k.queries.GetAllEvents(ctx, workflowId)
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

type notificationRecord struct {
	Topic            *string
	Message          string
	Serialization    string
	CreatedAtEpochMs int64
	Consumed         bool
}

func (k *Kernel) getAllNotifications(ctx context.Context, workflowId string) ([]notificationRecord, error) {
	rows, err := k.queries.GetAllNotifications(ctx, workflowId)
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
		if rec.Topic != nil && *rec.Topic == _Dbos_NULL_TOPIC {
			rec.Topic = nil
		}
		if r.Serialization != nil {
			rec.Serialization = *r.Serialization
		}
		results = append(results, rec)
	}
	return results, nil
}

type streamRecord struct {
	Key           string
	Value         string
	Serialization string
}

func (k *Kernel) getAllStreamEntries(ctx context.Context, workflowId string) ([]streamRecord, error) {
	rows, err := k.queries.GetAllStreamEntries(ctx, workflowId)
	if err != nil {
		return nil, fmt.Errorf("failed to query streams: %w", err)
	}
	records := make([]streamRecord, 0, len(rows))
	for _, r := range rows {
		if r.Value == _dbosStreamClosedSentinel {
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

type setWorkflowDelayDBInput struct {
	workflowId string
	delayUntil time.Time
	tx         pgx.Tx
}

func (k *Kernel) setWorkflowDelay(ctx context.Context, input setWorkflowDelayDBInput) error {
	if err := k.q(input.tx).SetWorkflowDelay(ctx, db.SetWorkflowDelayParams{
		DelayUntil:   input.delayUntil.UnixMilli(),
		UpdatedAt:    time.Now().UnixMilli(),
		WorkflowUuid: input.workflowId,
		Status:       string(WorkflowStatusDelayed),
	}); err != nil {
		return fmt.Errorf("failed to set workflow delay: %w", err)
	}
	return nil
}

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
	executorId         string
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

	iso := pgx.ReadCommitted
	if policyConcurrency != nil || policyRateLimit != nil {
		iso = pgx.RepeatableRead
	}
	tx, err := k.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	txq := k.queries.WithTx(tx)

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

	maxTasks := _defaultMaxTasksPerIteration
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
	var dequeuedIds []string
	if policyConcurrency == nil {
		dequeuedIds, err = txq.DequeueCandidatesSkipLocked(ctx, db.DequeueCandidatesSkipLockedParams{
			Name:           input.workflowName,
			EnqueuedStatus: string(WorkflowStatusEnqueued),
			AppVersion:     input.applicationVersion,
			MaxTasks:       int32(maxTasks),
		})
	} else {
		dequeuedIds, err = txq.DequeueCandidatesNoWait(ctx, db.DequeueCandidatesNoWaitParams{
			Name:           input.workflowName,
			EnqueuedStatus: string(WorkflowStatusEnqueued),
			AppVersion:     input.applicationVersion,
			MaxTasks:       int32(maxTasks),
		})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}

	if len(dequeuedIds) > 0 {
		k.logger.Debug("attempting to claim workflow(s)", "workflow_name", input.workflowName, "numTasks", len(dequeuedIds))
	}

	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIds {
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
			ExecutorID:     input.executorId,
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

func (k *Kernel) clearQueueAssignment(ctx context.Context, workflowId string) (bool, error) {
	n, err := k.queries.ClearQueueAssignment(ctx, db.ClearQueueAssignmentParams{
		EnqueuedStatus: string(WorkflowStatusEnqueued),
		WorkflowUuid:   workflowId,
		PendingStatus:  string(WorkflowStatusPending),
	})
	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment for workflow %s: %w", workflowId, err)
	}

	return n > 0, nil
}

type metricData struct {
	MetricName string  `json:"metric_name"`
	MetricType string  `json:"metric_type"`
	Value      float64 `json:"value"`
}

func (k *Kernel) getMetrics(ctx context.Context, startTime, endTime string) ([]metricData, error) {

	startTimeParsed, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		return nil, fmt.Errorf("invalid start_time format: %w", err)
	}
	endTimeParsed, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return nil, fmt.Errorf("invalid end_time format: %w", err)
	}

	startEpochMs := startTimeParsed.UnixMilli()
	endEpochMs := endTimeParsed.UnixMilli()

	var metrics []metricData

	workflowMetrics, err := k.getMetricWorkflowCount(ctx, startEpochMs, endEpochMs)
	if err != nil {
		return nil, err
	}
	metrics = append(metrics, workflowMetrics...)

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

type createScheduleDBInput struct {
	ScheduleId        string
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           string
	Status            ScheduleStatus
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
	tx                pgx.Tx
}

func scheduleFromRow(r db.WorkflowSchedule) WorkflowSchedule {
	sc := WorkflowSchedule{
		ScheduleId:        r.ScheduleID,
		ScheduleName:      r.ScheduleName,
		WorkflowName:      r.WorkflowName,
		Schedule:          r.Schedule,
		Status:            ScheduleStatus(r.Status),
		AutomaticBackfill: r.AutomaticBackfill,
	}
	if r.QueueName != nil {
		sc.QueueName = *r.QueueName
	} else {
		sc.QueueName = _dbosInternalQueueName
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
		ScheduleID:        input.ScheduleId,
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
	tx                   pgx.Tx
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
	tx           pgx.Tx
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
	tx           pgx.Tx
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

	queueName := _dbosInternalQueueName
	if schedule.QueueName != "" {
		queueName = schedule.QueueName
	}

	ser := resolveEncoder(ctx)

	var backfillAppVersion string
	backfillLatest, err := retryWithResult(ctx, func() (*VersionInfo, error) {
		return k.getLatestApplicationVersion(ctx)
	}, withRetrierLogger(k.logger))
	if err != nil {
		k.logger.Error("failed to fetch latest application version for schedule backfill", "schedule", input.ScheduleName, "error", err)
	} else if backfillLatest != nil {
		backfillAppVersion = backfillLatest.Name
	}

	tx, err := k.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	txq := k.queries.WithTx(tx)

	nextTime := scheduleEntry.Next(input.StartTime)
	now := time.Now()
	var workflowIds []string

	for nextTime.Before(input.EndTime) {
		workflowId := fmt.Sprintf("sched-%s-%s", input.ScheduleName, nextTime.Format(time.RFC3339))
		workflowIds = append(workflowIds, workflowId)

		_, err := txq.WorkflowExists(ctx, workflowId)
		if err == nil {
			nextTime = scheduleEntry.Next(nextTime)
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("failed to check workflow existence for %s: %w", workflowId, err)
		}

		encodedInput, encErr := ser.Encode(ScheduledWorkflowInput{
			ScheduledTime: nextTime,
			Context:       schedule.Context,
		})
		if encErr != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input for %s: %w", workflowId, encErr)
		}

		status := WorkflowStatus{
			Id:                 workflowId,
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
			return nil, fmt.Errorf("failed to enqueue backfill workflow %s: %w", workflowId, err)
		}

		nextTime = scheduleEntry.Next(nextTime)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit backfill transaction: %w", err)
	}
	return workflowIds, nil
}

func (k *Kernel) triggerSchedule(ctx context.Context, scheduleName string) (string, error) {
	if scheduleName == "" {
		return "", errors.New("schedule_name is required")
	}

	tx, err := k.pool.BeginTx(ctx, pgx.TxOptions{})
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
		queueName = _dbosInternalQueueName
	}

	now := time.Now()
	workflowId := fmt.Sprintf("sched-%s-trigger-%s", scheduleName, now.Format(time.RFC3339Nano))

	ser := resolveEncoder(ctx)
	encodedInput, err := ser.Encode(ScheduledWorkflowInput{
		ScheduledTime: now,
		Context:       schedule.Context,
	})
	if err != nil {
		return "", fmt.Errorf("failed to encode scheduled workflow input: %w", err)
	}

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
		Id:                 workflowId,
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

	return workflowId, nil
}

type VersionInfo struct {
	Id        string `json:"version_id"`
	Name      string `json:"version_name"`
	Timestamp int64  `json:"version_timestamp"`
	CreatedAt int64  `json:"created_at"`
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
			Id:        r.VersionID,
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
		Id:        r.VersionID,
		Name:      r.VersionName,
		Timestamp: r.VersionTimestamp,
		CreatedAt: r.CreatedAt,
	}, nil
}

func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()
	dropSql := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
	if _, err := conn.Exec(ctx, dropSql); err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}
	return nil
}

func (k *Kernel) resetSystemDB(ctx context.Context) error {

	config := k.pool.Config()
	if config == nil || config.ConnConfig == nil {
		return fmt.Errorf("failed to get pool configuration")
	}

	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in pool configuration")
	}

	k.pool.Close()

	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	err = dropDatabaseIfExists(ctx, conn, dbName)
	if err != nil {
		return err
	}

	return nil
}

func backoffWithJitter(retryAttempt int) time.Duration {
	exp := float64(_dbConnectionRetryBaseDelay) * math.Pow(_dbConnectionRetryFactor, float64(retryAttempt))

	if retryAttempt >= _dbConnectionRetryMaxRetries || exp > float64(_dbConnectionMaxDelay) {
		exp = float64(_dbConnectionMaxDelay)
	}

	jitter := 0.75 + rand.Float64()*0.5
	return time.Duration(exp * jitter)
}

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

func maskPasswordInKeyValueFormat(connStr string) string {

	re := regexp.MustCompile(`(?i)password\s*=\s*[^\s]+`)
	return re.ReplaceAllString(connStr, "password=***")
}

type retryConfig struct {
	maxRetries          int
	baseDelay           time.Duration
	maxDelay            time.Duration
	backoffFactor       float64
	jitterMin           float64
	jitterMax           float64
	retryConditionChain []func(error, *slog.Logger) bool
	logger              *slog.Logger
}

type retryOption func(*retryConfig)

func withRetrierLogger(logger *slog.Logger) retryOption {
	return func(c *retryConfig) {
		c.logger = logger
	}
}

func withRetryCondition(fns ...func(error, *slog.Logger) bool) retryOption {
	return func(c *retryConfig) {
		c.retryConditionChain = append(c.retryConditionChain, fns...)
	}
}

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

	for _, opt := range options {
		opt(config)
	}

	var lastErr error
	delay := config.baseDelay
	attempt := 0

	for {
		lastErr = fn()

		if lastErr == nil {
			return nil
		}

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

		if config.maxRetries >= 0 && attempt >= config.maxRetries {
			return lastErr
		}

		if config.logger != nil {
			config.logger.Debug("Retrying operation",
				"attempt", attempt+1,
				"max_retries", config.maxRetries,
				"delay", delay,
				"error", lastErr)
		}

		jitterRange := config.jitterMax - config.jitterMin
		jitterFactor := config.jitterMin + rand.Float64()*jitterRange
		jitteredDelay := time.Duration(float64(delay) * jitterFactor)

		select {
		case <-time.After(jitteredDelay):
		case <-ctx.Done():
			if config.logger != nil {
				config.logger.Debug("Retry operation cancelled", "error", ctx.Err())
			}
			return ctx.Err()
		}

		delay = min(time.Duration(float64(delay)*config.backoffFactor), config.maxDelay)

		attempt++
	}
}

func retryWithResult[T any](ctx context.Context, fn func() (T, error), options ...retryOption) (T, error) {
	var result T
	var capturedErr error

	wrappedFn := func() error {
		var err error
		result, err = fn()
		capturedErr = err
		return err
	}

	err := retry(ctx, wrappedFn, options...)

	if err != nil {
		return result, capturedErr
	}
	return result, nil
}

func (k *Kernel) exportWorkflow(ctx context.Context, workflowId string, exportChildren bool) ([]ExportedWorkflow, error) {
	tx, err := k.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for exportWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	workflowIds := []string{workflowId}
	if exportChildren {
		children, err := k.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
			workflowId: workflowId,
			tx:         tx,
		})
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			workflowIds = append(workflowIds, child.Id)
		}
	}

	txq := k.queries.WithTx(tx)
	exported := make([]ExportedWorkflow, 0, len(workflowIds))

	for _, wfId := range workflowIds {

		st, err := txq.ExportWorkflowStatus(ctx, wfId)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, newNonExistentWorkflowError(wfId)
			}
			return nil, fmt.Errorf("failed to export workflow_status for %s: %w", wfId, err)
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

		ops, err := txq.ExportOperationOutputs(ctx, wfId)
		if err != nil {
			return nil, fmt.Errorf("failed to export operation_outputs for %s: %w", wfId, err)
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

		evs, err := txq.ExportWorkflowEvents(ctx, wfId)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events for %s: %w", wfId, err)
		}
		var workflowEvents []map[string]any
		for _, ev := range evs {
			workflowEvents = append(workflowEvents, map[string]any{
				"workflow_uuid": ev.WorkflowUuid,
				"key":           ev.Key,
				"value":         ev.Value,
			})
		}

		hist, err := txq.ExportWorkflowEventsHistory(ctx, wfId)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events_history for %s: %w", wfId, err)
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

		strms, err := txq.ExportStreams(ctx, wfId)
		if err != nil {
			return nil, fmt.Errorf("failed to export streams for %s: %w", wfId, err)
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
	tx, err := k.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction for importWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	txq := k.queries.WithTx(tx)

	for _, wf := range workflows {
		status := wf.WorkflowStatus

		if err := txq.ImportWorkflowStatus(ctx, db.ImportWorkflowStatusParams{
			WorkflowUuid:            impStrNonNull(status["workflow_uuid"]),
			Status:                  impStr(status["status"]),
			Name:                    impStr(status["name"]),
			AuthenticatedUser:       impStr(status["authenticated_user"]),
			AssumedRole:             impStr(status["assumed_role"]),
			AuthenticatedRoles:      impStr(status["authenticated_roles"]),
			Output:                  impStr(status["output"]),
			Error:                   impStr(status["error"]),
			ErrorEncoded:            impStr(status["error_encoded"]),
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

		for _, op := range wf.OperationOutputs {
			if err := txq.ImportOperationOutput(ctx, db.ImportOperationOutputParams{
				WorkflowUuid:       impStrNonNull(op["workflow_uuid"]),
				FunctionID:         impInt32(op["function_id"]),
				FunctionName:       impStrNonNull(op["function_name"]),
				Output:             impStr(op["output"]),
				Error:              impStr(op["error"]),
				ErrorEncoded:       impStr(op["error_encoded"]),
				StartedAtEpochMs:   impInt64Ptr(op["started_at_epoch_ms"]),
				CompletedAtEpochMs: impInt64Ptr(op["completed_at_epoch_ms"]),
			}); err != nil {
				return fmt.Errorf("failed to import operation_outputs: %w", err)
			}
		}

		for _, ev := range wf.WorkflowEvents {
			if err := txq.ImportWorkflowEvent(ctx, db.ImportWorkflowEventParams{
				WorkflowUuid: impStrNonNull(ev["workflow_uuid"]),
				Key:          impStrNonNull(ev["key"]),
				Value:        impStrNonNull(ev["value"]),
			}); err != nil {
				return fmt.Errorf("failed to import workflow_events: %w", err)
			}
		}

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
