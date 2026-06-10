package dbos

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func getDatabaseURL() string {
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	return databaseURL
}

var (
	testDBURLs        sync.Map // *testing.T -> string; ensures setupDBOS and follow-up callers share the same database.
	usedTestDBs       sync.Map // *testing.T -> struct{}; tracks whether setupDBOS has initialized the test database.
	parallelTestCount atomic.Int64
	testDatabaseID    atomic.Uint64
	pgTemplateOnce    sync.Once
	pgTemplateURL     string
	pgTemplateName    string
	pgTemplateErr     error
	pgTemplateCloneMu sync.Mutex
)

var invalidDatabaseNameChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	if pgTemplateName != "" {
		config, err := pgx.ParseConfig(pgTemplateURL)
		if err == nil {
			config.Database = "postgres"
			conn, connectErr := pgx.ConnectConfig(context.Background(), config)
			if connectErr == nil {
				_ = dropDatabaseIfExists(context.Background(), conn, pgTemplateName)
				_ = conn.Close(context.Background())
			}
		}
	}
	os.Exit(exitCode)
}

func parallelTest(t *testing.T) {
	t.Helper()
	t.Parallel()
	parallelTestCount.Add(1)
	t.Cleanup(func() { parallelTestCount.Add(-1) })
}

func backendDatabaseURL(t *testing.T) string {
	t.Helper()
	if v, ok := testDBURLs.Load(t); ok {
		return v.(string)
	}
	url := createPostgresTestDatabase(t)
	testDBURLs.Store(t, url)
	t.Cleanup(func() {
		testDBURLs.Delete(t)
		usedTestDBs.Delete(t)
	})
	return url
}

func createPostgresTestDatabase(t *testing.T) string {
	t.Helper()
	ensurePostgresTemplate(t)

	config, err := pgx.ParseConfig(pgTemplateURL)
	require.NoError(t, err)
	adminConfig := config.Copy()
	adminConfig.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), adminConfig)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	dbName := testDatabaseName(t.Name())
	createSQL := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", pgx.Identifier{dbName}.Sanitize(), pgx.Identifier{pgTemplateName}.Sanitize())
	pgTemplateCloneMu.Lock()
	_, err = conn.Exec(context.Background(), createSQL)
	pgTemplateCloneMu.Unlock()
	require.NoError(t, err)

	config.Database = dbName
	databaseURL := config.ConnString()
	t.Cleanup(func() {
		cleanupConfig := adminConfig.Copy()
		cleanupConn, cleanupErr := pgx.ConnectConfig(context.Background(), cleanupConfig)
		require.NoError(t, cleanupErr)
		defer cleanupConn.Close(context.Background())
		require.NoError(t, dropDatabaseIfExists(context.Background(), cleanupConn, dbName))
	})
	return databaseURL
}

func ensurePostgresTemplate(t *testing.T) {
	t.Helper()
	pgTemplateOnce.Do(func() {
		config, err := pgx.ParseConfig(getDatabaseURL())
		if err != nil {
			pgTemplateErr = err
			return
		}
		adminConfig := config.Copy()
		adminConfig.Database = "postgres"
		conn, err := pgx.ConnectConfig(context.Background(), adminConfig)
		if err != nil {
			pgTemplateErr = err
			return
		}
		defer conn.Close(context.Background())

		pgTemplateName = testDatabaseName("template")
		_, err = conn.Exec(context.Background(), fmt.Sprintf(
			"CREATE DATABASE %s",
			pgx.Identifier{pgTemplateName}.Sanitize(),
		))
		if err != nil {
			pgTemplateErr = err
			return
		}

		templateConfig := config.Copy()
		templateConfig.Database = pgTemplateName
		pgTemplateURL = templateConfig.ConnString()
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: pgTemplateURL,
			AppName:     "test-template",
		})
		if err != nil {
			pgTemplateErr = err
			return
		}
		Shutdown(ctx, time.Minute)
	})
	require.NoError(t, pgTemplateErr)
}

func testDatabaseName(testName string) string {
	name := invalidDatabaseNameChars.ReplaceAllString(testName, "_")
	suffix := "_" + strconv.Itoa(os.Getpid()) + "_" + strconv.FormatUint(testDatabaseID.Add(1), 10)
	const maxPostgresIdentifierLength = 63
	maxNameLength := maxPostgresIdentifierLength - len("dbos_test_") - len(suffix)
	if len(name) > maxNameLength {
		name = name[:maxNameLength]
	}
	return "dbos_test_" + name + suffix
}

/* Test database reset */
func resetTestDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	// Clean up the test database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	require.NoError(t, err)

	dbName := parsedURL.Database
	if dbName == "" {
		t.Skip("DBOS_SYSTEM_DATABASE_URL does not specify a database name, skipping integration test")
	}

	postgresURL := parsedURL.Copy()
	postgresURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresURL)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	err = dropDatabaseIfExists(context.Background(), conn, dbName)
	require.NoError(t, err)
}

type setupDBOSOptions struct {
	dropDB                   bool
	checkLeaks               bool
	serializer               Serializer[any]
	schedulerPollingInterval time.Duration
}

/* Test database setup */
func setupDBOS(t *testing.T, opts setupDBOSOptions) DBOSContext {
	t.Helper()

	databaseURL := backendDatabaseURL(t)
	_, databaseWasUsed := usedTestDBs.Load(t)
	if opts.dropDB && databaseWasUsed {
		resetTestDatabase(t, databaseURL)
	}

	config := Config{
		DatabaseURL:              databaseURL,
		AppName:                  "test-app",
		Serializer:               opts.serializer,
		SchedulerPollingInterval: opts.schedulerPollingInterval,
	}

	dbosCtx, err := NewDBOSContext(context.Background(), config)
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)
	usedTestDBs.Store(t, struct{}{})

	// Register cleanup to run after test completes
	t.Cleanup(func() {
		dbosCtx.(*dbosContext).logger.Info("Cleaning up DBOS instance...")
		if dbosCtx != nil {
			Shutdown(dbosCtx, 30*time.Second) // Wait for workflows to finish and shutdown admin server and system database
		}
		dbosCtx = nil
		if opts.checkLeaks && parallelTestCount.Load() == 0 {
			goleak.VerifyNone(t,
				// Ignore pgx health checks
				// https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgxpool/pool.go#L417
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
			)
		}
	})

	return dbosCtx
}

/* Event struct provides a simple synchronization primitive that can be used to signal between goroutines. */
type Event struct {
	mu    sync.Mutex
	cond  *sync.Cond
	IsSet bool
}

func NewEvent() *Event {
	e := &Event{}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *Event) Wait() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for !e.IsSet {
		e.cond.Wait()
	}
}

func (e *Event) Set() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = true
	e.cond.Broadcast()
}

func (e *Event) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = false
}

// setWorkflowStatusPending sets the workflow's status to PENDING in the DB (clearing output, error, started_at_epoch_ms).
func setWorkflowStatusPending(t *testing.T, dbosCtx DBOSContext, workflowID string) {
	t.Helper()
	c, ok := dbosCtx.(*dbosContext)
	require.True(t, ok, "expected DBOSContext to be *dbosContext")
	SystemDatabase := c.systemDB
	updateQuery := fmt.Sprintf(`UPDATE %sworkflow_status
		SET status = $1, output = NULL, error = NULL, started_at_epoch_ms = NULL, updated_at = $2
		WHERE workflow_uuid = $3`, "")
	_, err := SystemDatabase.pool.Exec(context.Background(), updateQuery,
		WorkflowStatusPending, time.Now().UnixMilli(), workflowID)
	require.NoError(t, err, "failed to set workflow status to PENDING")
}

func queueEntriesAreCleanedUp(ctx DBOSContext) bool {
	maxTries := 10
	success := false
	exec, ok := ctx.(*dbosContext)
	if !ok {
		fmt.Println("Expected ctx to be of type *dbosContext in queueEntriesAreCleanedUp")
		return false
	}
	sdb := exec.systemDB
	for range maxTries {
		tx, err := sdb.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return false
		}

		query := fmt.Sprintf(`SELECT COUNT(*)
				  FROM %sworkflow_status
				  WHERE queue_name IS NOT NULL
					AND queue_name != $1
					AND status IN ('ENQUEUED', 'PENDING')`, "")

		var count int
		err = tx.QueryRow(ctx, query, _DBOS_INTERNAL_QUEUE_NAME).Scan(&count)
		tx.Rollback(ctx)

		if err != nil {
			return false
		}

		if count == 0 {
			success = true
			break
		}

		time.Sleep(1 * time.Second)
	}
	return success
}
