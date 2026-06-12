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
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/goleak"
)

func getDatabaseUrl(t *testing.T) string {
	t.Helper()
	pgContainerOnce.Do(func() {
		if databaseUrl := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); databaseUrl != "" {
			pgDatabaseUrl = databaseUrl
			return
		}
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		// context.Background() rather than t.Context(): the container outlives the

		container, err := postgres.Run(context.Background(), "postgres:16-alpine",
			postgres.WithDatabase("dbos"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword(password),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			pgContainerErr = err
			return
		}
		pgContainer = container
		pgDatabaseUrl, pgContainerErr = container.ConnectionString(context.Background(), "sslmode=disable")
	})
	require.NoError(t, pgContainerErr)
	return pgDatabaseUrl
}

var (
	testDBUrls        sync.Map
	usedTestDBs       sync.Map
	parallelTestCount atomic.Int64
	testDatabaseId    atomic.Uint64
	pgTemplateOnce    sync.Once
	pgTemplateUrl     string
	pgTemplateName    string
	pgTemplateErr     error
	pgTemplateCloneMu sync.Mutex
	pgContainerOnce   sync.Once
	pgContainer       *postgres.PostgresContainer
	pgContainerErr    error
	pgDatabaseUrl     string
)

var invalidDatabaseNameChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	if pgTemplateName != "" {
		config, err := pgx.ParseConfig(pgTemplateUrl)
		if err == nil {
			config.Database = "postgres"
			conn, connectErr := pgx.ConnectConfig(context.Background(), config)
			if connectErr == nil {
				_ = dropDatabaseIfExists(context.Background(), conn, pgTemplateName)
				_ = conn.Close(context.Background())
			}
		}
	}
	// The shared Postgres testcontainer (if started) must outlive every parallel
	// test, so it is torn down here rather than via a per-test t.Cleanup.
	if pgContainer != nil {
		_ = pgContainer.Terminate(context.Background())
	}
	os.Exit(exitCode)
}

func parallelTest(t *testing.T) {
	t.Helper()
	t.Parallel()
	parallelTestCount.Add(1)
	t.Cleanup(func() { parallelTestCount.Add(-1) })
}

func backendDatabaseUrl(t *testing.T) string {
	t.Helper()
	if v, ok := testDBUrls.Load(t); ok {
		return v.(string)
	}
	url := createPostgresTestDatabase(t)
	testDBUrls.Store(t, url)
	t.Cleanup(func() {
		testDBUrls.Delete(t)
		usedTestDBs.Delete(t)
	})
	return url
}

func createPostgresTestDatabase(t *testing.T) string {
	t.Helper()
	ensurePostgresTemplate(t)

	config, err := pgx.ParseConfig(pgTemplateUrl)
	require.NoError(t, err)
	adminConfig := config.Copy()
	adminConfig.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), adminConfig)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	dbName := testDatabaseName(t.Name())
	createSql := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", pgx.Identifier{dbName}.Sanitize(), pgx.Identifier{pgTemplateName}.Sanitize())
	pgTemplateCloneMu.Lock()
	_, err = conn.Exec(context.Background(), createSql)
	pgTemplateCloneMu.Unlock()
	require.NoError(t, err)

	databaseUrl := replaceDatabaseInUrl(t, pgTemplateUrl, dbName)
	t.Cleanup(func() {
		cleanupConfig := adminConfig.Copy()
		cleanupConn, cleanupErr := pgx.ConnectConfig(context.Background(), cleanupConfig)
		require.NoError(t, cleanupErr)
		defer cleanupConn.Close(context.Background())
		require.NoError(t, dropDatabaseIfExists(context.Background(), cleanupConn, dbName))
	})
	return databaseUrl
}

func ensurePostgresTemplate(t *testing.T) {
	t.Helper()
	pgTemplateOnce.Do(func() {
		config, err := pgx.ParseConfig(getDatabaseUrl(t))
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

		pgTemplateUrl = replaceDatabaseInUrl(t, getDatabaseUrl(t), pgTemplateName)
		ctx, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl: pgTemplateUrl,
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

// legitimately outlive a single test.
func verifyNoLeaks(t *testing.T) {
	t.Helper()
	goleak.VerifyNone(t,

		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),

		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
	)
}

// pgx.ConnConfig.Database and calling ConnString() does NOT work: ConnString

func replaceDatabaseInUrl(t *testing.T, baseUrl, dbName string) string {
	t.Helper()
	u, err := url.Parse(baseUrl)
	require.NoError(t, err)
	u.Path = "/" + dbName
	return u.String()
}

func testDatabaseName(testName string) string {
	name := invalidDatabaseNameChars.ReplaceAllString(testName, "_")
	suffix := "_" + strconv.Itoa(os.Getpid()) + "_" + strconv.FormatUint(testDatabaseId.Add(1), 10)
	const maxPostgresIdentifierLength = 63
	maxNameLength := maxPostgresIdentifierLength - len("dbos_test_") - len(suffix)
	if len(name) > maxNameLength {
		name = name[:maxNameLength]
	}
	return "dbos_test_" + name + suffix
}

func resetTestDatabase(t *testing.T, databaseUrl string) {
	t.Helper()

	parsedUrl, err := pgx.ParseConfig(databaseUrl)
	require.NoError(t, err)

	dbName := parsedUrl.Database
	if dbName == "" {
		t.Skip("DBOS_SYSTEM_DATABASE_URL does not specify a database name, skipping integration test")
	}

	postgresUrl := parsedUrl.Copy()
	postgresUrl.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresUrl)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	err = dropDatabaseIfExists(context.Background(), conn, dbName)
	require.NoError(t, err)
}

type setupDbosOptions struct {
	dropDB                   bool
	checkLeaks               bool
	serializer               Serializer[any]
	schedulerPollingInterval time.Duration
}

func setupDbos(t *testing.T, opts setupDbosOptions) DbosContext {
	t.Helper()

	databaseUrl := backendDatabaseUrl(t)
	_, databaseWasUsed := usedTestDBs.Load(t)
	if opts.dropDB && databaseWasUsed {
		resetTestDatabase(t, databaseUrl)
	}

	config := Config{
		DatabaseUrl:              databaseUrl,
		AppName:                  "test-app",
		Serializer:               opts.serializer,
		SchedulerPollingInterval: opts.schedulerPollingInterval,
	}

	dbosCtx, err := NewDbosContext(context.Background(), config)
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)
	usedTestDBs.Store(t, struct{}{})

	t.Cleanup(func() {
		dbosCtx.(*dbosContext).logger.Info("Cleaning up DBOS instance...")
		if dbosCtx != nil {
			Shutdown(dbosCtx, 30*time.Second)
		}
		dbosCtx = nil
		if opts.checkLeaks && parallelTestCount.Load() == 0 {
			verifyNoLeaks(t)
		}
	})

	return dbosCtx
}

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

func setWorkflowStatusPending(t *testing.T, dbosCtx DbosContext, workflowId string) {
	t.Helper()
	c, ok := dbosCtx.(*dbosContext)
	require.True(t, ok, "expected DbosContext to be *dbosContext")
	Kernel := c.kernel
	updateQuery := fmt.Sprintf(`UPDATE %sworkflow_status
		SET status = $1, output = NULL, error = NULL, started_at_epoch_ms = NULL, updated_at = $2
		WHERE workflow_uuid = $3`, "")
	_, err := Kernel.pool.Exec(context.Background(), updateQuery,
		WorkflowStatusPending, time.Now().UnixMilli(), workflowId)
	require.NoError(t, err, "failed to set workflow status to PENDING")
}

func queueEntriesAreCleanedUp(ctx DbosContext) bool {
	maxTries := 10
	success := false
	exec, ok := ctx.(*dbosContext)
	if !ok {
		fmt.Println("Expected ctx to be of type *dbosContext in queueEntriesAreCleanedUp")
		return false
	}
	sdb := exec.kernel
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
		err = tx.QueryRow(ctx, query, _dbosInternalQueueName).Scan(&count)
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
