package chaos_test

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCliPath string

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

func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()
	dropSql := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
	if _, err := conn.Exec(ctx, dropSql); err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}
	return nil
}

func (e *Event) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = false
}

func TestMain(m *testing.M) {

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintf(os.Stderr, "Failed to get current file path\n")
		os.Exit(1)
	}

	testDir := filepath.Dir(filename)
	projectRoot := filepath.Dir(testDir)
	cmdDir := filepath.Join(projectRoot, "cmd", "dbos")

	cliPath := filepath.Join(testDir, "dbos-cli-test")

	os.Remove(cliPath)

	buildCmd := exec.Command("go", "build", "-o", cliPath, ".")
	buildCmd.Dir = cmdDir
	buildOutput, buildErr := buildCmd.CombinedOutput()
	if buildErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to build CLI: %s\n", string(buildOutput))
		os.Exit(1)
	}

	absPath, err := filepath.Abs(cliPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}
	testCliPath = absPath

	startPostgresCmd := exec.Command(cliPath, "postgres", "start")
	startOutput, startErr := startPostgresCmd.CombinedOutput()
	if startErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to start postgres: %s\n", string(startOutput))
		os.Exit(1)
	}

	code := m.Run()

	os.Remove(cliPath)

	os.Exit(code)
}

func startPostgres(cliPath string) error {
	cmd := exec.Command(cliPath, "postgres", "start")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("postgres start failed: %s: %w", strings.TrimSpace(string(output)), err)
	}
	return nil
}

func stopPostgres(cliPath string) error {
	cmd := exec.Command(cliPath, "postgres", "stop")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("postgres stop failed: %s: %w", strings.TrimSpace(string(output)), err)
	}
	return nil
}

func retryCli(t *testing.T, label string, op func() error, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = op()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			require.NoError(t, lastErr, "Failed to %s after %s", label, timeout)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func PostgresChaosMonkey(t *testing.T, ctx context.Context, wg *sync.WaitGroup) {
	cliPath := testCliPath

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer t.Logf("Chaos Monkey: Exiting")

		ensureUp := func() {
			retryCli(t, "start postgres", func() error { return startPostgres(cliPath) }, 60*time.Second)
		}
		ensureDown := func() {
			retryCli(t, "stop postgres", func() error { return stopPostgres(cliPath) }, 60*time.Second)
		}

		for {

			select {
			case <-ctx.Done():
				ensureUp()
				return
			default:
			}

			downTime := time.Duration(rand.Float64()*2) * time.Second

			ensureDown()
			t.Logf("🐒 Chaos Monkey: Stopped PostgreSQL")

			select {
			case <-time.After(downTime):

				ensureUp()
				t.Logf("🐒 Chaos Monkey: Starting PostgreSQL")
			case <-ctx.Done():
				ensureUp()
				return
			}

			upTime := time.Duration(5+rand.Float64()*35) * time.Second
			select {
			case <-time.After(upTime):

			case <-ctx.Done():
				t.Logf("Chaos Monkey: Context cancelled during uptime")
				return
			}
		}
	}()
}

func setupDbos(t *testing.T) dbos.Context {
	t.Helper()

	databaseUrl := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseUrl == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseUrl = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}

	parsedUrl, err := pgx.ParseConfig(databaseUrl)
	require.NoError(t, err)

	dbName := parsedUrl.Database
	postgresUrl := parsedUrl.Copy()
	postgresUrl.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresUrl)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	err = dropDatabaseIfExists(context.Background(), conn, dbName)
	require.NoError(t, err)

	dbosCtx, err := dbos.NewDbosContext(context.Background(), dbos.Config{
		DatabaseUrl: databaseUrl,
		AppName:     "chaos-test",
		Logger:      slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)

	t.Cleanup(func() {
		if dbosCtx != nil {
			dbos.Shutdown(dbosCtx, 30*time.Second)
		}
	})

	return dbosCtx
}

func TestChaosWorkflow(t *testing.T) {
	dbosCtx := setupDbos(t)

	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	scheduledWorkflow := func(ctx dbos.Context, scheduledTime time.Time) (struct{}, error) {
		return struct{}{}, nil
	}

	stepOne := func(_ context.Context, x int) (int, error) {
		return x + 1, nil
	}

	stepTwo := func(_ context.Context, x int) (int, error) {
		return x + 2, nil
	}

	workflow := func(ctx dbos.Context, x int) (int, error) {

		x, err := dbos.Run(ctx, func(context context.Context) (int, error) {
			return stepOne(context, x)
		})
		if err != nil {
			return 0, fmt.Errorf("step one failed: %w", err)
		}

		x, err = dbos.Run(ctx, func(context context.Context) (int, error) {
			return stepTwo(context, x)
		})
		if err != nil {
			return 0, fmt.Errorf("step two failed: %w", err)
		}

		return x, nil
	}

	workflowWF := dbos.NewWorkflow(dbosCtx, workflow)

	dbos.NewWorkflow(dbosCtx, scheduledWorkflow, dbos.WithSchedule("* * * * * *"), dbos.WithWorkflowName("ScheduledChaosTest"))

	err := dbos.Start(dbosCtx)
	require.NoError(t, err)

	numWorkflows := 10000
	for i := range numWorkflows {
		if i%100 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}
		handle, err := workflowWF(dbosCtx, i)
		require.NoError(t, err, "failed to start workflow %d", i)

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)
		assert.Equal(t, i+3, result, "unexpected result for workflow %d", i)
	}

	scheduledWorkflows, err := dbos.ListWorkflows(dbosCtx,
		dbos.WithName("ScheduledChaosTest"),
		dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess}),
		dbos.WithSortDesc(),
		dbos.WithLimit(1),
		dbos.WithLoadInput(false),
		dbos.WithLoadOutput(false),
	)
	require.NoError(t, err, "failed to list scheduled workflows")

	assert.Equal(t, len(scheduledWorkflows), 1, "Expected exactly one scheduled workflow execution")

	latestWorkflow := scheduledWorkflows[0]
	timeSinceLastExecution := time.Since(latestWorkflow.CreatedAt)
	assert.Less(t, timeSinceLastExecution, 10*time.Second,
		"Last scheduled execution was %v ago, expected less than 60 seconds", timeSinceLastExecution)
}

func TestChaosRecv(t *testing.T) {
	dbosCtx := setupDbos(t)

	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	topic := "test_topic"

	numWorkflows := 10000
	signals := make([]*Event, numWorkflows)
	for i := range numWorkflows {
		signals[i] = NewEvent()
	}

	recvWorkflow := func(ctx dbos.Context, index int) (string, error) {

		signals[index].Set()

		value, err := dbos.Recv[string](ctx, topic, 10*time.Minute)
		if err != nil {
			return "", fmt.Errorf("failed to receive: %w", err)
		}
		return value, nil
	}

	recvWorkflowWF := dbos.NewWorkflow(dbosCtx, recvWorkflow)

	err := dbos.Start(dbosCtx)
	require.NoError(t, err)

	for i := range numWorkflows {
		if i%100 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}
		handle, err := recvWorkflowWF(dbosCtx, i)
		require.NoError(t, err, "failed to start workflow %d", i)

		signals[i].Wait()

		value := uuid.NewString()

		workflowId := handle.GetWorkflowId()
		err = dbos.Send(dbosCtx, workflowId, value, topic)
		require.NoError(t, err, "failed to send value for workflow %d", i)

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)
		assert.Equal(t, value, result, "unexpected result for workflow %d", i)
	}
}

func TestChaosEvents(t *testing.T) {
	dbosCtx := setupDbos(t)

	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	key := "test_key"

	eventWorkflow := func(ctx dbos.Context, _ string) (string, error) {
		value := uuid.NewString()
		err := dbos.SetEvent(ctx, key, value)
		if err != nil {
			return "", fmt.Errorf("failed to set event: %w", err)
		}
		return value, nil
	}

	eventWorkflowWF := dbos.NewWorkflow(dbosCtx, eventWorkflow)

	err := dbos.Start(dbosCtx)
	require.NoError(t, err)

	numWorkflows := 5000
	for i := range numWorkflows {
		if i%100 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}

		handle, err := eventWorkflowWF(dbosCtx, "")
		require.NoError(t, err, "failed to start workflow %d", i)
		wfId := handle.GetWorkflowId()

		value, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)

		retrievedValue, err := dbos.GetEvent[string](dbosCtx, wfId, key, 10*time.Minute)
		require.NoError(t, err, "failed to get event for workflow %d", i)
		assert.Equal(t, value, retrievedValue, "unexpected event value for workflow %d", i)
	}
}

func TestChaosQueues(t *testing.T) {
	dbosCtx := setupDbos(t)

	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	stepOne := func(ctx dbos.Context, x int) (int, error) {

		result, err := dbos.Run(ctx, func(context context.Context) (int, error) {
			return x + 1, nil
		})
		if err != nil {
			return 0, fmt.Errorf("step one failed: %w", err)
		}
		return result, nil
	}

	stepTwo := func(ctx dbos.Context, x int) (int, error) {

		result, err := dbos.Run(ctx, func(context context.Context) (int, error) {
			return x + 2, nil
		})
		if err != nil {
			return 0, fmt.Errorf("step two failed: %w", err)
		}
		return result, nil
	}

	stepOneWorkflow := dbos.NewWorkflow(dbosCtx, stepOne)
	stepTwoWorkflow := dbos.NewWorkflow(dbosCtx, stepTwo)

	workflow := func(ctx dbos.Context, x int) (int, error) {

		handle1, err := stepOneWorkflow(ctx, x)
		if err != nil {
			return 0, fmt.Errorf("failed to enqueue step one: %w", err)
		}
		x, err = handle1.GetResult()
		if err != nil {
			return 0, fmt.Errorf("failed to get result from step one: %w", err)
		}

		handle2, err := stepTwoWorkflow(ctx, x)
		if err != nil {
			return 0, fmt.Errorf("failed to enqueue step two: %w", err)
		}
		x, err = handle2.GetResult()
		if err != nil {
			return 0, fmt.Errorf("failed to get result from step two: %w", err)
		}
		return x, nil
	}

	mainWorkflow := dbos.NewWorkflow(dbosCtx, workflow)

	err := dbos.Start(dbosCtx)
	require.NoError(t, err)

	numWorkflows := 30
	for i := range numWorkflows {
		if i%10 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}

		handle, err := mainWorkflow(dbosCtx, i)
		require.NoError(t, err, "failed to enqueue workflow %d", i)

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)
		assert.Equal(t, i+3, result, "unexpected result for workflow %d", i)
	}
}
