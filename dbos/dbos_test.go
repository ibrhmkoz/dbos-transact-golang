package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	defer verifyNoLeaks(t)
	databaseUrl := backendDatabaseUrl(t)

	t.Run("CreatesDbosContext", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-app-id")
		t.Setenv("DBOS__VMID", "test-executor-id")
		ctx, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-initialize",
		})
		require.NoError(t, err)
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		require.NotNil(t, ctx)

		var _ Context = ctx

		appVersion := ctx.GetApplicationVersion()
		assert.Equal(t, "v1.0.0", appVersion)
		executorId := ctx.GetExecutorId()
		assert.Equal(t, "test-executor-id", executorId)
		appId := ctx.GetApplicationId()
		assert.Equal(t, "test-app-id", appId)
	})

	t.Run("FailsWithoutAppName", func(t *testing.T) {
		config := Config{
			DatabaseUrl: databaseUrl,
		}

		_, err := NewDbosContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected DbosError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("FailsWithoutDatabaseURLOrSystemDBPool", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewDbosContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected DbosError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: one of databaseURL, systemDBPool, or kernel must be provided"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("ConfigApplicationVersionAndExecutorID", func(t *testing.T) {
		t.Run("UsesConfigValues", func(t *testing.T) {
			// Clear env vars to ensure we're testing config values
			t.Setenv("DBOS__APPVERSION", "")
			t.Setenv("DBOS__VMID", "")

			ctx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl:        databaseUrl,
				AppName:            "test-config-values",
				ApplicationVersion: "config-v1.2.3",
				ExecutorId:         "config-executor-123",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			assert.Equal(t, "config-v1.2.3", ctx.GetApplicationVersion())
			assert.Equal(t, "config-executor-123", ctx.GetExecutorId())
		})

		t.Run("EnvVarsOverrideConfigValues", func(t *testing.T) {
			t.Setenv("DBOS__APPVERSION", "env-v2.0.0")
			t.Setenv("DBOS__VMID", "env-executor-456")

			ctx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl:        databaseUrl,
				AppName:            "test-env-override",
				ApplicationVersion: "config-v1.2.3",
				ExecutorId:         "config-executor-123",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			assert.Equal(t, "env-v2.0.0", ctx.GetApplicationVersion())
			assert.Equal(t, "env-executor-456", ctx.GetExecutorId())
		})

		t.Run("UsesDefaultsWhenEmpty", func(t *testing.T) {

			t.Setenv("DBOS__APPVERSION", "")
			t.Setenv("DBOS__VMID", "")

			ctx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl: databaseUrl,
				AppName:     "test-defaults",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			appVersion := ctx.GetApplicationVersion()
			assert.NotEmpty(t, appVersion, "ApplicationVersion should not be empty")
			assert.NotEqual(t, "", appVersion, "ApplicationVersion should have a default value")

			executorId := ctx.GetExecutorId()
			assert.Equal(t, "local", executorId)
		})

		t.Run("EnvVarsOverrideEmptyConfig", func(t *testing.T) {
			t.Setenv("DBOS__APPVERSION", "env-only-v3.0.0")
			t.Setenv("DBOS__VMID", "env-only-executor")

			ctx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl: databaseUrl,
				AppName:     "test-env-only",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			assert.Equal(t, "env-only-v3.0.0", ctx.GetApplicationVersion())
			assert.Equal(t, "env-only-executor", ctx.GetExecutorId())
		})
	})

	t.Run("SystemDBMigration", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-migration")
		t.Setenv("DBOS__VMID", "test-executor-id")

		ctx, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-migration",
		})
		require.NoError(t, err)
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		require.NotNil(t, ctx)

		dbosCtx, ok := ctx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtx.kernel)

		Kernel := dbosCtx.kernel

		dbCtx := context.Background()

		var exists bool
		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_status table should exist")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "operation_outputs table should exist")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_events')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_events table should exist")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'notifications')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "notifications table should exist")

		rows, err := Kernel.pool.Query(dbCtx, "SELECT workflow_uuid FROM dbos.workflow_status LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = Kernel.pool.Query(dbCtx, "SELECT workflow_uuid FROM dbos.operation_outputs LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = Kernel.pool.Query(dbCtx, "SELECT workflow_uuid FROM dbos.workflow_events LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = Kernel.pool.Query(dbCtx, "SELECT destination_uuid FROM dbos.notifications LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'dbos_migrations')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "dbos_migrations table should exist")

		var version int64
		var count int
		err = Kernel.pool.QueryRow(dbCtx, "SELECT COUNT(*) FROM dbos.dbos_migrations").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT version FROM dbos.dbos_migrations").Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(41), version, "migration version should be 41 (latest migration: add error_encoded)")

		Shutdown(ctx, 1*time.Minute)

		ctx2, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-migration-recreate",
		})
		require.NoError(t, err)
		defer func() {
			if ctx2 != nil {
				Shutdown(ctx2, 1*time.Minute)
			}
		}()

		require.NotNil(t, ctx2)
	})

	t.Run("KeyValueFormatConnectionString", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-keyvalue-format")
		t.Setenv("DBOS__VMID", "test-executor-id")

		originalUrl := databaseUrl
		parsedUrl, err := pgxpool.ParseConfig(originalUrl)
		require.NoError(t, err)

		user := parsedUrl.ConnConfig.User
		database := parsedUrl.ConnConfig.Database
		host := parsedUrl.ConnConfig.Host
		port := parsedUrl.ConnConfig.Port

		testPassword := "TEST_PASSWORD_UNIQUE_12345!@#$%"

		maskingTestCases := []struct {
			name    string
			connStr string
		}{
			{"NoSpaces", fmt.Sprintf("user=%s password=%s database=%s host=%s", user, testPassword, database, host)},
			{"SpaceBeforeEquals", fmt.Sprintf("user=%s password =%s database=%s host=%s", user, testPassword, database, host)},
			{"SpaceAfterEquals", fmt.Sprintf("user=%s password= %s database=%s host=%s", user, testPassword, database, host)},
			{"SpacesBothSides", fmt.Sprintf("user=%s password = %s database=%s host=%s", user, testPassword, database, host)},
			{"UppercaseKey", fmt.Sprintf("user=%s PASSWORD=%s database=%s host=%s", user, testPassword, database, host)},
			{"MixedCaseKey", fmt.Sprintf("user=%s Password=%s database=%s host=%s", user, testPassword, database, host)},
		}

		portSSL := ""
		if port != 0 {
			portSSL += fmt.Sprintf(" port=%d", port)
		}
		if strings.Contains(originalUrl, "sslmode=disable") {
			portSSL += " sslmode=disable"
		}
		for i := range maskingTestCases {
			maskingTestCases[i].connStr += portSSL
		}

		for _, tc := range maskingTestCases {
			t.Run("Masking_"+tc.name, func(t *testing.T) {
				masked, err := maskPassword(tc.connStr)
				require.NoError(t, err)
				assert.Contains(t, masked, "***", "password should be masked")
				passwordPattern := fmt.Sprintf("password=%s", testPassword)
				assert.NotContains(t, strings.ToLower(masked), strings.ToLower(passwordPattern), "password should not appear in plaintext")
			})
		}

		t.Run("DbosContextCreation", func(t *testing.T) {

			actualPassword := parsedUrl.ConnConfig.Password
			var keyValueConnStr string
			if actualPassword == "" {
				keyValueConnStr = fmt.Sprintf("user='%s' database=%s host=%s%s", user, database, host, portSSL)
			} else {
				keyValueConnStr = fmt.Sprintf("user='%s' password='%s' database=%s host=%s%s", user, actualPassword, database, host, portSSL)
			}

			ctx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl: keyValueConnStr,
				AppName:     "test-keyvalue-format",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			require.NotNil(t, ctx)

			dbosCtx, ok := ctx.(*dbosContext)
			require.True(t, ok)
			Kernel := dbosCtx.kernel

			var exists bool
			err = Kernel.pool.QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')").Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists)

			poolConnStr := Kernel.pool.Config().ConnString()
			maskedConnStr, err := maskPassword(poolConnStr)
			require.NoError(t, err)
			if actualPassword == "" {
				assert.NotContains(t, maskedConnStr, "password=")
			} else {
				assert.Contains(t, maskedConnStr, "password=***")
				assert.NotContains(t, maskedConnStr, fmt.Sprintf("password=%s", actualPassword))

			}
		})
	})

}

func TestContext(t *testing.T) {
	databaseUrl := backendDatabaseUrl(t)

	t.Run("PreservesContextValues", func(t *testing.T) {

		type contextKey string
		key1 := contextKey("test-key-1")
		key2 := contextKey("test-key-2")
		value1 := "test-value-1"
		value2 := 42

		baseCtx := context.Background()
		ctxWithValues := context.WithValue(baseCtx, key1, value1)
		ctxWithValues = context.WithValue(ctxWithValues, key2, value2)

		dbosCtx, err := NewDbosContext(ctxWithValues, Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-context-values",
		})
		require.NoError(t, err)
		defer func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 1*time.Minute)
			}
		}()

		require.NotNil(t, dbosCtx)

		assert.Equal(t, value1, dbosCtx.Value(key1), "DbosContext should preserve context value for key1")
		assert.Equal(t, value2, dbosCtx.Value(key2), "DbosContext should preserve context value for key2")

		nonExistentKey := contextKey("non-existent-key")
		assert.Nil(t, dbosCtx.Value(nonExistentKey), "DbosContext should return nil for non-existent keys")
	})

	t.Run("FromPreservesDerivedContextValues", func(t *testing.T) {
		type contextKey string
		key1 := contextKey("from-test-key-1")
		key2 := contextKey("from-test-key-2")
		key3 := contextKey("from-test-key-3")
		value1 := "old-value-1"
		value2 := 100
		value3 := "new-value-3"

		baseCtx := context.Background()
		baseCtx = context.WithValue(baseCtx, key1, value1)
		baseCtx = context.WithValue(baseCtx, key2, value2)
		derivedCtx := context.WithValue(baseCtx, key3, value3)

		dbosCtx, err := NewDbosContext(baseCtx, Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-context-from",
		})
		require.NoError(t, err)
		defer func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 1*time.Minute)
			}
		}()
		require.NotNil(t, dbosCtx)

		fromCtx := From(dbosCtx, derivedCtx)
		require.NotNil(t, fromCtx)

		// Value must return all values: from the base (old) and from the derived (new)
		assert.Equal(t, value1, fromCtx.Value(key1), "From DBOS context should return value from ancestor context")
		assert.Equal(t, value2, fromCtx.Value(key2), "From DBOS context should return value from ancestor context")
		assert.Equal(t, value3, fromCtx.Value(key3), "From DBOS context should return value from derived context")
	})
}

func TestCustomSystemDBSchema(t *testing.T) {
	defer verifyNoLeaks(t)
	t.Setenv("DBOS__APPVERSION", "v1.0.0")
	t.Setenv("DBOS__APPID", "test-custom-schema")
	t.Setenv("DBOS__VMID", "test-executor-id")

	databaseUrl := backendDatabaseUrl(t)
	customSchema := "dbos_custom_test"

	ctx, err := NewDbosContext(context.Background(), Config{
		DatabaseUrl:    databaseUrl,
		AppName:        "test-custom-schema-migration",
		DatabaseSchema: customSchema,
	})
	require.NoError(t, err)
	defer func() {
		if ctx != nil {
			Shutdown(ctx, 1*time.Minute)
		}
	}()

	require.NotNil(t, ctx)

	t.Run("CustomSchemaSetup", func(t *testing.T) {

		dbosCtx, ok := ctx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtx.kernel)

		Kernel := dbosCtx.kernel

		assert.Equal(t, customSchema, Kernel.schema, "schema name should match custom schema")

		dbCtx := context.Background()

		var exists bool
		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_status')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_status table should exist in custom schema")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'operation_outputs')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "operation_outputs table should exist in custom schema")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_events')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_events table should exist in custom schema")

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'notifications')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "notifications table should exist in custom schema")

		rows, err := Kernel.pool.Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.workflow_status LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = Kernel.pool.Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.operation_outputs LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = Kernel.pool.Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.workflow_events LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = Kernel.pool.Query(dbCtx, fmt.Sprintf("SELECT destination_uuid FROM %s.notifications LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		err = Kernel.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'dbos_migrations')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "dbos_migrations table should exist in custom schema")

		var version int64
		var count int
		err = Kernel.pool.QueryRow(dbCtx, fmt.Sprintf("SELECT COUNT(*) FROM %s.dbos_migrations", customSchema)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = Kernel.pool.QueryRow(dbCtx, fmt.Sprintf("SELECT version FROM %s.dbos_migrations", customSchema)).Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(41), version, "migration version should be 41 (latest migration: add error_encoded)")
	})

	type testWorkflowInput struct {
		PartnerWorkflowId string
		Message           string
	}

	var workflowBReadyEvent *Event

	sendGetEventWorkflow := func(ctx Context, input testWorkflowInput) (string, error) {

		err := Send(ctx, input.PartnerWorkflowId, input.Message, "test-topic")
		if err != nil {
			return "", err
		}

		result, err := GetEvent[string](ctx, input.PartnerWorkflowId, "response-key", 5*time.Hour)
		if err != nil {
			return "", err
		}

		return result, nil
	}

	recvSetEventWorkflow := func(ctx Context, input testWorkflowInput) (string, error) {

		if workflowBReadyEvent != nil {
			workflowBReadyEvent.Set()
		}

		receivedMsg, err := Recv[string](ctx, "test-topic", 5*time.Hour)
		if err != nil {
			return "", err
		}

		err = SetEvent(ctx, "response-key", "response-from-workflow-b")
		if err != nil {
			return "", err
		}

		return receivedMsg, nil
	}

	t.Run("CustomSchemaUsage", func(t *testing.T) {

		workflowBReadyEvent = NewEvent()

		sendGetEventWF := NewWorkflow(ctx, sendGetEventWorkflow)
		recvSetEventWF := NewWorkflow(ctx, recvSetEventWorkflow)

		Start(ctx)

		// Start workflow B first (receiver); it does not need its partner's ID
		handleB, err := recvSetEventWF(ctx, testWorkflowInput{
			Message: "test-message-from-b",
		})
		require.NoError(t, err, "failed to start recvSetEventWorkflow")
		workflowBId := handleB.GetWorkflowId()

		workflowBReadyEvent.Wait()

		handleA, err := sendGetEventWF(ctx, testWorkflowInput{
			PartnerWorkflowId: workflowBId,
			Message:           "test-message-from-a",
		})
		require.NoError(t, err, "failed to start sendGetEventWorkflow")
		workflowAId := handleA.GetWorkflowId()

		resultA, err := handleA.GetResult()
		require.NoError(t, err, "failed to get result from workflow A")
		assert.Equal(t, "response-from-workflow-b", resultA, "workflow A should receive response from workflow B")

		resultB, err := handleB.GetResult()
		require.NoError(t, err, "failed to get result from workflow B")
		assert.Equal(t, "test-message-from-a", resultB, "workflow B should receive message from workflow A")

		stepsA, err := GetWorkflowSteps(ctx, workflowAId)
		require.NoError(t, err, "failed to get workflow A steps")
		require.GreaterOrEqual(t, len(stepsA), 2, "workflow A should have at least 2 steps")
		require.LessOrEqual(t, len(stepsA), 3, "workflow A should have at most 3 steps")
		assert.Equal(t, "DBOS.send", stepsA[0].StepName, "first step should be Send")

		foundGetEvent := false
		for i := 1; i < len(stepsA); i++ {
			if stepsA[i].StepName == "DBOS.getEvent" {
				foundGetEvent = true
				break
			}
		}
		assert.True(t, foundGetEvent, "workflow A should have GetEvent step")

		stepsB, err := GetWorkflowSteps(ctx, workflowBId)
		require.NoError(t, err, "failed to get workflow B steps")
		require.GreaterOrEqual(t, len(stepsB), 2, "workflow B should have at least 2 steps")
		require.LessOrEqual(t, len(stepsB), 3, "workflow B should have at most 3 steps")
		assert.Equal(t, "DBOS.recv", stepsB[0].StepName, "first step should be Recv")

		foundSetEvent := false
		for i := 1; i < len(stepsB); i++ {
			if stepsB[i].StepName == "DBOS.setEvent" {
				foundSetEvent = true
				break
			}
		}
		assert.True(t, foundSetEvent, "workflow B should have SetEvent step")
	})
}

func TestCustomPool(t *testing.T) {
	defer verifyNoLeaks(t)

	type customPoolWorkflowInput struct {
		PartnerWorkflowId string
		Message           string
	}

	sendGetEventWorkflowCustom := func(ctx Context, input customPoolWorkflowInput) (string, error) {

		err := Send(ctx, input.PartnerWorkflowId, input.Message, "custom-pool-topic")
		if err != nil {
			return "", err
		}

		result, err := GetEvent[string](ctx, input.PartnerWorkflowId, "custom-response-key", 5*time.Hour)
		if err != nil {
			return "", err
		}

		return result, nil
	}

	recvSetEventWorkflowCustom := func(ctx Context, input customPoolWorkflowInput) (string, error) {

		receivedMsg, err := Recv[string](ctx, "custom-pool-topic", 5*time.Hour)
		if err != nil {
			return "", err
		}

		time.Sleep(1 * time.Second)

		err = SetEvent(ctx, "custom-response-key", "response-from-custom-pool-workflow")
		if err != nil {
			return "", err
		}

		return receivedMsg, nil
	}

	t.Run("CustomPool", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		poolConfig, err := pgxpool.ParseConfig(databaseUrl)
		require.NoError(t, err)

		poolConfig.MaxConns = 10
		poolConfig.MinConns = 5
		poolConfig.MaxConnLifetime = 2 * time.Hour
		poolConfig.MaxConnIdleTime = time.Minute * 2

		poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second
		// A custom pool owns its connection settings: DBOS cannot inject the
		// search_path, so the pool must route unqualified queries to the DBOS schema.
		poolConfig.ConnConfig.RuntimeParams = map[string]string{"search_path": "dbos"}

		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)

		config := Config{
			AppName:      "test-custom-pool",
			SystemDBPool: pool,
		}

		customdbosContext, err := NewDbosContext(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, customdbosContext)

		dbosCtx, ok := customdbosContext.(*dbosContext)
		defer Shutdown(dbosCtx, 10*time.Second)
		require.True(t, ok)

		Kernel := dbosCtx.kernel
		assert.Same(t, pool, Kernel.pool, "The pool in dbosContext should be the same as the custom pool provided")

		stats := Kernel.pool.Stat()
		assert.Equal(t, int32(10), stats.MaxConns(), "MaxConns should match custom pool config")

		sysdbConfig := Kernel.pool.Config()
		assert.Equal(t, int32(10), sysdbConfig.MaxConns)
		assert.Equal(t, int32(5), sysdbConfig.MinConns)
		assert.Equal(t, 2*time.Hour, sysdbConfig.MaxConnLifetime)
		assert.Equal(t, 2*time.Minute, sysdbConfig.MaxConnIdleTime)
		assert.Equal(t, 10*time.Second, sysdbConfig.ConnConfig.ConnectTimeout)

		sendGetEventCustomWF := NewWorkflow(customdbosContext, sendGetEventWorkflowCustom)
		recvSetEventCustomWF := NewWorkflow(customdbosContext, recvSetEventWorkflowCustom)

		err = Start(customdbosContext)
		require.NoError(t, err)
		defer Shutdown(dbosCtx, 1*time.Minute)

		// Start workflow B first (receiver); it does not need its partner's ID
		handleB, err := recvSetEventCustomWF(customdbosContext, customPoolWorkflowInput{
			Message: "custom-pool-message-from-b",
		})
		require.NoError(t, err, "failed to start recvSetEventWorkflowCustom")
		workflowBId := handleB.GetWorkflowId()

		// Small delay to ensure workflow B is ready to receive
		time.Sleep(100 * time.Millisecond)

		handleA, err := sendGetEventCustomWF(customdbosContext, customPoolWorkflowInput{
			PartnerWorkflowId: workflowBId,
			Message:           "custom-pool-message-from-a",
		})
		require.NoError(t, err, "failed to start sendGetEventWorkflowCustom")
		workflowAId := handleA.GetWorkflowId()

		resultA, err := handleA.GetResult()
		require.NoError(t, err, "failed to get result from workflow A")
		assert.Equal(t, "response-from-custom-pool-workflow", resultA, "workflow A should receive response from workflow B")

		resultB, err := handleB.GetResult()
		require.NoError(t, err, "failed to get result from workflow B")
		assert.Equal(t, "custom-pool-message-from-a", resultB, "workflow B should receive message from workflow A")

		stepsA, err := GetWorkflowSteps(customdbosContext, workflowAId)
		require.NoError(t, err, "failed to get workflow A steps")
		require.Len(t, stepsA, 3, "workflow A should have 3 steps (Send + GetEvent + Sleep)")
		assert.Equal(t, "DBOS.send", stepsA[0].StepName, "first step should be Send")
		assert.Equal(t, "DBOS.getEvent", stepsA[1].StepName, "second step should be GetEvent")
		assert.Equal(t, "DBOS.sleep", stepsA[2].StepName, "third step should be Sleep")

		stepsB, err := GetWorkflowSteps(customdbosContext, workflowBId)
		require.NoError(t, err, "failed to get workflow B steps")
		require.Len(t, stepsB, 3, "workflow B should have 3 steps (Recv + Sleep + SetEvent)")
		assert.Equal(t, "DBOS.recv", stepsB[0].StepName, "first step should be Recv")
		assert.Equal(t, "DBOS.sleep", stepsB[1].StepName, "second step should be Sleep")
		assert.Equal(t, "DBOS.setEvent", stepsB[2].StepName, "third step should be SetEvent")
	})

	wf := func(ctx Context, input string) (string, error) {
		return input, nil
	}

	t.Run("CustomPoolTakesPrecedence", func(t *testing.T) {
		invalidDatabaseUrl := "postgres://invalid:invalid@localhost:5432/invaliddb"
		databaseUrl := backendDatabaseUrl(t)
		poolConfig, err := pgxpool.ParseConfig(databaseUrl)
		require.NoError(t, err)
		// A custom pool owns its connection settings: DBOS cannot inject the
		// search_path, so the pool must route unqualified queries to the DBOS schema.
		poolConfig.ConnConfig.RuntimeParams = map[string]string{"search_path": "dbos"}
		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)

		config := Config{
			DatabaseUrl:  invalidDatabaseUrl,
			AppName:      "test-invalid-db-url",
			SystemDBPool: pool,
		}
		dbosCtx, err := NewDbosContext(context.Background(), config)
		require.NoError(t, err)

		wfDef := NewWorkflow(dbosCtx, wf)

		err = Start(dbosCtx)
		require.NoError(t, err)
		defer Shutdown(dbosCtx, 1*time.Minute)

		_, err = wfDef(dbosCtx, "test-input")
		require.NoError(t, err)
	})

	t.Run("InvalidCustomPool", func(t *testing.T) {
		databaseUrl := backendDatabaseUrl(t)
		poolConfig, err := pgxpool.ParseConfig(databaseUrl)
		require.NoError(t, err)
		poolConfig.ConnConfig.Host = "invalid-host"
		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)

		config := Config{
			DatabaseUrl:  databaseUrl,
			AppName:      "test-invalid-custom-pool",
			SystemDBPool: pool,
		}
		_, err = NewDbosContext(context.Background(), config)
		require.Error(t, err)
		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected DbosError, got %T", err)
		assert.Equal(t, InitializationError, dbosErr.Code)
		expectedMsg := "Error initializing DBOS Transact: failed to validate custom pool"
		assert.Contains(t, dbosErr.Message, expectedMsg)
	})

	t.Run("DirectKernel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		databaseUrl := backendDatabaseUrl(t)
		logger := slog.Default()

		poolConfig, err := pgxpool.ParseConfig(databaseUrl)
		require.NoError(t, err)
		poolConfig.MaxConns = 15
		poolConfig.MinConns = 3
		customPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		require.NoError(t, err)
		defer customPool.Close()

		kernelConfig := KernelConfig{
			DatabaseUrl:    databaseUrl,
			DatabaseSchema: "dbos_test_custom_direct",
			SystemDBPool:   customPool,
			Logger:         logger,
		}

		kernel, err := NewKernel(ctx, kernelConfig)
		require.NoError(t, err, "failed to create system database with custom pool")
		require.NotNil(t, kernel)

		kernel.Start()

		require.Eventually(t, func() bool {
			conn, err := kernel.pool.Acquire(ctx)
			require.NoError(t, err)
			defer conn.Release()
			err = conn.Ping(ctx)
			require.NoError(t, err)
			return true
		}, 5*time.Second, 100*time.Millisecond, "system database should be reachable")

		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		require.NoError(t, kernel.Shutdown(shutdownCtx))
		assert.Nil(t, kernel.loopCancel)
	})
}
