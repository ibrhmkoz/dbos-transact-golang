package dbos

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogger(t *testing.T) {
	defer verifyNoLeaks(t)
	databaseUrl := backendDatabaseUrl(t)

	t.Run("Default logger", func(t *testing.T) {
		dbosCtx, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-app",
		})
		require.NoError(t, err)
		err = Start(dbosCtx)
		require.NoError(t, err)
		t.Cleanup(func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 10*time.Second)
			}
		})

		ctx, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "Expected dbosCtx to be of type *dbosContext")
		require.NotNil(t, ctx.logger)

		ctx.logger.Info("Test message from default logger")

	})

	t.Run("Custom logger", func(t *testing.T) {

		var buf bytes.Buffer
		slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		slogLogger = slogLogger.With("service", "dbos-test", "environment", "test")

		dbosCtx, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl: databaseUrl,
			AppName:     "test-app",
			Logger:      slogLogger,
		})
		require.NoError(t, err)
		err = Start(dbosCtx)
		require.NoError(t, err)
		t.Cleanup(func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 10*time.Second)
			}
		})

		ctx := dbosCtx.(*dbosContext)
		require.NotNil(t, ctx.logger)

		ctx.logger.Info("Test message from custom logger", "test_key", "test_value")

		logOutput := buf.String()
		assert.Contains(t, logOutput, "service=dbos-test", "Expected log output to contain service=dbos-test")
		assert.Contains(t, logOutput, "environment=test", "Expected log output to contain environment=test")
		assert.Contains(t, logOutput, "test_key=test_value", "Expected log output to contain test_key=test_value")
	})
}
