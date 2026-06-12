package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolFromContext(t *testing.T, ctx DbosContext) *pgxpool.Pool {
	t.Helper()
	c, ok := ctx.(*dbosContext)
	require.True(t, ok)
	s := c.kernel
	return PgxPool(s.pool)
}

func TestShouldMigrate(t *testing.T) {
	ctx := setupDbos(t, setupDbosOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()
	migs := buildMigrations("dbos")
	latest := migs[len(migs)-1].version

	need, err := shouldMigrate(bg, pool, "dbos")
	require.NoError(t, err)
	assert.False(t, need, "fully migrated schema should not need migration")

	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", latest-1)
	require.NoError(t, err)
	need, err = shouldMigrate(bg, pool, "dbos")
	require.NoError(t, err)
	assert.True(t, need, "rewound schema should need migration")

	// initialised schema. shouldMigrate must report True.
	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", latest)
	require.NoError(t, err)
	need, err = shouldMigrate(bg, pool, "dbos")
	require.NoError(t, err)
	assert.False(t, need)

	_, err = pool.Exec(bg, "DROP TABLE dbos.dbos_migrations")
	require.NoError(t, err)
	need, err = shouldMigrate(bg, pool, "dbos")
	require.NoError(t, err)
	assert.True(t, need, "missing migration table should need migration")

	// A schema that does not exist should also need migration.
	need, err = shouldMigrate(bg, pool, "nonexistent_schema_xyz")
	require.NoError(t, err)
	assert.True(t, need, "nonexistent schema should need migration")
}

// migration must include IF [NOT] EXISTS guards so that re-running them

func TestOnlineMigrationsAreIdempotent(t *testing.T) {
	ctx := setupDbos(t, setupDbosOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	const rewindTo = int64(21)
	migs := buildMigrations("dbos")
	latest := migs[len(migs)-1].version

	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	logger := slog.Default()
	require.NoError(t, runMigrations(bg, pool, "dbos", logger))

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

func TestVersionNotBumpedOnMigrationFailure(t *testing.T) {
	ctx := setupDbos(t, setupDbosOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()
	migs := buildMigrations("dbos")
	latest := migs[len(migs)-1].version

	const rewindTo = int64(20)
	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	err = runMigrations(bg, pool, "dbos", slog.Default())
	require.Error(t, err, "migration 21 should fail because dbos.queues already exists")
	assert.Contains(t, err.Error(), "already exists")

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, rewindTo, version, "version should still be 20 (migration 21 failed inside its tx)")

	_, err = pool.Exec(bg, "DROP TABLE dbos.queues")
	require.NoError(t, err)
	require.NoError(t, runMigrations(bg, pool, "dbos", slog.Default()))
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

func TestRunnerResumesAfterInvalidIndex(t *testing.T) {
	ctx := setupDbos(t, setupDbosOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	const targetIndex = "idx_workflow_status_in_flight"
	const rewindTo = int64(31)
	migs := buildMigrations("dbos")
	latest := migs[len(migs)-1].version

	_, err := pool.Exec(bg, fmt.Sprintf(`DROP INDEX IF EXISTS dbos.%q`, targetIndex))
	require.NoError(t, err)
	_, err = pool.Exec(bg, fmt.Sprintf(
		`CREATE INDEX %q ON dbos.workflow_status (queue_name, status, priority, created_at) WHERE status IN ('ENQUEUED', 'PENDING')`,
		targetIndex))
	require.NoError(t, err)
	_, err = pool.Exec(bg, fmt.Sprintf(
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'dbos.%s'::regclass`,
		targetIndex))
	require.NoError(t, err)

	var valid bool
	require.NoError(t, pool.QueryRow(bg,
		fmt.Sprintf(`SELECT indisvalid FROM pg_index WHERE indexrelid = 'dbos.%s'::regclass`, targetIndex)).Scan(&valid))
	assert.False(t, valid)

	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	require.NoError(t, runMigrations(bg, pool, "dbos", slog.Default()))

	require.NoError(t, pool.QueryRow(bg,
		fmt.Sprintf(`SELECT indisvalid FROM pg_index WHERE indexrelid = 'dbos.%s'::regclass`, targetIndex)).Scan(&valid))
	assert.True(t, valid, "index should be valid after cleanup + rebuild")

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}
