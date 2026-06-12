package dbos

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolFromContext(t *testing.T, ctx Context) *pgxpool.Pool {
	t.Helper()
	c, ok := ctx.(*dbosContext)
	require.True(t, ok)
	s := c.kernel
	return s.pool
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

// The consolidated migration runs inside a transaction: a failure must not bump the
// recorded version.
func TestVersionNotBumpedOnMigrationFailure(t *testing.T) {
	ctx := setupDbos(t, setupDbosOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = 0")
	require.NoError(t, err)

	err = runMigrations(bg, pool, "dbos", slog.Default())
	require.Error(t, err, "re-running the consolidated migration must fail on existing objects")
	assert.Contains(t, err.Error(), "already exists")

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, int64(0), version, "version must not be bumped when the migration fails inside its tx")
}
