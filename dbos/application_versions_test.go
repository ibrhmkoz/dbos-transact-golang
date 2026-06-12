package dbos

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApplicationVersions(t *testing.T) {
	parallelTest(t)
	t.Run("StartRegistersCurrentVersion", func(t *testing.T) {
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
		require.NoError(t, dbosCtx.Start())

		latest, err := GetLatestApplicationVersion(dbosCtx)
		require.NoError(t, err)
		require.NotNil(t, latest)
		require.Equal(t, dbosCtx.GetApplicationVersion(), latest.Name)

		versions, err := ListApplicationVersions(dbosCtx)
		require.NoError(t, err)
		require.Len(t, versions, 1)
		require.Equal(t, latest.Name, versions[0].Name)
		require.Equal(t, latest.Id, versions[0].Id)
	})

	t.Run("CreateIsIdempotent", func(t *testing.T) {
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
		require.NoError(t, dbosCtx.Start())

		c := dbosCtx.(*dbosContext)
		// Re-registering the same version must not create a duplicate row.
		require.NoError(t, c.kernel.createApplicationVersion(c, c.applicationVersion))
		require.NoError(t, c.kernel.createApplicationVersion(c, c.applicationVersion))

		versions, err := ListApplicationVersions(dbosCtx)
		require.NoError(t, err)
		require.Len(t, versions, 1)
	})

	t.Run("SetLatestUpdatesTimestamp", func(t *testing.T) {
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
		require.NoError(t, dbosCtx.Start())

		c := dbosCtx.(*dbosContext)

		require.NoError(t, c.kernel.createApplicationVersion(c, "older-version"))
		require.NoError(t, c.kernel.updateApplicationVersionTimestamp(c, "older-version", time.Now().Add(-time.Hour).UnixMilli()))

		latest, err := GetLatestApplicationVersion(dbosCtx)
		require.NoError(t, err)
		require.Equal(t, dbosCtx.GetApplicationVersion(), latest.Name)

		require.NoError(t, SetLatestApplicationVersion(dbosCtx, "older-version"))

		latest, err = GetLatestApplicationVersion(dbosCtx)
		require.NoError(t, err)
		require.Equal(t, "older-version", latest.Name)

		versions, err := ListApplicationVersions(dbosCtx)
		require.NoError(t, err)
		require.Len(t, versions, 2)
		require.Equal(t, "older-version", versions[0].Name)
	})

	t.Run("GetLatestReturnsErrWhenEmpty", func(t *testing.T) {
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})

		require.NoError(t, dbosCtx.Start())
		c := dbosCtx.(*dbosContext)
		s := c.kernel
		_, err := s.pool.Exec(c, s.renderSql("DELETE FROM %sapplication_versions", ""))
		require.NoError(t, err)

		_, err = GetLatestApplicationVersion(dbosCtx)
		require.Error(t, err)
		var dbosErr *DbosError
		require.True(t, errors.As(err, &dbosErr), "expected *DbosError, got %T: %v", err, err)
		require.Equal(t, NoApplicationVersions, dbosErr.Code)
	})

	t.Run("SetLatestRequiresVersionName", func(t *testing.T) {
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true})
		require.NoError(t, dbosCtx.Start())

		err := SetLatestApplicationVersion(dbosCtx, "")
		require.Error(t, err)
	})
}
