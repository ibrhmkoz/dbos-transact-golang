package dbos

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// dialect.go contains PostgreSQL SQL fragments and error classification.

// DialectName identifies the backend. Stable string suitable for logging.
type DialectName string

const (
	DialectPostgres DialectName = "postgres"
)

// Dialect encapsulates per-backend SQL fragments and behaviours.
type Dialect interface {
	// Name returns a stable identifier for the dialect.
	Name() DialectName

	// SchemaPrefix returns the qualified-table prefix, e.g. `"dbos".` for
	// Includes the trailing dot.
	SchemaPrefix(schema string) string

	// RewriteQuery converts a canonical Postgres-style query (with $N
	// placeholders and a "%s" schema-prefix slot already rendered) into the
	// PostgreSQL native form. This is currently a no-op.
	RewriteQuery(query string) string

	// LockSkipLocked returns the "FOR UPDATE SKIP LOCKED" fragment, or "" for
	// PostgreSQL row-level locking fragment.
	LockSkipLocked() string

	// LockNoWait returns the "FOR UPDATE NOWAIT" fragment, or "".
	LockNoWait() string

	// SnapshotIsolation returns the IsoLevel to request when a transaction
	// needs snapshot-style semantics for queue dequeue. Postgres returns
	// RepeatableRead.
	SnapshotIsolation() IsoLevel

	// QueueDequeueIsolation returns the IsoLevel for the queue dequeue
	// transaction. snapshot=true requests snapshot semantics
	// snapshot=false allows the lighter read-committed path.
	ClaimIsolation(snapshot bool) IsoLevel

	// SupportsListenNotify reports whether the dialect supports
	// LISTEN/NOTIFY.
	SupportsListenNotify() bool

	// SupportsArrayParameters reports whether the dialect can bind a Go slice
	// as a single array-typed parameter (e.g. pg's `= ANY($1)` with $1=[]string).
	// PostgreSQL array parameters.
	SupportsArrayParameters() bool

	// SupportsDataModifyingCTE reports whether a CTE term may be an
	// INSERT/UPDATE/DELETE.
	SupportsDataModifyingCTE() bool

	// IsUniqueViolation reports whether err represents a unique-constraint
	// violation surfaced from the driver.
	IsUniqueViolation(err error) bool

	// IsForeignKeyViolation reports whether err represents a foreign-key
	// violation surfaced from the driver.
	IsForeignKeyViolation(err error) bool

	// IsRetryable reports whether err is a transient driver error that the
	// retry helper should re-attempt. The logger is optional; implementations
	// may emit a debug/warning describing why the retry was triggered. The
	// signature matches retryConfig.retryConditionChain so a method value can
	// be inserted into the chain directly.
	IsRetryable(err error, logger *slog.Logger) bool
}

// detectDialect identifies the backend from a DBOS database URL by parsing
// the scheme.
//
// Recognised schemes:
//
//	postgres:, postgresql: → DialectPostgres
func detectDialect(rawURL string) (DialectName, error) {
	if rawURL == "" {
		return "", fmt.Errorf("database URL is empty")
	}
	// libpq key=value DSNs (e.g. "user='User Name#$%&!' host=localhost ...")
	// can contain characters that url.Parse rejects as invalid URL escapes.
	// Detect this form before falling through to url.Parse so DSNs with funny
	// characters in quoted values still route to Postgres.
	if looksLikePostgresKVDSN(rawURL) {
		return DialectPostgres, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid database URL: %v", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "postgres", "postgresql":
		return DialectPostgres, nil
	case "":
		return "", fmt.Errorf("database URL has no scheme: %q", rawURL)
	default:
		return "", fmt.Errorf("unsupported database scheme %q (want postgres:)", u.Scheme)
	}
}

// looksLikePostgresKVDSN matches the libpq key=value connection-string form by
// checking for a leading canonical keyword. It is intentionally permissive: we
// only need to distinguish kv-DSN from outright garbage so we can route it to
// Postgres; the actual parse still happens in pgxpool.ParseConfig.
func looksLikePostgresKVDSN(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	for _, prefix := range []string{
		"user=", "host=", "hostaddr=", "port=", "dbname=", "database=",
		"password=", "sslmode=", "application_name=", "options=",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

/* ---------------------------------------------------------------------------
   Postgres
   ------------------------------------------------------------------------- */

type postgresDialect struct{}

func (postgresDialect) Name() DialectName { return DialectPostgres }
func (postgresDialect) SchemaPrefix(schema string) string {
	return pgx.Identifier{schema}.Sanitize() + "."
}
func (postgresDialect) RewriteQuery(q string) string { return q }
func (postgresDialect) LockSkipLocked() string       { return "FOR UPDATE SKIP LOCKED" }
func (postgresDialect) LockNoWait() string           { return "FOR UPDATE NOWAIT" }
func (postgresDialect) SnapshotIsolation() IsoLevel  { return IsoLevelRepeatableRead }
func (postgresDialect) ClaimIsolation(snapshot bool) IsoLevel {
	if snapshot {
		return IsoLevelRepeatableRead
	}
	return IsoLevelReadCommitted
}
func (postgresDialect) SupportsListenNotify() bool     { return true }
func (postgresDialect) SupportsArrayParameters() bool  { return true }
func (postgresDialect) SupportsDataModifyingCTE() bool { return true }

// pgErrCode extracts the SQLSTATE code from a pgconn.PgError, or "" if err is
// not a pg error.
func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func (postgresDialect) IsUniqueViolation(err error) bool {
	return pgErrCode(err) == pgerrcode.UniqueViolation
}
func (postgresDialect) IsForeignKeyViolation(err error) bool {
	return pgErrCode(err) == pgerrcode.ForeignKeyViolation
}

// IsRetryable matches transient PostgreSQL driver errors that can be safely
// retried: closed transaction handles, connection-level SQLSTATEs, pgx
// connect failures, EOF/closed-conn strings, and net.Error. Serialization /
// lock-not-available SQLSTATEs are intentionally excluded because those require
// retrying the entire transaction.
func (postgresDialect) IsRetryable(err error, logger *slog.Logger) bool {
	if err == nil {
		return false
	}
	// pgx surfaces ErrTxClosed for ops on a tx that has already finalized
	// the caller must retry with a fresh tx.
	if errors.Is(err, pgx.ErrTxClosed) {
		if logger != nil {
			logger.Warn("Transaction is closed, retrying requires a new transaction object", "error", err)
		}
		return true
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		switch pgerr.Code {
		case pgerrcode.ConnectionException,
			pgerrcode.ConnectionDoesNotExist,
			pgerrcode.ConnectionFailure,
			pgerrcode.SQLClientUnableToEstablishSQLConnection,
			pgerrcode.SQLServerRejectedEstablishmentOfSQLConnection,
			pgerrcode.AdminShutdown,
			pgerrcode.CrashShutdown,
			pgerrcode.CannotConnectNow:
			return true
		}
	}
	var cerr *pgconn.ConnectError
	if errors.As(err, &cerr) {
		return true
	}
	if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "conn closed") {
		return true
	}
	var nerr net.Error
	return errors.As(err, &nerr)
}

func dialectAnyClause(d Dialect, column string, placeholderIdx int) string {
	return fmt.Sprintf("%s = ANY($%d)", column, placeholderIdx)
}

func dialectLikeAnyClause(d Dialect, column string, placeholderIdx int) string {
	return fmt.Sprintf("%s LIKE ANY($%d)", column, placeholderIdx)
}

func encodeArrayParam(d Dialect, slice any) (any, error) {
	return slice, nil
}
