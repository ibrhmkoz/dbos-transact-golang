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

func validateDatabaseUrl(rawUrl string) error {
	if rawUrl == "" {
		return fmt.Errorf("database URL is empty")
	}

	if looksLikePostgresKVDSN(rawUrl) {
		return nil
	}
	u, err := url.Parse(rawUrl)
	if err != nil {
		return fmt.Errorf("invalid database URL: %v", err)
	}
	switch strings.ToLower(u.Scheme) {
	case "postgres", "postgresql":
		return nil
	case "":
		return fmt.Errorf("database URL has no scheme: %q", rawUrl)
	default:
		return fmt.Errorf("unsupported database scheme %q (want postgres:)", u.Scheme)
	}
}

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

func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func isUniqueViolation(err error) bool {
	return pgErrCode(err) == pgerrcode.UniqueViolation
}

func isForeignKeyViolation(err error) bool {
	return pgErrCode(err) == pgerrcode.ForeignKeyViolation
}

// lock-not-available SQLSTATEs are intentionally excluded because those require

func isRetryable(err error, logger *slog.Logger) bool {
	if err == nil {
		return false
	}

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
