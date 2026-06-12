package dbos

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Querier interface {
	Exec(ctx context.Context, query string, args ...any) (Result, error)
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) Row
}

type Tx interface {
	Querier
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Pool interface {
	Querier
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
	Ping(ctx context.Context) error
	Close()
}

type TxOptions struct {
	IsoLevel IsoLevel
	ReadOnly bool
}

type IsoLevel int

const (
	IsoLevelDefault IsoLevel = iota
	IsoLevelReadCommitted
	IsoLevelRepeatableRead
	IsoLevelSerializable
)

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

type Row interface {
	Scan(dest ...any) error
}

type Result interface {
	RowsAffected() (int64, error)
}

var ErrNoRows = pgx.ErrNoRows

func newPgxPool(p *pgxpool.Pool) Pool { return &pgxPoolAdapter{p: p} }

type pgxPoolAdapter struct{ p *pgxpool.Pool }

func (a *pgxPoolAdapter) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	tag, err := a.p.Exec(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgxResult{rows: tag.RowsAffected()}, nil
}

func (a *pgxPoolAdapter) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := a.p.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{r: rows}, nil
}

func (a *pgxPoolAdapter) QueryRow(ctx context.Context, q string, args ...any) Row {
	return &pgxRow{r: a.p.QueryRow(ctx, q, args...)}
}

func (a *pgxPoolAdapter) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	tx, err := a.p.BeginTx(ctx, pgxTxOpts(opts))
	if err != nil {
		return nil, err
	}
	return &pgxTxAdapter{tx: tx}, nil
}

func (a *pgxPoolAdapter) Ping(ctx context.Context) error { return a.p.Ping(ctx) }
func (a *pgxPoolAdapter) Close()                         { a.p.Close() }

func PgxPool(p Pool) *pgxpool.Pool {
	if a, ok := p.(*pgxPoolAdapter); ok {
		return a.p
	}
	return nil
}

func PgxTx(t Tx) pgx.Tx {
	if a, ok := t.(*pgxTxAdapter); ok {
		return a.tx
	}
	return nil
}

type pgxTxAdapter struct{ tx pgx.Tx }

func (t *pgxTxAdapter) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	tag, err := t.tx.Exec(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgxResult{rows: tag.RowsAffected()}, nil
}

func (t *pgxTxAdapter) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	rows, err := t.tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRows{r: rows}, nil
}

func (t *pgxTxAdapter) QueryRow(ctx context.Context, q string, args ...any) Row {
	return &pgxRow{r: t.tx.QueryRow(ctx, q, args...)}
}

func (t *pgxTxAdapter) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgxTxAdapter) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

type pgxRows struct{ r pgx.Rows }

func (r *pgxRows) Next() bool             { return r.r.Next() }
func (r *pgxRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *pgxRows) Err() error             { return r.r.Err() }
func (r *pgxRows) Close() error           { r.r.Close(); return nil }

type pgxRow struct{ r pgx.Row }

func (r *pgxRow) Scan(dest ...any) error {
	if err := r.r.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoRows
		}
		return err
	}
	return nil
}

type pgxResult struct{ rows int64 }

func (r pgxResult) RowsAffected() (int64, error) { return r.rows, nil }

func pgxTxOpts(o TxOptions) pgx.TxOptions {
	var iso pgx.TxIsoLevel
	switch o.IsoLevel {
	case IsoLevelReadCommitted:
		iso = pgx.ReadCommitted
	case IsoLevelRepeatableRead:
		iso = pgx.RepeatableRead
	case IsoLevelSerializable:
		iso = pgx.Serializable
	default:
		iso = ""
	}
	mode := pgx.ReadWrite
	if o.ReadOnly {
		mode = pgx.ReadOnly
	}
	return pgx.TxOptions{IsoLevel: iso, AccessMode: mode}
}
