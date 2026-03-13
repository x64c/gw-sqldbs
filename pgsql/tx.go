package pgsql

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/x64c/gw/sqldbs"
)

// Ensure pgsql.Tx implements sqldbs.Tx interface
var _ sqldbs.Tx = (*Tx)(nil)

type Tx struct {
	tx pgx.Tx
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *Tx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func (t *Tx) Exec(ctx context.Context, query string, args ...any) (sqldbs.Result, error) {
	tag, err := t.tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{tag: tag}, nil
}

func (t *Tx) Query(ctx context.Context, query string, args ...any) (sqldbs.Rows, error) {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{
		conn:    nil, // tx already owns the connection
		current: rows,
		batch:   nil,
	}, nil
}
