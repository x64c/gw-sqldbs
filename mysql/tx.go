package mysql

import (
	"context"
	"database/sql"

	"github.com/x64c/gw/sqldbs"
)

// Ensure mysql.Tx implements sqldbs.Tx interface
var _ sqldbs.Tx = (*Tx)(nil)

type Tx struct {
	tx *sql.Tx
}

func (t *Tx) Commit(_ context.Context) error {
	return t.tx.Commit()
}

func (t *Tx) Rollback(_ context.Context) error {
	return t.tx.Rollback()
}

func (t *Tx) Exec(ctx context.Context, query string, args ...any) (sqldbs.Result, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{result: result}, nil
}

func (t *Tx) Query(ctx context.Context, query string, args ...any) (sqldbs.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}
