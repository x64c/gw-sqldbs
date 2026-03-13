package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/x64c/gw/sqldbs"
)

// Ensure mysql.DB implements sqldbs.DB interface
var _ sqldbs.DB = (*DB)(nil)

type DB struct {
	conn  *sql.DB
	store *sqldbs.RawSQLStore
}

// Exec executes SQL statement like INSERT, UPDATE, DELETE.
func (d *DB) Exec(ctx context.Context, query string, args ...any) (sqldbs.Result, error) {
	result, err := d.conn.ExecContext(ctx, query, args...)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}
	return &Result{result: result}, nil
}

// QueryRows - Eager. Fail upfront on statement execution
func (d *DB) QueryRows(ctx context.Context, query string, args ...any) (sqldbs.Rows, error) {
	rows, err := d.conn.QueryContext(ctx, query, args...)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}

// QueryRow - Lazy. only fails at Scan()
func (d *DB) QueryRow(ctx context.Context, query string, args ...any) sqldbs.Row {
	row := d.conn.QueryRowContext(ctx, query, args...)
	return &Row{row: row}
}

func (d *DB) InsertStmt(ctx context.Context, query string, args ...any) (sqldbs.Result, error) {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
		return nil, fmt.Errorf("InsertStmt must start with INSERT")
	}
	result, err := d.conn.ExecContext(ctx, query, args...)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}
	return &Result{result: result}, nil
}

// MySQL doesn't have native COPY
// ToDo: emulate batch insert
func (d *DB) CopyFrom(_ context.Context, _ string, _ []string, _ [][]any) (int64, error) {
	return 0, fmt.Errorf("method CopyFrom not supported for MySQL")
}

func (d *DB) Listen(_ context.Context, _ string) (<-chan sqldbs.Notification, error) {
	return nil, fmt.Errorf("method Listen not supported for MySQL")
}

func (d *DB) Prepare(ctx context.Context, query string) (sqldbs.PreparedStmt, error) {
	stmt, err := d.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &PreparedStmt{stmt: stmt}, nil
}

func (d *DB) BeginTx(ctx context.Context) (sqldbs.Tx, error) {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx}, nil
}

func (d *DB) Ping(ctx context.Context) error {
	return d.conn.PingContext(ctx)
}

func (d *DB) SinglePlaceholder(_ ...int) string {
	return "?"
}

func (d *DB) Placeholders(cnt int, _ ...int) string {
	placeholders := make([]string, cnt)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ",")
}

func (d *DB) RawSQLStore() *sqldbs.RawSQLStore {
	return d.store
}
