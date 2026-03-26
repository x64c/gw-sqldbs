package pgsql

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/x64c/gw/sqldbs"
)

// Ensure pgsql.DB implements sqldbs.DB interface
var _ sqldbs.DB = (*DB)(nil)

type DB struct {
	pool      *pgxpool.Pool
	client    *Client
	mainStore *sqldbs.RawSQLStore
}

// Exec executes SQL statement like INSERT, UPDATE, DELETE.
func (d *DB) Exec(ctx context.Context, query string, args ...any) (sqldbs.Result, error) {
	tag, err := d.pool.Exec(ctx, query, args...)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}
	return &Result{tag: tag}, nil
}

// QueryRows - Eager. Fail upfront on statement execution
func (d *DB) QueryRows(ctx context.Context, query string, args ...any) (sqldbs.Rows, error) {
	rows, err := d.pool.Query(ctx, query, args...)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}
	return &Rows{
		conn:    nil, // Pool manages connection, no need to release here
		current: rows,
		batch:   nil, // single query, no batch
	}, nil
}

// QueryRow - Lazy. only fails at Scan()
func (d *DB) QueryRow(ctx context.Context, query string, args ...any) sqldbs.Row {
	row := d.pool.QueryRow(ctx, query, args...)
	return &Row{row: row}
}

func (d *DB) InsertStmt(ctx context.Context, query string, args ...any) (sqldbs.Result, error) {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") {
		return nil, fmt.Errorf("InsertStmt must start with INSERT")
	}
	// append RETURNING id if missing
	if !strings.Contains(strings.ToUpper(query), "RETURNING") {
		query += " RETURNING id"
		var id int64
		err := d.pool.QueryRow(ctx, query, args...).Scan(&id)
		if err != nil {
			return nil, err
		}
		return &Result{lastInsertID: id}, nil
	}

	tag, err := d.pool.Exec(ctx, query, args...)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	return &Result{tag: tag}, err
}

func (d *DB) CopyFrom(ctx context.Context, table string, columns []string, rows [][]any) (int64, error) {
	src := pgx.CopyFromRows(rows)
	count, err := d.pool.CopyFrom(ctx, pgx.Identifier{table}, columns, src)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	return count, err
}

func (d *DB) Listen(ctx context.Context, channel string) (<-chan sqldbs.Notification, error) {
	conn, err := d.pool.Acquire(ctx)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}

	notifyCh := make(chan sqldbs.Notification)

	go func() {
		defer conn.Release()
		defer close(notifyCh)

		_, err := conn.Exec(ctx, fmt.Sprintf("LISTEN %s;", pgx.Identifier{channel}.Sanitize()))
		if err != nil {
			log.Printf("[WARN] failed to LISTEN on %s: %v", channel, err)
			return
		}

		for {
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				log.Printf("[WARN] Listen loop ended for %s: %v", channel, err)
				return
			}
			select {
			case notifyCh <- sqldbs.Notification{
				Channel: notification.Channel,
				Payload: notification.Payload,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return notifyCh, nil
}

func (d *DB) Prepare(ctx context.Context, query string) (sqldbs.PreparedStmt, error) {
	conn, err := d.pool.Acquire(ctx)
	// NOTE: We can process a DBMS-specific error to produce a better abstracted error
	if err != nil {
		return nil, err
	}
	stmtName := fmt.Sprintf("stmt_%x", time.Now().UnixNano())
	_, err = conn.Conn().Prepare(ctx, stmtName, query)
	if err != nil {
		conn.Release()
		return nil, err
	}
	return &PreparedStmt{conn: conn, stmtName: stmtName}, nil
}

func (d *DB) BeginTx(ctx context.Context) (sqldbs.Tx, error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection failed: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("begin transaction failed: %w", err)
	}
	return &Tx{tx: tx}, nil
}

func (d *DB) Ping(ctx context.Context) error {
	return d.pool.Ping(ctx)
}

func (d *DB) Client() sqldbs.Client {
	return d.client
}


// Convenience: delegates to Client

func (d *DB) FirstPlaceholder() string {
	return d.client.FirstPlaceholder()
}

func (d *DB) NthPlaceholder(n int) string {
	return d.client.NthPlaceholder(n)
}

func (d *DB) InPlaceholders(start, cnt int) string {
	return d.client.InPlaceholders(start, cnt)
}

func (d *DB) RawSQLStore(name string) *sqldbs.RawSQLStore {
	return d.client.stores[name]
}

func (d *DB) MainRawSQLStore() *sqldbs.RawSQLStore {
	if d.mainStore == nil {
		panic("MainRawSQLStore not set — call SetMainRawSQLStore at boot")
	}
	return d.mainStore
}

func (d *DB) SetMainRawSQLStore(name string) {
	d.mainStore = d.client.stores[name]
}
