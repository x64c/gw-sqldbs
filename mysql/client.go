package mysql

import (
	"context"
	"database/sql"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/x64c/gw/sqldbs"
)

// Client implements sqldbs.Client for MySQL.
// One Client = one server (host + port + credentials).
type Client struct {
	conf  ClientConf
	store *sqldbs.RawSQLStore
	dbs   map[string]*DB
}

func NewClient(conf ClientConf) *Client {
	return &Client{
		conf: conf,
		dbs:  make(map[string]*DB),
	}
}

func (c *Client) CreateDB(name string, rawConf jsontext.Value) error {
	var dbConf DBConf
	if err := json.Unmarshal(rawConf, &dbConf); err != nil {
		return fmt.Errorf("mysql db: %w", err)
	}
	if _, exists := c.dbs[name]; exists {
		return fmt.Errorf("mysql db: %q already exists", name)
	}

	var dsn string
	if c.conf.DSN != "" {
		dsn = c.conf.DSN
	} else {
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=%s&sql_mode=ANSI_QUOTES&multiStatements=true",
			c.conf.User, c.conf.PW,
			c.conf.Host, c.conf.Port,
			dbConf.DB, c.conf.TZ,
		)
	}

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql db %q: %w", name, err)
	}
	// ToDo: get these values from conf
	conn.SetConnMaxLifetime(3 * time.Minute)
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(10)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return fmt.Errorf("mysql db %q ping: %w", name, err)
	}

	log.Printf("[INFO] mysql db %q initialized (%s)", name, dbConf.DB)
	c.dbs[name] = &DB{conn: conn, store: c.store}
	return nil
}

func (c *Client) DB(name string) (sqldbs.DB, bool) {
	db, ok := c.dbs[name]
	return db, ok
}

func (c *Client) RawSQLStore() *sqldbs.RawSQLStore {
	return c.store
}

func (c *Client) Close() error {
	for name, db := range c.dbs {
		log.Printf("[INFO] closing mysql db %q", name)
		if err := db.conn.Close(); err != nil {
			log.Printf("[ERROR] failed to close mysql db %q: %v", name, err)
		} else {
			log.Printf("[INFO] mysql db %q closed", name)
		}
	}
	return nil
}

// LoadRawSQL loads SQL statements from the given FS into this Client's store.
// Picks .sql (standard) and .mysql (dialect-specific) files.
func (c *Client) LoadRawSQL(sqlFS fs.FS) error {
	c.store = sqldbs.NewRawSQLStore()
	return loadRawStmtsToStore(c.store, sqlFS)
}

// PrepareClients loads MySQL client configs from .sqldb-clients-mysql.json
// and registers them into the provided client map.
func PrepareClients(appRoot string, clients map[string]sqldbs.Client, sqlFS fs.FS) error {
	confBytes, err := os.ReadFile(filepath.Join(appRoot, "config", ".sqldb-clients-mysql.json"))
	if err != nil {
		return err
	}
	var confs map[string]ClientConf
	if err = json.Unmarshal(confBytes, &confs); err != nil {
		return err
	}
	for name, conf := range confs {
		client := NewClient(conf)
		if err = client.LoadRawSQL(sqlFS); err != nil {
			return fmt.Errorf("mysql client %q: %w", name, err)
		}
		clients[name] = client
	}
	return nil
}
