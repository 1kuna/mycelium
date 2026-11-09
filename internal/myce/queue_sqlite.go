package myce

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"

	"modernc.org/sqlite"
)

// A Connector initializes every physical writer, including database/sql's
// replacement connections. A one-time db.Exec leaves connection-local PRAGMAs
// behind when a connection is discarded. Readers use the separate read-only DSN.
type queueSQLiteConnector struct {
	path       string
	driver     driver.Driver
	requireWAL bool
}

func openQueueSQLite(path string, requireWAL bool) *sql.DB {
	db := sql.OpenDB(&queueSQLiteConnector{path: path, driver: &sqlite.Driver{}, requireWAL: requireWAL})
	db.SetMaxOpenConns(1)
	return db
}

func (c *queueSQLiteConnector) Driver() driver.Driver { return c.driver }

func (c *queueSQLiteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.path)
	if err != nil {
		return nil, err
	}
	if err = configureQueueSQLite(ctx, conn.(sqlite.ExecQuerierContext), c.requireWAL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("queue SQLite durability: %w", err)
	}
	return conn, nil
}

func configureQueueSQLite(ctx context.Context, conn sqlite.ExecQuerierContext, requireWAL bool) error {
	type setting struct{ name, value, want string }
	settings := []setting{
		{"busy_timeout", "5000", "5000"},
		{"synchronous", "FULL", "2"},
	}
	if runtime.GOOS == "darwin" {
		// FULL/WAL alone uses ordinary fsync in SQLite's VFS, unlike Go's
		// File.Sync. fullfsync requests F_FULLFSYNC for commits AND checkpoints.
		settings = append(settings, setting{"fullfsync", "ON", "1"})
	}
	// Set flush policy before WAL creation/transition, then verify effective
	// settings. SQLite can successfully return a different journal mode.
	settings = append(settings, setting{"journal_mode", "WAL", "wal"})
	for _, s := range settings {
		// Offline conversion previews must not transition an existing DB.
		if requireWAL && s.name == "journal_mode" {
			continue
		}
		if _, err := conn.ExecContext(ctx, "PRAGMA "+s.name+"="+s.value, nil); err != nil {
			return fmt.Errorf("set %s: %w", s.name, err)
		}
	}
	for _, s := range settings {
		rows, err := conn.QueryContext(ctx, "PRAGMA "+s.name, nil)
		if err != nil {
			return fmt.Errorf("read %s: %w", s.name, err)
		}
		values := make([]driver.Value, 1)
		err = rows.Next(values)
		closeErr := rows.Close()
		if err != nil {
			return fmt.Errorf("read %s: %w", s.name, err)
		}
		if closeErr != nil {
			return fmt.Errorf("read %s: %w", s.name, closeErr)
		}
		if got := fmt.Sprint(values[0]); got != s.want {
			return fmt.Errorf("%s=%s, require %s", s.name, got, s.want)
		}
	}
	return nil
}
