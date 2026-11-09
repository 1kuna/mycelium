package myce

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"modernc.org/sqlite"
)

func TestQueueSQLiteReplacementDurability(t *testing.T) {
	for _, conversion := range []bool{false, true} {
		t.Run(fmt.Sprint("conversion=", conversion), func(t *testing.T) {
			q, clock := fixture(t)
			if conversion {
				config := ServiceConfig{DB: strings.TrimSuffix(q.lock.Name(), ".lock"), Checkpoints: q.root}
				q.Close()
				var err error
				q, err = openConversionQueue(config, clock)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { q.Close() })
			}
			ctx := context.Background()
			for generation := range 2 {
				conn, err := q.db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var mode string
				var sync, timeout, full int
				if err = conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
					t.Fatal(err)
				}
				for name, target := range map[string]*int{"synchronous": &sync, "busy_timeout": &timeout, "fullfsync": &full} {
					if err = conn.QueryRowContext(ctx, "PRAGMA "+name).Scan(target); err != nil {
						t.Fatal(err)
					}
				}
				if mode != "wal" || sync != 2 || timeout != 5000 || runtime.GOOS == "darwin" && full != 1 {
					t.Fatalf("connection %d: journal=%s sync=%d timeout=%d fullfsync=%d", generation, mode, sync, timeout, full)
				}
				// Weaken only this fixture's old connection, then force database/sql
				// to discard it. A replacement must not inherit connection defaults.
				if _, err = conn.ExecContext(ctx, "PRAGMA synchronous=NORMAL; PRAGMA fullfsync=OFF; PRAGMA busy_timeout=1"); err != nil {
					t.Fatal(err)
				}
				if err = conn.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
					t.Fatal(err)
				}
				_ = conn.Close()
			}
			// Exercise a real transaction through the next replacement too.
			submit(t, q, spec("experiment"))
		})
	}
}

func TestQueueSQLiteRefusesUnavailableWAL(t *testing.T) {
	db := openQueueSQLite(":memory:", false) // SQLite returns "memory", not an error.
	defer db.Close()
	if err := db.Ping(); err == nil || !strings.Contains(err.Error(), "journal_mode=memory, require wal") {
		t.Fatalf("accepted unavailable WAL: %v", err)
	}
}

type ignoredSQLitePragma struct{ sqlite.ExecQuerierContext }

func (c ignoredSQLitePragma) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query == "PRAGMA synchronous=FULL" {
		return nil, nil
	}
	return c.ExecQuerierContext.ExecContext(ctx, query, args)
}

func TestQueueSQLiteRefusesIgnoredDurabilitySetting(t *testing.T) {
	c, err := (&sqlite.Driver{}).Open(filepath.Join(t.TempDir(), "fixture.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	conn := c.(sqlite.ExecQuerierContext)
	if _, err = conn.ExecContext(context.Background(), "PRAGMA synchronous=NORMAL", nil); err != nil {
		t.Fatal(err)
	}
	if err = configureQueueSQLite(context.Background(), ignoredSQLitePragma{conn}, false); err == nil || !strings.Contains(err.Error(), "synchronous=1, require 2") {
		t.Fatalf("accepted ignored flush setting: %v", err)
	}
}

func TestQueueSQLiteConversionRequiresExistingWAL(t *testing.T) {
	q, clock := fixture(t)
	config := ServiceConfig{DB: strings.TrimSuffix(q.lock.Name(), ".lock"), Checkpoints: q.root}
	q.Close()
	db, err := sql.Open("sqlite", config.DB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		t.Fatal(err)
	}
	converted, err := openConversionQueue(config, clock)
	if converted != nil {
		converted.Close()
		t.Fatal("converted a non-WAL database")
	}
	if err == nil || !strings.Contains(err.Error(), "journal_mode=delete, require wal") {
		t.Fatal(err)
	}
	var mode string
	if err = db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "delete" {
		t.Fatalf("preview changed journal mode: %s %v", mode, err)
	}
}
