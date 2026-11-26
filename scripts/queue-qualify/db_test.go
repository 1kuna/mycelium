package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestEndpointClassNeverNamesTheHost(t *testing.T) {
	for host, want := range map[string]string{
		"/tmp/sock": "unix-socket", "localhost": "loopback", "127.0.0.1": "loopback", "::1": "loopback",
		"192.168.1.20": "private-network", "10.0.0.5": "private-network", "nas.example": "hostname", "8.8.8.8": "public-network",
	} {
		if got := endpointClass(host); got != want {
			t.Errorf("%s: got %s, want %s", host, got, want)
		}
	}
}

func TestConnectionStringErrorsStayPrivate(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "dsn")
	if err := os.WriteFile(file, []byte("postgres://u:hunter2@host:notaport/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := (&connFlags{file: file}).load()
	if err == nil || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "host") {
		t.Fatalf("parse error: %v", err)
	}
	t.Setenv("QQ_EMPTY", "")
	if _, err := (&connFlags{env: "QQ_EMPTY"}).load(); err == nil || !strings.Contains(err.Error(), "QQ_EMPTY") {
		t.Fatalf("missing variable should name the variable, not a value: %v", err)
	}
	// The longest secret wins, so the full string never leaves a fragment.
	r := newRedactor("postgres://bob@db.lan/queue", nil)
	r.secrets = append(r.secrets, "db.lan", "<host>")
	if got := r.text("dial postgres://bob@db.lan/queue: db.lan refused"); got != "dial <connection>: <host> refused" {
		t.Fatalf("redacted %q", got)
	}
}

func TestInfoAndPrepareAgainstDisposablePostgres(t *testing.T) {
	pg := startPostgres(t)
	tg := pg.target(t)
	ctx := context.Background()
	conn, err := tg.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	s, err := readServerInfo(ctx, conn, tg.class, 20)
	if err != nil {
		t.Fatal(err)
	}
	if s.SystemIdentifier == "" || s.RoundTrip.Count != 20 || s.EndpointClass != "loopback" {
		t.Fatalf("info %+v", s)
	}
	for _, c := range infoChecks(s) {
		if c.Status == statusFail {
			t.Errorf("fixture fails %s: %s", c.Name, c.Measured)
		}
	}
	for range 2 {
		c, err := tg.open(ctx, true, 0)
		if err != nil {
			t.Fatalf("open must be repeatable: %v", err)
		}
		c.Close(ctx)
	}
	// A database that already holds a real authority is refused.
	if _, err := conn.Exec(ctx, "CREATE TABLE public.myce_coordinator_v1(id int)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tg.open(ctx, true, 0); !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "dedicated database") {
		t.Fatalf("live authority not refused: %v", err)
	}
}

// Stage 2 creates its tables unqualified, so a real authority can sit in any
// schema on the search_path. The harness must find it there, and must run
// only in a database named for qualification.
func TestPrepareRefusesAQueueInAnySchemaOrAnUndedicatedDatabase(t *testing.T) {
	pg := startPostgres(t)
	ctx := context.Background()
	conn, err := pg.target(t).connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE SCHEMA queue; CREATE TABLE queue.myce_changes_v1(sequence bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.target(t).open(ctx, true, 0); !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "queue.myce_changes_v1") {
		t.Fatalf("a queue table in schema queue was not refused: %v", err)
	}
	var made bool
	conn.QueryRow(ctx, "SELECT to_regnamespace('myce_qualification') IS NOT NULL").Scan(&made)
	if made {
		t.Fatal("prepare wrote to a database holding a queue")
	}
	// The cluster's default database is not a dedicated qualification database.
	t.Setenv("QQ_OTHER_DSN", strings.Replace(pg.dsn(), "/"+fixtureDB+"?", "/postgres?", 1))
	other, err := (&connFlags{env: "QQ_OTHER_DSN"}).load()
	if err != nil {
		t.Fatal(err)
	}
	oconn, err := other.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer oconn.Close(ctx)
	if err := other.vet(ctx, oconn, true, 0); !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "dedicated") {
		t.Fatalf("database postgres was not refused: %v", err)
	}
}

// A queue created with a quoted upper-case name is still a queue.
func TestRefusalMatchesQueueTablesInAnyCase(t *testing.T) {
	pg := startPostgres(t)
	ctx := context.Background()
	tg := pg.target(t)
	conn, err := tg.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE public."MYCE_CHANGES_V1"(sequence bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tg.open(ctx, true, 0); !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "MYCE_CHANGES_V1") {
		t.Fatalf("an upper-case queue table was not refused: %v", err)
	}
}

// A role or database search_path that puts another schema ahead of
// pg_catalog can shadow the catalog: a pg_class view that hides the queue,
// or a current_database() that claims a qualification name. The harness
// pins search_path and names pg_catalog explicitly, so neither works.
func TestRefusalIgnoresAShadowedCatalog(t *testing.T) {
	pg := startPostgres(t)
	ctx := context.Background()
	tg := pg.target(t)
	conn, err := tg.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `CREATE TABLE public.myce_coordinator_v1(id int);
		CREATE SCHEMA shadow;
		CREATE VIEW shadow.pg_class AS SELECT * FROM pg_catalog.pg_class WHERE false;
		ALTER ROLE qualify SET search_path = shadow, pg_catalog, public`); err != nil {
		t.Fatal(err)
	}
	// The shadow is live for an ordinary session.
	plain, err := pgx.Connect(ctx, pg.dsn())
	if err != nil {
		t.Fatal(err)
	}
	var seen int
	plain.QueryRow(ctx, "SELECT count(*) FROM pg_class WHERE relname = 'myce_coordinator_v1'").Scan(&seen)
	plain.Close(ctx)
	if seen != 0 {
		t.Fatal("the fixture's shadow view is not in effect")
	}
	if _, err := tg.open(ctx, true, 0); !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "myce_coordinator_v1") {
		t.Fatalf("a queue hidden by a shadowed pg_class was not refused: %v", err)
	}

	// A function that claims a qualification name, on the cluster's default
	// database, through that database's own search_path.
	t.Setenv("QQ_OTHER_DSN", strings.Replace(pg.dsn(), "/"+fixtureDB+"?", "/postgres?", 1))
	other, err := (&connFlags{env: "QQ_OTHER_DSN"}).load()
	if err != nil {
		t.Fatal(err)
	}
	oconn, err := other.connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer oconn.Close(ctx)
	if _, err := oconn.Exec(ctx, `CREATE SCHEMA shadow;
		CREATE FUNCTION shadow.current_database() RETURNS name LANGUAGE sql AS $$ SELECT 'qualify'::name $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := other.open(ctx, true, 0); !errors.Is(err, errRefused) || !strings.Contains(err.Error(), "dedicated") {
		t.Fatalf("a shadowed current_database() was not refused: %v", err)
	}
}

// Every reconnect is vetted again before it writes: the same address can
// lead to a different server, and a session lock does not survive its
// connection.
func TestReconnectIsVettedAndRelocked(t *testing.T) {
	first, second := startPostgres(t), startPostgres(t)
	ctx := context.Background()
	tg := first.target(t)
	conn, err := tg.open(ctx, true, commitLockKey)
	if err != nil {
		t.Fatal(err)
	}
	// While this session holds the lock, another is refused it.
	if _, err := tg.open(ctx, true, commitLockKey); !errors.Is(err, errLockBusy) {
		t.Fatalf("lock not held: %v", err)
	}
	conn.Close(ctx)
	again, err := tg.open(ctx, true, commitLockKey)
	if err != nil {
		t.Fatalf("reconnect could not re-take the lock: %v", err)
	}
	if _, err := tg.open(ctx, true, commitLockKey); !errors.Is(err, errLockBusy) {
		t.Fatalf("the reconnect did not re-take the lock: %v", err)
	}
	again.Close(ctx)
	// The same target now reaches a different cluster, as a moved address or
	// a recreated container would.
	tg.cfg.Port = uint16(second.port)
	if _, err := tg.open(ctx, true, 0); !errors.Is(err, errRefused) || !errors.Is(err, errTargetChanged) {
		t.Fatalf("a reconnect to a different server was not refused: %v", err)
	}
	var made bool
	sconn, err := second.target(t).connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sconn.Close(ctx)
	sconn.QueryRow(ctx, "SELECT to_regnamespace('myce_qualification') IS NOT NULL").Scan(&made)
	if made {
		t.Fatal("the refused reconnect wrote to the other server")
	}
}
