package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if err := prepare(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err := prepare(ctx, conn); err != nil {
		t.Fatalf("prepare must be repeatable: %v", err)
	}
	// A database that already holds a real authority is refused.
	if _, err := conn.Exec(ctx, "CREATE TABLE public.myce_coordinator_v1(id int)"); err != nil {
		t.Fatal(err)
	}
	if err := prepare(ctx, conn); err == nil || !strings.Contains(err.Error(), "dedicated database") {
		t.Fatalf("live authority not refused: %v", err)
	}
	// cleanup drops only the qualification schema.
	t.Setenv("QQ_TEST_DSN", pg.dsn())
	var out strings.Builder
	if err := cmdCleanup(ctx, []string{"--dsn-env", "QQ_TEST_DSN"}, &out); err != nil {
		t.Fatal(err)
	}
	var schema, kept bool
	conn.QueryRow(ctx, "SELECT to_regnamespace('myce_qualification') IS NOT NULL, to_regclass('public.myce_coordinator_v1') IS NOT NULL").Scan(&schema, &kept)
	if schema || !kept {
		t.Fatalf("cleanup: qualification schema %v, unrelated table kept %v", schema, kept)
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
	if err := prepare(ctx, conn); err == nil || !strings.Contains(err.Error(), "queue.myce_changes_v1") {
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
	if err := prepare(ctx, oconn); err == nil || !strings.Contains(err.Error(), "dedicated") {
		t.Fatalf("database postgres was not refused: %v", err)
	}
}

// cleanup drops only what prepare created. An object someone else made that
// depends on those tables, or sits in the schema, stays, and cleanup fails.
func TestCleanupLeavesForeignDatabaseObjectsAndFails(t *testing.T) {
	pg := startPostgres(t)
	ctx := context.Background()
	conn, err := pg.target(t).connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	cleanup := func() error {
		var out strings.Builder
		return cmdCleanup(ctx, []string{"--dsn-env", "QQ_TEST_DSN"}, &out)
	}
	exists := func(rel string) bool {
		var ok bool
		conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", rel).Scan(&ok)
		return ok
	}
	for _, foreign := range []struct{ create, rel string }{
		{"CREATE VIEW public.their_view AS SELECT run FROM myce_qualification.durability_v1", "public.their_view"},
		{"CREATE TABLE myce_qualification.their_table(id int)", "myce_qualification.their_table"},
	} {
		if err := prepare(ctx, conn); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, foreign.create); err != nil {
			t.Fatal(err)
		}
		if err := cleanup(); err == nil {
			t.Fatalf("cleanup succeeded with %s in the way", foreign.rel)
		}
		if !exists(foreign.rel) || !exists("myce_qualification.durability_v1") {
			t.Fatalf("cleanup removed %s or the tables it depends on", foreign.rel)
		}
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS myce_qualification.their_table; DROP VIEW IF EXISTS public.their_view"); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanup(); err != nil || exists("myce_qualification.durability_v1") {
		t.Fatalf("cleanup of the harness's own objects: %v", err)
	}
}
