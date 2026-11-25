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
