package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRunRejectsUnknownCommandsWithUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), nil, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("no args: code %d, stderr %q", code, stderr.String())
	}
	stderr.Reset()
	if code := run(context.Background(), []string{"nope"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("unknown: code %d, stderr %q", code, stderr.String())
	}
}

// A failed connection must not print the host, user, database or password,
// even though the driver's own message names them.
func TestRunNeverPrintsConnectionDetails(t *testing.T) {
	t.Setenv("QQ_SECRET_DSN", "postgres://sekret-user:sekret-pass@127.0.0.1:9/sekret-db?connect_timeout=2&sslmode=disable")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"info", "--dsn-env", "QQ_SECRET_DSN", "--out", t.TempDir()}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code %d, stderr %q", code, stderr.String())
	}
	all := stdout.String() + stderr.String()
	for _, secret := range []string{"sekret-user", "sekret-pass", "sekret-db", "127.0.0.1"} {
		if strings.Contains(all, secret) {
			t.Fatalf("output leaks %q: %q", secret, all)
		}
	}
	if !strings.Contains(stderr.String(), "connect") {
		t.Fatalf("error lost its meaning: %q", stderr.String())
	}
}
