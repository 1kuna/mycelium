package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// The PostgreSQL paths run in the internal rehearsal; this covers the command
// surface that needs no database: dispatch, required paths and verify-only.
func TestQueueCutoverVerifyOnlyCommand(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "queue", "queue.db")
	q, err := myce.Open(db, filepath.Join(dir, "checkpoints"), myce.RealClock{}, myce.WithFileStore(filepath.Join(dir, "files"), 1<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"queue", "cutover", "--db", db}); err == nil || !strings.Contains(err.Error(), "--file-store") {
		t.Fatalf("cutover without its paths = %v", err)
	}
	var out bytes.Buffer
	err = sharedQueueMigrationCommand(context.Background(), "cutover", []string{"--verify-only", "--db", db,
		"--checkpoints", filepath.Join(dir, "checkpoints"), "--file-store", filepath.Join(dir, "files")}, &out)
	if err != nil || out.String() != "files: checked 0 references, 0 bytes, 0 problems\nverify-only: nothing changed\n" {
		t.Fatalf("verify-only = %q, %v", out.String(), err)
	}
}

// A production member advertises its --queue address to the other members,
// so a loopback address, which no other host can reach, is refused.
func TestSharedQueueMemberNeedsReachableEndpoint(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "fixture")
	dir := t.TempDir()
	err := run([]string{"queue", "--db", filepath.Join(dir, "queue.db"), "--checkpoints", filepath.Join(dir, "checkpoints"), "--discovery=false",
		"--queue", "http://127.0.0.1:7331", "--shared-authority", "host=192.0.2.10 dbname=myce user=myce"})
	if err == nil || !strings.Contains(err.Error(), "its own address on the local network") {
		t.Fatalf("loopback member endpoint = %v", err)
	}
	if err := run([]string{"join", "--shared-authority", "host=192.0.2.10"}); err == nil || !strings.Contains(err.Error(), "applies to myce queue") {
		t.Fatalf("worker with a shared authority = %v", err)
	}
}
