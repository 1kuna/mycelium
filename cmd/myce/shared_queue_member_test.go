package main

import (
	"path/filepath"
	"strings"
	"testing"
)

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
