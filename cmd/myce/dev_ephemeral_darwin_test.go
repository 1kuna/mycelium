package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestDevResolveEphemeralRefusal(t *testing.T) {
	binary := devTestBinary(t)
	path := filepath.Join(t.TempDir(), "ephemeral.yaml")
	inputs := t.TempDir()
	input := filepath.Join(inputs, "ephemeral-input.txt")
	if err := os.WriteFile(input, []byte("synthetic ephemeral payload\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := myce.IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	body := `name: private-transform
project: example
run: {command: [sh, -ec, 'cat ephemeral-input.txt > ephemeral-output.txt']}
inputs: {ephemeral-input.txt: ephemeral-input.txt}
outputs: {ephemeral-output.txt: results}
needs: {platform: cpu, cpu_cores: 1, memory_gb: 1}
advanced: {worker_data: ephemeral}
`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const script = `
rc=0
"$1" spec check --resolve --files-from "$3" "$2" || rc=$?
test "$rc" = 1
rc=0
"$1" submit --files-from "$3" "$2" || rc=$?
test "$rc" = 1
# Inspect workers while dev is still running, before its own teardown. The
# upload to the queue's file store is expected and outside worker custody.
remaining=$(find "$MYCE_DEV_SESSION_ROOT/workers" \
  \( -name ephemeral -o -name ephemeral-logs -o -name ephemeral-input.txt \
     -o -name ephemeral-output.txt -o -name "$4" \) -print)
test -z "$remaining" || { printf '%s\n' "$remaining"; exit 1; }
`
	cmd := exec.CommandContext(ctx, binary, "dev", "--", "/bin/sh", "-ec", script, "dev-test", binary, path, inputs, ref.SHA256)
	out, err := cmd.CombinedOutput()
	report := string(out)
	reason := "worker_data_ephemeral_v2 worker capability required: macOS has no qualified kernel boundary for attempt descendants and orphans"
	if err != nil || strings.Count(report, reason) < 2 ||
		!strings.Contains(report, "job_worker_data_unsupported") ||
		strings.Contains(report, "native_isolation_unavailable") {
		t.Fatalf("expected placement and submission refusals with no worker payload: %v\n%s", err, report)
	}
	t.Log("dev resolve and submit refused; no attempt data tree, log tree, payload or cache hash present")
}
