package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Exercise the actual adapter lowering, API and worker command runner. A FIFO
// holds the TERM handler mid-save so wrapper exit cannot race a tiny write.
func TestAdapterGracefulStopCheckpoint(t *testing.T) {
	for _, runner := range []string{"plain", "native"} {
		for _, platform := range []string{"cpu", "fake"} {
			for _, stop := range []string{"leave", "deadline", "cancel"} {
				for _, checks := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/checks=%t", runner, platform, stop, checks), func(t *testing.T) {
						testAdapterGracefulStopCheckpoint(t, runner, platform, stop, checks)
					})
				}
			}
		}
	}
}

func testAdapterGracefulStopCheckpoint(t *testing.T, runner, platform, stop string, checks bool) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	conversionWrite(t, filepath.Join(source, "run.sh"), `test -z "${MYCE_TOKEN:-}" || exit 99
echo "$MYCE_CHECKPOINT_DIR" > "$TEST_CHECKPOINT"
mkfifo "$TEST_GATE"
exec 3<> "$TEST_GATE"
trap 'if [ -f "$TEST_STOP" ]; then echo duplicate >> "$TEST_STOP"; exit 9; fi; echo term > "$TEST_STOP"; read gate <&3; echo saved > "$MYCE_CHECKPOINT_DIR/saved"; exit 0' TERM
echo ready > "$TEST_READY"
while :; do read line <&3; done
`)
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), NASCheckpoints: true, Token: "adapter-stop-fixture", GPU: platform, MemoryGB: 2, CPUBudgetCores: 1, Workers: 1, Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	gatePath, stopPath, readyPath := filepath.Join(root, "gate"), filepath.Join(root, "term"), filepath.Join(root, "ready")
	data := fmt.Sprintf(`schema: 1
project: adapter-stop-fixture
default: work
entries:
  work:
    source: {directory: %q}
    advanced: {resumable: true, isolation: unsandboxed, reason: fixture-owned synchronization files}
    execution:
      main:
        memory_gb: 0.125
        command: [sh, run.sh]
        env: {TEST_GATE: %q, TEST_STOP: %q, TEST_READY: %q, TEST_CHECKPOINT: %q}
    needs: {cpu_cores: 1, memory_gb: 0.25, host_memory_gb: 0.25, platform: %s}
`, source, gatePath, stopPath, readyPath, filepath.Join(root, "checkpoint"), platform)
	if runner == "plain" {
		data = strings.Replace(data, "    execution:\n      main:\n        memory_gb: 0.125\n        command:", "    run:\n      command:", 1)
		data = strings.Replace(data, "        env:", "      env:", 1)
	}
	if checks {
		data += `    checks: {result: [sh, -ec, 'test -s "$MYCE_CHECKPOINT_DIR/saved"']}
`
	}
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	p, selected, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	err = runPreparedProfile(ctx, root, p, selected, nil, nil, "stop-fixture", filepath.Join(root, "absent-notifier"), "", "", d.URL, "adapter-stop-fixture", &out, &diagnostics)
	if exitCode(err) != 74 {
		t.Fatalf("submit: %v %s %s", err, &out, &diagnostics)
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	get := func() myce.Job {
		t.Helper()
		j, e := d.Client().GetJob(ctx, receipt.JobID)
		if e != nil {
			t.Fatal(e)
		}
		return *j
	}
	wait(t, ctx, func() bool { _, e := os.Stat(readyPath); return e == nil })
	gate, err := os.OpenFile(gatePath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = gate.WriteString("save\n"); _ = gate.Close(); _ = d.Queue.Cancel(receipt.JobID) }()
	before := get()
	switch stop {
	case "leave":
		if err := d.Queue.Leave(before.Machine, false); err != nil {
			t.Fatal(err)
		}
	case "deadline":
		// Use the normal admission/preemption path, with fake GPU inventory.
		s := receipt.Spec
		s.Name, s.SubmissionKey, s.Class, s.Priority = "urgent", "urgent", "deadline", ""
		deadline := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
		s.Deadline = &deadline
		s.Resumable = false
		if s.Execution != nil {
			s.Execution.Main.Command = []string{"true"}
		} else {
			s.Run.Command = []string{"true"}
		}
		if _, err := d.Client().Submit(ctx, s); err != nil {
			t.Fatal(err)
		}
	case "cancel":
		if err := d.Queue.Cancel(receipt.JobID); err != nil {
			t.Fatal(err)
		}
	}
	// A broken wrapper can finish/release without ever notifying the child.
	wait(t, ctx, func() bool { _, e := os.Stat(stopPath); j := get(); return e == nil || j.ReleaseConfirmed })
	if _, err := os.Stat(stopPath); err != nil {
		t.Fatalf("released without child's TERM handler: state=%s release=%t", get().State, get().ReleaseConfirmed)
	}
	j := get()
	// Move the injected dev clock while the handler is held, without sleeping
	// through the real grace. The original stop deadline must still apply.
	var advanced struct {
		Now time.Time `json:"now"`
	}
	if err := d.Client().Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": 1}, &advanced); err != nil {
		t.Fatal(err)
	}
	if j.StopAt == nil || !advanced.Now.Before(*j.StopAt) {
		t.Fatal("save did not stay within the stop grace")
	}
	if get().ReleaseConfirmed {
		t.Fatal("released while the checkpoint save was held")
	}
	if _, err := gate.WriteString("save\n"); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool { return get().ReleaseConfirmed })
	checkpoint, err := os.ReadFile(filepath.Join(root, "checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(strings.TrimSpace(string(checkpoint)), "saved"))
	if err != nil || strings.TrimSpace(string(b)) != "saved" {
		t.Fatalf("checkpoint lost: %q %v; state=%s", b, err, get().State)
	}
}
