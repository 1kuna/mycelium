package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Runs inside TestDevStandaloneSmoke's owned `myce dev` process. TERM writes a
// newer generation; resume must read it through the real checkpoint transport.
func proveDevPauseYield(t *testing.T, ctx context.Context, binary string, client myce.Client, root string, env []string) {
	t.Helper()
	ready, effect := filepath.Join(root, "yield-ready"), filepath.Join(root, "yield-effect")
	spec := myce.Spec{Name: "checkpoint-yield", Project: "probe", Priority: "experiment", Resumable: true, Portable: true}
	spec.RequireTarget = true
	spec.Targets = []myce.ExecutionTarget{{OS: runtime.GOOS}}
	spec.Needs.CPUCores, spec.Needs.MemoryGB = 1, 1
	spec.Run.Command = []string{"sh", "-ec", `test -z "$MYCE_TOKEN"
save="$MYCE_CHECKPOINT_DIR/state.json"
if [ -f "$save" ]; then
  grep -q '"generation":8' "$save"
  printf resumed > "$2"
  exit 0
fi
trap 'printf '\''{"myce_checkpoint":{"generation":8},"value":8}'\'' > "$save"; exit 0' TERM
printf '{"myce_checkpoint":{"generation":7},"value":7}' > "$save"
printf ready > "$1"
while :; do sleep 0.02; done`, "sh", ready, effect}
	var j myce.Job
	if err := client.Call(ctx, "POST", "/api/jobs", spec, &j); err != nil {
		t.Fatal(err)
	}
	wait := func(condition func() bool) {
		t.Helper()
		for !condition() {
			if err := waitPause(ctx, 10*time.Millisecond); err != nil {
				t.Fatal("yield deadline", err)
			}
		}
	}
	read := func() myce.Job {
		t.Helper()
		current, err := client.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == "failed" {
			t.Fatalf("yield command failed: %+v", current)
		}
		return *current
	}
	wait(func() bool { _, err := os.Stat(ready); return err == nil && read().StartedAt != nil })
	cli := func(action string, args ...string) myce.Job {
		t.Helper()
		args = append([]string{action, j.ID, "--queue", client.URL}, args...)
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env, cmd.Dir = env, root
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v %s", action, err, sanitizeSmokeEvidence(string(out), root, client.Token))
		}
		var result myce.Job
		if err := json.Unmarshal(out, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	stopped := cli("pause", "--yield", "--project", "probe", "--reason", "free capacity")
	if stopped.State != "stopping" && stopped.State != "paused" {
		t.Fatal("yield not accepted", stopped.State)
	}
	wait(func() bool { return read().State == "paused" })
	paused := read()
	if paused.Progress != 8 || paused.Losses != 0 || !paused.ReleaseConfirmed || paused.ID != j.ID || paused.YieldPause != nil {
		t.Fatalf("yield lost final save/release: %+v", paused)
	}
	if _, err := os.Stat(effect); !os.IsNotExist(err) {
		t.Fatal("paused work resumed early", err)
	}
	cli("resume")
	wait(func() bool { current := read(); return current.State == "succeeded" && current.ReleaseConfirmed })
	if data, err := os.ReadFile(effect); err != nil || string(data) != "resumed" {
		t.Fatal("resume did not consume saved generation", err)
	}
	if current := read(); current.ID != j.ID || current.Attempt != paused.Attempt+1 || current.Losses != 0 {
		t.Fatal("resume changed identity or charged loss", current)
	}
	t.Log("PASS: owner CLI yield, TERM final generation, confirmed release before pause, same-ID resume consumed save")
}
