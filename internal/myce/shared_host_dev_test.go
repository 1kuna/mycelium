package myce

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedHostDevPressureCheckpointRequeueAndCancel(t *testing.T) {
	oldPrepare := workerCheckpointPrepare
	workerCheckpointPrepare = prepareCheckpoint
	t.Cleanup(func() { workerCheckpointPrepare = oldPrepare })
	oldRSS := workerMemoryRSS
	workerMemoryRSS = func(context.Context) (float64, error) { return 0, nil }
	t.Cleanup(func() { workerMemoryRSS = oldRSS })
	var pressure atomic.Pointer[SharedHostPressure]
	pressure.Store(&SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1})
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), NASCheckpoints: true, Token: "dev-fixture", GPU: "cpu", Workers: 1, Interval: 20 * time.Millisecond, SharedPressureSource: func(context.Context) (SharedHostPressure, error) { return *pressure.Load(), nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := d.Client()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	path := "/api/hosts/dev-host/maintenance"
	var status HostMaintenanceStatus
	readStatus := func() {
		t.Helper()
		if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool {
		readStatus()
		return len(status.Workers) == 1 && status.Workers[0].Fresh && status.ExternalMemory != nil
	})
	change := func(r HostMaintenanceRequest) {
		t.Helper()
		if err := c.Call(ctx, "POST", path, r, &status); err != nil {
			t.Fatal(err)
		}
	}
	change(HostMaintenanceRequest{Action: "drain"})
	await(t, func() bool { readStatus(); return status.Quiescent })
	cores, ram := 2, 2.0
	change(HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cores, MemoryBudgetGB: &ram})
	change(HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: .125}})
	change(HostMaintenanceRequest{Action: "resume"})
	s := cpuSpec(1, .01)
	s.Resumable = true
	s.SubmissionKey = "shared-pressure-resume"
	s.Run.Command = []string{"sh", "-c", `test "$GOMAXPROCS" = 1 || exit 40
if test "$MYCE_ATTEMPT" -gt 1; then
  grep -q '"value":42' "$MYCE_CHECKPOINT_DIR/checkpoint.json" || exit 41
  echo checkpoint-resumed
fi
trap 'printf '\''{"myce_checkpoint":{"generation":1},"value":42}'\'' > "$MYCE_CHECKPOINT_DIR/checkpoint.json"; exit 0' TERM
echo payload-ready
while :; do sleep .02; done`}
	var j Job
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	id := j.ID
	readJob := func() {
		t.Helper()
		if err := c.Call(ctx, "GET", "/api/jobs/"+id, nil, &j); err != nil {
			t.Fatal(err)
		}
		if j.State == "failed" {
			t.Fatalf("dev payload failed: %+v", j)
		}
	}
	hasLog := func(want string) bool {
		var logs []Log
		if err := c.Call(ctx, "GET", "/api/jobs/"+id+"/logs", nil, &logs); err != nil {
			t.Fatal(err)
		}
		for _, l := range logs {
			if strings.Contains(l.Text, want) {
				return true
			}
		}
		return false
	}
	await(t, func() bool { readJob(); return j.StartedAt != nil && hasLog("payload-ready") })
	// Advance only the dev clock to refresh the injected sensor immediately.
	setPressure := func(p SharedHostPressure) {
		t.Helper()
		pressure.Store(&p)
		if err := c.Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": signalRefresh.Seconds() + .01}, nil); err != nil {
			t.Fatal(err)
		}
	}
	setPressure(SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2})
	await(t, func() bool { readJob(); return j.State == "queued" })
	readStatus()
	if status.Workers[0].Machine.SharedHostYieldReason != "shared host memory pressure" || j.InfrastructureRetries != 0 || j.Losses != 0 || j.Spec.SubmissionKey != s.SubmissionKey {
		t.Fatal("pressure stop lost provenance or logical work", status, j)
	}
	setPressure(SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1})
	await(t, func() bool { readJob(); return j.Attempt == 2 && hasLog("checkpoint-resumed") })
	if err := c.Call(ctx, "POST", "/api/jobs/"+id+"/cancel", nil, nil); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { readJob(); return j.State == "cancelled" && j.ReleaseConfirmed })
	if j.InfrastructureRetries != 0 || j.Losses != 0 {
		t.Fatal("planned yield/cancel charged infrastructure", j)
	}
}
