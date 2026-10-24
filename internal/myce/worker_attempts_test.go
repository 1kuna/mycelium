package myce

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// #152: an admission drain must not hold a running actor's reports or custody.
func TestAttemptReportsAndReleasesDuringAdmissionDrain(t *testing.T) {
	q, _ := fixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	fifo := filepath.Join(t.TempDir(), "command")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	command, err := os.OpenFile(fifo, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { command.Close() })
	s := spec("experiment")
	s.Resumable = false
	s.Run.Command = []string{"sh", "-c", `exec 3<> "$1"; read line <&3; echo during-drain; read line <&3`, "fixture", fifo}
	j := submit(t, q, s)
	workerFixture(t, q)
	await(t, func() bool {
		clock.Advance(time.Millisecond)
		return job(t, q, j.ID).StartedAt != nil
	})
	const host = "fixture-worker-host"
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "drain"}); err != nil {
		t.Fatal(err)
	}
	// Observe two check-ins after draining: the first response must have reached
	// the actor before the machine loop can send the second request.
	for range 2 {
		clock.Advance(time.Second)
		seen := clock.Now()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			status, err := q.HostMaintenanceStatus(host)
			return err == nil && len(status.Workers) == 1 && !status.Workers[0].Machine.LastSeen.Before(seen)
		})
	}
	if _, err := command.WriteString("log\n"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	probeWait(t, ctx, func() bool {
		clock.Advance(time.Millisecond)
		logs, err := q.Logs(j.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range logs {
			if strings.Contains(l.Text, "during-drain") {
				return true
			}
		}
		return false
	})
	if got := job(t, q, j.ID); got.State != "running" {
		t.Fatal("drain stopped the running attempt", got.State)
	}
	if _, err := command.WriteString("finish\n"); err != nil {
		t.Fatal(err)
	}
	probeWait(t, ctx, func() bool {
		clock.Advance(time.Millisecond)
		got := job(t, q, j.ID)
		status, err := q.HostMaintenanceStatus(host)
		return err == nil && got.State == "succeeded" && got.ReleaseConfirmed && status.Quiescent && len(status.Attempts) == 0 && len(status.PendingReleases) == 0
	})
}

func TestAttemptReleasesWhenRAMDropsBeforeEntrypoint(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "entrypoint")
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	h.jobs = []Job{harnessJob(t, "ram-drop", "", "touch "+marker)}
	h.jobs[0].Spec.Needs.HostMemoryGB = 22
	w := harnessWorker(t, h)
	var free atomic.Uint64
	free.Store(math.Float64bits(64))
	w.HostMemorySource = func(context.Context) (float64, error) {
		return math.Float64frombits(free.Load()), nil
	}
	checkpoint := t.TempDir()
	w.checkpointPrepare = func(Job) (string, error) {
		// Admission succeeded; RAM changed while inputs/checkpoints were staged.
		free.Store(math.Float64bits(21.9))
		return checkpoint, nil
	}
	startHarness(t, w, h)
	deadline := time.After(3 * time.Second)
	for !h.completed("ram-drop") {
		select {
		case <-deadline:
			t.Fatal("RAM refusal retained assigned custody without starting or releasing")
		case <-time.After(5 * time.Millisecond):
		}
	}
	r := h.result("ram-drop")
	if r.Success || r.ReleasePending || r.ExecutionCause == nil || r.ExecutionCause.Operation != "launch" || r.ExecutionCause.Class != "resource_limit" || !strings.Contains(r.Error, "host RAM need 22 GiB, available 21.9 GiB") {
		t.Fatalf("RAM refusal must release with its admission diagnostic: %+v", r)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("entrypoint ran without its RAM reservation", err)
	}

	q, _ := fixture(t)
	m := machine("ram-worker", "fake", "ram-session")
	m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
	s := spec("experiment")
	s.Needs.HostMemoryGB = 22
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != j.ID {
		t.Fatal("fixture was not assigned")
	}
	r.Execution, r.Machine, r.Session = Execution{ID: j.ID, Attempt: assigned.Attempt}, m.ID, m.Session
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || !got.ReleaseConfirmed || got.StartedAt != nil || got.Losses != 0 || got.InfrastructureRetries != 0 {
		t.Fatalf("prelaunch RAM refusal must wait in the queue without a loss: %+v", got)
	}
}
