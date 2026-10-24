package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// These tests assert the pressure plan, rather than the current implementation.
func TestVersoPressureCheckpointSkew(t *testing.T) {
	for _, offset := range []int{-120, -30, -5, 5, 30, 120} {
		t.Run(strconv.Itoa(offset), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "job")
			now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			for attempt, unit := range map[int]string{1: "2", 2: "9"} {
				dir := filepath.Join(root, "attempt-"+strconv.Itoa(attempt))
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				p := filepath.Join(dir, "checkpoint.json")
				if err := os.WriteFile(p, []byte(unit), 0600); err != nil {
					t.Fatal(err)
				}
				stamp := now
				if offset > 0 && attempt == 1 || offset < 0 && attempt == 2 {
					stamp = now.Add(time.Duration(offset) * time.Second)
				}
				if err := os.Chtimes(p, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			dest, err := prepareCheckpoint(Job{ID: "synthetic-skew", Attempt: 3, Checkpoint: root})
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dest, "checkpoint.json"))
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != "9" {
				t.Fatalf("completed progress regressed: resumed=%s want=9 skew_seconds=%d", data, offset)
			}
		})
	}
}

func TestProductionDuringMissingNAS(t *testing.T) {
	q, clock := fixture(t)
	s := spec("production")
	s.Resumable = false
	j := submit(t, q, s)
	fakeCheckpointAvailability(t, q, false)
	m := machine("synthetic-only", "fake")
	m.CheckpointUnavailable = true
	if reply := check(t, q, m, nil); reply.Job == nil {
		clock.Advance(31 * time.Second)
		t.Fatalf("production blocked by NAS: %s", job(t, q, j.ID).State)
	}
}

func TestProductionPreemptsDuringNASLoss(t *testing.T) {
	q, clock := fixture(t)
	exp := submit(t, q, spec("experiment"))
	m := machine("worker", "fake")
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("experiment did not start")
	}
	fakeCheckpointAvailability(t, q, false)
	m.CheckpointUnavailable = true
	s := spec("production")
	s.Resumable = false
	prod := submit(t, q, s)
	reply := check(t, q, m, running)
	if !reply.Stop {
		t.Fatal("NAS outage prevents production preemption")
	}
	clock.Advance(10 * time.Second)
	if !check(t, q, m, running).Kill {
		t.Fatal("experiment not force-stopped at grace")
	}
	// Physical release is reported by the worker; never credit only a deadline.
	if err := q.Complete(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: exp.ID, Attempt: running.Attempt}, Interrupted: true}); err != nil && err != ErrStale {
		t.Fatal(err)
	}
	if reply := check(t, q, m, nil); reply.Job == nil || reply.Job.ID != prod.ID {
		t.Fatal("production not assigned after release")
	}
}

func TestWorkerProductionWithoutNAS(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	q, _ := fixture(t)
	s := spec("production")
	s.Resumable = false
	s.Run.Command = []string{"true"}
	submit(t, q, s)
	j := check(t, q, w.Machine, nil).Job
	if j == nil {
		t.Fatal("production not assigned")
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	store.state.Session = j.Session
	j.Checkpoint = filepath.Join(t.TempDir(), "missing", "job")
	p, err := w.start(store, *j)
	if err != nil {
		t.Fatalf("production requires NAS: %v", err)
	}
	f := <-p.done
	if !f.result.Success {
		t.Fatalf("production failed: %+v", f)
	}
}

func TestBlockedNASDoesNotHoldQueue(t *testing.T) {
	q, _ := fixture(t)
	s := spec("production")
	s.Resumable = false
	submit(t, q, s)
	// Drain a probe left pending by setup before replacing the fault seam.
	await(t, func() bool {
		_, err := q.nasProbe.run(q.root, func() (bool, error) { return true, nil })
		return err == nil
	})
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	// This fixture is about blocked storage, so retain the real bounded I/O
	// adapter explicitly rather than the synchronous availability fake.
	q.checkpointProbe = func() bool {
		available, _ := q.nasProbe.run(q.root, func() (bool, error) {
			if calls.Add(1) == 1 {
				close(entered)
			}
			<-release
			return false, nil
		})
		return available
	}
	done := make(chan Reply, 1)
	go func() { r, _ := q.Checkin(Checkin{Machine: machine("worker", "fake")}); done <- r }()
	select {
	case <-entered:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("filesystem fault did not enter")
	}
	select {
	case r := <-done:
		if r.Job == nil {
			t.Fatal("production blocked by hung NAS")
		}
	case <-time.After(heartbeatFilesystemBudget + time.Second):
		t.Fatal("checkin hangs behind NAS syscall")
	}
	tick := make(chan error, 1)
	go func() { tick <- q.Tick() }()
	select {
	case err := <-tick:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(heartbeatFilesystemBudget + time.Second):
		t.Fatal("NAS holds queue loop")
	}
	if calls.Load() != 1 {
		t.Fatal("hung probes accumulate")
	}
}

func TestBlockedCheckpointPreparationIsBounded(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	w.checkpointPrepare = func(j Job) (string, error) { close(entered); <-release; return "", errors.New("NAS unreachable") }
	done := make(chan error, 1)
	go func() { _, err := w.start(store, recoveryJob(t, []string{"true"})); done <- err }()
	select {
	case <-entered:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("filesystem fault did not enter")
	}
	select {
	case err := <-done:
		if !errors.Is(err, errWorkerCheckpoint) {
			t.Fatalf("%v", err)
		}
	case <-time.After(checkpointPreparationBudget + time.Second):
		t.Fatal("checkpoint preparation hangs start")
	}
}

func TestCheckpointGenerationOverridesAttemptAndMtime(t *testing.T) {
	for _, offset := range []int{-120, -30, -5, 5, 30, 120} {
		t.Run(strconv.Itoa(offset), func(t *testing.T) {
			j := recoveryJob(t, []string{"true"})
			j.Attempt = 3
			clock := &fakeClock{now: time.Unix(1000, 0)}
			for attempt, generation := range map[int]int{1: 9, 2: 2} {
				dir := filepath.Join(j.Checkpoint, fmt.Sprintf("attempt-%d", attempt))
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(dir, "checkpoint.json")
				data := fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":{"unit":%d}}`, generation, generation)
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
				stamp := clock.Now().Add(time.Duration(offset*attempt) * time.Second)
				if err := os.Chtimes(path, stamp, stamp); err != nil {
					t.Fatal(err)
				}
			}
			dest, err := prepareCheckpoint(j)
			if err != nil {
				t.Fatal(err)
			}
			generation, err := checkpointProgress(dest)
			if err != nil || generation != 9 {
				t.Fatalf("generation %d: %v", generation, err)
			}
		})
	}
}

func TestReconciliationUsesDurableGeneration(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("old", "fake")
	old := check(t, q, m, nil).Job
	clock.Advance(LossAfter)
	check(t, q, observer(), nil)
	n := machine("current", "fake")
	current := check(t, q, n, nil).Job
	dir := filepath.Join(q.root, current.Checkpoint, fmt.Sprintf("attempt-%d", current.Attempt))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "checkpoint.json")
	if err := os.WriteFile(path, []byte(`{"myce_checkpoint":{"generation":9},"value":9}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Unix(1, 0), time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: old.ID, Attempt: old.Attempt, Progress: 2}})
	if err != nil || !reply.Kill {
		t.Fatalf("durable newer save lost: %+v %v", reply, err)
	}
}

func TestWorkerForceStopsBlockedSaveAtGrace(t *testing.T) {
	w := recoveryWorker(t)
	clock := &workerFakeClock{now: time.Unix(1000, 0)}
	w.Clock = clock
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sh", "-c", `trap 'echo saving; cat "$MYCE_CHECKPOINT_DIR/save-block"; exit 0' TERM; echo ready; while :; do sleep 0.01; done`})
	w = workerWithPreparedCheckpoint(t, w, store, j)
	p, err := w.start(store, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop(clock.Now(), 0, true)
	if err := syscall.Mkfifo(filepath.Join(attemptCheckpoint(j), "save-block"), 0600); err != nil {
		t.Fatal(err)
	}
	logged := func(text string) bool {
		data, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		return strings.Contains(string(data), text)
	}
	await(t, func() bool { return logged("ready") })
	p.stop(clock.Now(), 10*time.Second, false)
	await(t, func() bool { return logged("saving") })
	clock.Advance(10 * time.Second)
	p.enforceGrace(clock.Now())
	select {
	case f := <-p.done:
		if !f.result.Interrupted || f.result.Success {
			t.Fatalf("blocked save not interrupted: %+v", f.result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("force-stop waits on checkpoint save")
	}
}

func TestLegacyCachedTimestampCannotOverrideGeneration(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("old", "fake")
	old := check(t, q, m, nil).Job
	clock.Advance(LossAfter)
	check(t, q, observer(), nil)
	n := machine("current", "fake")
	current := check(t, q, n, nil).Job
	dir := filepath.Join(q.root, current.Checkpoint, fmt.Sprintf("attempt-%d", current.Attempt))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":2},"value":2}`), 0600); err != nil {
		t.Fatal(err)
	}
	// A queue restored from the old protocol may contain a machine-clock mtime.
	q.s.Jobs[current.ID].Progress = clock.Now().UnixNano()
	reply, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: old.ID, Attempt: old.Attempt, Progress: 9}})
	if err != nil || reply.Kill || reply.Hold || job(t, q, old.ID).Machine != m.ID {
		t.Fatalf("cached timestamp overruled generation: %+v %v", reply, err)
	}
}

func TestLegacyMetadataWithoutGenerationLoads(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"myce_checkpoint":{"description":"legacy"},"step":9}`)
	if err := os.WriteFile(filepath.Join(j.Checkpoint, "checkpoint.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatalf("unsequenced legacy checkpoint rejected: %v", err)
	}
	loaded, err := os.ReadFile(filepath.Join(dest, "checkpoint.json"))
	if err != nil || string(loaded) != string(data) {
		t.Fatalf("legacy bytes changed: %v", err)
	}
}

func TestWorkerClearsWithdrawnPreparationFailure(t *testing.T) {
	w := recoveryWorker(t)
	j := recoveryJob(t, []string{"true"})
	w.CheckpointRoot = filepath.Dir(j.Checkpoint)
	w.checkpointPrepare = func(Job) (string, error) { return "", errors.New("NAS unavailable during preparation") }
	reports := make(chan bool, 1)
	assigned := false
	server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/workers/checkin" {
			if req.URL.Path == "/api/workers/complete" {
				return
			}
			http.NotFound(rw, req)
			return
		}
		var c Checkin
		if err := json.NewDecoder(req.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		c, err := normalizeAttemptCheckin(c)
		if err != nil {
			t.Error(err)
			return
		}
		reply := Reply{}
		if !assigned {
			assigned = true
			j.Machine, j.Session = c.Machine.ID, c.Machine.Session
			reply.Job = &j
		} else if len(c.Attempts) != 0 {
			if len(c.Attempts) != 1 {
				t.Error("expected one held attempt")
				return
			}
			a := c.Attempts[0]
			reply.Attempts = []AttemptDirective{{Execution: a.Execution, Session: a.Session, Stop: true, Kill: true}}
		} else {
			reports <- c.Machine.CheckpointUnavailable
			reply.Leave = true
		}
		if err := json.NewEncoder(rw).Encode(attemptReply(c, reply)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case unavailable := <-reports:
		if unavailable {
			t.Fatal("withdrawn preparation failure permanently blocks a recovered worker")
		}
	default:
		t.Fatal("worker never reported after withdrawing the failed preparation")
	}
}

func TestFailedPreparationDoesNotPublishGeneration(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 2
	source := filepath.Join(j.Checkpoint, "attempt-1")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "a-checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":9},"value":9}`), 0600); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(source, "z-payload")
	if err := syscall.Mkfifo(blocked, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCheckpoint(j); err == nil {
		t.Fatal("unsupported payload should fail preparation")
	}
	if _, err := os.Stat(attemptCheckpoint(j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed preparation published an incomplete attempt: %v", err)
	}
	if err := os.Rename(blocked, blocked+".tmp"); err != nil {
		t.Fatal(err)
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	if generation, err := checkpointProgress(dest); err != nil || generation != 9 {
		t.Fatalf("retry lost the durable checkpoint: %d %v", generation, err)
	}
	saved := filepath.Join(dest, "a-checkpoint.json")
	if err := os.WriteFile(saved, []byte(`{"myce_checkpoint":{"generation":10},"value":10}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCheckpoint(j); err != nil {
		t.Fatal(err)
	}
	if generation, err := checkpointProgress(dest); err != nil || generation != 10 {
		t.Fatalf("prepared retry overwrote its own save: %d %v", generation, err)
	}
}

func TestCheckpointGenerationInNestedAttemptNamedDirectory(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 3
	for attempt, generation := range map[int]int{1: 9, 2: 2} {
		dir := filepath.Join(j.Checkpoint, fmt.Sprintf("attempt-%d", attempt), "attempt-solver")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		data := fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":%d}`, generation, generation)
		if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "attempt-solver", "checkpoint.json"))
	if err != nil || !strings.Contains(string(data), `"value":9`) {
		t.Fatalf("nested generation lost: %s %v", data, err)
	}
}

func TestPreparationRejectsConcurrentPublication(t *testing.T) {
	j := recoveryJob(t, []string{"true"})
	j.Attempt = 2
	source := filepath.Join(j.Checkpoint, "attempt-1")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(source, "checkpoint.json")
	if err := os.WriteFile(manifest, []byte(`{"myce_checkpoint":{"generation":9},"value":9}`), 0600); err != nil {
		t.Fatal(err)
	}
	copyFile := func(dst io.Writer, src io.Reader) (int64, error) {
		// Publish after WalkDir enumerates files; the new payload is outside that snapshot.
		if err := os.WriteFile(filepath.Join(source, "payload"), []byte("new durable payload"), 0600); err != nil {
			return 0, err
		}
		if err := os.WriteFile(manifest+".tmp", []byte(`{"myce_checkpoint":{"generation":10},"value":{"payload":"payload"}}`), 0600); err != nil {
			return 0, err
		}
		if err := os.Rename(manifest+".tmp", manifest); err != nil {
			return 0, err
		}
		current, err := os.Open(manifest)
		if err != nil {
			return 0, err
		}
		defer current.Close()
		return io.Copy(dst, current)
	}
	if _, err := prepareCheckpointCopy(j, copyFile); err == nil || !strings.Contains(err.Error(), "changed during preparation") {
		t.Fatalf("accepted a manifest without its concurrent payload: %v", err)
	}
	if _, err := os.Stat(attemptCheckpoint(j)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published concurrent partial snapshot: %v", err)
	}
	dest, err := prepareCheckpoint(j)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(dest, "payload")); err != nil || string(data) != "new durable payload" {
		t.Fatalf("retry omitted the durable payload: %s %v", data, err)
	}
}
