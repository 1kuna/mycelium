package myce

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// HTTP observation samples time outside q.mu, including before lock acquisition.
// Queue fixtures therefore need the same synchronized clock as worker fixtures.
type fakeClock = workerFakeClock

// Explicit policies and historical spans used by regression fixtures. These
// are intentionally not production retention defaults.
const AttemptFilesGrace = 24 * time.Hour
const diagnosticTailBytes = 64 << 10

const statusCacheLifetime = time.Second

const externalBucketWidth = 15 * time.Minute
const externalBucketCount = 7*24*4 + 1

const jobHistoryRetention = 7 * 24 * time.Hour
const submissionRetention = 30 * 24 * time.Hour
const workCurveMaxAge = 30 * 24 * time.Hour
const attemptHistoryLimit = 4

const testFileRetention = 7 * 24 * time.Hour
const testLogRetention = 7 * 24 * time.Hour

func fixture(t testing.TB) (*Queue, *fakeClock) {
	t.Helper()
	c := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	q.fileRetention, q.logRetention = testFileRetention, testLogRetention
	// These fixtures exercise explicit historical operator minimums.
	q.lossTimeouts = [3]time.Duration{ProductionLossAfter, DeadlineLossAfter, LossAfter}
	q.s.PublicationHorizon = filePublicationGrace
	return q, c
}

// Whole-state rollback comparisons include asynchronous diagnostic commits.
// Join those commits before taking a locked snapshot of the transaction state.
func marshalQueueState(t *testing.T, q *Queue) ([]byte, error) {
	t.Helper()
	drainAdmissionWait(t, q)
	q.mu.Lock()
	defer q.mu.Unlock()
	return json.Marshal(q.s)
}

// Scheduling and recovery tests model availability without timing filesystem I/O.
func fakeCheckpointAvailability(t testing.TB, q *Queue, available bool) {
	t.Helper()
	// Evidence publication also calls the probe, outside q.mu. Join it before
	// replacing the callback; locking only the state write cannot protect it.
	drainAdmissionWait(t, q)
	// Join Open's pending observation before replacing its probe seam.
	await(t, func() bool {
		_, err := q.nasProbe.run(q.root, func() (bool, error) { return true, nil })
		return err == nil
	})
	q.checkpointProbe = func() bool { return available }
	q.mu.Lock()
	q.nasAvailable = available
	q.mu.Unlock()
}

func spec(priority string) Spec {
	s := Spec{Name: "job", Project: "test", Priority: priority, Resumable: true}
	s.Run.Command = []string{"echo", "done"}
	s.Needs.MemoryGB = 8
	s.Needs.Platform.Any = true
	if priority == "deadline" {
		d := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
		s.Deadline = &d
	}
	return s
}
func submit(t testing.TB, q *Queue, s Spec) *Job {
	t.Helper()
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func machine(id, gpu string, host ...string) Machine {
	var physicalHost string
	if len(host) > 0 {
		physicalHost = host[0]
	}
	return Machine{Capabilities: []string{"native_isolation_landlock"}, CPUIsolation: "select-none", HostID: physicalHost, ID: id, Session: "session-" + id, GPU: gpu, FreeMemoryGB: 8, HostMemoryMeasured: true, HostAvailableMemoryGB: 64, FreeHostMemoryGB: 64, CapabilityProfile: &CapabilityProfile{Version: 0, OS: "linux", Arch: "amd64", LogicalCPUs: 64, MemoryLayout: "separate", SystemMemoryBytes: 64 << 30, Accelerators: []AcceleratorProfile{{Type: gpu, MemoryBytes: 64 << 30}}}}
}
func check(t testing.TB, q *Queue, m Machine, r *Job) Reply {
	t.Helper()
	c := Checkin{Machine: m, period: time.Second} // Simulate the current worker timing header.
	if r != nil {
		c.Running = &Execution{ID: r.ID, Attempt: r.Attempt}
		c.Machine.FreeMemoryGB = max(0, m.FreeMemoryGB-r.Spec.Needs.MemoryGB)
	}
	v, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	// Resolve checkpoints as a worker does against the queue's root, so tests
	// that hand the job to worker internals never write into the package
	// directory. Use checkRelative to see the wire's relative paths.
	// Job and Attempts may share one record; resolve each path once.
	resolve := func(j *Job) {
		if j != nil && j.Checkpoint != "" && !filepath.IsAbs(j.Checkpoint) {
			j.Checkpoint = filepath.Join(q.root, j.Checkpoint)
		}
	}
	resolve(v.Job)
	for i := range v.Attempts {
		resolve(v.Attempts[i].Job)
	}
	return v
}

func checkRelative(t testing.TB, q *Queue, m Machine) *Job {
	t.Helper()
	v, err := q.Checkin(Checkin{Machine: m, period: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return v.Job
}
func finish(t *testing.T, q *Queue, j *Job, success bool) {
	t.Helper()
	err := q.Complete(result(j, success))
	if err != nil {
		t.Fatal(err)
	}
}

// Synthetic acknowledgement of a delivered planned stop, distinct from an
// independently failing payload. Real adapters establish this through D5 facts.
func finishInterrupted(t *testing.T, q *Queue, j *Job) {
	t.Helper()
	r := result(j, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
}
func result(j *Job, success bool) Result {
	return Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: success, Error: "test error"}
}
func job(t *testing.T, q *Queue, id string) *Job {
	t.Helper()
	s, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range s.Jobs {
		if j.ID == id {
			return j
		}
	}
	t.Fatal("missing job")
	return nil
}

// Inspect raw stored state without Snapshot's maintenance/projections. Async
// admission evidence can update a live job after its submitting call returns.
func storedTestJob(t testing.TB, q *Queue, id string) Job {
	t.Helper()
	q.mu.RLock()
	defer q.mu.RUnlock()
	j := q.s.Jobs[id]
	if j == nil {
		t.Fatalf("missing stored job %q", id)
	}
	return cloneJob(j)
}

func requireState(t *testing.T, q *Queue, id, state string) {
	t.Helper()
	if j := job(t, q, id); j.State != state {
		t.Fatalf("state=%s want=%s: %s", j.State, state, j.Reason)
	}
}

func TestMakingRoom(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, waiting   string
		resumable, stop bool
	}{{"production stops experiment", "production", true, true}, {"deadline stops experiment", "deadline", true, true}, {"production stops nonresumable", "production", false, true}, {"deadline preserves nonresumable", "deadline", false, false}, {"experiment cannot stop peer", "experiment", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			s := spec("experiment")
			s.Resumable = tc.resumable
			submit(t, q, s)
			m := machine("one", "fake")
			running := check(t, q, m, nil).Job
			submit(t, q, spec(tc.waiting))
			reply := check(t, q, m, running)
			if reply.Stop != tc.stop {
				t.Fatalf("stop=%v want=%v", reply.Stop, tc.stop)
			}
			grace := 10.0
			if tc.stop && reply.GraceSeconds != grace {
				t.Fatalf("grace=%v", reply.GraceSeconds)
			}
		})
	}
	t.Run("no preempt when idle machine fits", func(t *testing.T) {
		q, _ := fixture(t)
		submit(t, q, spec("experiment"))
		a := machine("one", "fake")
		r := check(t, q, a, nil).Job
		check(t, q, machine("two", "fake"), nil)
		submit(t, q, spec("production"))
		if check(t, q, a, r).Stop {
			t.Fatal("idle machine exists")
		}
	})
	t.Run("production stops deadline", func(t *testing.T) {
		q, _ := fixture(t)
		submit(t, q, spec("deadline"))
		m := machine("one", "fake")
		r := check(t, q, m, nil).Job
		submit(t, q, spec("production"))
		if !check(t, q, m, r).Stop {
			t.Fatal("production must stop resumable deadline")
		}
	})
	t.Run("no preempt when reclaimed memory insufficient", func(t *testing.T) {
		q, _ := fixture(t)
		submit(t, q, spec("experiment"))
		m := machine("one", "fake")
		r := check(t, q, m, nil).Job
		s := spec("production")
		s.Needs.MemoryGB = 9
		submit(t, q, s)
		if check(t, q, m, r).Stop {
			t.Fatal("machine cannot fit even after stopping")
		}
	})
	t.Run("only one victim per waiting job", func(t *testing.T) {
		q, _ := fixture(t)
		submit(t, q, spec("experiment"))
		a := machine("one", "fake")
		r := check(t, q, a, nil).Job
		submit(t, q, spec("experiment"))
		b := machine("two", "fake")
		r2 := check(t, q, b, nil).Job
		submit(t, q, spec("production"))
		check(t, q, a, r)
		check(t, q, b, r2)
		s, _ := q.Snapshot()
		n := 0
		for _, j := range s.Jobs {
			if j.State == "stopping" {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("stopped %d jobs", n)
		}
	})
}
func TestSuddenLossAndLateReturn(t *testing.T) {
	t.Parallel()
	for _, resumable := range []bool{true, false} {
		t.Run("resumable="+stringBool(resumable), func(t *testing.T) {
			q, c := fixture(t)
			s := spec("experiment")
			s.Resumable = resumable
			submit(t, q, s)
			a := machine("one", "fake")
			a.Models = []string{"loaded"}
			old := check(t, q, a, nil).Job
			c.Advance(59 * time.Second)
			requireState(t, q, old.ID, "running")
			c.Advance(time.Second)
			check(t, q, observer(), nil)
			requireState(t, q, old.ID, "queued")
			snap, _ := q.Snapshot()
			for _, lost := range snap.Machines {
				if lost.ID == a.ID && (!lost.Gone || len(lost.Models) != 0) {
					t.Fatal("lost inventory not cleared")
				}
			}
			next := check(t, q, machine("two", "fake"), nil).Job
			if next.Attempt != 2 || next.Checkpoint != old.Checkpoint {
				t.Fatal("retry lost checkpoint or attempt")
			}
			reply := check(t, q, a, old)
			if !reply.Stop || !reply.Kill {
				t.Fatal("returning stale copy must be killed")
			}
			oldKey := releaseKey(HostRelease{Job: old.ID, Machine: old.Machine, Session: old.Session, Attempt: old.Attempt})
			if release, ok := q.s.HostReleases[oldKey]; !ok || release.CPUCores != 1 {
				t.Fatal("loss did not retain the old attempt's default core")
			}
			if err := q.Complete(result(old, true)); !errors.Is(err, ErrStale) {
				t.Fatalf("old completion accepted: %v", err)
			}
			if _, ok := q.s.HostReleases[oldKey]; ok {
				t.Fatal("rejected result did not settle its exact release")
			}
			current := job(t, q, next.ID)
			if current.State != "running" || current.Attempt != next.Attempt || current.Machine != next.Machine || current.Session != next.Session || current.ReleaseConfirmed {
				t.Fatal("old completion changed replacement attempt", current)
			}
			if err := q.Complete(result(old, true)); !errors.Is(err, ErrStale) {
				t.Fatalf("repeated old completion accepted: %v", err)
			}
			if err := q.AppendLog(LogChunk{Result: result(old, true), Text: "late"}); !errors.Is(err, ErrStale) {
				t.Fatal("old logs accepted")
			}
			finish(t, q, next, true)
		})
	}
}
func stringBool(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// compactStatusFinding reads the public status endpoint as a consumer would.
func compactStatusFinding(t *testing.T, q *Queue, path, id string) *JobFinding {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer secret")
	API{Queue: q, Token: "secret"}.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("status %s: %d", path, w.Code)
	}
	if path == "/api/status" {
		// The default compact view: the finding is its own field and its own
		// unclipped text line, not a tail cut off the reason.
		var view CompactStatus
		if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		for _, j := range view.Jobs {
			if j.ID == id {
				if j.Finding != nil && (strings.Contains(j.Reason, "finding (") || !slices.Contains(view.Lines(true), "  "+j.Finding.Text())) {
					t.Fatalf("compact finding duplicated or clipped: %q %q", j.Reason, view.Lines(true))
				}
				return j.Finding
			}
		}
		t.Fatalf("compact status omitted job %s", id)
	}
	var snap Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
		t.Fatalf("status %s: %v", path, err)
	}
	for _, j := range snap.Jobs {
		if j.ID == id {
			return j.Finding
		}
	}
	t.Fatalf("status %s omitted job %s", path, id)
	return nil
}

func TestThreeAttempts(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	s := spec("experiment")
	s.Resumable = false
	j := submit(t, q, s)
	m := machine("one", "fake")
	for i := 1; i <= 3; i++ {
		r := check(t, q, m, nil).Job
		if r == nil || r.Attempt != i {
			t.Fatalf("attempt %d: %+v", i, r)
		}
		advanceReportingPeer(t, q, c, observer(), LossAfter)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		// Reusing the same worker requires cleanup of its exact lost copy.
		released := result(r, false)
		released.ReleaseOnly = true
		if err := q.Complete(released); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			// The owner sees a Mycelium-side requeue, not a command failure.
			if public, err := q.jobSnapshot(j.ID); err != nil || public.Finding == nil || public.Finding.Code != "infrastructure_retry" {
				t.Fatalf("requeue finding: %+v %v", public, err)
			}
			if f := compactStatusFinding(t, q, "/api/status", j.ID); f == nil || f.Code != "infrastructure_retry" {
				t.Fatalf("default compact status lost the requeue finding: %+v", f)
			}
		}
	}
	requireState(t, q, j.ID, "failed")
	if !strings.Contains(job(t, q, j.ID).Reason, "3 infrastructure losses") {
		t.Fatal("missing plain failure reason")
	}
	public, err := q.jobSnapshot(j.ID)
	if err != nil || public.Finding == nil || public.Finding.Owner != "mycelium" || !strings.Contains(public.Finding.What, "3 attempts") {
		t.Fatalf("exhaustion finding: %+v %v", public, err)
	}
	if f := compactStatusFinding(t, q, "/api/status?jobs=recent", j.ID); f == nil || *f != *public.Finding {
		t.Fatalf("recent compact status lost the failure finding: %+v", f)
	}
	waited, err := q.waitIndex().wait(t.Context(), WaitRequest{IDs: []string{j.ID}})
	if err != nil || len(waited.Jobs) != 1 || waited.Jobs[0].Finding == nil || *waited.Jobs[0].Finding != *public.Finding {
		t.Fatalf("wait finding differs from job GET: %+v %v", waited, err)
	}
	if check(t, q, m, nil).Job != nil {
		t.Fatal("fourth attempt assigned")
	}
}
func TestCancelAndFailure(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	a := submit(t, q, spec("experiment"))
	if err := q.Cancel(a.ID); err != nil {
		t.Fatal(err)
	}
	requireState(t, q, a.ID, "cancelled")
	b := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	if err := q.Cancel(b.ID); err != nil {
		t.Fatal(err)
	}
	if !check(t, q, m, r).Stop {
		t.Fatal("running cancellation did not signal")
	}
	finish(t, q, r, false)
	requireState(t, q, b.ID, "cancelled")
	c := submit(t, q, spec("experiment"))
	r = check(t, q, m, nil).Job
	finish(t, q, r, false)
	requireState(t, q, c.ID, "failed")
}
func TestPersistenceAndAssignmentRedelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.db")
	clock := &fakeClock{now: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)}
	q, err := Open(path, t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	// A slow checkpoint probe can temporarily withhold either assignment response.
	assignment := func() *Job {
		var assigned *Job
		await(t, func() bool {
			assigned = check(t, q, m, nil).Job
			return assigned != nil
		})
		return assigned
	}
	r := assignment()
	if r.ID != j.ID || r.Attempt != 1 {
		t.Fatalf("assignment %+v want job %s attempt 1", r, j.ID)
	}
	if retry := assignment(); retry.ID != r.ID || retry.Attempt != 1 {
		t.Fatal("assignment response lost")
	}
	if err := q.AppendLog(LogChunk{Result: result(r, true), Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	q.Close()
	q, err = Open(path, t.TempDir(), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	fakeCheckpointAvailability(t, q, true)
	requireState(t, q, j.ID, "running")
	if reply := check(t, q, m, r); reply.Stop {
		t.Fatal("queue restart lost running job")
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) != 1 || logs[0].Text != "hello" {
		t.Fatalf("logs %+v %v", logs, err)
	}
	finish(t, q, r, true)
	requireState(t, q, j.ID, "succeeded")
}
func TestValidationAndYAML(t *testing.T) {
	for _, p := range []string{"[cuda]", "any", "{required: [xpu]}", "{preferred: [metal]}", "{any: true}"} {
		s := spec("experiment")
		data := "name: demo\nproject: test\npriority: experiment\nrun:\n  command: [echo, hello]\nneeds:\n  memory_gb: 8\n  platform: " + p + "\nenvironment: torch-cuda\n"
		if err := yaml.Unmarshal([]byte(data), &s); err != nil {
			t.Fatal(err)
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
	for _, change := range []func(*Spec){func(s *Spec) { s.Needs.MemoryGB = -1 }, func(s *Spec) { s.Priority = "unknown" }, func(s *Spec) { s.Priority = "deadline"; s.Deadline = nil }, func(s *Spec) { s.Needs.Platform = Platform{} }, func(s *Spec) { s.Needs.Platform = Platform{Required: []string{"unknown"}} }, func(s *Spec) { s.Needs.Platform.Required = []string{"cuda"} }, func(s *Spec) { s.Environment = Environment{Name: "a", LockFile: "b"} }, func(s *Spec) { s.Run.Command = nil }} {
		s := spec("experiment")
		change(&s)
		if s.Validate() == nil {
			t.Fatal("invalid job accepted")
		}
	}
}

func observer() Machine { m := machine("observer", "metal"); m.FreeMemoryGB = 1; return m }

// Operator-authored publication window used only by lifecycle fixtures.
const filePublicationGrace = 6 * time.Hour

func (q *Queue) dbPathForTest(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}
