package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }
func fixture(t *testing.T) (*Queue, *fakeClock) {
	t.Helper()
	c := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(t.TempDir(), "queue.db"), t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	// Scheduling tests model NAS availability without timing the shared disk's fsync.
	await(t, func() bool {
		_, err := q.nasProbe.run(q.root, func() (bool, error) { return true, nil })
		return err == nil
	})
	q.checkpointProbe = func() bool {
		info, err := os.Stat(q.root)
		return err == nil && info.IsDir()
	}
	q.nasAvailable = q.checkpointProbe()
	return q, c
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
func submit(t *testing.T, q *Queue, s Spec) *Job {
	t.Helper()
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func machine(id, gpu string) Machine {
	return Machine{ID: id, Session: "session-" + id, GPU: gpu, FreeMemoryGB: 8, CapabilityProfile: &CapabilityProfile{Version: 0, MemoryLayout: "separate", SystemMemoryBytes: 64 << 30, Accelerators: []AcceleratorProfile{{Type: gpu, MemoryBytes: 64 << 30}}}}
}
func check(t *testing.T, q *Queue, m Machine, r *Job) Reply {
	t.Helper()
	c := Checkin{Machine: m}
	if r != nil {
		c.Running = &Execution{ID: r.ID, Attempt: r.Attempt}
		c.Machine.FreeMemoryGB = max(0, m.FreeMemoryGB-r.Spec.Needs.MemoryGB)
	}
	v, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func finish(t *testing.T, q *Queue, j *Job, success bool) {
	t.Helper()
	err := q.Complete(result(j, success))
	if err != nil {
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
func requireState(t *testing.T, q *Queue, id, state string) {
	t.Helper()
	if j := job(t, q, id); j.State != state {
		t.Fatalf("state=%s want=%s: %s", j.State, state, j.Reason)
	}
}

func TestSchedulingOrder(t *testing.T) {
	cases := []struct {
		name          string
		first, second Spec
		models, envs  []string
		wantSecond    bool
	}{
		{name: "production before deadline", first: spec("deadline"), second: spec("production"), wantSecond: true},
		{name: "deadline before experiment", first: spec("experiment"), second: spec("deadline"), wantSecond: true},
		{name: "production before loaded experiment", first: spec("experiment"), second: spec("production"), models: []string{"loaded"}, wantSecond: true},
		{name: "loaded model before waiting age", first: spec("experiment"), second: spec("experiment"), models: []string{"loaded"}, wantSecond: true},
		{name: "ready environment before waiting age", first: spec("experiment"), second: spec("experiment"), envs: []string{"ready"}, wantSecond: true},
		{name: "earliest deadline", first: spec("deadline"), second: spec("deadline"), wantSecond: true},
		{name: "longest waiting", first: spec("experiment"), second: spec("experiment")},
		{name: "readiness before earliest deadline", first: spec("deadline"), second: spec("deadline"), models: []string{"loaded"}, wantSecond: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q, c := fixture(t)
			if strings.Contains(tc.name, "loaded experiment") {
				tc.first.Needs.Model = "loaded"
			}
			if strings.Contains(tc.name, "model") || strings.Contains(tc.name, "readiness") {
				tc.second.Needs.Model = "loaded"
			}
			if strings.Contains(tc.name, "environment") {
				tc.second.Environment.Name = "ready"
			}
			if tc.name == "earliest deadline" {
				d := tc.first.Deadline.Add(-time.Hour)
				tc.second.Deadline = &d
			}
			if strings.Contains(tc.name, "readiness") {
				d := tc.second.Deadline.Add(time.Hour)
				tc.second.Deadline = &d
			}
			a := submit(t, q, tc.first)
			c.Advance(time.Second)
			b := submit(t, q, tc.second)
			m := machine("one", "fake")
			m.Models = tc.models
			m.Environments = tc.envs
			got := check(t, q, m, nil).Job
			want := a.ID
			if tc.wantSecond {
				want = b.ID
			}
			if got == nil || got.ID != want {
				t.Fatalf("got %+v want %s", got, want)
			}
		})
	}
}
func TestEligibility(t *testing.T) {
	for _, name := range []string{"memory", "required GPU", "allowed machines", "environment not ready", "leaving machine", "gone machine"} {
		t.Run(name, func(t *testing.T) {
			j := &Job{Spec: spec("experiment"), State: "queued"}
			m := machine("one", "cuda")
			switch name {
			case "memory":
				j.Spec.Needs.MemoryGB = 9
			case "required GPU":
				j.Spec.Needs.Platform = Platform{Required: []string{"metal"}}
			case "allowed machines":
				j.Spec.AllowedMachines = []string{"two"}
			case "environment not ready":
				j.Spec.Environment.Name = "mlx"
			case "leaving machine":
				m.Leaving = true
			case "gone machine":
				m.Gone = true
			}
			if fits(j, &m, m.FreeMemoryGB) {
				t.Fatal("ineligible job fits")
			}
		})
	}
	for _, gpu := range []string{"cuda", "xpu", "metal", "fake"} {
		t.Run("any on "+gpu, func(t *testing.T) {
			j := &Job{Spec: spec("experiment")}
			m := machine("one", gpu)
			if !fits(j, &m, 8) {
				t.Fatal("any must fit")
			}
		})
	}
}
func TestPreferredPlatform(t *testing.T) {
	q, _ := fixture(t)
	cuda := machine("cuda", "cuda")
	metal := machine("mac", "metal")
	check(t, q, cuda, nil)
	check(t, q, metal, nil)
	s := spec("experiment")
	s.Needs.Platform = Platform{Preferred: []string{"metal"}}
	j := submit(t, q, s)
	if check(t, q, cuda, nil).Job != nil {
		t.Fatal("preferred idle machine should receive job")
	}
	if got := check(t, q, metal, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("preferred machine did not get job")
	}
	q2, _ := fixture(t)
	j2 := submit(t, q2, s)
	if got := check(t, q2, cuda, nil).Job; got == nil || got.ID != j2.ID {
		t.Fatal("preference must permit another platform when preferred is unavailable")
	}
}
func TestMakingRoom(t *testing.T) {
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
			grace := 120.0
			if tc.waiting == "production" {
				grace = 10
				if !tc.resumable {
					grace = 0
				}
			}
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
func TestGraceAndFrontOfLine(t *testing.T) {
	q, c := fixture(t)
	submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	c.Advance(time.Second)
	other := submit(t, q, spec("experiment"))
	submit(t, q, spec("production"))
	reply := check(t, q, m, r)
	if !reply.Stop {
		t.Fatal("missing stop")
	}
	// Keep checking in so this tests grace, not sudden loss.
	c.Advance(3 * time.Second)
	check(t, q, m, r)
	c.Advance(3 * time.Second)
	check(t, q, m, r)
	c.Advance(3 * time.Second)
	if check(t, q, m, r).Kill {
		t.Fatal("killed before grace")
	}
	c.Advance(time.Second)
	if reply := check(t, q, m, r); !reply.Kill {
		t.Fatal("must kill stale copy at 10 seconds")
	}
	requireState(t, q, r.ID, "queued")
	production := check(t, q, m, nil).Job
	if production.Spec.Priority != "production" {
		t.Fatal("production should run first")
	}
	finish(t, q, production, true)
	resumed := check(t, q, m, nil).Job
	if resumed == nil || resumed.ID != r.ID || resumed.ID == other.ID || resumed.Attempt != 2 {
		t.Fatalf("did not return to front: %+v", resumed)
	}
}
func TestPlannedLeave(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		resumable, now, stop bool
	}{{"resumable", true, false, true}, {"wait for nonresumable", false, false, false}, {"force nonresumable", false, true, true}, {"force resumable", true, true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			s := spec("experiment")
			s.Resumable = tc.resumable
			submit(t, q, s)
			m := machine("one", "fake")
			r := check(t, q, m, nil).Job
			if err := q.Leave(m.ID, tc.now); err != nil {
				t.Fatal(err)
			}
			reply := check(t, q, m, r)
			if !reply.Leave || reply.Stop != tc.stop {
				t.Fatalf("reply %+v", reply)
			}
			finish(t, q, r, true)
			if tc.stop {
				requireState(t, q, r.ID, "queued")
			} else {
				requireState(t, q, r.ID, "succeeded")
			}
			if reply := check(t, q, m, nil); reply.Job != nil || !reply.Leave {
				t.Fatal("leaving worker got more work")
			}
			snp, _ := q.Snapshot()
			if !snp.Machines[0].Gone {
				t.Fatal("finished leaving machine should be gone")
			}
			m.Session = "new-session"
			check(t, q, m, nil)
		})
	}
}
func TestSuddenLossAndLateReturn(t *testing.T) {
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
			if err := q.Complete(result(old, true)); !errors.Is(err, ErrStale) {
				t.Fatalf("old completion accepted: %v", err)
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
func TestThreeAttempts(t *testing.T) {
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
		c.Advance(LossAfter)
		check(t, q, observer(), nil)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	requireState(t, q, j.ID, "failed")
	if !strings.Contains(job(t, q, j.ID).Reason, "3 infrastructure losses") {
		t.Fatal("missing plain failure reason")
	}
	if check(t, q, m, nil).Job != nil {
		t.Fatal("fourth attempt assigned")
	}
}
func TestWaitingReasonAndGiveUp(t *testing.T) {
	for _, kind := range []string{"platform", "pinned", "environment", "memory"} {
		t.Run(kind, func(t *testing.T) {
			q, c := fixture(t)
			s := spec("experiment")
			want := ""
			switch kind {
			case "platform":
				s.Needs.Platform = Platform{Required: []string{"metal"}}
				want = "needs metal"
			case "pinned":
				s.AllowedMachines = []string{"mac"}
				want = "allowed machines: mac"
			case "environment":
				s.Environment.Name = "mlx"
				want = "environment not ready: mlx"
			case "memory":
				s.Needs.MemoryGB = 80
				want = "80.0 GB"
			}
			giveUp := c.Now().Add(time.Minute)
			s.GiveUpAt = &giveUp
			j := submit(t, q, s)
			check(t, q, machine("one", "fake"), nil)
			got := job(t, q, j.ID)
			if !strings.Contains(got.Reason, "no machine in the fleet") || !strings.Contains(got.Reason, want) {
				t.Fatalf("reason %s", got.Reason)
			}
			c.Advance(time.Minute)
			requireState(t, q, j.ID, "failed")
		})
	}
}
func TestCancelAndFailure(t *testing.T) {
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
	for _, change := range []func(*Spec){func(s *Spec) { s.Needs.MemoryGB = 0 }, func(s *Spec) { s.Priority = "unknown" }, func(s *Spec) { s.Priority = "deadline"; s.Deadline = nil }, func(s *Spec) { s.Needs.Platform = Platform{} }, func(s *Spec) { s.Needs.Platform = Platform{Required: []string{"cpu"}} }, func(s *Spec) { s.Needs.Platform.Required = []string{"cuda"} }, func(s *Spec) { s.Environment = Environment{Name: "a", LockFile: "b"} }, func(s *Spec) { s.Run.Command = nil }} {
		s := spec("experiment")
		change(&s)
		if s.Validate() == nil {
			t.Fatal("invalid job accepted")
		}
	}
}

func TestPinnedMachineLeavesAndLateReturnBeforeReassignment(t *testing.T) {
	t.Run("pinned job waits after planned leave", func(t *testing.T) {
		q, _ := fixture(t)
		s := spec("experiment")
		s.AllowedMachines = []string{"one"}
		j := submit(t, q, s)
		m := machine("one", "fake")
		r := check(t, q, m, nil).Job
		if err := q.Leave("one", false); err != nil {
			t.Fatal(err)
		}
		finish(t, q, r, true)
		check(t, q, m, nil)
		if check(t, q, machine("two", "fake"), nil).Job != nil {
			t.Fatal("pinned job moved to disallowed machine")
		}
		got := job(t, q, j.ID)
		if got.State != "queued" || !strings.Contains(got.Reason, "allowed machines: one") {
			t.Fatalf("waiting pinned job: %+v", got)
		}
	})
	t.Run("late return before reassignment", func(t *testing.T) {
		q, c := fixture(t)
		j := submit(t, q, spec("experiment"))
		m := machine("one", "fake")
		old := check(t, q, m, nil).Job
		c.Advance(LossAfter)
		check(t, q, observer(), nil)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		reply := check(t, q, m, old)
		if reply.Stop {
			t.Fatal("live copy before reassignment should continue")
		}
		requireState(t, q, j.ID, "running")
	})
	t.Run("cancel remains cancelled after loss", func(t *testing.T) {
		q, c := fixture(t)
		j := submit(t, q, spec("experiment"))
		m := machine("one", "fake")
		old := check(t, q, m, nil).Job
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		c.Advance(LossAfter)
		check(t, q, observer(), nil)
		requireState(t, q, j.ID, "cancelled")
		if !check(t, q, m, old).Kill {
			t.Fatal("cancelled stale copy not stopped")
		}
		if check(t, q, m, nil).Job != nil {
			t.Fatal("cancelled job restarted")
		}
	})
}

func observer() Machine { m := machine("observer", "metal"); m.FreeMemoryGB = 1; return m }

func TestResumableThreeInfrastructureLosses(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	for i := 0; i < 3; i++ {
		if check(t, q, m, nil).Job == nil {
			t.Fatal("no assignment")
		}
		c.Advance(LossAfter)
		check(t, q, observer(), nil)
	}
	got := job(t, q, j.ID)
	if got.Losses != 3 || got.State != "failed" {
		t.Fatalf("after %d counted losses: state=%s, want failed", got.Losses, got.State)
	}
	if check(t, q, m, nil).Job != nil {
		t.Fatal("failed job received a fourth execution")
	}
}

func TestRestartProductionPreemptionAfterReport(t *testing.T) {
	dir := t.TempDir()
	root := t.TempDir()
	c := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
	path := filepath.Join(dir, "q.db")
	q, err := Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	r := check(t, q, m, nil).Job
	// An unreported worker must retain its recovery protection.
	other := machine("two", "fake")
	otherJob := submit(t, q, spec("experiment"))
	otherRun := check(t, q, other, nil).Job
	if otherRun == nil || otherRun.ID != otherJob.ID {
		t.Fatal("no second assignment")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	check(t, q, m, r)
	p := spec("production")
	p.AllowedMachines = []string{m.ID}
	submit(t, q, p)
	if !check(t, q, m, r).Stop {
		t.Fatalf("reconnected worker still running experiment for %s", q.restartUntil.Sub(c.Now()))
	}
	p.AllowedMachines = []string{other.ID}
	submit(t, q, p)
	if got := job(t, q, otherRun.ID); got.State != "running" || got.Losses != 0 {
		t.Fatalf("unreported worker lost restart protection: %+v", got)
	}
}
