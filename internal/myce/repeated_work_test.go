package myce

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func repeatedFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	a := cpuMachine("a-slow", "slow-host", "cpu", 2, 32)
	a.ExtensionFingerprint = "runtime-v1"
	b := cloneMachine(&a)
	b.ID, b.HostID, b.Session = "z-fast", "fast-host", "fast-session"
	check(t, q, a, nil)
	check(t, q, b, nil)
	return q, clock, a, b, cpuSpec(2, .1)
}

func cleanRepeatedResult(j *Job) Result {
	r := result(j, true)
	zero := 0.0
	r.MemoryPeak = &AttemptMemoryPeak{Pressure: &MemoryPressureEvidence{Reason: "eligible"}}
	r.Resources = &AttemptResources{CPU: AttemptCPUTime{Source: "cgroup_v2", UsageSeconds: &zero, ThrottledSeconds: &zero}, IO: AttemptIO{Pressure: &IOPressure{Some: &PressureStall{}}}}
	return r
}

func teachRepeated(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, duration time.Duration) Job {
	t.Helper()
	s.AllowedMachines = []string{m.ID}
	j := startObservation(t, q, m, s)
	clock.Advance(duration)
	if err := q.Complete(cleanRepeatedResult(&j)); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestRepeatedWorkLearnsPlacementAndBusyFallback(t *testing.T) {
	q, clock, slow, fast, s := repeatedFixture(t)
	for range 2 {
		teachRepeated(t, q, clock, slow, s, 3*time.Second)
		teachRepeated(t, q, clock, fast, s, time.Second)
	}
	next := submit(t, q, s)
	if got := check(t, q, slow, nil).Job; got != nil {
		t.Fatal("slower machine stole comparable work", got.ID)
	}
	run := check(t, q, fast, nil).Job
	if run == nil || run.ID != next.ID {
		t.Fatal("did not prefer learned faster machine", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, next.ID)
	if got.Timing.PredictedSeconds == nil || *got.Timing.PredictedSeconds != 1 || got.Timing.Samples != 2 || !strings.Contains(JobTiming(got), "observed range") || !strings.Contains(JobTiming(got), "exact command/source") {
		t.Fatalf("missing prediction/comparability/uncertainty: %+v", got.Timing)
	}
	// Both cores are now held on the fast host. A forecast cannot create cores,
	// reserve a future slot or make a currently admissible slow host wait.
	waiting := submit(t, q, s)
	if got := check(t, q, fast, run).Job; got != nil {
		t.Fatal("estimate granted busy capacity", got.ID)
	}
	if got := check(t, q, slow, nil).Job; got == nil || got.ID != waiting.ID {
		t.Fatal("waited for busy faster machine", got)
	}
}

func TestRepeatedWorkEvidenceFencesAndPersistence(t *testing.T) {
	q, clock, m, _, s := repeatedFixture(t)
	first := teachRepeated(t, q, clock, m, s, 2*time.Second)
	if err := q.Complete(cleanRepeatedResult(&first)); err != nil {
		t.Fatal(err)
	}
	base := &Job{Spec: first.Spec, State: "queued"}
	q.applyLearnedNeeds(base)
	if e, ok := q.estimateCompletion(base, q.s.Machines[m.ID]); !ok || e.samples != 1 || e.service != 2 {
		t.Fatal("missing sample or duplicate counted", e, ok)
	}
	for _, change := range []struct {
		name string
		edit func(*Job)
	}{
		{"source", func(j *Job) {
			j.Spec.Files = FileManifest{".myce-source.tar": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
		}},
		{"command", func(j *Job) { j.Spec.Run.Command = []string{"echo", "changed"} }},
		{"environment", func(j *Job) { j.Spec.Run.Env = map[string]string{"INPUT_SIZE": "2"} }},
		{"resumed", func(j *Job) { j.Spec.Resumable, j.Attempt = true, 1 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			copy := *base
			change.edit(&copy)
			if _, ok := q.estimateCompletion(&copy, q.s.Machines[m.ID]); ok {
				t.Fatal("incompatible command reused speed")
			}
		})
	}
	var database string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&database); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(database, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	check(t, reopened, m, nil)
	if e, ok := reopened.estimateCompletion(base, reopened.s.Machines[m.ID]); !ok || e.samples != 1 || e.service != 2 {
		t.Fatal("speed did not survive restart", e, ok)
	}
	clock.Advance(workCurveMaxAge + time.Second)
	if _, ok := reopened.estimateCompletion(base, reopened.s.Machines[m.ID]); ok {
		t.Fatal("expired evidence reused")
	}
}

func TestRepeatedWorkPreservesPlacementRules(t *testing.T) {
	for _, rule := range []string{"value", "tier", "fairness"} {
		t.Run(rule, func(t *testing.T) {
			q, clock, slow, fast, s := repeatedFixture(t)
			if rule == "value" {
				slow.CapabilityProfile.SystemMemoryBytes = 32 << 30
				check(t, q, slow, nil)
			}
			teachRepeated(t, q, clock, slow, s, 3*time.Second)
			teachRepeated(t, q, clock, fast, s, time.Second)
			if rule == "value" {
				wanted := submit(t, q, s)
				if check(t, q, fast, nil).Job != nil {
					t.Fatal("speed overrode least-valuable fit")
				}
				if got := check(t, q, slow, nil).Job; got == nil || got.ID != wanted.ID {
					t.Fatal("lower-value host lost", got)
				}
				return
			}
			if rule == "tier" {
				preemptionPolicy(t, q)
			}
			other := s
			other.Project = "fresh-project"
			if rule == "tier" {
				other.Project = "urgent"
			}
			wanted := submit(t, q, other)
			clock.Advance(time.Millisecond)
			lower := submit(t, q, s)
			// The fair/higher-tier winner has unknown speed and selects the stable
			// slow target. Its turn must remain available while learned work uses
			// the otherwise idle fast target; priority does not pin a machine.
			if got := check(t, q, slow, nil).Job; got == nil || got.ID != wanted.ID {
				t.Fatal("speed changed tier/fair winner on its chosen target")
			}
			if got := check(t, q, fast, nil).Job; got == nil || got.ID != lower.ID {
				t.Fatal("independent learned work did not use spare capacity")
			}
		})
	}
}

func TestRepeatedWorkKeepsMemoryGrouping(t *testing.T) {
	q, clock, m, _, s := repeatedFixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory = "auto"
	for _, source := range []string{"a", "b"} {
		path := filepath.Join(t.TempDir(), "source")
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(q.files.blob(ref), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		s.Files = FileManifest{".myce-source.tar": ref}
		s.RequireTarget, s.ProfileMemory = true, strings.Repeat("c", 64)
		s.AllowedMachines = []string{m.ID}
		j := startObservation(t, q, m, s)
		clock.Advance(time.Second)
		r := cleanRepeatedResult(&j)
		peak := .1
		r.MemoryPeak.HostGB = &peak
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	memory, speeds := 0, 0
	for _, point := range q.s.WorkObservations {
		if point.MemorySamples > 0 {
			memory++
			if point.MemorySamples != 2 || point.Key.RepeatedWork != "" {
				t.Fatal("fragmented broader memory evidence", point)
			}
		}
		if point.Repeated != nil {
			speeds++
			if point.Repeated.Samples != 1 || point.MemorySamples != 0 {
				t.Fatal("speed crossed source identity or duplicated memory", point)
			}
		}
	}
	if memory != 1 || speeds != 2 {
		t.Fatal("wrong memory/speed identities", memory, speeds)
	}
}

func TestRepeatedWorkRejectsUncleanAttempts(t *testing.T) {
	for _, scenario := range []string{"failed", "interrupted", "stale", "memory pressure", "missing pressure", "missing counters", "throttled", "io stalls", "mixed host"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, m, _, s := repeatedFixture(t)
			j := startObservation(t, q, m, s)
			r := cleanRepeatedResult(&j)
			switch scenario {
			case "failed":
				r.Success = false
			case "interrupted":
				r.Interrupted = true
			case "stale":
				r.Attempt++
			case "memory pressure":
				r.MemoryPeak.Constrained = true
			case "missing pressure":
				r.MemoryPeak = nil
			case "missing counters":
				r.Resources = nil
			case "throttled":
				v := .1
				r.Resources.CPU.ThrottledSeconds = &v
			case "io stalls":
				r.Resources.IO.Pressure.Some.TotalUS = 1
			case "mixed host":
				competing := submit(t, q, cpuSpec(1, .1))
				if err := q.update(func() error { q.assign(q.s.Jobs[competing.ID], q.s.Machines[m.ID]); return nil }); err != nil {
					t.Fatal(err)
				}
				finish(t, q, q.s.Jobs[competing.ID], true)
			}
			clock.Advance(time.Second)
			err := q.Complete(r)
			if scenario == "stale" {
				if !errors.Is(err, ErrStale) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for _, point := range q.s.WorkObservations {
				if point.Repeated != nil {
					t.Fatal("unclean comparison trained", point.Repeated)
				}
			}
		})
	}
}

// Laptop proof: real HTTP API and supervised shell commands, two synthetic
// inventories, measured real command durations and clean counter fixtures. The
// command simulates different service speeds on one laptop; native throughput
// and real sensor completeness still require separate acceptance.
func TestRepeatedWorkRealCommandProof(t *testing.T) {
	q, clock, slow, fast, s := repeatedFixture(t)
	s.Run.Command = []string{"sh", "-c", `case "$MYCE_MACHINE" in a-slow) sleep 0.3;; *) sleep 0.01;; esac; printf 'repeat-proof-ok\n'`}
	server := httptest.NewServer(API{Queue: q, Token: "fixture-only"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture-only"}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for range 2 {
		for _, m := range []Machine{slow, fast} {
			s.AllowedMachines = []string{m.ID}
			j, err := c.Submit(ctx, s)
			if err != nil {
				t.Fatal(err)
			}
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != j.ID {
				t.Fatal("no assignment", run)
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			scope := commandScope{StateDir: t.TempDir(), Clock: RealClock{}}
			if err := scope.prepare(); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			if err := runRecordedCommand(ctx, run.Spec.Run.Command, t.TempDir(), []string{"PATH=" + os.Getenv("PATH"), "MYCE_MACHINE=" + m.ID}, &out, scope, "proof.json"); err != nil {
				t.Fatal(err)
			}
			duration := time.Since(started)
			if out.String() != "repeat-proof-ok\n" {
				t.Fatalf("output mismatch: %q", out.String())
			}
			clock.Advance(duration)
			if err := q.Complete(cleanRepeatedResult(run)); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s supervised command: %.4fs, verified output repeat-proof-ok", m.ID, duration.Seconds())
		}
	}
	s.AllowedMachines = nil
	next, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if check(t, q, slow, nil).Job != nil {
		t.Fatal("slower machine selected")
	}
	run := check(t, q, fast, nil).Job
	if run == nil || run.ID != next.ID {
		t.Fatal("faster machine not selected", run)
	}
	t.Log("four supervised commands returned identical output; two measured samples per machine; next selected fast; contention counters synthetic")
}
