package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// One record per kind of work (MY-49): its last accepted duration and accepted
// peak. These fake-clock tests show that both are recorded and that the
// duration is used where a prediction was used.

func lastDurationFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine, Spec) {
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

func teachDuration(t *testing.T, q *Queue, clock *fakeClock, m Machine, s Spec, duration time.Duration) Job {
	t.Helper()
	s.AllowedMachines = []string{m.ID}
	j := startObservation(t, q, m, s)
	clock.Advance(duration)
	finish(t, q, &j, true)
	return j
}

func TestLastDurationRecordedAndUsedForPlacement(t *testing.T) {
	q, clock, slow, fast, s := lastDurationFixture(t)
	teachDuration(t, q, clock, slow, s, 3*time.Second)
	teachDuration(t, q, clock, fast, s, 5*time.Second)
	// The last accepted duration replaces the earlier one; it is not averaged.
	taught := teachDuration(t, q, clock, fast, s, time.Second)
	key := q.s.Jobs[taught.ID].Timing.Key
	if p := q.s.WorkObservations[identityHash(key)]; p.LastSeconds != 1 {
		t.Fatalf("last accepted duration not replaced: %+v", p)
	}
	next := submit(t, q, s)
	if got := check(t, q, slow, nil).Job; got != nil {
		t.Fatal("slower machine stole comparable work", got.ID)
	}
	run := check(t, q, fast, nil).Job
	if run == nil || run.ID != next.ID {
		t.Fatal("did not prefer the shorter last accepted duration", run)
	}
	if err := q.Started(result(run, true)); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, next.ID)
	if got.Timing.LastAcceptedSeconds == nil || *got.Timing.LastAcceptedSeconds != 1 || !strings.Contains(JobTiming(got), "last accepted duration 1.00s") {
		t.Fatalf("status did not show the last accepted duration: %+v", got.Timing)
	}
	// Both cores are now held on the fast host. A duration cannot create cores,
	// reserve a future slot or make a currently admissible slow host wait.
	waiting := submit(t, q, s)
	if got := check(t, q, fast, run).Job; got != nil {
		t.Fatal("duration granted busy capacity", got.ID)
	}
	if got := check(t, q, slow, nil).Job; got == nil || got.ID != waiting.ID {
		t.Fatal("waited for busy faster machine", got)
	}
}

func TestLastDurationIdentityFencesAndRestart(t *testing.T) {
	q, clock, m, _, s := lastDurationFixture(t)
	first := teachDuration(t, q, clock, m, s, 2*time.Second)
	base := &Job{Spec: first.Spec, State: "queued"}
	q.applyLearnedNeeds(base)
	if e, ok := q.estimateCompletion(base, q.s.Machines[m.ID]); !ok || e.seconds != 2 {
		t.Fatal("missing last accepted duration", e, ok)
	}
	for _, change := range []struct {
		name string
		edit func(*Job)
	}{
		{"command", func(j *Job) { j.Spec.Run.Command = []string{"echo", "changed"} }},
		{"environment", func(j *Job) { j.Spec.Run.Env = map[string]string{"INPUT_SIZE": "2"} }},
		{"resumed", func(j *Job) { j.Spec.Resumable, j.Attempt = true, 1 }},
	} {
		t.Run(change.name, func(t *testing.T) {
			copy := *base
			change.edit(&copy)
			if _, ok := q.estimateCompletion(&copy, q.s.Machines[m.ID]); ok {
				t.Fatal("another kind of work reused the duration")
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
	// History has no age limit: a year later it is still the last accepted run.
	clock.Advance(365 * 24 * time.Hour)
	check(t, reopened, m, nil)
	if e, ok := reopened.estimateCompletion(base, reopened.s.Machines[m.ID]); !ok || e.seconds != 2 {
		t.Fatal("duration did not survive restart and age", e, ok)
	}
}

// Durations live on the kind of work's memory record without fragmenting it
// by source bytes, and a duration-only update leaves memory evidence alone.
func TestLastDurationSharesTheMemoryRecord(t *testing.T) {
	q, clock, m, _, s := lastDurationFixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory = "auto"
	s.RequireTarget, s.ProfileMemory = true, strings.Repeat("c", 64)
	s.AllowedMachines = []string{m.ID}
	for i, source := range []string{"a", "b"} {
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
		j := startObservation(t, q, m, s)
		clock.Advance(time.Duration(i+1) * time.Second)
		r := result(&j, true)
		peak := .1
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
	}
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("source bytes fragmented the record: %+v", q.s.WorkObservations)
	}
	var id string
	var record WorkObservation
	for id, record = range q.s.WorkObservations {
	}
	if record.MemorySamples != 2 || record.MemoryPeak == nil || *record.MemoryPeak.HostGB != .1 || record.LastSeconds != 2 {
		t.Fatalf("record lost its peak or last duration: %+v", record)
	}
	// An attempt with no eligible memory sample adds only its duration, even
	// to an invalidated record: identity, provenance and memory stay as they were.
	record.StaleReason = "runtime identity changed"
	q.s.WorkObservations[id] = record
	j := startObservation(t, q, m, s)
	clock.Advance(4 * time.Second)
	finish(t, q, &j, true)
	after := q.s.WorkObservations[id]
	if after.LastSeconds != 4 || after.StaleReason != record.StaleReason || after.Job != record.Job || after.Samples != record.Samples || after.MemorySamples != 2 || after.MemoryPeak != record.MemoryPeak {
		t.Fatalf("duration-only update changed memory evidence: before=%+v after=%+v", record, after)
	}
}
