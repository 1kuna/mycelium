package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var coverageSource FileRef

// Profile-shaped submissions carry a frozen source archive in the file store.
func coverageStore(t *testing.T, q *Queue, dir string) {
	t.Helper()
	if err := q.ConfigureFileStore(dir, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	coverageSource = ref
	if err := os.WriteFile(q.files.blob(ref), []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
}

func coverageEntrySpec(name, recipe, key string) Spec {
	s := cpuSpec(1, 1)
	s.Name, s.ProfileMemory, s.SubmissionKey = name, identityHash(recipe), key
	s.RequireTarget, s.AuthoringOrigin = true, OriginAdapter
	s.Files = FileManifest{".myce-source.tar": coverageSource}
	return s
}

// The first machine is the one placement prefers (the less scarce host).
func coverageMachines() (Machine, Machine) {
	linux, darwin := cpuMachine("linux", "linux", "cpu", 8, 32), cpuMachine("darwin", "darwin", "cpu", 8, 32)
	darwin.CapabilityProfile.OS, darwin.CapabilityProfile.Arch = "darwin", "arm64"
	return darwin, linux
}

// #853/#1341: an entry without targets tries an unseen machine type once; a
// failure there is a coverage finding that skips the type until the recipe changes.
func TestEntryCoverageFindingSkipsTypeUntilRecipeChanges(t *testing.T) {
	q, clock := fixture(t)
	store := t.TempDir()
	coverageStore(t, q, store)
	preferred, other := coverageMachines()
	check(t, q, preferred, nil)
	check(t, q, other, nil)

	j := submit(t, q, coverageEntrySpec("test", "recipe-1", "first"))
	if j.EntryCoverage == nil || len(j.Spec.Targets) != 0 {
		t.Fatal("entry without targets has no coverage stamp", j.EntryCoverage)
	}
	// First run on an unseen type.
	first := check(t, q, preferred, nil).Job
	if first == nil || first.ID != j.ID {
		t.Fatal("unseen machine type not tried", first)
	}
	failure := result(first, false)
	failure.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "not_found"}
	if err := q.Complete(failure); err != nil {
		t.Fatal(err)
	}
	got := job(t, q, j.ID)
	if got.State != "queued" || !slices.ContainsFunc(got.AttemptHistory, func(h AttemptOutcome) bool { return strings.Contains(h.Reason, "coverage finding") }) || len(got.EntryCoverage.Findings) != 1 || got.EntryCoverage.Findings[0].Target != (ExecutionTarget{"darwin", "arm64", "cpu"}) || got.EntryCoverage.Findings[0].Action == "" {
		t.Fatalf("failure on unseen type not a coverage finding: %s %+v", got.State, got.EntryCoverage)
	}
	if again := check(t, q, preferred, nil).Job; again != nil {
		t.Fatal("skipped machine type retried", again.ID)
	}
	second := check(t, q, other, nil).Job
	if second == nil || second.ID != j.ID {
		t.Fatal("job not moved to another machine type", second)
	}
	finish(t, q, second, true)
	requireState(t, q, j.ID, "succeeded")

	// The skip is durable and applies to later submissions of the same recipe.
	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	fakeCheckpointAvailability(t, q, true)
	coverageStore(t, q, store)
	check(t, q, preferred, nil)
	check(t, q, other, nil)
	same := submit(t, q, coverageEntrySpec("test", "recipe-1", "same"))
	if got := check(t, q, preferred, nil).Job; got != nil {
		t.Fatal("skip lost across restart or submissions", got.ID)
	}
	ran := check(t, q, other, nil).Job
	if ran == nil || ran.ID != same.ID {
		t.Fatal(ran)
	}
	finish(t, q, ran, true)

	// A recipe change clears the skip; success there is then proven.
	changed := submit(t, q, coverageEntrySpec("test", "recipe-2", "changed"))
	retried := check(t, q, preferred, nil).Job
	if retried == nil || retried.ID != changed.ID {
		t.Fatal("recipe change kept the old skip", retried)
	}
	finish(t, q, retried, true)
	// A failure on a type this recipe already succeeded on is the job's own.
	proven := submit(t, q, coverageEntrySpec("test", "recipe-2", "proven"))
	ran = check(t, q, preferred, nil).Job
	if ran == nil || ran.ID != proven.ID {
		t.Fatal(ran)
	}
	finish(t, q, ran, false)
	if got := job(t, q, proven.ID); got.State != "failed" || len(got.EntryCoverage.Findings) != 0 {
		t.Fatalf("proven-type failure became coverage: %s %s", got.State, got.Reason)
	}
}

// A recipe that fails on every type it can use has a real failure, not a gap:
// the job fails after one attempt per type and the skips are cleared.
func TestEntryCoverageFailureEverywhereIsTheJobsFailure(t *testing.T) {
	q, _ := fixture(t)
	coverageStore(t, q, t.TempDir())
	preferred, other := coverageMachines()
	check(t, q, preferred, nil)
	check(t, q, other, nil)
	j := submit(t, q, coverageEntrySpec("broken", "recipe", "broken"))
	finish(t, q, check(t, q, preferred, nil).Job, false)
	requireState(t, q, j.ID, "queued")
	finish(t, q, check(t, q, other, nil).Job, false)
	if got := job(t, q, j.ID); got.State != "failed" || !strings.HasPrefix(got.Reason, "command failed") {
		t.Fatalf("%s %s", got.State, got.Reason)
	}
	if _, kept := q.s.EntryCoverage[entryCoverageKey(j.EntryCoverage)]; kept {
		t.Fatal("skips kept for a recipe that never succeeded")
	}
	next := submit(t, q, coverageEntrySpec("broken", "recipe", "fixed-source"))
	if got := check(t, q, preferred, nil).Job; got == nil || got.ID != next.ID {
		t.Fatal("cleared type still skipped", got)
	}
}

// #1841: live recipes of the same entry must not reopen each other's failed
// targets. Both commands fail deterministically, so each must fail normally
// after exploring the two available types once.
func TestEntryCoverageConcurrentRecipesFailOncePerTarget(t *testing.T) {
	q, clock := fixture(t)
	coverageStore(t, q, t.TempDir())
	preferred, other := coverageMachines()
	check(t, q, preferred, nil)
	check(t, q, other, nil)
	a := submit(t, q, coverageEntrySpec("broken", "recipe-a", "a"))
	firstA := check(t, q, preferred, nil).Job
	b := submit(t, q, coverageEntrySpec("broken", "recipe-b", "b"))
	if a.EntryCoverage.Key != b.EntryCoverage.Key || a.EntryCoverage.Recipe == b.EntryCoverage.Recipe {
		t.Fatal("fixture must share an entry with distinct immutable recipes")
	}
	firstB := check(t, q, other, nil).Job
	if firstA == nil || firstB == nil || firstA.ID != a.ID || firstB.ID != b.ID {
		t.Fatal("both recipes must run concurrently", firstA, firstB)
	}
	clock.Advance(time.Second)
	finish(t, q, firstA, false)
	clock.Advance(time.Second)
	finish(t, q, firstB, false)
	requireState(t, q, a.ID, "queued")
	requireState(t, q, b.ID, "queued")
	secondB, secondA := check(t, q, preferred, nil).Job, check(t, q, other, nil).Job
	if secondA == nil || secondB == nil || secondA.ID != a.ID || secondB.ID != b.ID {
		t.Fatal("recipes revisited failed targets instead of swapping types", secondA, secondB)
	}
	clock.Advance(time.Second)
	finish(t, q, secondA, false)
	clock.Advance(time.Second)
	finish(t, q, secondB, false)
	for _, id := range []string{a.ID, b.ID} {
		got := job(t, q, id)
		if got.State != "failed" || !strings.HasPrefix(got.Reason, "command failed") || got.Attempt != 2 || len(got.EntryCoverage.Findings) != 2 {
			t.Fatalf("recipe did not terminate after each type: state=%s attempt=%d reason=%s coverage=%+v", got.State, got.Attempt, got.Reason, got.EntryCoverage)
		}
		seen := map[ExecutionTarget]bool{}
		for _, f := range got.EntryCoverage.Findings {
			if seen[f.Target] || f.At.After(clock.Now()) || f.At.IsZero() {
				t.Fatal("repeated target or invalid finding time", f)
			}
			seen[f.Target] = true
		}
	}
	if len(q.s.EntryCoverage) != 0 {
		t.Fatal("skips kept for recipes that never succeeded", q.s.EntryCoverage)
	}
}

// Old entry-keyed records must keep both their skips and proven successes
// through another recipe's writes, migration, and queue restarts.
func TestEntryCoverageLegacyRecordSurvivesConcurrentRecipeAndRestart(t *testing.T) {
	q, clock := fixture(t)
	store := t.TempDir()
	coverageStore(t, q, store)
	preferred, other := coverageMachines()
	check(t, q, preferred, nil)
	check(t, q, other, nil)
	a := submit(t, q, coverageEntrySpec("test", "recipe-a", "a"))
	// Decode the previous release's record shape, stored at the plain entry key.
	var legacy entryCoverageRecord
	if err := json.Unmarshal([]byte(`{"recipe":"`+a.EntryCoverage.Recipe+`","succeeded":[{"os":"linux","arch":"amd64","platform":"cpu"}],"skipped":[{"target":{"os":"darwin","arch":"arm64","platform":"cpu"},"job":"previous","attempt":1,"at":"2026-10-08T11:00:00Z","action":"repair the entry"}]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.s.EntryCoverage = map[string]entryCoverageRecord{a.EntryCoverage.Key: legacy}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reopen := func() {
		t.Helper()
		path, root := q.dbPathForTest(t), q.root
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		var err error
		q, err = Open(path, root, clock)
		if err != nil {
			t.Fatal(err)
		}
		reopened := q
		t.Cleanup(func() { reopened.Close() })
		fakeCheckpointAvailability(t, q, true)
		coverageStore(t, q, store)
	}
	reopen()
	for i, key := range []string{"b-before", "b-after"} {
		b := submit(t, q, coverageEntrySpec("test", "recipe-b", key))
		run := check(t, q, preferred, nil).Job
		if run == nil || run.ID != b.ID {
			t.Fatal("other recipe inherited the skip", run)
		}
		finish(t, q, run, true)
		if run := check(t, q, preferred, nil).Job; run != nil {
			t.Fatal("other recipe erased the skip", run.ID)
		}
		run = check(t, q, other, nil).Job
		if run == nil || run.ID != a.ID {
			t.Fatal("proven target not assigned", run)
		}
		finish(t, q, run, i == 0)
		if i == 0 {
			if _, kept := q.s.EntryCoverage[a.EntryCoverage.Key]; kept {
				t.Fatal("legacy record not migrated on write")
			}
			reopen()
			a = submit(t, q, coverageEntrySpec("test", "recipe-a", "a-proven"))
		} else if got := job(t, q, a.ID); got.State != "failed" || len(got.EntryCoverage.Findings) != 0 {
			t.Fatal("other recipe erased the proven success", got.State, got.EntryCoverage)
		}
	}
}

// Production entries keep explicit targets (MY-19); explicit targets bind and
// never get a coverage stamp.
func TestEntryCoverageProductionRequiresTargets(t *testing.T) {
	q, _ := fixture(t)
	coverageStore(t, q, t.TempDir())
	preferred, _ := coverageMachines()
	check(t, q, preferred, nil)
	s := coverageEntrySpec("prod", "recipe", "prod")
	s.Priority = "production"
	if _, err := q.Submit(s); err == nil || !strings.Contains(err.Error(), "job_placement_invalid") || !strings.Contains(err.Error(), "explicit targets") {
		t.Fatal("production entry without targets accepted", err)
	}
	s.Targets = []ExecutionTarget{{"darwin", "arm64", "cpu"}}
	if j := submit(t, q, s); j.EntryCoverage != nil {
		t.Fatal("explicit targets got a coverage stamp")
	}
}

// A ceiling is attempt-owned, including older workers that report exit 1 plus
// kernel events without setting OutOfMemory. It must never poison coverage.
func TestEntryCoverageResourceFailuresKeepTypeEligible(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		auto, production, oom bool
		cause                 *RuntimeCause
		events                *MemoryEventCounts
	}{
		{name: "resource limit", cause: &RuntimeCause{Operation: "command", Class: "resource_limit"}},
		{name: "reported resource OOM", oom: true, cause: &RuntimeCause{Operation: "command", Class: "resource_limit"}, events: &MemoryEventCounts{OOM: 1, OOMKill: 3, OOMGroupKill: 1}},
		{name: "RSS guard", oom: true, cause: &RuntimeCause{Operation: "command", Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: 3 << 30, LimitBytes: 2 << 30, Quality: "owned_session_rss"}}},
		{name: "kernel OOM", oom: true, events: &MemoryEventCounts{Max: 43719, OOM: 1, OOMKill: 3, OOMGroupKill: 1}},
		{name: "kernel kill at ceiling", oom: true, events: &MemoryEventCounts{Max: 1, OOMKill: 1}},
		{name: "uncapped pool pressure", auto: true, events: &MemoryEventCounts{OOMKill: 1}},
		{name: "production explicit limit", production: true, oom: true, events: &MemoryEventCounts{OOM: 1, OOMGroupKill: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			coverageStore(t, q, t.TempDir())
			preferred, other := coverageMachines()
			preferred.MemoryEnforcement = cgroupMemoryEnforcement
			check(t, q, preferred, nil)
			check(t, q, other, nil)
			s := coverageEntrySpec("limited", "recipe", "first")
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 0, 2
			limit := 2.125
			if tc.oom {
				s.MemoryLimitGB = &limit
			}
			if tc.auto {
				s.Needs.Memory, s.Needs.HostMemoryGB = "auto", 0
			}
			if tc.production {
				s.Priority, s.Class = "production", "production"
				s.Targets = []ExecutionTarget{{"darwin", "arm64", "cpu"}}
			}
			queued := submit(t, q, s)
			run := check(t, q, preferred, nil).Job
			if run == nil || run.ID != queued.ID {
				t.Fatal("not assigned", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			r := result(run, false)
			code := 1
			r.ExecutionCause = &RuntimeCause{Operation: "command", Class: "command_exit", ExitCode: &code}
			if tc.cause != nil {
				r.ExecutionCause = tc.cause
			}
			peak, resident := 2.13, 2.08
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &resident, NonreclaimableGB: &resident, CgroupGB: &peak, Events: tc.events}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, queued.ID)
			if got.EntryCoverage != nil && len(got.EntryCoverage.Findings) != 0 {
				t.Fatal("resource failure learned coverage", got.EntryCoverage)
			}
			for _, rec := range q.s.EntryCoverage {
				if len(rec.Skipped) != 0 {
					t.Fatal("resource failure persisted skip", rec)
				}
			}
			want, wantCause := "failed", "resource_limit"
			if tc.auto {
				want, wantCause = "queued", memoryPressureCause
			} else if tc.cause != nil {
				wantCause = tc.cause.Class
			}
			if got.State != want || got.Losses != 0 || got.InfrastructureRetries != 0 {
				t.Fatalf("state=%s reason=%s losses=%d infra=%d", got.State, got.Reason, got.Losses, got.InfrastructureRetries)
			}
			if len(got.AttemptHistory) != 1 {
				t.Fatal(got.AttemptHistory)
			}
			h := got.AttemptHistory[0]
			if h.OutOfMemory != tc.oom || h.Interrupted != tc.auto || h.ExecutionCause == nil || h.ExecutionCause.Class != wantCause || strings.Contains(h.Reason, "coverage") {
				t.Fatalf("lost cause: %+v", h)
			}
			if len(got.MemoryAttempts) != 1 || got.MemoryAttempts[0].MemoryEnvelope == nil || got.MemoryAttempts[0].MemoryPeak == nil {
				t.Fatal("lost memory evidence", got.MemoryAttempts)
			}
			facts := got.MemoryAttempts[0]
			if facts.MemoryEnvelope.HostGB != run.MemoryEnvelope.HostGB || facts.MemoryEnvelope.LimitHostGB != run.MemoryEnvelope.LimitHostGB {
				t.Fatal("lost bound", facts.MemoryEnvelope)
			}
			events := facts.MemoryPeak.Events
			if (events == nil) != (tc.events == nil) || events != nil && *events != *tc.events {
				t.Fatal("lost bound/events", got.MemoryAttempts)
			}
			if tc.oom && (run.Memory == nil || run.Memory.LimitGB != limit || h.Reason != "command exceeded explicit memory limit") {
				t.Fatalf("lost explicit limit: %+v, %s", run.Memory, h.Reason)
			}
			nextSpec := s
			nextSpec.SubmissionKey = "next"
			next := submit(t, q, nextSpec)
			if tc.auto {
				retry := check(t, q, preferred, nil).Job
				if retry == nil || retry.ID != queued.ID || retry.Memory == nil || retry.Memory.RAM.ChargeGB <= run.Memory.RAM.ChargeGB {
					t.Fatal("memory recovery did not retain type/grow bound", retry)
				}
			} else if retry := check(t, q, preferred, nil).Job; retry == nil || retry.ID != next.ID {
				t.Fatal("type was skipped", retry)
			}
		})
	}
}
