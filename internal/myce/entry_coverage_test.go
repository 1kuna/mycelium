package myce

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
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
	finish(t, q, first, false)
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
	if _, kept := q.s.EntryCoverage[j.EntryCoverage.Key]; kept {
		t.Fatal("skips kept for a recipe that never succeeded")
	}
	next := submit(t, q, coverageEntrySpec("broken", "recipe", "fixed-source"))
	if got := check(t, q, preferred, nil).Job; got == nil || got.ID != next.ID {
		t.Fatal("cleared type still skipped", got)
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
