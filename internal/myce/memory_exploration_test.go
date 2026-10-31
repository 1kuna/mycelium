package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func memoryExplorationFixture(t *testing.T) (*Queue, *fakeClock, Machine, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
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
	if err := os.WriteFile(q.files.blob(ref), []byte("source"), 0600); err != nil {
		t.Fatal(err)
	}
	linux := cpuMachine("linux-worker", "linux-host", "cpu", 8, 32)
	darwin := cpuMachine("darwin-worker", "darwin-host", "cpu", 8, 32)
	darwin.CapabilityProfile.OS, darwin.CapabilityProfile.Arch = "darwin", "arm64"
	linux.HostMemoryBudgetGB, darwin.HostMemoryBudgetGB = 32, 8
	for _, m := range []*Machine{&linux, &darwin} {
		m.ExtensionFingerprint = "cpu-v1"
		m.MemoryCeilingSlackGB = .125
		check(t, q, *m, nil)
	}
	s := cpuSpec(2, 0)
	s.Name, s.RequireTarget, s.Needs.Memory = "gate-full", true, "auto"
	s.Priority = "production"
	s.Files = FileManifest{".myce-source.tar": ref}
	return q, clock, linux, darwin, s
}

func memoryExplorationRun(t *testing.T, q *Queue, m Machine, s Spec) *Job {
	t.Helper()
	s.Targets = []ExecutionTarget{machineTarget(&m)}
	queued := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != queued.ID {
		t.Fatal("profile not assigned", job(t, q, queued.ID))
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	return run
}

func memoryExplorationComplete(t *testing.T, q *Queue, clock *fakeClock, run *Job, peak float64, constrained, oom bool) {
	t.Helper()
	clock.Advance(time.Second)
	r := result(run, !oom)
	r.OutOfMemory = oom
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak, Constrained: constrained, Source: processGroupMemorySource}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
}

func memoryExplorationSeed(t *testing.T, q *Queue, clock *fakeClock, linux, darwin Machine, s Spec, lower, prior float64) {
	t.Helper()
	if lower > 0 {
		bootstrap := s
		bootstrap.Needs.Memory, bootstrap.Needs.HostMemoryGB = "", min(lower+1, 7.75)
		memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, darwin, bootstrap), lower, true, false)
	}
	memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, linux, s), prior, false, false)
}

func memoryExplorationRefused(t *testing.T, q *Queue, s Spec) {
	t.Helper()
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "no_qualified_target" || !strings.Contains(api.Message, memoryCapacityRemedies) {
		t.Fatalf("expected conservative refusal with remedies: %v", err)
	}
}

// The reviewer's production profile must not exchange an accepted 12 GiB peak
// for either an undersized hint or an unconstrained first-run budget.
func TestMemoryExplorationForeignPriorRefuses(t *testing.T) {
	for _, tc := range []struct {
		name               string
		lower, prior, hint float64
	}{
		{"foreign only", 0, 12, 0},
		{"undersized hint", 0, 12, 1},
		{"local lower bound exceeds cap", 12, 12, 1},
		{"hint exceeds cap", 3, 3.634326934814453, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock, linux, darwin, s := memoryExplorationFixture(t)
			s.Name = "ordinary-cpu-production"
			memoryExplorationSeed(t, q, clock, linux, darwin, s, tc.lower, tc.prior)
			s.Targets = []ExecutionTarget{machineTarget(&darwin)}
			s.Needs.HostMemoryGB = tc.hint
			memoryExplorationRefused(t, q, s)
			if len(q.s.MemoryExplorations) != 0 {
				t.Fatal("qualification consumed a probe")
			}
		})
	}
}

func TestMemoryExplorationLargestEligibleHost(t *testing.T) {
	q, clock, linux, darwin, s := memoryExplorationFixture(t)
	memoryExplorationSeed(t, q, clock, linux, darwin, s, 3, 3.634326934814453)
	s.Targets = []ExecutionTarget{machineTarget(&darwin)}
	s.Needs.HostMemoryGB = 1 // A fitting hint is a prerequisite, not the probe cap.
	small := darwin
	small.ID, small.HostID, small.Session = "small-worker", "small-host", "small-session"
	small.HostMemoryBudgetGB = 5.25
	check(t, q, small, nil)
	large := darwin
	large.ID, large.HostID, large.Session = "large-worker", "large-host", "large-session"
	large.HostMemoryBudgetGB = 32
	check(t, q, large, nil)
	first := submit(t, q, s)
	if run := check(t, q, darwin, nil).Job; run != nil {
		t.Fatal("explored even though a qualified host fits the foreign prior", run)
	}
	// The largest physical host is not eligible when it cannot fit the CPU need.
	profile := *large.CapabilityProfile
	profile.LogicalCPUs = 1
	large.CapabilityProfile = &profile
	check(t, q, large, nil)
	clock.Advance(time.Millisecond)
	second := submit(t, q, s)
	if run := check(t, q, small, nil).Job; run != nil {
		t.Fatal("probe chose a smaller eligible allowance", run)
	}
	run := check(t, q, darwin, nil).Job
	if run == nil || run.ID != first.ID || run.MemoryEnvelope.Source != boundedMemoryExploration || run.Spec.Needs.HostMemoryGB != 7.75 || run.MemoryEnvelope.HostGB > 8 {
		t.Fatal("missing full-allowance bounded attempt", run, job(t, q, first.ID).State, job(t, q, first.ID).Reason)
	}
	if len(q.s.MemoryExplorations) != 1 {
		t.Fatal("assignment did not consume exactly one probe")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	stale := result(run, true)
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatal("bounded attempt bypassed stale-result fence", err)
	}
	memoryExplorationComplete(t, q, clock, run, 4, false, false)
	next := check(t, q, darwin, nil).Job
	if next == nil || next.ID != second.ID || next.Spec.Needs.HostMemoryGB != 5 || next.MemoryEnvelope.Source != "accepted peak × 1.25" {
		t.Fatal("clean local learning did not supersede the foreign prior", next)
	}
	if len(q.s.MemoryExplorations) != 1 {
		t.Fatal("accepted learning consumed another probe")
	}
}

func TestMemoryExplorationFailureCannotLoop(t *testing.T) {
	for _, oom := range []bool{true, false} {
		name := "constrained success"
		if oom {
			name = "OOM"
		}
		t.Run(name, func(t *testing.T) {
			q, clock, linux, darwin, s := memoryExplorationFixture(t)
			memoryExplorationSeed(t, q, clock, linux, darwin, s, 3, 3.634326934814453)
			s.Targets = []ExecutionTarget{machineTarget(&darwin)}
			first := submit(t, q, s)
			clock.Advance(time.Millisecond)
			pending := submit(t, q, s)
			run := check(t, q, darwin, nil).Job
			if run == nil || run.ID != first.ID || run.MemoryEnvelope.Source != boundedMemoryExploration {
				t.Fatal("not a bounded probe", run)
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			// Even a small constrained peak must not reopen the exception.
			memoryExplorationComplete(t, q, clock, run, 3, !oom, oom)
			got := job(t, q, run.ID)
			if got.Attempt != 1 || got.OOMRetries != 0 || oom && (got.State != "failed" || !strings.Contains(got.Reason, memoryCapacityRemedies)) || !oom && got.State != "succeeded" {
				t.Fatal("probe retried or changed constrained success semantics", got.State, got.Reason, got.Attempt, got.OOMRetries)
			}
			for _, point := range q.s.WorkObservations {
				if point.Key.Target.OS == "linux" && (point.MemoryPeak == nil || point.MemoryRecordedAt.IsZero()) {
					t.Fatal("probe failure erased the conservative foreign prior", point)
				}
			}
			store := q.files.root
			q = memoryFactsRestart(t, q, clock)
			if err := q.ConfigureFileStore(store, 1<<20, true); err != nil {
				t.Fatal(err)
			}
			check(t, q, linux, nil)
			if next := check(t, q, darwin, nil).Job; next != nil {
				t.Fatal("already queued successor repeated the exploration", next)
			}
			if got := job(t, q, pending.ID); got.Attempt != 0 || got.State != "queued" {
				t.Fatal("pending successor acquired another attempt", got)
			}
			memoryExplorationRefused(t, q, s)
			if run := check(t, q, darwin, nil).Job; run != nil {
				t.Fatal("exploration loop after restart", run)
			}
		})
	}
}
