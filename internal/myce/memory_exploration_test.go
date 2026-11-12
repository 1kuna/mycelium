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

// A Linux peak above the fixture's 8 GiB Darwin allowance: only the bounded
// exception can size such work on Darwin. A peak that fits is a floor for an
// ordinary first run instead (TestForeignPeakFloorsFirstRunOnAnotherOS).
const foreignPeakAboveDarwinAllowance = 9.0

func memoryExplorationSeed(t *testing.T, q *Queue, clock *fakeClock, linux, darwin Machine, s Spec, lower, prior float64) {
	t.Helper()
	if lower > 0 {
		bootstrap := s
		bootstrap.Needs.Memory, bootstrap.Needs.HostMemoryGB = "", min(lower+1, 7.75)
		memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, darwin, bootstrap), lower, true, false)
	}
	memoryExplorationComplete(t, q, clock, memoryExplorationRun(t, q, linux, s), prior, false, false)
}

// #850 changes a sizing-only refusal into a durable wait; dispatch must still
// refuse to start below the prior or reopen a spent exploration claim.
func memoryExplorationRefused(t *testing.T, q *Queue, s Spec) {
	t.Helper()
	if s.Needs.HostMemoryGB > 8 { // This fixture's declared Darwin allowance.
		_, err := q.Submit(s)
		var api *HTTPError
		if !errors.As(err, &api) || api.Code != "no_qualified_target" {
			t.Fatalf("hard requirement must refuse: %v", err)
		}
		return
	}
	queued := submit(t, q, s)
	why := memoryEvidenceReason(t, q, queued)
	if !strings.HasPrefix(why, memoryEvidenceRecovering+":") {
		t.Fatalf("sizing-only mismatch must wait: %s", why)
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
	memoryExplorationSeed(t, q, clock, linux, darwin, s, 3, foreignPeakAboveDarwinAllowance)
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
	occupancy := s
	occupancy.Name = "unrelated-occupant"
	occupancy.Needs.Memory, occupancy.Needs.HostMemoryGB = "", 1
	occupancy.AllowedMachines = []string{darwin.ID}
	busy := memoryExplorationRun(t, q, darwin, occupancy)
	first := submit(t, q, s)
	if run := check(t, q, darwin, busy).Job; run != nil {
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
	if run := check(t, q, darwin, busy).Job; run != nil || len(q.s.MemoryExplorations) != 0 {
		t.Fatal("probe ignored occupancy or qualification spent its claim", run)
	}
	memoryExplorationComplete(t, q, clock, busy, 1, false, false)
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
	// A different host has no calibrated transfer error, so its next run
	// remains a physical probe even after this clean local completion.
	next := check(t, q, small, nil).Job
	if next == nil || next.ID != second.ID || next.Spec.Needs.HostMemoryGB != 5 || next.MemoryEnvelope.Basis != "exploratory" {
		t.Fatal("uncalibrated same-OS transfer lost its physical bound", next, job(t, q, second.ID).Reason)
	}
	localSpec := s
	localSpec.AllowedMachines = []string{darwin.ID}
	local := submit(t, q, localSpec)
	learned := check(t, q, darwin, nil).Job
	if learned == nil || learned.ID != local.ID || learned.Spec.Needs.HostMemoryGB != 4+minimumHostMemoryGB || learned.MemoryEnvelope.Source != "accepted peak + measured uncertainty" {
		t.Fatal("clean local learning did not supersede the foreign prior", learned, job(t, q, local.ID).Reason)
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
			memoryExplorationSeed(t, q, clock, linux, darwin, s, 3, foreignPeakAboveDarwinAllowance)
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
			if got := job(t, q, pending.ID); got.Attempt != 0 || got.State != "queued" || !strings.Contains(got.Reason, memoryCapacityRemedies) {
				t.Fatal("pending successor acquired another attempt", got)
			}
			memoryExplorationRefused(t, q, s)
			if run := check(t, q, darwin, nil).Job; run != nil {
				t.Fatal("exploration loop after restart", run)
			}
			// Waiting cannot clear a spent probe; fail at the next tick with
			// one action instead of staying queued for days.
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, pending.ID); got.State != "failed" || got.DiagnosticCode != deadEndWait || !strings.HasPrefix(got.Reason, "dead end: no host can size") || !strings.Contains(got.Reason, "action: ") || strings.Contains(got.Reason, "operator") {
				t.Fatal("dead-end memory wait kept waiting", got.State, got.Reason)
			}
		})
	}
}

// A probe refused at launch (no process, no peak) learned nothing. The queue
// returns the claim and measures again instead of parking the job behind
// "bounded exploration already used".
func TestMemoryExplorationUnlaunchedProbeIsReturned(t *testing.T) {
	q, clock, linux, darwin, s := memoryExplorationFixture(t)
	memoryExplorationSeed(t, q, clock, linux, darwin, s, 3, foreignPeakAboveDarwinAllowance)
	s.Targets = []ExecutionTarget{machineTarget(&darwin)}
	queued := submit(t, q, s)
	run := check(t, q, darwin, nil).Job
	if run == nil || run.ID != queued.ID || run.MemoryEnvelope.Source != boundedMemoryExploration || len(q.s.MemoryExplorations) != 1 {
		t.Fatal("not a bounded probe", run)
	}
	refused := result(run, false)
	refused.Interrupted = true
	refused.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", PrelaunchRefused: true}
	clock.Advance(time.Second)
	if err := q.Complete(refused); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(q.s.MemoryExplorations) != 0 {
		t.Fatal("unlaunched probe kept its claim")
	}
	if got := job(t, q, queued.ID); got.State != "queued" {
		t.Fatal("unlaunched probe ended the job", got.State, got.Reason)
	}
	clock.Advance(time.Second)
	darwin.HostAvailableMemoryGB--
	retry := check(t, q, darwin, nil).Job
	if retry == nil || retry.ID != queued.ID || retry.MemoryEnvelope.Source != boundedMemoryExploration || len(q.s.MemoryExplorations) != 1 {
		t.Fatal("queue did not probe again", retry, job(t, q, queued.ID).Reason)
	}
	// A sampled peak is evidence; that claim stays spent.
	if err := q.Started(result(retry, false)); err != nil {
		t.Fatal(err)
	}
	memoryExplorationComplete(t, q, clock, retry, 3, false, false)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(q.s.MemoryExplorations) != 1 {
		t.Fatal("measured probe returned its claim")
	}
}
