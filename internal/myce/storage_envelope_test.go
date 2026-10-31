package myce

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func storageFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec, FileRef) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	c := deliveryClient(t, q)
	_, source := retentionUpload(t, c, "frozen source")
	_, out := retentionUpload(t, c, "1234")
	m := cpuMachine("storage-worker", "storage-host", "cpu", 8, 32)
	check(t, q, m, nil)
	s := cpuSpec(1, 1)
	s.Name, s.Project = "storage-profile", "synthetic"
	s.Files = FileManifest{".myce-source.tar": source}
	s.Outputs = map[string]string{"a": "results", "b": "results"}
	return q, clock, m, s, out
}
func storageResult(j *Job, ref FileRef, n uint64) Result {
	r := result(j, true)
	host := 1.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	r.StoragePeak = &AttemptStoragePeak{ScratchBytes: n, ManagedBytes: n, Files: 2, AllocationUnit: 1, Samples: 2, Complete: true, Source: managedStorageSource}
	r.OutputRefs = FileManifest{"a": ref, "b": ref}
	return r
}
func onlyStorage(t *testing.T, q *Queue) StorageObservation {
	t.Helper()
	if len(q.s.StorageObservations) != 1 {
		t.Fatalf("storage observations: %+v", q.s.StorageObservations)
	}
	for _, p := range q.s.StorageObservations {
		return p
	}
	panic("unreachable")
}

func TestStorageEnvelopeRejectedSamplesPreserveNeighbours(t *testing.T) {
	for _, scenario := range []string{"memory constrained", "pressure unavailable", "disk constrained", "incomplete", "failed", "stale", "interrupted", "release pending", "peak absent"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock, m, s, out := storageFixture(t)
			first := startObservation(t, q, m, s)
			clock.Advance(time.Second)
			if err := q.Complete(storageResult(&first, out, 4)); err != nil {
				t.Fatal(err)
			}
			before := onlyStorage(t, q)
			next := startObservation(t, q, m, s)
			r := storageResult(&next, out, 2)
			switch scenario {
			case "memory constrained":
				r.MemoryPeak.Constrained = true
			case "pressure unavailable":
				r.MemoryPeak.PressureUnavailable = true
			case "disk constrained":
				r.StoragePeak.Constrained = true
				r.StoragePeak.ManagedBytes = 4
				r.StoragePeak.ScratchBytes = 4
			case "incomplete":
				r.StoragePeak.Complete = false
			case "failed":
				r.Success = false
				r.StoragePeak.ManagedBytes = 6
				r.StoragePeak.ScratchBytes = 6
			case "stale":
				r.Attempt++
			case "interrupted":
				r.Interrupted = true
			case "release pending":
				r.ReleasePending = true
			case "peak absent":
				r.StoragePeak = nil
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
			after := onlyStorage(t, q)
			if after.Managed != before.Managed || after.Output.Peak != before.Output.Peak || after.Output.Min != before.Output.Min || after.Output.PositiveError != before.Output.PositiveError || after.RecordedAt != before.RecordedAt {
				t.Fatalf("rejection erased accepted need: %+v", after)
			}
			if scenario == "stale" {
				if after.LowerBound != nil {
					t.Fatal("stale execution mutated lower bound")
				}
				return
			}
			if scenario == "failed" && (after.LowerBound == nil || after.LowerBound.ManagedBytes != 6 || after.LowerBoundExecution != r.Execution) {
				t.Fatal("failure lower bound/provenance lost", after)
			}
			if scenario == "disk constrained" {
				e := q.storageEnvelope(q.s.Jobs[next.ID], &m)
				if e.ManagedBytes == nil || *e.ManagedBytes != 4 || !strings.Contains(e.Exploration, "fresh storage admission") {
					t.Fatal("constrained boundary lost larger-exploration requirement", e)
				}
			}
			facts := job(t, q, next.ID).StorageAttempts
			if len(facts) != 1 || facts[0].LearningReason == "eligible" {
				t.Fatalf("rejected learning reason: %+v", facts)
			}
			q = storageRestart(t, q, clock)
			if got := onlyStorage(t, q); got.Managed != before.Managed || got.Output.Peak != before.Output.Peak || got.Output.Min != before.Output.Min {
				t.Fatal("restart changed accepted ranges", got)
			}
		})
	}
}
func TestStorageEnvelopeColdPriorIdentityAndAge(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	s.Disk = &DiskNeeds{OutputBytes: 100}
	first := startObservation(t, q, m, s)
	e := first.StorageEnvelope
	if e.OutputBytes == nil || *e.OutputBytes != 100 || e.ScratchBytes != nil || e.PredictedNeed != nil || !strings.Contains(e.Exploration, "bounded") {
		t.Fatal("output hint invented scratch", e)
	}
	clock.Advance(time.Second)
	if err := q.Complete(storageResult(&first, out, 4)); err != nil {
		t.Fatal(err)
	}
	base := q.storageEnvelope(q.s.Jobs[first.ID], &m)
	if base.OutputBytes == nil || *base.OutputBytes != 8 {
		t.Fatal("prior survived accepted output", base)
	}
	for _, change := range []string{"source", "args", "environment", "shape", "variant", "recipe"} {
		t.Run(change, func(t *testing.T) {
			j := cloneJob(q.s.Jobs[first.ID])
			target := m
			switch change {
			case "source":
				ref := j.Spec.Files[".myce-source.tar"]
				ref.SHA256 = strings.Repeat("a", 64)
				j.Spec.Files[".myce-source.tar"] = ref
			case "args":
				j.Spec.Args = []string{"different"}
			case "environment":
				j.Spec.Run.Env = map[string]string{"OUTPUT_MODE": "other"}
			case "shape":
				j.Spec.Files["item"] = FileRef{SHA256: out.SHA256, SizeBytes: 100}
			case "variant":
				target.CapabilityProfile = &CapabilityProfile{OS: "other", Arch: "amd64"}
			case "recipe":
				j.Spec.Run.Command = []string{"other-command"}
			}
			got := q.storageEnvelope(&j, &target)
			if got.ScratchBytes != nil || got.Samples != 0 {
				t.Fatalf("incompatible identity inherited estimate: %+v", got)
			}
		})
	}
	// Host identity alone does not pin or invalidate evidence.
	target := m
	target.ID = "another-worker"
	target.HostID = "another-host"
	if got := q.storageEnvelope(q.s.Jobs[first.ID], &target); got.Samples != 1 {
		t.Fatal("evidence pinned a host", got)
	}
	clock.Advance(workCurveMaxAge + time.Nanosecond)
	if got := q.storageEnvelope(q.s.Jobs[first.ID], &m); got.Samples != 0 || got.ScratchBytes != nil {
		t.Fatal("expired evidence reused", got)
	}
}
func TestStorageEnvelopeStatusAndUngatedWriter(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := storageResult(&first, out, 4)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	before := onlyStorage(t, q)
	// The same runtime validity marker protects the new accepted writer too.
	q.recordStorageNeeds(eligibleMemorySample{})
	q.recordStorageNeeds(eligibleMemorySample{job: q.s.Jobs[first.ID], result: r, storageKey: first.StorageEnvelope.Key})
	if !reflect.DeepEqual(before, onlyStorage(t, q)) {
		t.Fatal("ungated storage writer accepted evidence")
	}
	snap, err := q.snapshot(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.StorageObservations) != 1 || snap.StorageObservations[0].Managed.need(1) != 4 {
		t.Fatal("status omitted evidence", snap.StorageObservations)
	}
	var page bytes.Buffer
	if err := statusPage.Execute(&page, snap); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"Storage evidence", "storage-profile", "shape", "hard limit unknown", "unenforced; observation only"} {
		if !strings.Contains(page.String(), text) {
			t.Errorf("status missing %q", text)
		}
	}
	// The API must retain the same fields through its privacy projection.
	var got Job
	if err := deliveryClient(t, q).Call(context.Background(), "GET", "/api/jobs/"+first.ID, nil, &got); err != nil {
		t.Fatal(err)
	}
	if got.StorageEnvelope == nil || len(got.StorageAttempts) != 1 || got.StorageAttempts[0].LearningReason != "eligible" {
		t.Fatal("job GET lost storage evidence", got.StorageEnvelope, got.StorageAttempts)
	}
}

func storageRestart(t *testing.T, q *Queue, clock Clock) *Queue {
	t.Helper()
	root, limit, checkpoints := q.files.root, q.files.limit, true
	reopened := memoryFactsRestart(t, q, clock)
	if err := reopened.ConfigureFileStore(root, limit, checkpoints); err != nil {
		t.Fatal(err)
	}
	return reopened
}

func TestStorageEnvelopeQualifiedVariantsAndRejectedFirstSample(t *testing.T) {
	q, clock, m, s := observationFixture(t)

	_, input := retentionUpload(t, deliveryClient(t, q), "input")
	s.Files = FileManifest{"input": input}
	first := startObservation(t, q, m, s)
	r := storageResult(&first, input, 4)
	r.OutputRefs = FileManifest{}
	for name := range first.Spec.Outputs {
		r.OutputRefs[name] = input
	}
	r.MemoryPeak.Constrained = true
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	p := onlyStorage(t, q)
	if p.Managed.Samples != 0 || p.Output.Samples != 0 || p.LowerBound == nil {
		t.Fatal("rejected first sample seeded accepted storage", p)
	}
	second := startObservation(t, q, m, s)
	if second.StorageEnvelope.ScratchBytes != nil || second.StorageEnvelope.Exploration == "" {
		t.Fatal("lower bound became accepted need", second.StorageEnvelope)
	}
	r = storageResult(&second, input, 4)
	r.OutputRefs = FileManifest{}
	for name := range second.Spec.Outputs {
		r.OutputRefs[name] = input
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	stored := q.s.Jobs[second.ID]
	target := m
	target.ID = "other"
	target.HostID = "other-host"
	e := stored.Extensions[0]
	qualified := newExtensionReport(e, &target, stored.Spec.workType())
	qualified.Setup, qualified.Qualified, qualified.CheckID = true, true, "independent-check"
	target.ExtensionReports = []ExtensionReport{qualified}
	if got := q.storageEnvelope(stored, &target); got.Samples != 1 {
		t.Fatal("qualified host transfer refused", got)
	}
	target.ExtensionReports = nil
	if got := q.storageEnvelope(stored, &target); got.Key != nil || got.ScratchBytes != nil {
		t.Fatal("storage evidence qualified an unsupported variant", got)
	}
	if len(q.s.Machines) != 1 || len(stored.Spec.AllowedMachines) != 0 {
		t.Fatal("learning changed placement or pinned a host")
	}
}

func TestStorageEnvelopeOutputWithoutScratchCoverage(t *testing.T) {
	q, clock, m, s, out := storageFixture(t)
	first := startObservation(t, q, m, s)
	r := storageResult(&first, out, 0)
	r.StoragePeak = nil
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	p := onlyStorage(t, q)
	if p.Output.Peak != 8 || p.Output.Samples != 1 || p.Managed.Samples != 0 {
		t.Fatal("verified output did not teach independently", p)
	}
	next := startObservation(t, q, m, s)
	e := next.StorageEnvelope
	if e.OutputBytes == nil || *e.OutputBytes != 8 || e.ScratchBytes != nil || e.PredictedNeed != nil || e.Exploration == "" {
		t.Fatal("output-only evidence invented scratch", e)
	}
}

func TestStorageEnvelopeCPUOnAcceleratorMachine(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	c := deliveryClient(t, q)
	_, source := retentionUpload(t, c, "frozen source")
	_, out := retentionUpload(t, c, "1234")
	s := cpuSpec(1, 1)
	s.Name, s.Project = "storage-profile", "synthetic"
	s.Files = FileManifest{".myce-source.tar": source}
	s.Outputs = map[string]string{"a": "results", "b": "results"}
	m := m3Machine()
	submitted := submit(t, q, s)
	first := assignedJob(machineCheck(t, q, m))
	if first == nil || first.ID != submitted.ID || first.StorageEnvelope.Key.Variant.Platform != "cpu" {
		t.Fatal("CPU storage assignment", first)
	}
	if err := q.Started(result(first, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := q.Complete(storageResult(first, out, 4)); err != nil {
		t.Fatal(err)
	}
	p := onlyStorage(t, q)
	if p.Managed.Samples != 1 || p.Key.Variant.Platform != "cpu" {
		t.Fatal("CPU completion lost its target identity", p)
	}
	next := q.storageEnvelope(q.s.Jobs[first.ID], &m)
	if next.Samples != 1 || next.ManagedBytes == nil || *next.ManagedBytes != 4 {
		t.Fatal("CPU evidence unavailable on accelerator host", next)
	}
}
