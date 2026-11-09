package myce

import (
	"errors"
	"path/filepath"
	"runtime"

	"testing"
	"time"
)

func placementMachine(id, layout string, ram, accelerator float64, e Extension) Machine {
	m := machine(id, "fake")
	m.TotalMemoryGB, m.FreeMemoryGB = accelerator, accelerator
	m.HostAvailableMemoryGB, m.HostMemoryMeasured = ram, true
	m.UnifiedMemory = layout == "unified"
	m.CapabilityProfile = &CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, MemoryLayout: layout, SystemMemoryBytes: uint64(ram * (1 << 30)), LogicalCPUs: 8, Accelerators: []AcceleratorProfile{{Type: "fake", MemoryBytes: uint64(accelerator * (1 << 30))}}}
	m.ExtensionReports = []ExtensionReport{{Digest: e.Digest, Work: "kind:sample", Platform: "fake", Qualified: true}, {Digest: e.Digest, Work: "program:sample-script", Platform: "fake", Qualified: true}}
	if len(e.Variants) > 0 {
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
		m.ExtensionFingerprint = "test-fingerprint"
		for i := range m.ExtensionReports {
			old := m.ExtensionReports[i]
			r := newExtensionReport(e, &m, old.Work)
			r.Setup = true
			r.Qualified = true
			r.CheckID = "test-check"
			m.ExtensionReports[i] = r
		}
	}
	return m
}

func TestCapabilityLearnedNeedsPersistAndScope(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)}
	db, checkpoints := filepath.Join(t.TempDir(), "q.db"), t.TempDir()
	q, err := Open(db, checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("big", "unified", 128, 128, e)
	check(t, q, m, nil)
	s := placementSpec(true)
	j := submit(t, q, s)
	clock.Advance(time.Millisecond)
	queued := submit(t, q, s)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != j.ID {
		t.Fatal("missing first assignment")
	}
	r := result(run, true)
	r.Usage = &SizeHint{MemoryGB: 40, HostMemoryGB: 4}
	// Accepted learning requires a started, pressure-qualified attempt.
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	host := r.Usage.HostMemoryGB
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	stale := r
	stale.Attempt++
	if err := q.Complete(stale); !errors.Is(err, ErrStale) {
		t.Fatal("accepted stale measurements", err)
	}
	if len(q.s.ProgramNeeds) != 0 {
		t.Fatal("stale result taught memory needs")
	}
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if job(t, q, queued.ID).Spec.Needs.MemoryGB != 40 {
		t.Fatal("already queued program did not learn")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, checkpoints, clock)
	if err != nil {
		t.Fatal(err)
	}
	learned := submit(t, q, s)
	if learned.Spec.Needs.MemoryGB != 40 || learned.Spec.Needs.HostMemoryGB != 4 {
		t.Fatal("measurements did not survive restart", learned.Spec.Needs)
	}
	small := placementMachine("small", "separate", 64, 24, e)
	if check(t, q, small, nil).Job != nil {
		t.Fatal("learned accelerator needs admitted to 24 GiB")
	}
	for _, changed := range []func(*Spec){func(s *Spec) { s.Project = "another" }, func(s *Spec) { s.Inputs = map[string]string{"script": "immutable/script-v2"} }} {
		copy := cloneJob(&Job{Spec: s}).Spec
		changed(&copy)
		if submit(t, q, copy).Spec.Needs.MemoryGB != e.MemoryGB {
			t.Fatal("learning crossed program identity")
		}
	}

	t.Run("ExtensionVariantLearnedNeeds", func(t *testing.T) {

		q, _ := fixture(t)
		e := compositionExtension(t, false)
		e.ProgramTypes = []string{"sample-script"}
		e.Variants[0].Baselines["program:sample-script"] = fakeExtension(t).Baselines["program:sample-script"]
		e = freezeComposition(t, e)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		m := placementMachine("big", "separate", 128, 128, e)
		m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
		target := e.Variants[0].Target
		for i := range m.ExtensionReports {
			m.ExtensionReports[i].Target = &target
		}
		check(t, q, m, nil)
		s := placementSpec(true)
		s.Extension = extensionKey(e)
		first := submit(t, q, s)
		run := check(t, q, m, nil).Job
		if run == nil || run.ID != first.ID {
			t.Fatal("no assignment")
		}
		queued := submit(t, q, s)
		r := result(run, true)
		r.Usage = &SizeHint{MemoryGB: 40, HostMemoryGB: 4}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		host := r.Usage.HostMemoryGB
		r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		next := submit(t, q, s)
		for _, j := range []*Job{job(t, q, queued.ID), next} {
			if j.Spec.Needs.MemoryGB != 40 || j.Spec.Needs.HostMemoryGB != 4 {
				t.Fatalf("variant discarded learned 40/4 GiB floors: %+v", j.Spec.Needs)
			}
		}
		// Qualification still selects the variant, but learned reservations exclude
		// a host that fits the recipe's original 1/0.1 GiB floors only.
		small := placementMachine("small", "separate", 128, 24, e)
		small.CapabilityProfile.OS, small.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
		for i := range small.ExtensionReports {
			small.ExtensionReports[i].Target = &target
		}
		if check(t, q, small, nil).Job != nil {
			t.Fatal("variant admitted below learned memory reservation")
		}

	})
}

func TestCapabilityOOMRetriesOnceOnStrictlyBiggerHypha(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	small := placementMachine("small", "separate", 64, 24, e)
	check(t, q, small, nil)
	j := submit(t, q, placementSpec(true))
	run := check(t, q, small, nil).Job
	r := result(run, false)
	r.OutOfMemory = true
	clock.Advance(time.Second)
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "queued" || got.OOMRetries != 1 || got.Losses != 0 {
		t.Fatal(got)
	}
	if len(q.s.ProgramNeeds) != 0 {
		t.Fatal("OOM became learned successful needs")
	}
	if err := q.Complete(r); !errors.Is(err, ErrStale) {
		t.Fatal("duplicate OOM was not stale", err)
	}
	if check(t, q, small, nil).Job != nil {
		t.Fatal("OOM retry on same hypha")
	}
	equal := placementMachine("equal", "separate", 64, 24, e)
	if check(t, q, equal, nil).Job != nil {
		t.Fatal("OOM retry on equal capacity")
	}
	tradeoff := placementMachine("tradeoff", "separate", 32, 48, e)
	if check(t, q, tradeoff, nil).Job != nil {
		t.Fatal("OOM retry on smaller system pool")
	}
	big := placementMachine("big", "unified", 128, 128, e)
	big.ExtensionReports = nil
	if check(t, q, big, nil).Job != nil {
		t.Fatal("OOM retry without qualification")
	}
	big = placementMachine("big", "unified", 128, 128, e)
	retry := check(t, q, big, nil).Job
	if retry == nil || retry.ID != j.ID || retry.Attempt != 2 {
		t.Fatal("bigger qualified hypha did not retry", retry)
	}
	r = result(retry, false)
	r.OutOfMemory = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "failed" || got.OOMRetries != 1 || got.Losses != 0 {
		t.Fatal("second OOM was retried", got)
	}

	t.Run("CapabilityOOMDoesNotOverrideCancellation", func(t *testing.T) {

		q, clock := fixture(t)
		e := fakeExtension(t)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		m := placementMachine("hypha", "separate", 64, 24, e)
		check(t, q, m, nil)
		j := submit(t, q, placementSpec(true))
		run := check(t, q, m, nil).Job
		clock.Advance(time.Second)
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		r := result(run, false)
		r.OutOfMemory = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if got := job(t, q, j.ID); got.State != "cancelled" || got.OOMRetries != 0 {
			t.Fatal("OOM bypassed cancellation")
		}

	})
}

func placementSpec(program bool) Spec {
	s := Spec{Name: "placement", Project: "test", Priority: "experiment", Kind: "sample"}
	if program {
		s.Kind = ""
		s.Program = &Program{Type: "sample-script", File: "script"}
		s.Inputs = map[string]string{"script": "immutable/script-v1"}
	}
	return s
}

func TestCapabilityFirstProductionProgramUsesLeastValueFreeWithHold(t *testing.T) {
	q, _ := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	small := placementMachine("a-small", "separate", 64, 24, e)
	big := placementMachine("z-big", "unified", 128, 128, e)
	check(t, q, small, nil)
	check(t, q, big, nil)
	if err := q.ConfigureHosts(map[string]HostPolicy{small.ID: {ProductionHold: true}, big.ID: {ProductionHold: true}}); err != nil {
		t.Fatal(err)
	}
	s := placementSpec(true)
	s.Priority = "production"
	j := submit(t, q, s)
	if got := job(t, q, j.ID).ProductionHost; got != small.ID {
		t.Fatalf("production chose %s, want %s", got, small.ID)
	}
	if check(t, q, big, nil).Job != nil {
		t.Fatal("production consumed scarce held host")
	}
	if r := check(t, q, small, nil); r.Job == nil || r.Job.ID != j.ID || r.Job.Spec.StartBy == nil {
		t.Fatal("production gate or capability placement lost", r.Job)
	}
}

func TestCapabilityPreferenceRespectsEligibility(t *testing.T) {
	for _, reason := range []string{"restricted"} {
		t.Run(reason, func(t *testing.T) {
			q, _ := fixture(t)
			e := fakeExtension(t)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			small := placementMachine("small", "separate", 64, 24, e)
			big := placementMachine("big", "unified", 128, 128, e)
			if reason == "unqualified" {
				big.ExtensionReports = nil
			}
			if reason == "live-memory" {
				big.FreeMemoryGB = 0
			}
			check(t, q, small, nil)
			check(t, q, big, nil)
			if reason == "disabled" {
				if err := setMaintenance(t, q, big.ID, "disable"); err != nil {
					t.Fatal(err)
				}
			}
			if reason == "busy" {
				s := spec("experiment")
				s.AllowedMachines = []string{big.ID}
				submit(t, q, s)
				if check(t, q, big, nil).Job == nil {
					t.Fatal("busy fixture did not start")
				}
			}
			s := placementSpec(true)
			if reason == "restricted" {
				s.AllowedMachines = []string{small.ID}
			}
			j := submit(t, q, s)
			if got := check(t, q, small, nil).Job; got == nil || got.ID != j.ID {
				t.Fatalf("preference bypassed eligibility %s: reason=%s free=%g host=%g", reason, job(t, q, j.ID).Reason, q.s.Machines[small.ID].FreeMemoryGB, q.s.Machines[small.ID].FreeHostMemoryGB)
			}
		})
	}
}
