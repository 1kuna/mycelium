package myce

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func placementMachine(id, layout string, ram, accelerator float64, e Extension) Machine {
	m := machine(id, "fake")
	m.TotalMemoryGB, m.FreeMemoryGB = accelerator, accelerator
	m.HostAvailableMemoryGB, m.HostMemoryMeasured = ram, true
	m.UnifiedMemory = layout == "unified"
	m.CapabilityProfile = &CapabilityProfile{MemoryLayout: layout, SystemMemoryBytes: uint64(ram * (1 << 30)), LogicalCPUs: 8, Accelerators: []AcceleratorProfile{{Type: "fake", MemoryBytes: uint64(accelerator * (1 << 30))}}}
	m.ExtensionReports = []ExtensionReport{{Digest: e.Digest, Work: "kind:sample", Platform: "fake", Qualified: true}, {Digest: e.Digest, Work: "program:sample-script", Platform: "fake", Qualified: true}}
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
	if job(t, q, queued.ID).Spec.Needs.MemoryGB != 50 {
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
	if learned.Spec.Needs.MemoryGB != 50 || learned.Spec.Needs.HostMemoryGB != 5 {
		t.Fatal("measurements did not survive restart", learned.Spec.Needs)
	}
	small := placementMachine("small", "separate", 64, 24, e)
	if check(t, q, small, nil).Job != nil {
		t.Fatal("learned accelerator needs admitted to 24 GiB")
	}
	for _, changed := range []func(*Spec){
		func(s *Spec) { s.Project = "another" },
		func(s *Spec) { s.Program.Args = []string{"changed"} },
		func(s *Spec) { s.Inputs = map[string]string{"script": "immutable/script-v2"} },
		func(s *Spec) { s.Run.Env = map[string]string{"MODEL": "new"} },
	} {
		copy := cloneJob(&Job{Spec: s}).Spec
		changed(&copy)
		if submit(t, q, copy).Spec.Needs.MemoryGB != e.MemoryGB {
			t.Fatal("learning crossed program identity")
		}
	}
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

func TestCapabilityMemoryLayoutAndHints(t *testing.T) {
	for _, tc := range []struct {
		name, layout                               string
		ram, accelerator, needRAM, needAccelerator float64
		fits                                       bool
	}{
		{"ram-heavy-separate", "separate", 64, 24, 48, 2, true},
		{"accelerator-heavy-separate", "separate", 64, 24, 2, 40, false},
		{"accelerator-heavy-unified", "unified", 128, 128, 2, 40, true},
		{"shared-pool-counted-once", "unified", 64, 64, 40, 40, false},
		{"profile-caps-overreported-free", "separate", 64, 24, 2, 40, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			e := fakeExtension(t)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			s := placementSpec(false)
			s.SizeHint = SizeHint{MemoryGB: tc.needAccelerator, HostMemoryGB: tc.needRAM}
			submit(t, q, s)
			m := placementMachine("hypha", tc.layout, tc.ram, tc.accelerator, e)
			if tc.name == "profile-caps-overreported-free" {
				m.TotalMemoryGB, m.FreeMemoryGB = 128, 128
			}
			clock.Advance(time.Second)
			if got := check(t, q, m, nil).Job != nil; got != tc.fits {
				t.Fatalf("placed=%v want=%v", got, tc.fits)
			}
		})
	}
}

func TestCapabilityFirstProgramUsesMostCapableFree(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	small := placementMachine("small", "separate", 64, 24, e)
	big := placementMachine("big", "unified", 128, 128, e)
	check(t, q, small, nil)
	check(t, q, big, nil)
	j := submit(t, q, placementSpec(true))
	clock.Advance(time.Second)
	if r := check(t, q, small, nil); r.Job != nil {
		t.Fatal("first program assigned to smaller free hypha")
	}
	if r := check(t, q, big, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("most capable free hypha not selected")
	}
}
