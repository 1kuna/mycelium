package myce

import (
	"context"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestExtensionLegacyConfigurationCompatibility(t *testing.T) {
	for _, program := range []bool{false, true} {
		s := placementSpec(program)
		s.Run.Env = map[string]string{"BATCH_SIZE": "8", "MYCE_USAGE_FILE": "caller-must-not-control-report"}
		if err := s.Validate(); err != nil {
			t.Fatalf("documented legacy run.env rejected (program=%v): %v", program, err)
		}
		e := fakeExtension(t)
		q, _ := fixture(t)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		j := submit(t, q, s)
		m := placementMachine("legacy", "separate", 128, 128, e)
		check(t, q, m, nil)
		run := check(t, q, m, nil).Job
		if run == nil || run.ID != j.ID {
			t.Fatal("missing legacy assignment")
		}
		if err := e.lower(&run.Spec, &m); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(run.Spec.Run.Env, s.Run.Env) || !reflect.DeepEqual(run.Spec.Run.Command, e.Run) || run.Spec.Execution != nil {
			t.Fatalf("legacy lowering lost configuration: %+v", run.Spec.Run)
		}
	}
}

func TestExtensionMissingVariantReason(t *testing.T) {
	for _, dimension := range []string{"arch", "os", "backend"} {
		t.Run(dimension, func(t *testing.T) {
			e := compositionExtension(t, false)
			switch dimension {
			case "arch":
				e.Variants[0].Target.Arch = "amd64"
				if runtime.GOARCH == "amd64" {
					e.Variants[0].Target.Arch = "arm64"
				}
			case "os":
				e.Variants[0].Target.OS = "linux"
				if runtime.GOOS == "linux" {
					e.Variants[0].Target.OS = "darwin"
				}
			case "backend":
				e.Variants[0].Target.Platform = "cuda"
			}
			e = freezeComposition(t, e)
			root := t.TempDir()
			if err := InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			fp, err := ExtensionFingerprint(context.Background(), "fake", "0")
			if err != nil {
				t.Fatal(err)
			}
			reports := LocalExtensionReports(root, "fake", fp)
			if len(reports) == 0 {
				t.Fatal("missing target omitted from local reports")
			}
			assertReason := func(reason string) {
				t.Helper()
				for _, want := range []string{"no variant matches", runtime.GOOS, runtime.GOARCH, "fake"} {
					if !strings.Contains(reason, want) {
						t.Errorf("reason %q does not name missing host OS/arch/backend (%s)", reason, want)
					}
				}
				for _, misleading := range []string{"D4", "setup", "check required"} {
					if strings.Contains(reason, misleading) {
						t.Errorf("misleading target reason: %s", reason)
					}
				}
			}
			for _, r := range reports {
				assertReason(r.Reason)
				if r.Setup || r.Qualified {
					t.Fatal("missing target reported ready")
				}
			}
			m := machine("unmatched", "fake")
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
			j := Job{Spec: Spec{Extension: extensionKey(e)}, Extensions: []Extension{e}}
			for _, rs := range [][]ExtensionReport{nil, reports} {
				m.ExtensionReports = rs
				if extensionCanRun(&j, &m) {
					t.Fatal("missing target eligible")
				}
				assertReason(extensionWaitingReason(&j, map[string]*Machine{m.ID: &m}))
			}
			for _, check := range []bool{false, true} {
				_, err = PrepareExtension(context.Background(), root, e, "fake", "0", check, io.Discard)
				if err == nil {
					t.Fatal("missing target setup/check succeeded")
				}
				assertReason(err.Error())
			}
		})
	}
}

func TestExtensionVariantLearnedNeeds(t *testing.T) {
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
		if j.Spec.Needs.MemoryGB != 50 || j.Spec.Needs.HostMemoryGB != 5 {
			t.Fatalf("variant discarded learned 50/5 GiB floors: %+v", j.Spec.Needs)
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
}

func TestExtensionVariantResourceAlternatives(t *testing.T) {
	for _, tc := range []struct {
		name, backend string
		explicit      bool
		cores         int
		qualified     bool
		want          bool
		program       bool
		alternative   bool
		fair          bool
	}{
		{"explicit mixed CPU CUDA uses GPU-less host", "cpu", true, 2, true, true, false, false, false},
		{"kind mixed CPU CUDA uses GPU-less host", "cpu", false, 2, true, true, false, false, false},
		{"program uses only CPU learned needs", "cpu", false, 2, true, true, true, false, false},
		{"any qualified fitting alternative dispatches", "cpu", false, 2, true, true, false, true, false},
		{"CUDA ignores larger CPU variant", "cuda", true, 1, true, true, false, false, false},
		{"GPU variant retains its fair resource family", "cuda", true, 1, true, true, false, false, true},
		{"CPU variant exceeds host cores", "cpu", true, 1, true, false, false, false, false},
		{"CPU variant requires qualification", "cpu", true, 2, false, false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, _ := fixture(t)
			e := compositionExtension(t, false)
			e.ProgramTypes = []string{"sample-script"}
			e.Variants[0].Baselines["program:sample-script"] = fakeExtension(t).Baselines["program:sample-script"]
			e.Variants[0].Target.Platform = "cuda"
			e.Variants[0].Needs = ResourceNeeds{MemoryGB: 1, HostMemoryGB: .1}
			cpu := e.Variants[0]
			cpu.Target.Platform = "cpu"
			cpu.Needs = ResourceNeeds{CPUCores: 2, MemoryGB: 1}
			e.Variants = append(e.Variants, cpu)
			e = freezeComposition(t, e)
			selected := e
			if tc.alternative {
				other := compositionExtension(t, false)
				other.Version = "2"
				other.Variants = append([]ExtensionVariant(nil), e.Variants...)
				other.ProgramTypes = e.ProgramTypes
				selected = freezeComposition(t, other)
				if err := q.AddExtension(selected); err != nil {
					t.Fatal(err)
				}
				e.Variants[1].Needs.CPUCores = 4
				e = freezeComposition(t, e)
			}
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			m := cpuMachine("worker", "host", tc.backend, tc.cores, 16)
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
			m.ExtensionFingerprint = "variant-fixture"
			s := Spec{Name: "mixed", Project: "test"}
			if tc.explicit {
				s.Extension = extensionKey(e)
			} else {
				s.Kind = "sample"
			}
			if tc.program {
				s.Kind = ""
				s.Program = &Program{Type: "sample-script", File: "script.sh"}
				s.Inputs = map[string]string{"script.sh": "scripts/example.sh"}
				q.s.ProgramNeeds = map[string]SizeHint{
					programNeedsKey(&Job{Spec: s}, e, "cpu"):  {MemoryGB: 2, HostMemoryGB: 3},
					programNeedsKey(&Job{Spec: s}, e, "cuda"): {MemoryGB: 40, HostMemoryGB: 2},
				}
			}
			r := newExtensionReport(e, &m, s.workType())
			r.Setup, r.Qualified, r.CheckID = true, tc.qualified, "variant-check"
			m.ExtensionReports = []ExtensionReport{r}
			if tc.alternative {
				r := newExtensionReport(selected, &m, s.workType())
				r.Setup, r.Qualified, r.CheckID = true, true, "other-variant-check"
				m.ExtensionReports = append(m.ExtensionReports, r)
			}
			check(t, q, m, nil)
			j := submit(t, q, s)
			if tc.fair {
				q.s.FairUsage = map[string]FairUsage{fairUsageKey(q.priorityMatch(j.Spec).Tier, "gpu", s.Project): {Seconds: 100, At: q.clock.Now()}}
				s.Project = "other"
				j = submit(t, q, s)
			}
			run := check(t, q, m, nil).Job
			if (run != nil) != tc.want {
				t.Fatalf("assignment=%v, want=%v; reason=%s", run, tc.want, job(t, q, j.ID).Reason)
			}
			if !tc.want {
				return
			}
			want := selected.variant(machineTarget(&m)).Needs
			if tc.program {
				want.HostMemoryGB = 3
			}
			want.Platform = Platform{Required: []string{tc.backend}}
			canonical := Spec{Needs: want}
			if err := normalizeResources(&canonical); err != nil {
				t.Fatal(err)
			}
			if run.ID != j.ID {
				t.Fatalf("assigned project %s, want %s", run.Spec.Project, j.Spec.Project)
			}
			if !reflect.DeepEqual(run.Spec.Needs, canonical.Needs) {
				t.Fatalf("wrong variant reservation: %+v, want %+v", run.Spec.Needs, canonical.Needs)
			}
			if chosen := qualifiedExtension(run, &m); chosen == nil || chosen.Digest != selected.Digest {
				t.Fatal("attempt did not retain the selected alternative")
			}
			if err := q.Started(result(run, true)); err != nil {
				t.Fatal("selected variant rejected at Started", err)
			}
		})
	}
}
