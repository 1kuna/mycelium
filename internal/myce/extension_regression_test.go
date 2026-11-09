package myce

import (
	"context"
	"io"
	"reflect"
	"runtime"

	"testing"
)

func TestExtensionLegacyConfigurationCompatibility(t *testing.T) {
	for _, program := range []bool{true} {
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
	for _, dimension := range []string{"arch"} {
		t.Run(dimension, func(t *testing.T) {
			e := compositionExtension(t, false)
			switch dimension {
			case "arch":
				e.Variants[0].Target.Arch = "amd64"
				if runtime.GOARCH == "amd64" {
					e.Variants[0].Target.Arch = "arm64"
				}
			}
			e = freezeComposition(t, e)
			root := t.TempDir()
			if err := InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			for _, check := range []bool{true} {
				_, err := PrepareExtension(context.Background(), root, e, "fake", "0", check, io.Discard)
				if err == nil {
					t.Fatal("missing target setup/check succeeded")
				}
			}
		})
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
	}{{"any qualified fitting alternative dispatches", "cpu", false, 2, true, true, false, true, false}, {"CUDA ignores larger CPU variant", "cuda", true, 1, true, true, false, false, false}} {
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
