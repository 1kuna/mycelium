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
