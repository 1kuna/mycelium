package myce

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBenchmarkPlacementPredictsFinishOnEligibleHypha(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, production := range []bool{false, true} {
			t.Run(map[bool]string{false: "experiment", true: "production"}[production], func(t *testing.T) {
				q, clock := fixture(t)
				e := fakeExtension(t)
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
				slow := placementMachine("slow", "unified", 128, 128, e)
				fast := placementMachine("fast", "separate", 64, 24, e)
				for _, m := range []*Machine{&slow, &fast} {
					rate := 1.0
					if m.ID == "fast" {
						rate = 4
					}
					m.CapabilityProfile.Benchmark = &BenchmarkResult{Measurements: []Throughput{{Resource: "accelerator", Work: "program:fake-program", ExtensionDigest: e.Digest, Method: "qualified-reference-v1", Rate: rate, Unit: "references/s"}}}
				}
				// Use the extension's actual program type; the input and method are identical.
				for _, m := range []*Machine{&slow, &fast} {
					m.CapabilityProfile.Benchmark.Measurements[0].Work = "program:" + e.ProgramTypes[0]
				}
				check(t, q, slow, nil)
				check(t, q, fast, nil)
				if production {
					if err := q.ConfigureHosts(map[string]HostPolicy{slow.ID: {ProductionHold: true}, fast.ID: {ProductionHold: true}}); err != nil {
						t.Fatal(err)
					}
				}
				s := placementSpec(true)
				if production {
					s.Priority = "production"
				}
				j := submit(t, q, s)
				clock.Advance(time.Second)
				if got := check(t, q, slow, nil).Job; got != nil {
					t.Fatal("memory capacity outranked measured finish time")
				}
				if got := check(t, q, fast, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("faster eligible hypha did not receive job")
				}
			})
		}

	})

	t.Run("BenchmarkPreferenceRespectsEligibility", func(t *testing.T) {
		for _, why := range []string{"availability", "unqualified", "disabled", "memory", "allowed"} {
			t.Run(why, func(t *testing.T) {
				q, _ := fixture(t)
				e := fakeExtension(t)
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
				slow, fast := placementMachine("slow", "unified", 64, 64, e), placementMachine("fast", "unified", 128, 128, e)
				for _, m := range []*Machine{&slow, &fast} {
					rate := 1.0
					if m.ID == "fast" {
						rate = 100
					}
					m.CapabilityProfile.Benchmark = &BenchmarkResult{Measurements: []Throughput{{Resource: "accelerator", Method: "qualified-reference-v1", ExtensionDigest: e.Digest, Work: "kind:sample", Rate: rate, Unit: "references/s"}}}
				}
				if why == "availability" {
					fast.CapabilityProfile.Availability = &Availability{Available: false}
				}
				if why == "unqualified" {
					fast.ExtensionReports = nil
				}
				if why == "memory" {
					fast.FreeMemoryGB = 0
				}
				check(t, q, slow, nil)
				check(t, q, fast, nil)
				if why == "disabled" {
					if err := setMaintenance(t, q, fast.ID, "disable"); err != nil {
						t.Fatal(err)
					}
				}
				s := placementSpec(false)
				if why == "allowed" {
					s.AllowedMachines = []string{slow.ID}
				}
				j := submit(t, q, s)
				if got := check(t, q, slow, nil).Job; got == nil || got.ID != j.ID {
					t.Fatal("faster ineligible hypha blocked work", why)
				}
			})
		}

	})

	t.Run("BenchmarkMixedMeasurementsDoNotStrandJobs", func(t *testing.T) {
		q, _ := fixture(t)
		e := fakeExtension(t)
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
		}
		slow := placementMachine("slow", "unified", 128, 128, e)
		fast := placementMachine("fast", "separate", 64, 24, e)
		unknown := placementMachine("unknown", "unified", 96, 96, e)
		for _, m := range []*Machine{&slow, &fast} {
			rate := 1.0
			if m.ID == "fast" {
				rate = 4
			}
			m.CapabilityProfile.Benchmark = &BenchmarkResult{Measurements: []Throughput{{Resource: "accelerator", Method: "qualified-reference-v1", Work: "kind:sample", ExtensionDigest: e.Digest, Rate: rate, Unit: "references/s"}}}
		}
		for _, m := range []Machine{slow, fast, unknown} {
			check(t, q, m, nil)
		}
		j := submit(t, q, placementSpec(false))
		if check(t, q, slow, nil).Job != nil || check(t, q, unknown, nil).Job != nil {
			t.Fatal("slower or unknown hypha outranked measured finish")
		}
		if got := check(t, q, fast, nil).Job; got == nil || got.ID != j.ID {
			t.Fatal("comparison cycle stranded work")
		}

	})
}

func TestBenchmarkDailyOwnerReturnCancelsMeasurement(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		var active atomic.Bool
		measured := []string{}
		c := BenchmarkConfig{Clock: RealClock{}, StateDir: newTestStateRoot(t), Machine: Machine{GPU: "fake"}, DailyUse: true, OwnerSignals: func(context.Context) OwnerSignals {
			s := idleSignals()
			if active.Load() {
				s.IdleSeconds = 0
			}
			return s
		}, Measure: func(ctx context.Context, r string) ([]Throughput, error) {
			if r != "cpu" {
				t.Error("owner return allowed later benchmark payload", r)
			}
			measured = append(measured, r)
			active.Store(true)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(8 * time.Second):
				t.Fatal("owner return did not interrupt benchmark")
				return nil, nil
			}
		}}
		p := CapabilityProfile{}
		if err := RunOnboardingBenchmark(context.Background(), c, &p); err != nil {
			t.Fatal(err)
		}
		if p.Benchmark.State != "partial" {
			t.Fatal(measured, p.Benchmark)
		}

	})

	t.Run("BenchmarkDailyActiveNeverMeasures", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		c := BenchmarkConfig{Clock: RealClock{}, StateDir: newTestStateRoot(t), Machine: Machine{GPU: "fake"}, DailyUse: true, OwnerSignals: func(context.Context) OwnerSignals {
			cancel()
			s := idleSignals()
			s.IdleSeconds = 0
			return s
		}, Measure: func(context.Context, string) ([]Throughput, error) {
			t.Fatal("benchmark ran while owner active")
			return nil, nil
		}}
		p := CapabilityProfile{}
		if err := RunOnboardingBenchmark(ctx, c, &p); err != nil {
			t.Fatal(err)
		}
		if p.Benchmark.State != "partial" || len(p.Benchmark.Measurements) != 1 {
			t.Fatal(p.Benchmark)
		}

	})
}

func TestBenchmarkRequiresQualificationAndCorrectReference(t *testing.T) {
	for _, mode := range []string{"unqualified", "good", "wrong-output"} {
		t.Run(mode, func(t *testing.T) {
			base := fakeExtension(t)
			base.Files["extension.yaml"] = []byte(strings.Replace(string(base.Files["extension.yaml"]), "check: [sh, check.sh]", "check: [sh, check.sh]\nbenchmark: [sh, benchmark.sh]", 1))
			script := `test -z "$MYCE_TOKEN" || exit 99
cat "$MYCE_CHECK_INPUT" > "$MYCE_CHECK_OUTPUT"
printf '%s' '{"units":8,"seconds":2,"unit":"references/s","reference_units":1}' > "$MYCE_BENCHMARK_OUTPUT"
`
			if mode == "wrong-output" {
				script = strings.Replace(script, "cat \"$MYCE_CHECK_INPUT\"", "echo '[9,9]'", 1)
			}
			base.Files["benchmark.sh"] = []byte(script)
			e, err := extensionFromFiles(base.Files)
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			if err = InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			if mode != "unqualified" {
				installQualified(t, root, e, "")
			}
			fingerprint, err := ExtensionFingerprint(context.Background(), "fake", "")
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("MYCE_TOKEN", "must-not-inherit")
			measured, err := measureReferences(context.Background(), BenchmarkConfig{Clock: RealClock{}, StateDir: newTestStateRoot(t), ExtensionRoot: root, Machine: Machine{GPU: "fake"}}, []Extension{e}, fingerprint)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "unqualified" {
				if len(measured) != 0 {
					t.Fatal("unqualified benchmark ran")
				}
				return
			}
			if len(measured) != 2 {
				t.Fatal(measured)
			}
			for _, m := range measured {
				if (m.Rate == 4) != (mode == "good") {
					t.Fatal(mode, m)
				}
			}
			for _, r := range LocalExtensionReports(root, "fake", fingerprint) {
				if !r.Qualified {
					t.Fatal("measurement altered qualification")
				}
			}
		})
	}
}

// A physical CPU-only host has no accelerator reference to qualify. An
// accelerator host still reports partial until its own reference is measured.
func TestBenchmarkCPUOnlyNeedsNoAcceleratorReference(t *testing.T) {
	for _, platform := range []string{"cpu", "fake"} {
		t.Run(platform, func(t *testing.T) {
			clock := &workerFakeClock{now: time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)}
			p := CapabilityProfile{LogicalCPUs: 4, SystemMemoryBytes: 8 << 30}
			c := BenchmarkConfig{Clock: clock, StateDir: t.TempDir(), Machine: Machine{GPU: platform}, OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} }, Measure: func(_ context.Context, resource string) ([]Throughput, error) {
				if resource == "accelerator" {
					return nil, nil
				}
				clock.Advance(time.Millisecond)
				return []Throughput{{Resource: resource, Rate: 100, Unit: "bytes/s"}}, nil
			}}
			if err := RunOnboardingBenchmark(context.Background(), c, &p); err != nil {
				t.Fatal(err)
			}
			wantState, wantCount := "complete", 4
			if platform != "cpu" {
				wantState, wantCount = "partial", 5
			}
			if p.Benchmark.State != wantState || len(p.Benchmark.Measurements) != wantCount {
				t.Fatalf("%s host: %+v", platform, p.Benchmark)
			}
		})
	}
}
