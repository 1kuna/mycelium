package myce

import (
	"context"
	"encoding/json"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBenchmarkPlacementPredictsFinishOnEligibleHypha(t *testing.T) {
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
}

func TestBenchmarkSerialPersistenceAndHardwareInvalidation(t *testing.T) {
	clock := &workerFakeClock{now: time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)}
	root := t.TempDir()
	calls := []string{}
	p := CapabilityProfile{OS: "test", Arch: "test", LogicalCPUs: 4, SystemMemoryBytes: 8 << 30}
	c := BenchmarkConfig{Clock: clock, StateDir: root, Machine: Machine{GPU: "fake"}, OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} }, Measure: func(ctx context.Context, r string) ([]Throughput, error) {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		calls = append(calls, r)
		clock.Advance(time.Millisecond)
		return []Throughput{{Resource: r, Rate: 100, Unit: "bytes/s", Method: "fake"}}, nil
	}}
	if err := RunOnboardingBenchmark(context.Background(), c, &p); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"cpu", "memory", "disk", "network", "accelerator"}) {
		t.Fatal(calls)
	}
	data, err := os.ReadFile(filepath.Join(root, "capability-profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	var disk CapabilityProfile
	if json.Unmarshal(data, &disk) != nil || disk.Version != 0 || disk.Benchmark.State != "complete" || disk.Benchmark.Seconds != .005 {
		t.Fatal(string(data))
	}
	calls = nil
	if err = RunOnboardingBenchmark(context.Background(), c, &p); err != nil || len(calls) != 0 {
		t.Fatal("did not reuse unchanged profile", err, calls)
	}
	p.LogicalCPUs++
	if err = RunOnboardingBenchmark(context.Background(), c, &p); err != nil || len(calls) != 5 {
		t.Fatal("changed hardware reused stale throughput", err, calls)
	}
}

func TestBenchmarkDailyOwnerReturnCancelsMeasurement(t *testing.T) {
	var active atomic.Bool
	measured := []string{}
	c := BenchmarkConfig{Clock: RealClock{}, StateDir: newTestStateRoot(t), Machine: Machine{GPU: "fake"}, DailyUse: true, OwnerSignals: func(context.Context) OwnerSignals {
		s := idleSignals()
		if active.Load() {
			s.IdleSeconds = 0
		}
		return s
	}, Measure: func(ctx context.Context, r string) ([]Throughput, error) {
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
	if !reflect.DeepEqual(measured, []string{"cpu"}) || p.Benchmark.State != "partial" {
		t.Fatal(measured, p.Benchmark)
	}
}

func TestBenchmarkDailyActiveNeverMeasures(t *testing.T) {
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
}

func TestBenchmarkRankingTransferCostAndMissingRates(t *testing.T) {
	q, _ := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	a, b := placementMachine("a", "unified", 128, 128, e), placementMachine("b", "separate", 64, 24, e)
	for _, m := range []*Machine{&a, &b} {
		m.CapabilityProfile.Benchmark = &BenchmarkResult{Measurements: []Throughput{
			{Resource: "accelerator", Work: "kind:sample", ExtensionDigest: e.Digest, Method: "qualified-reference-v1", Rate: 2, Unit: "references/s"},
			{Resource: "network", Method: "file-store-download-8MiB-sha256-v1", Rate: 100, Unit: "bytes/s"},
		}}
	}
	a.CapabilityProfile.Benchmark.Measurements[0].Rate = 4
	a.CapabilityProfile.Benchmark.Measurements[1].Rate = 1
	s := placementSpec(false)
	s.Files = FileManifest{"input": {SHA256: "fake", SizeBytes: 100}}
	j := &Job{Spec: s, Extensions: []Extension{e}}
	if better, known := q.benchmarkBetter(j, &b, &a); !known || !better {
		t.Fatal("transfer cost did not prefer 1.5s over 100.25s")
	}
	for _, bad := range []float64{0, math.NaN(), math.Inf(1)} {
		b.CapabilityProfile.Benchmark.Measurements[0].Rate = bad
		if better, known := q.benchmarkBetter(j, &b, &a); !known || better {
			t.Fatal("invalid measurement outranked a known estimate")
		}
	}
}

func TestBenchmarkPreferenceRespectsEligibility(t *testing.T) {
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
}

func TestBenchmarkRequiresQualificationAndCorrectReference(t *testing.T) {
	for _, mode := range []string{"unqualified", "good", "wrong-output", "wrong-timing"} {
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
			if mode == "wrong-timing" {
				script = strings.Replace(script, `"seconds":2`, `"seconds":0`, 1)
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

func TestBenchmarkCoreRealFileStore(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 32<<20, false); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test-token"}.Handler())
	defer api.Close()
	c := BenchmarkConfig{Clock: RealClock{}, StateDir: newTestStateRoot(t), Client: Client{URL: api.URL, Token: "test-token"}}
	for _, r := range []string{"cpu", "memory", "disk", "network"} {
		measured, err := measureResource(context.Background(), c, nil, "", r)
		if err != nil || len(measured) != 1 || !validRate(measured[0].Rate) {
			t.Fatal(r, measured, err)
		}
	}
}

func TestBenchmarkMixedMeasurementsDoNotStrandJobs(t *testing.T) {
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
