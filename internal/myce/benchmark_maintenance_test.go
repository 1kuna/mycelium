package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestS6ReviewOnboardingWhileDrained(t *testing.T) {
	q, _ := fixture(t)
	m := machine("benchmark-worker", "xpu", "host")
	check(t, q, m, nil)
	if st := maintenanceChange(t, q, "drain", false); !st.Quiescent {
		t.Fatal(st)
	}
	root := newTestStateRoot(t)
	if err := saveWorkerJSON(root, "state.json", workerState{Session: m.Session}); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(root, "benchmark-after-drain")
	base := fakeExtension(t)
	base.Files["extension.yaml"] = []byte(strings.Replace(string(base.Files["extension.yaml"]), "check: [sh, check.sh]", "check: [sh, check.sh]\nbenchmark: [sh, benchmark.sh]", 1))
	base.Files["benchmark.sh"] = []byte("printf started > \"" + marker + "\"\ncat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\nprintf '%s' '{\"units\":8,\"seconds\":2,\"unit\":\"references/s\",\"reference_units\":1}' > \"$MYCE_BENCHMARK_OUTPUT\"\n")
	e, err := extensionFromFiles(base.Files)
	if err != nil {
		t.Fatal(err)
	}
	extRoot := t.TempDir()
	if err = InstallExtension(extRoot, e); err != nil {
		t.Fatal(err)
	}
	installQualified(t, extRoot, e, "")
	settled := make(chan struct{})
	var once sync.Once
	handler := API{q, "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		if r.URL.Path == "/api/workers/checkin" {
			snap, _ := q.Snapshot()
			for _, current := range snap.Machines {
				if current.ID == m.ID && current.Session != m.Session && !current.BenchmarkPending {
					once.Do(func() { close(settled) })
				}
			}
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	defer func() {
		cancel()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	w := Worker{OnboardingBenchmark: true, StateDir: root, ExtensionRoot: extRoot, Machine: m, Client: Client{URL: server.URL, Token: "test"}, Interval: time.Millisecond,
		OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} },
		Benchmark: func(ctx context.Context, c BenchmarkConfig, p *CapabilityProfile) error {
			// Simulate accelerator inventory only; execute the product's real benchmark
			// selection, qualified reference replay and command supervisor on the laptop.
			c.Machine.GPU = "fake"
			c.Measure = func(ctx context.Context, resource string) ([]Throughput, error) {
				if resource != "accelerator" {
					return []Throughput{{Resource: resource, Rate: 1, Unit: "bytes/s"}}, nil
				}
				identity, err := ExtensionFingerprint(ctx, "fake", "")
				if err != nil {
					return nil, err
				}
				return measureReferences(ctx, c, []Extension{e}, identity)
			}
			return RunOnboardingBenchmark(ctx, c, p)
		}}
	go func() { done <- w.Run(ctx) }()
	select {
	case <-settled:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not settle")
	}
	st, err := q.HostMaintenanceStatus("host")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(marker)
	t.Logf("qualified real shell benchmark=%q; marker_err=%v; healthy=%t quiescent=%t preparations=%d", data, err, st.Healthy, st.Quiescent, st.Preparations)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatal("onboarding benchmark ran after drain without queue custody or launch authorization", err)
	}
}

// Use real qualification/reference selection and process supervision. Only the
// accelerator identity and non-accelerator measurements are synthetic.
func maintenanceBenchmarkWorker(t *testing.T, m Machine) (Worker, string, string) {
	t.Helper()
	root := newTestStateRoot(t)
	if err := saveWorkerJSON(root, "state.json", workerState{Session: m.Session}); err != nil {
		t.Fatal(err)
	}
	marker, release := filepath.Join(root, "started"), filepath.Join(root, "release")
	base := fakeExtension(t)
	base.Files["extension.yaml"] = []byte(strings.Replace(string(base.Files["extension.yaml"]), "check: [sh, check.sh]", "check: [sh, check.sh]\nbenchmark: [sh, benchmark.sh]", 1))
	base.Files["benchmark.sh"] = []byte("printf started > \"" + marker + "\"\nwhile [ ! -f \"" + release + "\" ]; do sleep 0.01; done\ncat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\nprintf '%s' '{\"units\":8,\"seconds\":2,\"unit\":\"references/s\",\"reference_units\":1}' > \"$MYCE_BENCHMARK_OUTPUT\"\n")
	e, err := extensionFromFiles(base.Files)
	if err != nil {
		t.Fatal(err)
	}
	extRoot := t.TempDir()
	if err = InstallExtension(extRoot, e); err != nil {
		t.Fatal(err)
	}
	installQualified(t, extRoot, e, "")
	w := Worker{OnboardingBenchmark: true, StateDir: root, ExtensionRoot: extRoot, Machine: m, Interval: time.Millisecond,
		OwnerSignals: func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop"} },
		Benchmark: func(ctx context.Context, c BenchmarkConfig, p *CapabilityProfile) error {
			c.Machine.GPU = "fake"
			c.Measure = func(ctx context.Context, resource string) ([]Throughput, error) {
				if resource != "accelerator" {
					return []Throughput{{Resource: resource, Rate: 1, Unit: "bytes/s"}}, nil
				}
				identity, err := ExtensionFingerprint(ctx, "fake", "")
				if err != nil {
					return nil, err
				}
				return measureReferences(ctx, c, []Extension{e}, identity)
			}
			return RunOnboardingBenchmark(ctx, c, p)
		}}
	return w, marker, release
}

func TestMaintenanceRunningBenchmark(t *testing.T) {
	for _, action := range []string{"drain", "disable", "disable-stop"} {
		t.Run(action, func(t *testing.T) {
			q, _ := fixture(t)
			w, marker, release := maintenanceBenchmarkWorker(t, machine("benchmark-worker", "xpu", "host"))
			server := httptest.NewServer(API{q, "test"}.Handler())
			defer server.Close()
			w.Client = Client{URL: server.URL, Token: "test"}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop")
				}
			}()
			await(t, func() bool { _, err := os.Stat(marker); return err == nil })
			name := action
			if action == "disable-stop" {
				name = "disable"
			}
			st := maintenanceChange(t, q, name, action == "disable-stop")
			if !st.Healthy || st.Quiescent || st.Preparations != 1 {
				t.Fatal("running benchmark lost custody", st)
			}
			// A stale completion cannot discharge the real command, and a sibling must
			// not receive queued work while this host is onboarding.
			snap, _ := q.Snapshot()
			current := *snap.Machines[0]
			if _, err := q.Checkin(Checkin{Machine: current, BenchmarkDone: "wrong-attempt"}); err != nil {
				t.Fatal(err)
			}
			st, _ = q.HostMaintenanceStatus("host")
			if st.Quiescent || st.Preparations != 1 {
				t.Fatal("wrong attempt discharged benchmark", st)
			}
			if action != "disable-stop" {
				// Drain/disable preserve the command already running behind its gate.
				if _, err := os.Stat(release); !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				if err := os.WriteFile(release, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			await(t, func() bool {
				st, err := q.HostMaintenanceStatus("host")
				return err == nil && st.Healthy && st.Quiescent && st.Preparations == 0
			})
		})
	}
}

func TestMaintenanceDelayedBenchmarkCommand(t *testing.T) {
	for _, action := range []string{"drain", "disable", "disable-stop"} {
		t.Run(action, func(t *testing.T) {
			q, _ := fixture(t)
			w, marker, release := maintenanceBenchmarkWorker(t, machine("benchmark-worker", "xpu", "host"))
			// All early resource checks succeed. Maintenance arrives while the actual
			// benchmark runner is waiting, before the final shell supervisor check.
			entered, proceed := make(chan struct{}), make(chan struct{})
			benchmark := w.Benchmark
			w.Benchmark = func(ctx context.Context, c BenchmarkConfig, p *CapabilityProfile) error {
				parent := ctx
				calls := 0
				ctx = withLaunchGate(ctx, func() error {
					calls++
					if calls == 6 { // Five resource checks, then the real shell supervisor.
						close(entered)
						select {
						case <-proceed:
						case <-parent.Done():
							return parent.Err()
						}
					}
					return checkLaunchGate(parent)
				})
				return benchmark(ctx, c, p)
			}
			server := httptest.NewServer(API{q, "test"}.Handler())
			defer server.Close()
			w.Client = Client{URL: server.URL, Token: "test"}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- w.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop")
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("no benchmark authorization")
			}
			name := action
			if action == "disable-stop" {
				name = "disable"
			}
			st := maintenanceChange(t, q, name, action == "disable-stop")
			if st.Quiescent || st.Preparations != 1 {
				t.Fatal("missing benchmark custody", st)
			}
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Fatal(err)
			}
			close(proceed)
			await(t, func() bool {
				st, err := q.HostMaintenanceStatus("host")
				return err == nil && st.Quiescent && st.Preparations == 0
			})
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("benchmark shell ran after maintenance", err)
			}
			st, _ = q.HostMaintenanceStatus("host")
			if !st.Quiescent || st.Preparations != 0 {
				t.Fatal("exact cleanup not settled", st)
			}
		})
	}
}

func TestMaintenanceBenchmarkExcludesOtherWork(t *testing.T) {
	q, _, m := modelFixture(t, "host")
	m.BenchmarkPending = true
	reply := check(t, q, m, nil)
	if reply.BenchmarkPlan == nil {
		t.Fatal("no benchmark plan")
	}
	if err := q.AuthorizeLaunch(*reply.BenchmarkPlan); err != nil {
		t.Fatal(err)
	}
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	submit(t, q, spec("experiment"))
	waiting := addModelCall(t, q, "experiment")
	if waiting.Machine != "" {
		t.Fatal("model call overlapped benchmark")
	}
	sibling := machine("sibling", "fake", "host")
	sibling.ModelURL = m.ModelURL
	sibling.ModelConfigHash = m.ModelConfigHash
	r := check(t, q, sibling, nil)
	if r.Job != nil || len(r.ModelPlan) != 0 || len(r.CopyPlan) != 0 || len(r.EnvironmentPlan) != 0 {
		t.Fatal("sibling admitted work during benchmark", r)
	}
	snap, _ := q.Snapshot()
	if len(snap.LoadedModels) != 0 {
		t.Fatal("pin started during benchmark", snap.LoadedModels)
	}
}

func TestMaintenanceBenchmarkPreservesActivePin(t *testing.T) {
	q, _, m := modelFixture(t, "host")
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	reportModel(t, q, m, "ready")
	attempt := slot.Attempt
	m.BenchmarkPending = true
	reply, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: attempt, State: "ready"}}})
	if err != nil {
		t.Fatal(err)
	}
	if reply.BenchmarkPlan != nil || slot.State != "ready" || slot.Attempt != attempt || !q.slotPinned(slot) {
		t.Fatal("onboarding disrupted existing pin", reply, slot)
	}
}
