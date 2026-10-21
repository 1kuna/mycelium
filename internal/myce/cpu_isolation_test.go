package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestCPUIsolationMetalCommand(t *testing.T) {
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	h.jobs = []Job{harnessJob(t, "metal-cpu", "", `test "$MYCE_PLATFORM" = cpu && test -z "$MYCE_TOKEN" && echo cpu-ok`)}
	w := harnessWorker(t, h)
	w.CPUIsolationProbe = nil // Exercise the real contract, never a successful override.
	w.OwnerSignals = func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop", Heat: "nominal"} }
	w.cpuRuntimeProbe = &MachineProbe{OS: "darwin", Arch: "arm64", Files: fstest.MapFS{}}
	w.Machine.CapabilityProfile.OS, w.Machine.CapabilityProfile.Arch = "darwin", "arm64"
	devices := []MachineDevice{{ID: "0", Platform: "metal", Identity: "apple", Selector: "0", MemoryMode: "unified", BudgetGB: 8, FreeMemoryGB: 8, UnifiedMemory: true, CapabilityProfile: w.Machine.CapabilityProfile}}
	w.Machine.Devices = devices
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return devices, nil }
	startHarness(t, w, h)
	await(t, func() bool { return h.completed("metal-cpu") })
	if r := h.result("metal-cpu"); !r.Success {
		t.Fatalf("Metal host refused CPU command: %+v", r)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	data, _ := json.Marshal(h.reports[len(h.reports)-1].Machine)
	if !strings.Contains(string(data), `"cpu_isolation":"not-enforced"`) {
		t.Fatalf("missing honest isolation status: %s", data)
	}
}

func TestCPUIsolationUnsupportedBeforePlacement(t *testing.T) {
	q, _ := fixture(t)
	j := submit(t, q, cpuSpec(1, 1))
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w := Worker{Machine: cpuMachine("unsupported", "host", "fake", 4, 8), StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}, Clock: RealClock{}, Interval: time.Millisecond,
		InventoryProbe: func(context.Context) ([]MachineDevice, error) {
			devices := inventoryReviewDevices()
			for i := range devices {
				devices[i].CapabilityProfile = cpuMachine("fixture", "host", "fake", 4, 8).CapabilityProfile
			}
			return devices, nil
		},
		OwnerSignals:      func(context.Context) OwnerSignals { return OwnerSignals{Power: "desktop", Heat: "nominal"} },
		CPUIsolationProbe: func(context.Context) error { return errors.New("private runtime detail") },
		HostMemorySource:  func(context.Context) (float64, error) { return 8, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	await(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
	s, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	got := cpuIsolationJob(t, s, j.ID)
	if got.State != "queued" || got.Attempt != 0 || got.Losses != 0 || got.Machine != "" || !strings.Contains(got.Reason, "cpu_accelerator_isolation_unavailable") {
		t.Fatalf("unsupported CPU target was offered instead of withheld: %+v", got)
	}
}

func TestCPUIsolationLaunchQualificationLost(t *testing.T) {
	for _, priority := range []string{"experiment", "production"} {
		t.Run(priority, func(t *testing.T) {
			q, _ := fixture(t)
			spec := cpuSpec(1, 1)
			spec.Priority = priority
			j := submit(t, q, spec)
			m := cpuMachine("host", "host", "cpu", 4, 8)
			r := check(t, q, m, nil)
			if r.Job == nil {
				t.Fatal("CPU fixture was not assigned")
			}
			completion := result(r.Job, false)
			completion.Interrupted = true
			completion.ExecutionCause = &RuntimeCause{Operation: "launch", Class: "cpu_accelerator_isolation_unavailable"}
			completion.Error = "private runtime detail"
			if err := q.Complete(completion); err != nil {
				t.Fatal(err)
			}
			s, _ := q.publicSnapshot()
			got := cpuIsolationJob(t, s, j.ID)
			if got.State != "queued" || got.Losses != 0 || got.InfrastructureRetries != 0 || !strings.Contains(got.Reason, "cpu_accelerator_isolation_unavailable") || strings.Contains(got.Reason, "private") {
				t.Fatalf("qualification loss became a generic infrastructure failure: %+v", got)
			}
			if cpuTargetReason(s.Machines[0]) == "" {
				t.Fatal("launch refusal did not withdraw CPU readiness")
			}
		})
	}
}

func TestCPUIsolationLinuxRuntimeMasks(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu"} {
		for _, outcome := range []string{"hidden", "visible", "error"} {
			t.Run(platform+"/"+outcome, func(t *testing.T) {
				calls := 0
				probe := MachineProbe{OS: "linux", Arch: "amd64", Files: fstest.MapFS{}, CommandEnv: func(_ context.Context, env map[string]string, name string, args ...string) ([]byte, error) {
					calls++
					if env["CUDA_VISIBLE_DEVICES"] != "" || env["ZE_AFFINITY_MASK"] != "2147483647" || env["ONEAPI_DEVICE_SELECTOR"] != "!*:*" || env["ZE_ENABLE_PCI_ID_DEVICE_ORDER"] != "1" {
						t.Fatalf("select-none masks changed: %v", env)
					}
					if platform == "cuda" && (name != "python3" || !strings.Contains(strings.Join(args, " "), "cudaGetDeviceCount")) || platform == "xpu" && name != "sycl-ls" {
						t.Fatalf("runtime enumeration missing: %s %v", name, args)
					}
					if outcome == "error" {
						return nil, errors.New("private driver failure")
					}
					if outcome == "visible" {
						return []byte("GPU visible"), nil
					}
					if platform == "cuda" {
						return []byte("none\n"), nil
					}
					return []byte("CPU only"), nil
				}}
				w := Worker{Machine: Machine{Devices: []MachineDevice{{Platform: platform}}}, cpuRuntimeProbe: &probe}
				status := w.cpuIsolationStatus()
				if calls != 1 || (outcome == "hidden" && status != "select-none") || (outcome != "hidden" && status != cpuIsolationUnavailable) {
					t.Fatalf("qualification=%s calls=%d", status, calls)
				}
			})
		}
	}
}

func TestCPUIsolationMacEmptyInventory(t *testing.T) {
	w := Worker{Machine: Machine{Devices: []MachineDevice{}}, cpuRuntimeProbe: &MachineProbe{OS: "darwin", Arch: "arm64", Files: fstest.MapFS{}}}
	if got := w.cpuIsolationStatus(); got != "not-enforced" {
		t.Fatalf("CPU-only Mac qualification=%s", got)
	}
}

func TestCPUIsolationUnknownPlacement(t *testing.T) {
	for _, status := range []string{"", cpuIsolationUnavailable, "private invalid status"} {
		q, _ := fixture(t)
		m := m3Machine()
		m.CPUIsolation = status
		j := submit(t, q, cpuSpec(1, 1))
		if assignedJob(machineCheck(t, q, m)) != nil {
			t.Fatal("unqualified CPU target assigned")
		}
		s, _ := q.publicSnapshot()
		got := cpuIsolationJob(t, s, j.ID)
		if got.Attempt != 0 || got.Losses != 0 || got.DiagnosticCode != cpuIsolationUnavailable || strings.Contains(got.Reason, "private") {
			t.Fatalf("unqualified target diagnostic: %+v", got)
		}
	}
}

func cpuIsolationJob(t *testing.T, s Snapshot, id string) *Job {
	t.Helper()
	for _, j := range s.Jobs {
		if j.ID == id {
			return j
		}
	}
	t.Fatal("job missing")
	return nil
}
