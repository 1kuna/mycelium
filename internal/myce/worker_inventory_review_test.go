package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func awaitInventoryReview(t *testing.T, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-timeout.C:
			t.Fatal("inventory condition not reached")
		}
	}
}

func inventoryReviewDevices() []MachineDevice {
	d := fakeMachineDevices()
	for i := range d {
		d[i].Platform, d[i].Selector = "fake", fmt.Sprint(i)
	}
	return d
}

func runInventoryReviewWorker(t *testing.T, q *Queue, w Worker) Client {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	w.Client = Client{URL: server.URL, Token: "test"}
	w.StateDir, w.Clock, w.Interval = newTestStateRoot(t), RealClock{}, time.Millisecond
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.CPUIsolationProbe = func(context.Context) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		s, _ := q.Snapshot()
		for _, j := range s.Jobs {
			_ = q.Cancel(j.ID)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("inventory worker did not stop")
		}
	})
	return w.Client
}

func TestInventoryReviewDeviceAllowanceRealAPI(t *testing.T) {
	for _, selection := range []string{"1", "", "0,1"} {
		t.Run("selection="+selection, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("inventory-worker", "fake", "inventory-host")
			m.Device = selection
			client := runInventoryReviewWorker(t, q, Worker{Machine: m, InventoryProbe: func(context.Context) ([]MachineDevice, error) {
				devices := inventoryReviewDevices()
				// Runtime ordinals can differ from host-local component IDs.
				devices[0].Selector, devices[1].Selector = "1", "0"
				return devices, nil
			}})
			awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
			s, _ := q.Snapshot()
			wantIDs := []string{"0", "1"}
			if selection == "1" {
				wantIDs = []string{"1"}
			}
			ids := []string{}
			for _, d := range s.Machines[0].Devices {
				ids = append(ids, d.ID)
			}
			if !slices.Equal(ids, wantIDs) {
				t.Fatalf("reported devices %v, want %v", ids, wantIDs)
			}
			marker := filepath.Join(t.TempDir(), "devices")
			spec := spec("experiment")
			spec.Resumable = false
			spec.Run.Command = []string{"sh", "-c", `printf '%s' "$MYCE_DEVICES" > "$1"`, "sh", marker}
			var j Job
			if err := client.Call(context.Background(), "POST", "/api/jobs", spec, &j); err != nil {
				t.Fatal(err)
			}
			awaitInventoryReview(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
			data, err := os.ReadFile(marker)
			if err != nil || string(data) != wantIDs[0] || !slices.Equal(job(t, q, j.ID).AllocatedDevices, []string{wantIDs[0]}) {
				t.Fatalf("payload devices=%q placement=%v err=%v", data, job(t, q, j.ID).AllocatedDevices, err)
			}
		})
	}
}

func TestInventoryReviewRecoveryRealAPI(t *testing.T) {
	for _, startup := range []string{"healthy", "empty", "probe-error"} {
		t.Run(startup, func(t *testing.T) {
			q, _ := fixture(t)
			var phase atomic.Int32
			t.Cleanup(func() {
				if t.Failed() {
					s, _ := q.Snapshot()
					for _, j := range s.Jobs {
						t.Logf("job state=%s reason=%s", j.State, j.Reason)
					}
					for _, m := range s.Machines {
						t.Logf("machine gpu=%s memory=%s cpu=%s host-memory=%s", m.GPU, m.MemoryError, m.CPUCapacityReason, m.HostMemoryError)
					}
				}
			})
			if startup == "healthy" {
				phase.Store(1)
			}
			probe := func(context.Context) ([]MachineDevice, error) {
				switch phase.Load() {
				case 0:
					if startup == "probe-error" {
						return nil, errors.New("Level Zero unavailable")
					}
					return []MachineDevice{}, nil
				case 2:
					return nil, errors.New("transient driver failure")
				default:
					return inventoryReviewDevices(), nil
				}
			}
			runInventoryReviewWorker(t, q, Worker{Machine: machine("recover", "fake", "recover-host"), InventoryProbe: probe})
			awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
			if startup == "healthy" {
				awaitInventoryReview(t, func() bool {
					s, _ := q.Snapshot()
					return len(s.Machines[0].Devices) == 2 && s.Machines[0].GPU == "fake"
				})
				phase.Store(2)
			}
			awaitInventoryReview(t, func() bool {
				s, _ := q.Snapshot()
				return s.Machines[0].GPU == "cpu" && s.Machines[0].MemoryError == "device_probe_unavailable"
			})
			j := submit(t, q, cpuSpec(1, 1))
			awaitInventoryReview(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
			phase.Store(3)
			awaitInventoryReview(t, func() bool {
				s, _ := q.Snapshot()
				return len(s.Machines[0].Devices) == 2 && s.Machines[0].GPU == "fake" && s.Machines[0].MemoryError == ""
			})
			gpu := submit(t, q, spec("experiment"))
			awaitInventoryReview(t, func() bool { return job(t, q, gpu.ID).State == "succeeded" })
		})
	}
}

func TestInventoryReviewIdentityRecovery(t *testing.T) {
	t.Run("replacement", func(t *testing.T) {
		old := fakeMachineDevices()
		changed := fakeMachineDevices()
		changed[1].Identity = "replacement-card"
		withheld := reconcileDevices(old, changed)
		for i := 0; i < 2; i++ {
			if len(withheld) != 2 || withheld[1].Reason != "device_identity_mismatch" || withheld[1].Identity != old[1].Identity {
				t.Fatalf("mismatch admitted replacement: %+v", withheld)
			}
			withheld = reconcileDevices(withheld, changed)
		}
		recovered := reconcileDevices(withheld, old)
		if len(recovered) != 2 || recovered[1].Reason != "" {
			t.Fatalf("matching identity stayed withheld: %+v", recovered)
		}
	})
	t.Run("driver-and-name", func(t *testing.T) {
		old, changed := fakeMachineDevices(), fakeMachineDevices()
		changed[0].Fingerprint, changed[1].Fingerprint = "new driver/name", "new driver/name"
		for _, d := range reconcileDevices(old, changed) {
			if d.Reason != "" {
				t.Fatalf("driver/name changed identity: %+v", d)
			}
		}
		w := Worker{Machine: Machine{Devices: old}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return changed, nil }}
		if _, err := w.forAttempt(Job{AllocatedDevices: []string{"1"}}); err != nil {
			t.Fatalf("driver change blocked launch: %v", err)
		}
	})
	t.Run("launch-probe-error", func(t *testing.T) {
		w := Worker{Machine: Machine{Devices: fakeMachineDevices()}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return nil, errors.New("offline") }}
		_, err := w.forAttempt(Job{AllocatedDevices: []string{"1"}})
		var rule *rejectionError
		if !errors.As(err, &rule) || rule.code != "device_probe_unavailable" {
			t.Fatalf("probe failure reason: %v", err)
		}
	})
}

func TestInventoryReviewPersistedWithholdingExpires(t *testing.T) {
	for _, reason := range []string{"device_identity_mismatch", "device_probe_unavailable", "device_memory_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			w := Worker{Clock: RealClock{}, Machine: machine("saved", "fake")}
			w.StateDir = newTestStateRoot(t)
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			old := fakeMachineDevices()
			old[1].Reason = reason
			if err := saveWorkerJSON(s.dir, "devices.json", old); err != nil {
				t.Fatal(err)
			}
			w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return fakeMachineDevices(), nil }
			if err := w.initInventory(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			if w.Machine.Devices[1].Reason != "" {
				t.Fatal("persisted condition outlived probe")
			}
		})
	}
}

func TestInventoryReviewBudgetReasons(t *testing.T) {
	for _, unified := range []bool{true, false} {
		for _, cap := range []float64{0, 32} {
			t.Run(fmt.Sprintf("unified=%t/cap=%g", unified, cap), func(t *testing.T) {
				w := Worker{Clock: RealClock{}, StateDir: newTestStateRoot(t), Machine: machine("budget", "fake")}
				d := inventoryReviewDevices()[0]
				d.BudgetGB, d.FreeMemoryGB, d.TotalMemoryGB = cap, 64, 96
				d.UnifiedMemory, d.MemoryMeasured = unified, true
				w.Machine.Devices = []MachineDevice{d}
				s, err := w.openStore()
				if err != nil {
					t.Fatal(err)
				}
				defer s.close()
				if err := w.initInventory(context.Background(), s); err != nil {
					t.Fatal(err)
				}
				got := w.Machine.Devices[0]
				if unified && cap == 0 {
					if got.BudgetGB != 64 || got.Reason != "" {
						t.Fatalf("measured unified capacity lost: %+v", got)
					}
				} else if cap == 0 && got.Reason != "device_budget_unavailable" {
					t.Fatalf("missing budget reason: %+v", got)
				}
				if cap > 0 && got.machine(w.Machine).FreeMemoryGB != 32 {
					t.Fatal("operator cap exceeded")
				}
			})
		}
	}
}

func TestInventoryReviewUnifiedLocalProbe(t *testing.T) {
	for _, platform := range []string{"metal", "cuda"} {
		for _, cap := range []float64{0, 12} {
			t.Run(fmt.Sprintf("%s/cap=%g", platform, cap), func(t *testing.T) {
				available := 20
				p := MachineProbe{OS: "darwin", Arch: "arm64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
					switch name {
					case "/usr/sbin/system_profiler":
						return []byte("Metal Support: Metal 3"), nil
					case "nvidia-smi":
						if slices.Contains(args, "--query-gpu=uuid,driver_version,pci.bus_id") {
							return []byte("GPU-unified, driver"), nil
						}
						return []byte("NVIDIA GB10, N/A"), nil
					case "vm_stat":
						return []byte(fmt.Sprintf("Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: %d.\nPages inactive: 0.\n", available*262144)), nil
					}
					return nil, exec.ErrNotFound
				}}
				reserve := 4.0
				if platform == "cuda" {
					p.Arch, reserve = "amd64", 16
				}
				w := Worker{Machine: Machine{GPU: platform, FreeMemoryGB: cap}}
				// GB10 reserves 16 GiB; use an allowance above that reserve.
				if platform == "cuda" && cap > 0 {
					w.Machine.FreeMemoryGB = 18
				}
				devices, err := w.localInventoryWithProbe(context.Background(), p)
				if err != nil || len(devices) != 1 {
					t.Fatalf("inventory: %v %v", devices, err)
				}
				want := float64(available) - reserve
				if cap > 0 {
					want = w.Machine.FreeMemoryGB - reserve
				}
				if d := devices[0]; d.Platform != platform || d.BudgetGB != want || d.FreeMemoryGB != want || d.Reason != "" {
					t.Fatalf("unified capacity: %+v, want %g", d, want)
				}
				available = 17
				if platform == "metal" {
					available = 10
				}
				devices, err = w.localInventoryWithProbe(context.Background(), p)
				if err != nil || devices[0].FreeMemoryGB != float64(available)-reserve {
					t.Fatalf("live memory did not apply below cap: %+v %v", devices, err)
				}
			})
		}
	}
}

func TestInventoryReviewPublicProbeReason(t *testing.T) {
	q, _ := fixture(t)
	m := machine("diagnostic", "fake", "diagnostic-host")
	m.Devices, m.MemoryError = []MachineDevice{}, "device_probe_unavailable"
	check(t, q, m, nil)
	s, err := q.publicSnapshot()
	if err != nil || s.Machines[0].MemoryError != "device_probe_unavailable" {
		t.Fatalf("public typed reason lost: %v %v", s.Machines, err)
	}
}

func TestInventoryReviewCPUHasNoProbeError(t *testing.T) {
	q, _ := fixture(t)
	m := machine("cpu-only", "cpu", "cpu-only-host")
	m.Device = "0" // Dev's diagnostic selector does not offer an accelerator.
	runInventoryReviewWorker(t, q, Worker{Machine: m, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return []MachineDevice{}, nil }})
	awaitInventoryReview(t, func() bool { s, _ := q.Snapshot(); return len(s.Machines) == 1 })
	s, _ := q.Snapshot()
	if s.Machines[0].MemoryError != "" {
		t.Fatal("CPU-only inventory reported an accelerator probe error")
	}
}

func TestInventoryReviewCUDAProbeFailureTyped(t *testing.T) {
	p := MachineProbe{OS: "linux", Arch: "amd64", Command: func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("driver failed") }}
	_, err := p.Accelerators(context.Background())
	var rule *rejectionError
	if !errors.As(err, &rule) || rule.code != "device_probe_unavailable" {
		t.Fatalf("failed CUDA inventory swallowed: %v", err)
	}
	p.Command = func(context.Context, string, ...string) ([]byte, error) { return nil, exec.ErrNotFound }
	if devices, err := p.Accelerators(context.Background()); err != nil || len(devices) != 0 {
		t.Fatalf("GPU-less host: %v %v", devices, err)
	}
}

func TestInventoryReviewSplitWorkerDiagnostic(t *testing.T) {
	q, _ := fixture(t)
	m := machine("one", "fake", "physical")
	m.Devices = inventoryReviewDevices()
	check(t, q, m, nil)
	m.ID = "two"
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}, nil)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 409 || httpErr.Code != "machine_worker_conflict" || !strings.Contains(httpErr.Message, "run one machine worker per host; see M4 install conversion") {
		t.Fatalf("split worker diagnostic: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w := Worker{Machine: m, Clock: RealClock{}, StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return inventoryReviewDevices(), nil }}
	err = w.Run(ctx)
	if !errors.As(err, &httpErr) || httpErr.Code != "machine_worker_conflict" {
		t.Fatalf("worker did not stop on split configuration: %v", err)
	}
}

func TestInventoryReviewSingleAttemptStateIsStrict(t *testing.T) {
	for _, attempts := range [][]string{nil, {"a", "b"}} {
		dir := t.TempDir()
		if err := saveWorkerJSON(dir, "state.json", workerState{Attempts: attempts}); err != nil {
			t.Fatal(err)
		}
		if _, err := readSingleAttemptState(filepath.Join(dir, "state.json")); err == nil {
			t.Fatalf("accepted %d attempts", len(attempts))
		}
	}
}
