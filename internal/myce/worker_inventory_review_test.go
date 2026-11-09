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

	"sync/atomic"
	"testing"
	"time"
)

func awaitInventoryReview(t *testing.T, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	// A real check-in can wait behind queue I/O: the fleet gate recorded a
	// 5.686s mutex wait while this five-second fixture deadline was running.
	// Use the shared worker fixture backstop; it does not bound product work.
	timeout := time.NewTimer(testWaitTimeout)
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
	if w.StateDir == "" {
		w.StateDir = newTestStateRoot(t)
	}
	w.Clock, w.Interval = RealClock{}, time.Millisecond
	w.EnvironmentLimit = 1 << 20
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
	for _, selection := range []string{"1"} {
		t.Run("selection="+selection, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("inventory-worker", "fake", "inventory-host")
			m.Device = selection
			stateDir := newTestStateRoot(t)
			client := runInventoryReviewWorker(t, q, Worker{StateDir: stateDir, Machine: m, InventoryProbe: func(context.Context) ([]MachineDevice, error) {
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
			spec := spec("experiment")
			spec.Resumable = false
			spec.Run.Command = []string{"sh", "-c", `printf '%s' "$MYCE_DEVICES" > "$1"`, "sh", "{work}/devices"}
			var j Job
			if err := client.Call(context.Background(), "POST", "/api/jobs", spec, &j); err != nil {
				t.Fatal(err)
			}
			awaitInventoryReview(t, func() bool { return job(t, q, j.ID).State == "succeeded" })
			marker := filepath.Join(stateDir, "job-files", j.ID, "1", "devices")
			data, err := os.ReadFile(marker)
			if err != nil || string(data) != wantIDs[0] || !slices.Equal(job(t, q, j.ID).AllocatedDevices, []string{wantIDs[0]}) {
				t.Fatalf("payload devices=%q placement=%v err=%v", data, job(t, q, j.ID).AllocatedDevices, err)
			}
		})
	}

	t.Run("InventoryRereviewUnmatchedSelectors", func(t *testing.T) {

		for _, selection := range []string{"GPU-nope", "0,GPU-nope"} {
			t.Run(selection, func(t *testing.T) {
				w := recoveryWorker(t)
				w.Machine.Device = selection
				w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return inventoryReviewDevices(), nil }
				s, err := w.openStore()
				if err != nil {
					t.Fatal(err)
				}
				defer s.close()
				err = w.initInventory(context.Background(), s)
				if selection == "7" || selection == "GPU-nope" {
					var rule *rejectionError
					if !errors.As(err, &rule) || rule.code != "device_selector_unmatched" {
						t.Fatalf("startup did not refuse unmatched allowance: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if w.inventoryReason != "device_selector_unmatched" || len(w.Machine.Devices) != 1 {
					t.Fatalf("partial allowance silently dropped: %+v reason=%s", w.Machine.Devices, w.inventoryReason)
				}
				q, _ := fixture(t)
				clock := &workerFakeClock{now: q.clock.Now()}
				q.clock = clock
				// Use a separate real worker with the partial allowance for public status.
				m := machine("selected", "fake", "selected-host")
				m.Device = selection
				client := runInventoryReviewWorker(t, q, Worker{Machine: m, InventoryProbe: w.InventoryProbe})
				awaitInventoryReview(t, func() bool {
					clock.Advance(time.Second) // Expire the observational status generation.
					// Read the actual public wire diagnostic, without coupling the test
					// to the machine struct added by this repair.
					var wire struct {
						Machines []struct {
							ID              string `json:"id"`
							InventoryReason string `json:"inventory_reason"`
							MemoryError     string `json:"memory_error"`
						} `json:"machines"`
					}
					if err := client.Call(context.Background(), "GET", "/api/status", nil, &wire); err != nil {
						return false
					}
					for _, m := range wire.Machines {
						if m.ID == "selected" {
							return m.InventoryReason == "device_selector_unmatched" && m.MemoryError == ""
						}
					}
					return false
				})
				gpu := submit(t, q, spec("experiment"))
				awaitInventoryReview(t, func() bool { return job(t, q, gpu.ID).State == "succeeded" })
			})
		}

	})
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
	t.Run("driver-and-name", func(t *testing.T) {
		old, changed := fakeMachineDevices(), fakeMachineDevices()
		changed[0].Fingerprint, changed[1].Fingerprint = "new driver/name", "new driver/name"
		w := Worker{Machine: Machine{Devices: old}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return changed, nil }}
		if _, err := w.forAttempt(Job{AllocatedDevices: []string{"1"}}); err != nil {
			t.Fatalf("driver change blocked launch: %v", err)
		}
	})
}

func TestInventoryReviewUnifiedLocalProbe(t *testing.T) {
	for _, platform := range []string{"cuda"} {
		for _, cap := range []float64{12} {
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
					case "/usr/bin/vm_stat":
						return []byte(fmt.Sprintf("Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: %d.\nPages inactive: 0.\n", available*262144)), nil
					}
					return nil, exec.ErrNotFound
				}}
				reserve := 0.0
				if platform == "cuda" {
					p.OS, p.Arch = "linux", "arm64"
					p = withCUDAFacts(p, true, 1)
					read := p.ReadFile
					p.ReadFile = func(path string) ([]byte, error) {
						if path == "/proc/meminfo" {
							return []byte(fmt.Sprintf("MemAvailable: %d kB\n", available*1048576)), nil
						}
						return read(path)
					}
				}
				w := Worker{Machine: Machine{GPU: platform, FreeMemoryGB: cap}}
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
				available = 10 // Measured free RAM below the explicit 12 GiB cap.
				devices, err = w.localInventoryWithProbe(context.Background(), p)
				if err != nil || devices[0].FreeMemoryGB != float64(available)-reserve {
					t.Fatalf("live memory did not apply below cap: %+v %v", devices, err)
				}
			})
		}
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
	if !errors.As(err, &httpErr) || httpErr.Status != 409 || httpErr.Code != "machine_worker_conflict" {
		t.Fatalf("split worker diagnostic: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w := Worker{EnvironmentLimit: 1 << 20, Machine: m, Clock: RealClock{}, StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return inventoryReviewDevices(), nil }}
	err = w.Run(ctx)
	if !errors.As(err, &httpErr) || httpErr.Code != "machine_worker_conflict" {
		t.Fatalf("worker did not stop on split configuration: %v", err)
	}
}
