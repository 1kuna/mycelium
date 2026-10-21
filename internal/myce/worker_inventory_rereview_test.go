package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Exercise the real runner and queue retry policy with exact probe ordering,
// independently of heartbeat probes. The queue and worker share fake time.
func TestInventoryRereviewLaunchProbeRetry(t *testing.T) {
	for _, resumable := range []bool{false, true} {
		for _, probe := range []int{2, 3} {
			for _, mismatch := range []bool{false, true} {
				t.Run(fmt.Sprintf("resumable=%t/probe=%d/mismatch=%t", resumable, probe, mismatch), func(t *testing.T) {
					q, clock := fixture(t)
					server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
					defer server.Close()
					w := recoveryWorker(t)
					w.Clock = clock
					w.Client = Client{URL: server.URL, Token: "test"}
					w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
					root, err := w.openStore()
					if err != nil {
						t.Fatal(err)
					}
					defer root.close()
					root.state.Session = w.Machine.Session
					spec := spec("production")
					spec.Resumable = resumable
					spec.Run.Command = []string{"sh", "-c", "exit 0"}
					submitted := submit(t, q, spec)
					devices := inventoryReviewDevices()[:1]
					for attempt := 1; attempt <= 2; attempt++ {
						reply := check(t, q, w.Machine, nil)
						if reply.Job == nil {
							t.Fatalf("no retry assignment: %+v", job(t, q, submitted.ID))
						}
						j := *reply.Job
						store, err := newAttemptStore(root, j)
						if err != nil {
							t.Fatal(err)
						}
						runner := workerWithPreparedCheckpoint(t, w, store, j)
						runner.Machine.Device = "0"
						runner.Machine.Devices = devices
						calls := 0
						runner.InventoryProbe = func(context.Context) ([]MachineDevice, error) {
							calls++
							if attempt == 1 && calls == probe {
								if !mismatch {
									return nil, errors.New("transient probe failure")
								}
								changed := append([]MachineDevice(nil), devices...)
								changed[0].Identity = "swapped-card"
								return changed, nil
							}
							return devices, nil
						}
						bound, err := runner.forAttempt(j)
						if err != nil {
							t.Fatal(err)
						}
						p, err := bound.start(store, j)
						var r Result
						if attempt == 1 {
							if p != nil || err == nil {
								t.Fatal("launch probe did not refuse execution")
							}
							r = startFailureOutcome(result(&j, false), err)
							data, e := os.ReadFile(filepath.Join(store.dir, fmt.Sprintf("launch-%s-%d.outcome.json", j.ID, j.Attempt)))
							var facts memberOutcome
							if e == nil {
								e = decodeWorkerJSON(data, &facts)
							}
							if e != nil || !facts.recordedInfrastructure() || len(facts.Infrastructure) != 1 || facts.Infrastructure[0].Kind != launchRefused {
								t.Errorf("missing launch-refused fact: %+v %v", facts, e)
							}
						} else {
							if err != nil {
								t.Fatal(err)
							}
							r = d5Finish(t, p)
						}
						if err = q.Complete(r); err != nil {
							t.Fatal(err)
						}
						got := job(t, q, submitted.ID)
						want := "queued"
						if attempt == 2 {
							want = "succeeded"
						}
						if got.State != want || got.InfrastructureRetries != 1 || got.Attempt != attempt {
							t.Fatalf("state=%s attempt=%d retries=%d reason=%s; want %s", got.State, got.Attempt, got.InfrastructureRetries, got.Reason, want)
						}
					}
				})
			}
		}
	}
}

func TestInventoryRereviewDockerProbeRefusal(t *testing.T) {
	w := recoveryWorker(t)
	s, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	d := inventoryReviewDevices()[0]
	w.selectedDevice = &d
	w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return nil, errors.New("offline") }
	j := recoveryJob(t, []string{"true"})
	_, err = w.startDocker(s, j, "", nil)
	if err == nil || !startFailureOutcome(result(&j, false), err).Interrupted {
		t.Fatalf("Docker probe error was not infrastructure: %v", err)
	}
}

func TestInventoryRereviewUnmatchedSelectors(t *testing.T) {
	for _, selection := range []string{"7", "GPU-nope", "0,7", "0,GPU-nope"} {
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
			// Use a separate real worker with the partial allowance for public status.
			m := machine("selected", "fake", "selected-host")
			m.Device = selection
			client := runInventoryReviewWorker(t, q, Worker{Machine: m, InventoryProbe: w.InventoryProbe})
			awaitInventoryReview(t, func() bool {
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
}

func TestInventoryRereviewAdditionWithUsedID(t *testing.T) {
	old := fakeMachineDevices()
	added := old[0]
	added.ID = "0"
	added.Identity = "GPU-added"
	added.Selector = "GPU-added"
	// Discovery inserted the new CUDA card first. Its ordinal collides with
	// a saved ID, but all observed IDs and all hardware identities are unique.
	current := []MachineDevice{added, old[0], old[1]}
	current[1].ID, current[2].ID = "1", "2"
	if err := validateInventory(current); err != nil {
		t.Fatal(err)
	}
	got := reconcileDevices(old, current)
	if len(got) != 3 || got[2].ID != "2" || got[2].Identity != added.Identity || got[0].Identity != old[0].Identity || got[1].Identity != old[1].Identity || got[2].Reason != "" {
		t.Fatalf("addition dropped or reassigned saved identity: %+v", got)
	}
}

func TestInventoryRereviewUnifiedCapBelowReserve(t *testing.T) {
	p := MachineProbe{OS: "linux", Arch: "arm64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "nvidia-smi" {
			return nil, exec.ErrNotFound
		}
		if len(args) > 0 && args[0] == "--query-gpu=uuid,driver_version,pci.bus_id" {
			return []byte("GPU-unified, driver"), nil
		}
		return []byte("NVIDIA GB10, N/A"), nil
	}}
	p = withCUDAFacts(p, true, 1)
	w := Worker{Machine: Machine{GPU: "cuda", FreeMemoryGB: 10}}
	devices, err := w.localInventoryWithProbe(context.Background(), p)
	if err != nil || len(devices) != 1 || devices[0].Reason != "device_budget_unavailable" || devices[0].FreeMemoryGB != 0 {
		t.Fatalf("budget error became probe failure: %+v %v", devices, err)
	}
}

func TestInventoryRereviewAllowanceSingleCardMasks(t *testing.T) {
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	h.jobs = []Job{
		harnessJob(t, "cuda-mask", "0", `test "$MYCE_DEVICES" = 0 && test "$CUDA_VISIBLE_DEVICES" = GPU-fake-a && test "$ZE_AFFINITY_MASK" = 2147483647 && test "$ONEAPI_DEVICE_SELECTOR" = '!*:*'`),
		harnessJob(t, "xpu-mask", "1", `test "$MYCE_DEVICES" = 1 && test -z "$CUDA_VISIBLE_DEVICES" && test "$ZE_AFFINITY_MASK" = 3 && test "$ONEAPI_DEVICE_SELECTOR" = level_zero:0`),
	}
	w := harnessWorker(t, h)
	w.Machine.Device = "0,1"
	startHarness(t, w, h)
	await(t, func() bool { return h.completed("cuda-mask") && h.completed("xpu-mask") })
	for _, id := range []string{"cuda-mask", "xpu-mask"} {
		if r := h.result(id); !r.Success {
			t.Fatalf("allowance widened %s mask: %+v", id, r)
		}
	}
}

func TestQCNativeGroupLaunchProbeRetry(t *testing.T) {
	for _, resumable := range []bool{false, true} {
		for _, probe := range []int{3, 4} {
			for _, mismatch := range []bool{false, true} {
				t.Run(fmt.Sprintf("resumable=%t/probe=%d/mismatch=%t", resumable, probe, mismatch), func(t *testing.T) {
					q, clock := fixture(t)
					server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
					defer server.Close()
					w := recoveryWorker(t)
					w.Clock = clock
					w.Machine.HostMemoryMeasured = true
					w.Machine.FreeHostMemoryGB = 64
					w.Machine.HostAvailableMemoryGB = 64
					w.Client = Client{URL: server.URL, Token: "test"}
					w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
					root, err := w.openStore()
					if err != nil {
						t.Fatal(err)
					}
					defer root.close()
					root.state.Session = w.Machine.Session
					spec := spec("production")
					spec.Resumable = resumable
					spec.Run.Command = []string{"sh", "-c", "exit 0"}
					spec.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sh", "-c", "sleep 60"}, MemoryGB: 1}}
					spec.Needs.HostMemoryGB = 2
					submitted := submit(t, q, spec)
					devices := inventoryReviewDevices()[:1]
					for attempt := 1; attempt <= 2; attempt++ {
						reply := check(t, q, w.Machine, nil)
						if reply.Job == nil {
							t.Fatalf("no retry assignment: %+v", job(t, q, submitted.ID))
						}
						j := *reply.Job
						store, err := newAttemptStore(root, j)
						if err != nil {
							t.Fatal(err)
						}
						runner := workerWithPreparedCheckpoint(t, w, store, j)
						runner.Machine.Device = "0"
						runner.Machine.Devices = devices
						calls := 0
						runner.InventoryProbe = func(context.Context) ([]MachineDevice, error) {
							calls++
							if attempt == 1 && calls == probe {
								if !mismatch {
									return nil, errors.New("transient probe failure")
								}
								changed := append([]MachineDevice(nil), devices...)
								changed[0].Identity = "swapped-card"
								return changed, nil
							}
							return devices, nil
						}
						bound, err := runner.forAttempt(j)
						if err != nil {
							t.Fatal(err)
						}
						p, err := bound.start(store, j)
						var r Result
						if attempt == 1 {
							if p != nil {
								r = d5Finish(t, p)
								if r.Success || !r.Interrupted {
									t.Fatalf("group refusal not infrastructure: %+v", r)
								}
							} else if err == nil {
								t.Fatal("launch probe did not refuse execution")
							}
							if p == nil {
								r = startFailureOutcome(result(&j, false), err)
							}
						} else {
							if err != nil {
								t.Fatal(err)
							}
							r = d5Finish(t, p)
						}
						if err = q.Complete(r); err != nil {
							t.Fatal(err)
						}
						got := job(t, q, submitted.ID)
						want := "queued"
						if attempt == 2 {
							want = "succeeded"
						}
						if got.State != want || got.InfrastructureRetries != 1 || got.Attempt != attempt {
							t.Fatalf("state=%s attempt=%d retries=%d reason=%s; want %s", got.State, got.Attempt, got.InfrastructureRetries, got.Reason, want)
						}
					}
				})
			}
		}
	}
}
