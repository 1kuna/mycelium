package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestDeviceFaultAutoClear(t *testing.T) {
	for _, scenario := range []string{"healthy", "new fault", "monitor gap", "escalated", "unknown core", "unknown memory", "permission denied", "probe exit"} {
		t.Run(scenario, func(t *testing.T) {
			q, clock := fixture(t)
			var journal bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&journal)
			t.Cleanup(func() { log.SetOutput(previous) })
			m := m3Machine()
			m.Devices[0].Platform = "xpu"
			body := xpuHealthyHealth
			var probeErr error
			p := MachineProbe{Command: func(context.Context, string, ...string) ([]byte, error) { return []byte(body), probeErr }}
			m.Devices[0].pciAddress = "0000:03:00.0"
			w := Worker{Machine: m, Clock: clock, Interval: 2 * time.Second,
				DeviceFaultMatchers: map[string]DeviceFaultMatcher{"xpu": intelKernelFault},
				DeviceHealthProbe:   p.DeviceHealth}
			root := &workerStore{dir: t.TempDir()}
			root.deviceHealth.timeout, root.deviceFaults.timeout = -1, -1
			var lines []KernelLogLine
			var logErr error
			w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
				return KernelLogSnapshot{BootID: "boot", Lines: lines}, logErr
			}
			if err := w.initDeviceFaults(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			scan := func() {
				t.Helper()
				if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
					t.Fatal(err)
				}
			}
			lines = []KernelLogLine{{ID: "1", At: clock.Now().Add(-2 * time.Second), Message: "xe 0000:03:00.0: GPU HANG"}}
			scan()
			key := deviceFaultKey(m.Devices[0])
			if scenario == "escalated" {
				f := w.faults.Devices[key]
				f.RepeatedTeardown = true
				w.faults.Devices[key] = f
			}
			wantRejection := ""
			switch scenario {
			case "unknown core":
				body = strings.Replace(body, `"core_temperature":{"status":"OK"}`, `"core_temperature":{"status":"Unknown"}`, 1)
				wantRejection = "health_component_unknown: core_temperature"
			case "unknown memory":
				body = strings.Replace(body, `"memory_health":{"status":"OK"}`, `"memory_health":{"status":"Unknown"}`, 1)
				wantRejection = "health_component_unknown: memory"
			case "permission denied":
				probeErr = &os.PathError{Op: "open", Path: "/private/vendor-sensitive", Err: os.ErrPermission}
				wantRejection = "health_probe_permission_denied"
			case "probe exit":
				cmd := exec.Command("sh", "-c", "exit 7")
				probeErr = cmd.Run()
				wantRejection = "health_probe_exit: exit_status=7"
			}
			clock.Advance(8 * time.Second)
			lines = nil
			scan() // First normal probe: ten seconds after the actual fault.
			clock.Advance(4 * time.Second)
			if scenario == "new fault" {
				lines = []KernelLogLine{{ID: "2", At: clock.Now(), Message: "xe 0000:03:00.0: Engine reset: engine_class=ccs"}}
			}
			if scenario == "monitor gap" {
				logErr = errors.New("unreadable")
			}
			scan()
			if w.Machine.Devices[0].Reason == "" {
				t.Fatal("cleared before measured recovery window")
			}
			clock.Advance(6 * time.Second)
			logErr = nil
			scan()
			if scenario != "healthy" {
				if w.Machine.Devices[0].Reason == "" {
					t.Fatal("cleared without uninterrupted healthy proof")
				}
				if wantRejection != "" {
					failure := w.Machine.Devices[0].RecoveryBlocked
					if failure == nil || failure.Error() != wantRejection || failure.ObservedAt.IsZero() {
						t.Fatal("missing typed rejection", failure)
					}
					if _, err := q.Checkin(Checkin{Machine: w.Machine}); err != nil {
						t.Fatal(err)
					}
					snapshot, err := q.statusSnapshot()
					if err != nil {
						t.Fatal(err)
					}
					status, err := json.Marshal(snapshot)
					if err != nil || !bytes.Contains(status, []byte(failure.Code)) || strings.Contains(string(status)+journal.String(), "vendor-sensitive") {
						t.Fatal("status missing rejection or leaked raw error", string(status), err)
					}
					if strings.Count(journal.String(), "device recovery proof rejected:") != 1 || !strings.Contains(journal.String(), wantRejection) {
						t.Fatal("missing or repeated rejection log", journal.String())
					}
					// A new healthy sample retires the rejection but still must
					// establish the original measured recovery interval.
					body, probeErr = xpuHealthyHealth, nil
					clock.Advance(2 * time.Second)
					scan()
					if w.Machine.Devices[0].RecoveryBlocked != nil || w.Machine.Devices[0].Reason == "" {
						t.Fatal("healthy reading did not reset rejection/streak")
					}
				}
				return
			}
			if w.Machine.Devices[0].Reason != "" || w.Machine.Devices[0].FreeMemoryGB != 0 {
				t.Fatal("clear must wait for inventory before restoring capacity", w.Machine.Devices[0])
			}
			recovery := w.Machine.Devices[0].Recovery
			if recovery == nil || recovery.FaultEvent == "" || recovery.ObservationSeconds != 10 || recovery.ClearedAt.Sub(recovery.HealthySince) < 10*time.Second {
				t.Fatal("missing measured clear evidence", recovery)
			}
			data, err := os.ReadFile(filepath.Join(root.dir, deviceFaultFile))
			var saved deviceQuarantine
			if err != nil || decodeWorkerJSON(data, &saved) != nil || saved.Cleared[key] != *recovery {
				t.Fatal("clear evidence not durable", err)
			}
			w.Machine.Devices = m.Devices
			if err := w.initDeviceFaults(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			if w.Machine.Devices[0].Reason != "" || w.Machine.Devices[0].Recovery == nil {
				t.Fatal("recovery lost on restart")
			}
			// A new fault retires clear authority while retaining its measured
			// window. An operator clear cannot resurrect that old proof.
			lines = []KernelLogLine{{ID: "2", At: clock.Now().Add(-2 * time.Second), Message: "xe 0000:03:00.0: GPU HANG"}}
			scan()
			if w.Machine.Devices[0].Recovery != nil {
				t.Fatal("new fault retained old recovery authority")
			}
			if err := saveWorkerJSON(root.dir, "devices.json", w.Machine.Devices); err != nil {
				t.Fatal(err)
			}
			if err := RequestDeviceQuarantineClear(root.dir, "0"); err != nil {
				t.Fatal(err)
			}
			scan()
			if w.Machine.Devices[0].Recovery != nil || w.faults.RecoveryWindows[key] != recovery.ObservationSeconds {
				t.Fatal("operator clear revived old proof or erased historical calibration")
			}
		})
	}
}

func TestDeviceFaultSlowObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := Worker{Machine: m3Machine(), Clock: RealClock{}, Interval: time.Second,
			DeviceFaultSource: func(context.Context, time.Time) (KernelLogSnapshot, error) {
				<-time.After(50 * time.Millisecond)
				return KernelLogSnapshot{BootID: "boot"}, nil
			},
			DeviceHealthProbe: func(context.Context, MachineDevice) (bool, error) {
				<-time.After(75 * time.Millisecond)
				return true, nil
			}}
		root := &workerStore{dir: t.TempDir()}
		if err := w.initDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		key := deviceFaultKey(w.Machine.Devices[0])
		w.faults.Devices[key] = deviceFault{Reason: "device_fault_xe", Event: strings.Repeat("a", 64), At: w.Clock.Now().Add(-2 * time.Second)}
		w.applyDeviceQuarantine()
		// Both probes exceed the collection budget. The kernel read starts
		// before health completes; that health result must survive to a later scan.
		for range 10 {
			if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			<-time.After(time.Second)
			synctest.Wait()
		}
		if w.Machine.Devices[0].Reason != "" || w.Machine.Devices[0].Recovery == nil {
			t.Fatal("slow completed observations never established recovery", w.Machine.Devices[0])
		}
	})
}

// Native xpu-smi schema. Diagnostic warnings are outside stdout in the real
// runner, so command success alone never replaces these required readings.
const xpuHealthyHealth = `{"core_temperature":{"status":"OK"},"power_health":{"status":"OK"},"memory_health":{"status":"OK"},"memory_temperature":{"status":"Unknown"},"frequency_health":{"status":"OK"},"unsupported_item_health":{"status":"Unknown"},"device_id":0}`

func TestDeviceHealthVendorEvidence(t *testing.T) {
	for _, tc := range []struct {
		platform, body, failure string
	}{
		{"xpu", xpuHealthyHealth, ""},
		{"xpu", `{"core_temperature":{"status":"OK"},"power_health":{"status":"OK"},"memory_health":{"status":"Unknown"}}`, "health_component_unknown: memory"},
		{"xpu", `{"core_temperature":{"status":"Unknown"},"power_health":{"status":"OK"},"memory_health":{"status":"Unknown"}}`, "health_component_unknown: core_temperature"},
		{"xpu", `{"core_temperature":{"status":"OK"},"power_health":{"status":"OK"},"memory_health":{"status":"Warning"}}`, "health_component_failed: memory"},
		{"xpu", `{"core_temperature":{"status":"OK"},"power_health":{"status":"Unknown"},"memory_health":{"status":"OK"}}`, "health_component_unknown: power"},
		{"xpu", strings.Replace(xpuHealthyHealth, `"frequency_health":{"status":"OK"}`, `"frequency_health":{"status":"Warning"}`, 1), "health_component_failed: frequency"},
		{"xpu", `{}`, "health_component_missing: core_temperature"},
		{"xpu", `{"core_temperature":{"status":"OK"},"memory_health":{"status":"OK"}}`, "health_component_missing: power"},
		{"xpu", `{"error":"private diagnostic"}`, "health_probe_error"},
		{"xpu", `not json`, "health_probe_invalid_output"},
		{"cuda", `<nvidia_smi_log><gpu><uuid>GPU-test</uuid><gpu_recovery_action>None</gpu_recovery_action></gpu></nvidia_smi_log>`, ""},
		{"cuda", `<nvidia_smi_log><gpu><uuid>GPU-test</uuid><gpu_recovery_action>Reset</gpu_recovery_action></gpu></nvidia_smi_log>`, "health_probe_unhealthy"},
		{"cuda", `<nvidia_smi_log><gpu><uuid>GPU-other</uuid><gpu_recovery_action>None</gpu_recovery_action></gpu></nvidia_smi_log>`, "health_probe_identity_mismatch"},
	} {
		t.Run(fmt.Sprint(tc.platform, "/", tc.body), func(t *testing.T) {
			p := MachineProbe{Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
				want := "nvidia-smi -q -x -i GPU-test"
				if tc.platform == "xpu" {
					want = "xpu-smi health -d 0000:03:00.0 -j"
				}
				if name+" "+strings.Join(args, " ") != want {
					t.Fatal("probe lost physical identity", name, args)
				}
				return []byte(tc.body), nil
			}}
			normal, err := p.DeviceHealth(context.Background(), MachineDevice{Platform: tc.platform, Identity: "GPU-test", pciAddress: "0000:03:00.0"})
			failure := deviceHealthFailure(normal, err)
			if tc.failure == "" {
				if !normal || err != nil || failure != nil {
					t.Fatal(normal, err, failure)
				}
			} else if normal || failure == nil || failure.Error() != tc.failure {
				t.Fatal(normal, err, failure)
			}
		})
	}
}

func TestQuarantineHealthMeasuredInvocationCount(t *testing.T) {
	_, clock := fixture(t)
	m := m3Machine()
	m.Devices[0].Platform = "xpu"
	calls := 0
	w := Worker{Clock: clock, Machine: m, faults: &deviceQuarantine{Devices: map[string]deviceFault{}, RecoveryWindows: map[string]float64{}},
		DeviceHealthProbe: func(context.Context, MachineDevice) (bool, error) { calls++; return false, nil }}
	key := deviceFaultKey(m.Devices[0])
	w.faults.Devices[key] = deviceFault{Reason: "device_fault_xe", Event: "fault", At: clock.Now().Add(-time.Minute)}
	root := &workerStore{dir: t.TempDir()}
	for range 600 {
		w.quarantineHealth(t.Context(), root)
		clear(w.faultHealth) // The kernel scan consumes each failed proof.
		clock.Advance(100 * time.Millisecond)
	}
	if calls != 1 {
		t.Fatal("recovery polled inside the measured failure window", calls)
	}
	w.quarantineHealth(t.Context(), root)
	clear(w.faultHealth)
	if calls != 2 {
		t.Fatal("due recovery probe did not run", calls)
	}
	// A new fault is a new decision, independent of the old event's backoff.
	clear(w.faultHealth)
	w.faults.Devices[key] = deviceFault{Reason: "device_fault_xe", Event: "new-fault", At: clock.Now()}
	w.quarantineHealth(t.Context(), root)
	if calls != 3 {
		t.Fatal("new fault inherited old probe delay", calls)
	}
}

func TestQuarantineHealthStalledProbeKeepsKernelMonitoring(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, scans atomic.Int32
		release := make(chan struct{})
		w := Worker{Clock: RealClock{}, Interval: time.Second, Machine: m3Machine(),
			DeviceFaultSource: func(context.Context, time.Time) (KernelLogSnapshot, error) {
				scans.Add(1)
				return KernelLogSnapshot{BootID: "boot"}, nil
			},
			DeviceHealthProbe: func(context.Context, MachineDevice) (bool, error) {
				calls.Add(1)
				<-release // Includes a vendor process start that cannot be cancelled.
				return true, nil
			}}
		root := &workerStore{dir: t.TempDir()}
		if err := w.initDeviceFaults(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		key := deviceFaultKey(w.Machine.Devices[0])
		w.faults.Devices[key] = deviceFault{Reason: "device_fault_xe", Event: "fault", At: w.Clock.Now().Add(-time.Second)}
		scan := func() {
			t.Helper()
			if err := w.refreshDeviceFaults(t.Context(), root); err != nil {
				t.Fatal(err)
			}
		}
		before := scans.Load()
		for range 5 {
			scan()
			<-time.After(time.Second)
			synctest.Wait()
		}
		if calls.Load() != 1 || scans.Load() < before+5 || w.Machine.Devices[0].Reason == "" {
			t.Fatal("stalled health must retain quarantine and allow kernel scans", calls.Load(), scans.Load(), w.Machine.Devices[0])
		}
		close(release)
		for range 20 {
			scan()
			<-time.After(time.Second)
			synctest.Wait()
		}
		if w.Machine.Devices[0].Reason != "" || w.Machine.Devices[0].Recovery == nil {
			t.Fatal("completed health did not resume measured recovery", w.Machine.Devices[0])
		}
	})
}
