package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeviceFaultAutoClear(t *testing.T) {
	for _, scenario := range []string{"healthy", "new fault", "monitor gap", "escalated"} {
		t.Run(scenario, func(t *testing.T) {
			_, clock := fixture(t)
			m := m3Machine()
			m.Devices[0].pciAddress = "0000:03:00.0"
			w := Worker{Machine: m, Clock: clock, Interval: 2 * time.Second,
				DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault},
				DeviceHealthProbe:   func(context.Context, MachineDevice) (bool, error) { return true, nil }}
			root := &workerStore{dir: t.TempDir()}
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
		})
	}
}

func TestDeviceHealthVendorEvidence(t *testing.T) {
	for _, tc := range []struct {
		platform, body string
		normal         bool
	}{
		{"xpu", `{"core_temperature":{"status":"OK"},"power":{"status":"OK"},"memory":{"status":"OK"}}`, true},
		{"xpu", `{"core_temperature":{"status":"OK"},"power":{"status":"OK"},"memory":{"status":"Warning"}}`, false},
		{"xpu", `{}`, false},
		{"cuda", `<nvidia_smi_log><gpu><uuid>GPU-test</uuid><gpu_recovery_action>None</gpu_recovery_action></gpu></nvidia_smi_log>`, true},
		{"cuda", `<nvidia_smi_log><gpu><uuid>GPU-test</uuid><gpu_recovery_action>Reset</gpu_recovery_action></gpu></nvidia_smi_log>`, false},
		{"cuda", `<nvidia_smi_log><gpu><uuid>GPU-other</uuid><gpu_recovery_action>None</gpu_recovery_action></gpu></nvidia_smi_log>`, false},
	} {
		t.Run(fmt.Sprint(tc.platform, "/", tc.normal, "/", tc.body), func(t *testing.T) {
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
			if err != nil || normal != tc.normal {
				t.Fatal(normal, err)
			}
		})
	}
}
