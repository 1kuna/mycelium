package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	w.vendorGPUActive = true
	for range 2400 {
		w.quarantineHealth(t.Context(), root)
		clear(w.faultHealth)
		clock.Advance(100 * time.Millisecond)
	}
	if calls != 2 {
		t.Fatal("busy sibling caused vendor initialization", calls)
	}
	w.vendorGPUActive = false
	w.quarantineHealth(t.Context(), root)
	if calls != 3 {
		t.Fatal("quiescent recovery did not resume", calls)
	}
	// A new fault is a new decision, independent of the old event's backoff.
	clear(w.faultHealth)
	w.faults.Devices[key] = deviceFault{Reason: "device_fault_xe", Event: "new-fault", At: clock.Now()}
	w.quarantineHealth(t.Context(), root)
	if calls != 4 {
		t.Fatal("new fault inherited old probe delay", calls)
	}
}

func TestQuarantinePendingHealthDoesNotProbeBusySibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, scans atomic.Int32
		release := make(chan struct{})
		w := Worker{Clock: RealClock{}, Interval: time.Second,
			Machine: Machine{Devices: []MachineDevice{
				{ID: "0", Platform: "xpu", Identity: "card-0"},
				{ID: "1", Platform: "xpu", Identity: "card-1"},
				{ID: "2", Platform: "xpu", Identity: "card-2"},
			}},
			DeviceFaultSource: func(context.Context, time.Time) (KernelLogSnapshot, error) {
				scans.Add(1)
				return KernelLogSnapshot{BootID: "boot"}, nil
			},
			DeviceHealthProbe: func(context.Context, MachineDevice) (bool, error) {
				if calls.Add(1) == 1 {
					<-release // Outlast the asynchronous collection budget.
				}
				return true, nil
			}}
		root := &workerStore{dir: t.TempDir()}
		if err := w.initDeviceFaults(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		for _, d := range w.Machine.Devices[:2] {
			w.faults.Devices[deviceFaultKey(d)] = deviceFault{Reason: "device_fault_xe", Event: d.Identity, At: w.Clock.Now().Add(-time.Second)}
		}
		scan := func() {
			t.Helper()
			if err := w.refreshDeviceFaults(t.Context(), root); err != nil {
				t.Fatal(err)
			}
		}
		scan()
		if calls.Load() != 1 {
			t.Fatal("first health query did not remain pending", calls.Load())
		}
		// A healthy sibling takes accelerator/model custody while the first
		// query is pending. A later control observation must fence that batch.
		w.vendorGPUActive = true
		before := scans.Load()
		scan()
		if scans.Load() <= before {
			t.Fatal("pending health blocked kernel monitoring")
		}
		close(release)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatal("pending batch initialized a busy sibling", calls.Load())
		}
		w.vendorGPUActive = false
		for range 30 {
			scan()
			<-time.After(time.Second)
			synctest.Wait()
		}
		for _, d := range w.Machine.Devices[:2] {
			if d.Reason != "" || d.Recovery == nil {
				t.Fatal("quiescent recovery did not resume", d)
			}
		}
	})
}

func TestQuarantineCustodyAtVendorLaunch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	marker := filepath.Join(dir, "starts")
	// The production runner permits fake service-manager executables in tests.
	// This absolute script cannot resolve or execute an installed vendor tool.
	script := filepath.Join(dir, "systemctl")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf 'started\\n' >> \"$1\"\nprintf '%s\\n' '{\"core_temperature\":{\"status\":\"OK\"},\"power\":{\"status\":\"OK\"},\"memory\":{\"status\":\"OK\"}}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	releaseFactory := sync.OnceFunc(func() { close(resume) })
	defer releaseFactory()
	var factories atomic.Int32
	previous := workerMachineProbe
	workerMachineProbe = func() MachineProbe {
		if factories.Add(1) == 1 {
			close(entered)
			<-resume // Custody arrives after selection, before process creation.
		}
		p := LocalMachineProbe()
		run := p.Command
		p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			if name != "xpu-smi" || strings.Join(args, " ") != "health -d 0000:03:00.0 -j" {
				return nil, fmt.Errorf("unexpected health command %q", name)
			}
			return run(ctx, script, marker)
		}
		return p
	}
	defer func() { workerMachineProbe = previous }()
	w := Worker{Clock: RealClock{}, Interval: time.Second,
		Machine: Machine{Devices: []MachineDevice{{ID: "0", Identity: "card", Platform: "xpu", pciAddress: "0000:03:00.0"}}},
		DeviceFaultSource: func(context.Context, time.Time) (KernelLogSnapshot, error) {
			return KernelLogSnapshot{BootID: "boot"}, nil
		}}
	root := &workerStore{dir: t.TempDir()}
	if err := w.initDeviceFaults(ctx, root); err != nil {
		t.Fatal(err)
	}
	key := deviceFaultKey(w.Machine.Devices[0])
	w.faults.Devices[key] = deviceFault{Reason: "device_fault_xe", Event: "fault", At: w.Clock.Now().Add(-time.Second)}
	w.quarantineHealth(ctx, root)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("health probe did not reach production factory", ctx.Err())
	}
	w.vendorGPUActive = true
	w.quarantineHealth(ctx, root)
	releaseFactory()
	waitHealth := func() {
		t.Helper()
		root.deviceHealth.mu.Lock()
		pending := root.deviceHealth.pending
		root.deviceHealth.mu.Unlock()
		if pending != nil {
			select {
			case <-pending.done:
			case <-ctx.Done():
				t.Fatal("health query did not finish", ctx.Err())
			}
		}
	}
	waitHealth()
	if data, err := os.ReadFile(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("vendor process started after custody acquisition: %q, %v", data, err)
	}
	if health := w.quarantineHealth(ctx, root); len(health) != 0 || len(w.faultHealthDue) != 0 {
		t.Fatal("deferred process became a health observation or retry delay", health, w.faultHealthDue)
	}
	w.vendorGPUActive = false
	w.quarantineHealth(ctx, root)
	waitHealth()
	health := w.quarantineHealth(ctx, root)
	if !health[key].normal {
		t.Fatal("idle vendor health did not resume", health)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "started\n" {
		t.Fatalf("idle probe invocation count: %q, %v", data, err)
	}
}

func TestVendorHealthCustodyOrdersOnlyProcessStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gate vendorProbeStart
		ctx := withVendorProbeStart(t.Context(), &gate)
		starting, finishStart := make(chan struct{}), make(chan struct{})
		finishQuery, done, acquired := make(chan struct{}), make(chan error, 1), make(chan struct{})
		go func() {
			err := startProbeCommand(ctx, func() error {
				close(starting)
				<-finishStart // Simulated fork/exec, not the command's completion.
				return nil
			})
			<-finishQuery
			done <- err
		}()
		<-starting
		go func() { gate.setBusy(true); close(acquired) }()
		synctest.Wait()
		select {
		case <-acquired:
			t.Fatal("custody overtook process creation")
		default:
		}
		close(finishStart)
		synctest.Wait()
		select {
		case <-acquired:
		default:
			t.Fatal("custody waited for query completion")
		}
		select {
		case <-done:
			t.Fatal("query did not remain pending")
		default:
		}
		calls := 0
		if err := startProbeCommand(ctx, func() error { calls++; return nil }); !errors.Is(err, errVendorProbeDeferred) || calls != 0 {
			t.Fatal("new vendor process overtook custody", calls, err)
		}
		close(finishQuery)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		gate.setBusy(false)
		if err := startProbeCommand(ctx, func() error { calls++; return nil }); err != nil || calls != 1 {
			t.Fatal("idle process start did not resume", calls, err)
		}
	})
}
