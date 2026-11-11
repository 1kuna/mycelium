package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// #149: a kernel fault must fence only its card before another job can hang there.
func TestDeviceFaultQuarantine(t *testing.T) {
	var journal testLogBuffer
	previous := log.Writer()
	log.SetOutput(&journal)
	t.Cleanup(func() { log.SetOutput(previous) })
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices[0].pciAddress, m.Devices[1].pciAddress = "0000:03:00.0", "0000:07:00.0"
	w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault}}
	root := &workerStore{dir: t.TempDir()}
	var lines []KernelLogLine
	boot := "boot-a"
	w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: boot, Lines: lines}, nil
	}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	first := submit(t, q, spec("production"))
	var clientStates []string
	observe := func(want string) *Job {
		t.Helper()
		j, err := client.GetJob(context.Background(), first.ID)
		if err != nil {
			t.Fatal(err)
		}
		clientStates = append(clientStates, j.State)
		if j.State != want || terminal(j.State) || j.State == "stopping" {
			t.Fatalf("job-v1 states=%v, want %s", clientStates, want)
		}
		return j
	}
	a := assignedJob(machineCheck(t, q, w.Machine))
	if a == nil || a.ID != first.ID || !slices.Equal(a.AllocatedDevices, []string{"0"}) {
		t.Fatal("device 0 assignment", a)
	}
	second := submit(t, q, spec("production"))
	b := assignedJob(machineCheck(t, q, w.Machine, a))
	if b == nil || b.ID != second.ID || !slices.Equal(b.AllocatedDevices, []string{"1"}) {
		t.Fatal("device 1 assignment", b)
	}
	machineCheck(t, q, w.Machine, a, b)
	observe("running")
	clock.Advance(5 * time.Second)
	lines = []KernelLogLine{{ID: "123", Message: "xe 0000:03:00.0: [drm] Engine memory CAT error. Fault response: Unsuccessful -EINVAL"}}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	reply := machineCheck(t, q, w.Machine, a, b)
	if w.Machine.Devices[0].Reason != "device_fault_xe" || w.Machine.Devices[0].FreeMemoryGB != 0 || w.Machine.Devices[1].Reason != "" {
		t.Fatal("device quarantine", w.Machine.Devices)
	}
	detail := "device=0 pci=0000:03:00.0 reason=device_fault_xe fault_event=" + w.faults.Devices[deviceFaultKey(m.Devices[0])].Event
	if strings.Count(journal.String(), "device quarantined: "+detail) != 1 {
		t.Fatal("missing quarantine journal line", journal.String())
	}
	got := observe("queued")
	if got.State != "queued" || got.Losses != 1 || got.InfrastructureRetries != 1 {
		t.Fatalf("fault must use infra policy: state=%s losses=%d retries=%d", got.State, got.Losses, got.InfrastructureRetries)
	}
	neighbor := job(t, q, second.ID)
	if neighbor.State != "running" || neighbor.Losses != 0 || neighbor.Attempt != b.Attempt {
		t.Fatal("neighbor changed", neighbor)
	}
	stopped := false
	for _, d := range reply.Attempts {
		if d.ID == a.ID {
			stopped = d.Stop && d.Kill
		}
		if d.ID == b.ID && d.Stop {
			t.Fatal("neighbor stopped")
		}
	}
	if !stopped {
		t.Fatal("faulted attempt not stopped")
	}
	// A stale successful result discharges custody, never resurrects the faulted attempt.
	if err := q.Complete(Result{Machine: a.Machine, Session: a.Session, Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Success: true}); !errors.Is(err, ErrStale) {
		t.Fatal(err)
	}
	observe("queued")
	// #1142: freeing the last healthy card must not replay the fault there,
	// including when this old worker reports no new recovery evidence.
	if err := q.Complete(Result{Machine: b.Machine, Session: b.Session, Execution: Execution{ID: b.ID, Attempt: b.Attempt}, Success: true}); err != nil {
		t.Fatal(err)
	}
	if resumed := assignedJob(machineCheck(t, q, w.Machine)); resumed != nil {
		t.Fatal("fault cascaded onto last healthy device", resumed)
	}
	held := observe("queued")
	if held.Hold == nil || held.Hold.Kind != deviceFaultRetryHold || len(held.AttemptHistory) != 1 || held.AttemptHistory[0].DeviceFault == nil {
		t.Fatal("typed fault/hold evidence missing", held.Hold, held.AttemptHistory)
	}
	t.Logf("job-v1 states: %v", clientStates)
	// Quarantine survives refresh/restart; an explicit clear doesn't replay the old fault.
	w.Machine.Devices = slices.Clone(m.Devices)
	w.faults = nil
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.Machine.Devices[0].Reason != "device_fault_xe" {
		t.Fatal("quarantine lost on restart")
	}
	if err := saveWorkerJSON(root.dir, "devices.json", w.Machine.Devices); err != nil {
		t.Fatal(err)
	}
	if err := RequestDeviceQuarantineClear(root.dir, "0"); err != nil {
		t.Fatal(err)
	}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	w.Machine.Devices = slices.Clone(m.Devices)
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.Machine.Devices[0].Reason != "" {
		t.Fatal("cleared fault replayed")
	}
	// Two healthy cards now remain; retry on this host is allowed again.
	resumed := assignedJob(machineCheck(t, q, w.Machine))
	if resumed == nil || resumed.ID != first.ID || resumed.Attempt != a.Attempt+1 {
		t.Fatal("retry with spare healthy capacity", resumed)
	}
	if strings.Count(journal.String(), "device quarantined: "+detail) != 1 || strings.Count(journal.String(), "device quarantine cleared: "+detail) != 1 {
		t.Fatal("quarantine transitions must be logged once", journal.String())
	}
	lines = []KernelLogLine{{ID: "124", Message: "xe 0000:03:00.0: GPU HANG"}}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	boot, lines = "boot-b", nil
	w.Machine.Devices = slices.Clone(m.Devices)
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.Machine.Devices[0].Reason != "" {
		t.Fatal("fresh boot retained quarantine")
	}
}

// #595/#300: the 2026-11-02 sequence. A fault 273 ms after TERM and 1 ms before
// exit on the stopped card is teardown; a fault on an idle card still quarantines.
func TestDeviceFaultTeardownClassification(t *testing.T) {
	var journal testLogBuffer
	previous := log.Writer()
	log.SetOutput(&journal)
	t.Cleanup(func() { log.SetOutput(previous) })
	clock := &fakeClock{now: time.Date(2026, 11, 2, 4, 0, 56, 942_000_000, time.UTC)}
	m := m3Machine()
	m.Devices[0].pciAddress, m.Devices[1].pciAddress = "0000:03:00.0", "0000:07:00.0"
	w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault}}
	w.control = &processControl{clock: clock}
	root := &workerStore{dir: t.TempDir()}
	var lines []KernelLogLine
	w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: "boot", Lines: lines}, nil
	}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	scan := func(next ...KernelLogLine) {
		t.Helper()
		lines = next
		if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
	}
	term := clock.Now()
	victim := &process{job: Job{ID: "victim", Attempt: 1, Session: "s", AllocatedDevices: []string{"0"}}}
	w.control.set(victim)
	victim.stopRequested = term
	victim.outcome = memberOutcome{Signals: []deliveredSignal{{Signal: "TERM", At: term}}, Exit: &memberExit{Status: 143, ObservedAt: term.Add(274 * time.Millisecond)}}
	clock.Advance(808 * time.Millisecond) // Custody released before the next scan.
	w.control.remove(victim.job)
	clock.Advance(4 * time.Second)
	fault := KernelLogLine{ID: "1", At: term.Add(273 * time.Millisecond), Message: "xe 0000:03:00.0: [drm] Fault response: Unsuccessful -ENOENT"}
	reset := KernelLogLine{ID: "2", At: term.Add(275 * time.Millisecond), Message: "xe 0000:03:00.0: [drm] GT0: Engine reset: engine_class=ccs"}
	scan(fault, reset)
	if w.Machine.Devices[0].Reason != "" || w.faults.Teardowns[deviceFaultKey(m.Devices[0])].Reason != "device_fault_xe" {
		t.Fatalf("teardown fault quarantined card: devices=%+v teardowns=%v", w.Machine.Devices, w.faults.Teardowns)
	}
	if !strings.Contains(journal.String(), "device fault attributed to teardown: device=0") {
		t.Fatal("teardown not recorded", journal.String())
	}
	// A natural exit 1 ms after a fault the scan sees first is still teardown.
	natural := &process{job: Job{ID: "natural", Attempt: 1, Session: "s", AllocatedDevices: []string{"0"}}}
	w.control.set(natural)
	at := clock.Now()
	scan(KernelLogLine{ID: "3", At: at, Message: "xe 0000:03:00.0: [drm] Fault response: Unsuccessful -EINVAL"})
	natural.mu.Lock()
	natural.outcome.Exit = &memberExit{ObservedAt: at.Add(time.Millisecond)}
	natural.mu.Unlock()
	clock.Advance(5 * time.Second)
	scan()
	if w.Machine.Devices[0].Reason != "" {
		t.Fatal("fault just before exit quarantined card", w.Machine.Devices)
	}
	// Device 1 had no stop or exit: unchanged quarantine.
	scan(KernelLogLine{ID: "4", At: clock.Now().Add(-2 * time.Second), Message: "xe 0000:07:00.0: [drm] GPU HANG"})
	if w.Machine.Devices[1].Reason != "device_fault_xe" || w.Machine.Devices[0].Reason != "" {
		t.Fatal("uncorrelated fault did not quarantine its card", w.Machine.Devices)
	}
	// Teardown evidence ages out; a later fault on the same card quarantines.
	clock.Advance(teardownRetention + time.Second)
	w.control.remove(natural.job)
	clock.Advance(teardownRetention + time.Second)
	scan(KernelLogLine{ID: "5", At: clock.Now().Add(-2 * time.Second), Message: "xe 0000:03:00.0: [drm] GPU HANG"})
	if w.Machine.Devices[0].Reason != "device_fault_xe" {
		t.Fatal("uncorrelated same-card fault did not quarantine", w.Machine.Devices)
	}
}

// #595: a failed kernel-log read is visible in worker and queue status.
func TestDeviceFaultReadFailureReported(t *testing.T) {
	var journal testLogBuffer
	previous := log.Writer()
	log.SetOutput(&journal)
	t.Cleanup(func() { log.SetOutput(previous) })
	q, clock := fixture(t)
	w := Worker{Machine: m3Machine(), Clock: clock}
	unreadable := errors.New("kernel log not readable")
	failing := true
	w.DeviceFaultSource = kernelLogSource(func() ([]byte, error) { return []byte("boot\n"), nil }, func(context.Context, []string) ([]byte, error) {
		if failing {
			return nil, errors.New("journal denied")
		}
		return nil, nil
	}, func(context.Context, time.Time) ([]KernelLogLine, error) { return nil, unreadable })
	root := &workerStore{dir: t.TempDir()}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.Machine.FaultMonitorError != "kernel log not readable" || strings.Count(journal.String(), "fault monitor unavailable") != 1 {
		t.Fatalf("read failure not reported: %q %s", w.Machine.FaultMonitorError, journal.String())
	}
	machineCheck(t, q, w.Machine)
	snapshot, err := q.Snapshot()
	if err != nil || len(snapshot.Machines) != 1 || snapshot.Machines[0].FaultMonitorError != "kernel log not readable" {
		t.Fatalf("status lacks fault monitor error: %v %+v", err, snapshot.Machines)
	}
	failing = false
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.Machine.FaultMonitorError != "" || !strings.Contains(journal.String(), "fault monitor recovered") {
		t.Fatal("recovery not reported", w.Machine.FaultMonitorError)
	}
}

// #595 safeguard: a card that faults at every stop, or crashes its own jobs,
// matches the teardown window each time. The third incident in an hour quarantines.
func TestDeviceFaultRepeatedTeardownQuarantines(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 2, 4, 0, 0, 0, time.UTC)}
	m := m3Machine()
	m.Devices[0].pciAddress = "0000:03:00.0"
	w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault}}
	w.DeviceHealthProbe = func(context.Context, MachineDevice) (bool, error) { return true, nil }
	w.control = &processControl{clock: clock}
	root := &workerStore{dir: t.TempDir()}
	var lines []KernelLogLine
	w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: "boot", Lines: lines}, nil
	}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		clock.Advance(10 * time.Minute)
		stop := clock.Now()
		job := Job{ID: fmt.Sprint("job-", i), Attempt: 1, Session: "s", AllocatedDevices: []string{"0"}}
		w.control.set(&process{job: job, stopRequested: stop})
		w.control.remove(job)
		clock.Advance(5 * time.Second)
		// One incident logs several fault lines milliseconds apart.
		lines = []KernelLogLine{
			{ID: fmt.Sprint(2 * i), At: stop.Add(273 * time.Millisecond), Message: "xe 0000:03:00.0: [drm] Fault response: Unsuccessful -ENOENT"},
			{ID: fmt.Sprint(2*i + 1), At: stop.Add(275 * time.Millisecond), Message: "xe 0000:03:00.0: [drm] Fault response: Unsuccessful -EINVAL"},
		}
		if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		if quarantined := w.Machine.Devices[0].Reason == "device_fault_xe"; quarantined != (i == 2) {
			t.Fatalf("incident %d: quarantined=%v", i+1, quarantined)
		}
	}
	// Vendor health alone cannot erase the repeated-fault escalation.
	lines = nil
	for range 2 {
		clock.Advance(time.Hour)
		if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
	}
	if !w.Machine.Devices[0].RepeatedTeardownFault || w.Machine.Devices[0].Reason == "" {
		t.Fatal("automatic health proof erased escalation", w.Machine.Devices[0])
	}
}

// #595: replay each observed 2026-11-02/03 form independently so a recognized
// neighbor cannot hide an unrecognized reset-only or schedule-disable incident.
func TestDeviceFaultObservedXeForms(t *testing.T) {
	forms := []string{
		"Engine memory CAT error [18]: class=bcs, logical_mask: 0x1, guc_id=10",
		"Fault response: Unsuccessful -ENOENT",
		"Fault response: Unsuccessful -EINVAL",
		"Engine reset: engine_class=ccs, logical_mask: 0x1, guc_id=6, state=0x289",
		"Schedule disable timeout",
		"Schedule disable failed to respond, guc_id=13",
		"Xe device coredump has been created",
		"trying reset from guc_exec_queue_timedout_job [xe]",
		"reset queued", "reset started", "reset done",
	}
	for _, form := range forms {
		t.Run(form, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 11, 3, 4, 16, 12, 0, time.UTC)}
			m := m3Machine()
			m.Devices[0].pciAddress = "0000:03:00.0"
			w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault}}
			w.control = &processControl{clock: clock}
			root := &workerStore{dir: t.TempDir()}
			var lines []KernelLogLine
			w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
				return KernelLogSnapshot{BootID: "boot", Lines: lines}, nil
			}
			if err := w.initDeviceFaults(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			for incident := range 3 {
				stop := clock.Now()
				j := Job{ID: fmt.Sprint(incident), Attempt: 1, AllocatedDevices: []string{"0"}}
				w.control.set(&process{job: j, stopRequested: stop})
				w.control.remove(j)
				clock.Advance(5 * time.Second)
				lines = []KernelLogLine{{ID: fmt.Sprint(incident), At: stop.Add(273 * time.Millisecond), Message: "xe 0000:03:00.0: [drm] Tile0: GT0: " + form}}
				if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
					t.Fatal(err)
				}
				key := deviceFaultKey(m.Devices[0])
				if incident < 2 && w.faults.Teardowns[key].Reason != "device_fault_xe" {
					t.Fatal("form missing from teardown classification")
				}
				if got := w.Machine.Devices[0].Reason; (got == "device_fault_xe") != (incident == 2) {
					t.Fatalf("incident %d: quarantine=%q", incident, got)
				}
				clock.Advance(10 * time.Minute)
			}
			if got := intelKernelFault(MachineDevice{pciAddress: "0000:07:00.0"}, lines[0].Message); got != "" {
				t.Fatal("cross-device fault", got)
			}
		})
	}
}
