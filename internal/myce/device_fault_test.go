package myce

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// #149: a kernel fault must fence only its card before another job can hang there.
func TestDeviceFaultQuarantine(t *testing.T) {
	var journal bytes.Buffer
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
	w.DeviceFaultSource = func(context.Context) (KernelLogSnapshot, error) {
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
	// Free only the healthy neighbor, then assign the same logical job anew.
	if err := q.Complete(Result{Machine: b.Machine, Session: b.Session, Execution: Execution{ID: b.ID, Attempt: b.Attempt}, Success: true}); err != nil {
		t.Fatal(err)
	}
	resumed := assignedJob(machineCheck(t, q, w.Machine))
	if resumed == nil || resumed.ID != first.ID || resumed.Attempt != a.Attempt+1 || !slices.Equal(resumed.AllocatedDevices, []string{"1"}) {
		t.Fatal("healthy-device retry", resumed)
	}
	observe("running")
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
