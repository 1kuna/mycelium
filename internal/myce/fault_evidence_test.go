package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestFaultEvidenceAllocationAndAttemptFences(t *testing.T) {
	q, clock := fixture(t)
	m := m3Machine()
	m.Devices[0].pciAddress = "0000:03:00.0"
	w := Worker{Machine: m, Clock: clock, hostMemorySamples: map[attemptKey]hostMemorySample{}}
	calls := 0
	w.FaultHostSource = func() (map[string]string, []string) {
		calls++
		return map[string]string{"meminfo": "MemAvailable: 100000 kB", "buddyinfo": "Node 0 zone Normal 1 2 3"}, nil
	}
	var lines []KernelLogLine
	w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: "boot", Lines: lines}, nil
	}
	root := &workerStore{dir: t.TempDir()}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("production"))
	a := assignedJob(machineCheck(t, q, m))
	if a == nil || a.ID != j.ID {
		t.Fatal(a)
	}
	w.Machine.Devices[0].Platform = "cuda"
	// Reuse the real cgroup reader with an inert fake filesystem. No local PID
	// inventory, GPU, kernel journal or host-memory sampler is invoked.
	memory, err := fakeCeilings(t).create("fault-attempt", 1<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(memory.path, "cgroup.procs"), []byte("42\n43\n"), 0600); err != nil {
		t.Fatal(err)
	}
	p := &process{job: *a, memory: memory}
	w.control = &processControl{clock: clock, processes: map[attemptKey]*process{jobKey(*a): p}}
	w.hostMemorySamples[jobKey(*a)] = hostMemorySample{gb: 2, deviceGB: 7, at: clock.Now(), deviceAt: clock.Now(), source: "synthetic"}
	clock.Advance(5 * time.Second)
	lines = []KernelLogLine{
		{ID: "123", At: clock.Now().Add(-time.Second), Message: "NVRM: _memdescAllocInternal Out of memory [NV_ERR_NO_MEMORY] private-message-must-not-survive"},
		{ID: "124", At: clock.Now(), Message: "NVRM: allocation failed with big page size, retrying with default page size"},
	}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	e := w.Machine.Devices[0].FaultEvidence
	if len(e) != 2 || calls != 1 {
		t.Fatalf("burst must retain both lines with one read: events=%d reads=%d", len(e), calls)
	}
	if w.Machine.Devices[0].Reason != "" {
		t.Fatal("allocation observation quarantined a card")
	}
	if len(e[0].Attempts) != 1 || len(e[0].Attempts[0].PIDs) != 2 || *e[0].Attempts[0].DeviceGB != 7 || e[0].Attempts[0].HostAt != clock.Now().Add(-5*time.Second) || e[0].KernelAt != lines[0].At {
		t.Fatalf("attribution: %+v", e[0])
	}
	reported := m3Machine()
	reported.Devices[0].FaultEvidence = e
	machineCheck(t, q, reported, a)
	got := job(t, q, a.ID)
	if got.State != "running" || len(got.FaultEvidence) != 2 {
		t.Fatalf("job evidence missing: %+v", got.FaultEvidence)
	}
	if err := q.Complete(result(a, true)); err != nil {
		t.Fatal(err)
	}
	got = job(t, q, a.ID)
	if len(got.AttemptHistory) != 1 || len(got.AttemptHistory[0].FaultEvidence) != 2 {
		t.Fatal("terminal history lost snapshot")
	}
	data, _ := json.Marshal(w.faults)
	if strings.Contains(string(data), "private-message") {
		t.Fatal("raw kernel line persisted")
	}
	// Replay and restart do not reread memory or multiply events.
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(w.Machine.Devices[0].FaultEvidence) != 2 {
		t.Fatal("restart replayed allocation")
	}
	// An old session/attempt cannot attach telemetry to a new execution.
	old := got.FaultEvidence
	fake := *e[0]
	fake.Event = "stale"
	fake.Attempts = []FaultAttempt{{Execution: Execution{ID: a.ID, Attempt: a.Attempt + 1}, Session: a.Session}}
	m.Devices[0].FaultEvidence = []*FaultEvidence{&fake}
	q.acceptFaultEvidence(&m)
	if len(got.FaultEvidence) != len(old) {
		t.Fatal("stale snapshot attached")
	}
}

func TestFaultEvidenceXeBeforeTeardownDecision(t *testing.T) {
	_, clock := fixture(t)
	m := m3Machine()
	m.Devices[0].pciAddress = "0000:03:00.0"
	w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault}, FaultHostSource: func() (map[string]string, []string) { return nil, []string{"meminfo"} }}
	var lines []KernelLogLine
	w.DeviceFaultSource = func(context.Context, time.Time) (KernelLogSnapshot, error) {
		return KernelLogSnapshot{BootID: "boot", Lines: lines}, nil
	}
	root := &workerStore{dir: t.TempDir()}
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	lines = []KernelLogLine{{ID: "1", At: clock.Now(), Message: "xe 0000:03:00.0: Engine reset"}}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if len(w.Machine.Devices[0].FaultEvidence) != 1 || w.Machine.Devices[0].Reason != "" {
		t.Fatal("evidence waited for teardown decision")
	}
	captured := w.Machine.Devices[0].FaultEvidence[0].CapturedAt
	clock.Advance(2 * time.Second)
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if len(w.Machine.Devices[0].FaultEvidence) != 1 || w.Machine.Devices[0].FaultEvidence[0].CapturedAt != captured || w.Machine.Devices[0].Reason != "device_fault_xe" {
		t.Fatal("delayed classifier lost original evidence")
	}
}

func TestFaultEvidenceBoundedRetention(t *testing.T) {
	var ring []*FaultEvidence
	for i := 0; i < 100; i++ {
		e := &FaultEvidence{Event: fmt.Sprint(i), Device: "0", Host: map[string]string{"meminfo": strings.Repeat("0", 8192)}}
		ring = retainFaultEvidence(ring, e)
	}
	bytes := 0
	for _, e := range ring {
		bytes += faultEvidenceBytes(e)
	}
	if bytes > diagnosticReportBytes || len(ring) >= 100 || ring[len(ring)-1].Replaced == 0 {
		t.Fatal("unbounded history", bytes, len(ring))
	}
	large := &FaultEvidence{Host: map[string]string{"buddyinfo": strings.Repeat("0", faultSectionBytes), "vmstat": strings.Repeat("0", faultSectionBytes), "meminfo": strings.Repeat("0", faultSectionBytes)}}
	boundFaultEvidence(large)
	if faultEvidenceBytes(large) > diagnosticReportBytes || len(large.Unavailable) == 0 {
		t.Fatal("one oversized snapshot bypassed bound")
	}
}

func TestSignalEvidenceSyntheticExitsAndCoreMetadata(t *testing.T) {
	_, clock := fixture(t)
	for _, signal := range []syscall.Signal{syscall.SIGSEGV, syscall.SIGBUS, syscall.SIGABRT} {
		t.Run(signal.String(), func(t *testing.T) {
			e := nativeWaitExit(syscall.WaitStatus(signal)|0x80, clock.Now())
			w := Worker{SignalEvidenceSource: func(pid int, at time.Time, dir string) *SignalEvidence {
				evidence := &SignalEvidence{CoreCapture: "disabled_soft_limit"}
				applyCoreMetadata(evidence, []byte(fmt.Sprintf(`{"COREDUMP_PID":"42","COREDUMP_SIGNAL":"%d","COREDUMP_TIMESTAMP":"%d","COREDUMP_FILENAME":"/operator-cores/core.42","COREDUMP_PROC_MAPS":"1000-2000 r-xp 00001000 08:01 123 /runtime/libc.so.6\n","COREDUMP_ENVIRON":"TOKEN=secret"}`, signal, at.UnixMicro())), pid, at)
				return evidence
			}}
			w.captureSignalEvidence(&exec.Cmd{Process: &os.Process{Pid: 42}}, e)
			r := classifyAttemptOutcome(Result{}, attemptCause{memberOutcome: memberOutcome{Exit: e}})
			if r.SignalEvidence == nil || len(r.SignalEvidence.Libraries) != 1 || r.SignalEvidence.Libraries[0].Name != "libc.so.6" || r.SignalEvidence.Signal != crashSignal(signal) || r.SignalEvidence.CoreCapture != "systemd_core_recorded" {
				t.Fatal(r.SignalEvidence)
			}
			data, _ := json.Marshal(r)
			if strings.Contains(string(data), "TOKEN") || strings.Contains(string(data), "secret") {
				t.Fatal("environment retained")
			}
		})
	}
	data := []byte(`{"MESSAGE":"python[42]: segfault at ffffffe0 ip 0000000 error 4 in libc.so.6"}`)
	if kernelFaultAddress(data, 42) != "ffffffe0" || kernelFaultAddress(data, 43) != "" {
		t.Fatal("fault address/PID matching")
	}
	if coreCaptureState("|/usr/bin/handler", "Max core file size 0 unlimited bytes") != "disabled_soft_limit" {
		t.Fatal("disabled core capture hidden")
	}
	e := &SignalEvidence{CoreCapture: "unavailable"}
	applyCoreMetadata(e, []byte(`{"COREDUMP_PID":"42","COREDUMP_TIMESTAMP":"1","COREDUMP_SIGNAL":"11","COREDUMP_FILENAME":"/old/core"}`), 42, clock.Now())
	if e.CorePointer != "" {
		t.Fatal("old PID core misattributed")
	}
}

func TestFaultEvidenceLegacyWire(t *testing.T) {
	wire := &atomic.Bool{}
	c := Checkin{Machine: Machine{Devices: []MachineDevice{{FaultEvidence: []*FaultEvidence{{Event: "one"}}}}}}
	if func() bool {
		legacy, _ := faultWireInput(c, false)
		_, present := faultWireInput(legacy, true)
		return present
	}() || func() bool { _, present := faultWireInput(c, true); return !present }() {
		t.Fatal("legacy stripping mutated local evidence")
	}
	wire.Store(true)
	if func() bool { _, present := faultWireInput(c, wire.Load()); return !present }() {
		t.Fatal("negotiated evidence missing")
	}
}

func BenchmarkFaultEvidenceCapture(b *testing.B) {
	clock := &fakeClock{now: time.Unix(100, 0)}
	w := Worker{Clock: clock, FaultHostSource: func() (map[string]string, []string) {
		return readFaultHost(func(string) ([]byte, error) { return []byte("MemAvailable: 100000 kB\n"), nil })
	}}
	d := MachineDevice{ID: "0"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.captureFault(d, KernelLogLine{At: clock.Now()}, "event", "nvrm_allocation", "cuda_candidates_no_kernel_device")
	}
}

func TestFaultEvidenceResultDowngrade(t *testing.T) {
	calls, applied := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, present := fields["signal_evidence"]; present {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"code":"request_invalid","error":"unknown field"}`))
			return
		}
		applied++
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	wire := &atomic.Bool{}
	wire.Store(true)
	client := Client{URL: server.URL, faultWire: wire}
	result := Result{SignalEvidence: &SignalEvidence{Signal: "SIGSEGV"}}
	if err := client.Call(context.Background(), "POST", "/api/workers/complete", &result, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || applied != 1 || wire.Load() || result.SignalEvidence == nil {
		t.Fatal("downgrade lost custody/evidence or applied twice", calls, applied)
	}
}
func TestSignalEvidenceShellStatusUnknown(t *testing.T) {
	_, clock := fixture(t)
	e := nativeWaitExit(syscall.WaitStatus(139<<8), clock.Now())
	w := Worker{SignalEvidenceSource: func(int, time.Time, string) *SignalEvidence { return &SignalEvidence{CoreCapture: "unavailable"} }}
	w.captureSignalEvidencePID(42, "", e, true)
	if e.Signal != "" || e.Evidence == nil || e.Evidence.Signal != "unknown" {
		t.Fatal("shell exit manufactured a signal", e)
	}
}
