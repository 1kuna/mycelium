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
	"slices"
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
			w := Worker{SignalEvidenceSource: func(request SignalCapture) *SignalEvidence {
				pid, at := request.PID, request.ObservedAt
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
	if coreCaptureState("core.%p", "Max core file size 0 unlimited bytes") != "disabled_soft_limit" {
		t.Fatal("disabled core capture hidden")
	}
	if coreCaptureState("|/usr/bin/handler", "Max core file size 0 unlimited bytes") != "pipe_handler_configured_no_record" {
		t.Fatal("pipe handler availability inferred from ignored soft limit")
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
	w := Worker{SignalEvidenceSource: func(SignalCapture) *SignalEvidence { return &SignalEvidence{CoreCapture: "unavailable"} }}
	w.captureSignalEvidencePID(42, "", e, true)
	if e.Signal != "" || e.Evidence == nil || e.Evidence.Signal != "unknown" {
		t.Fatal("shell exit manufactured a signal", e)
	}
}

func TestSignalEvidencePublishesExitBeforeBlockedCapture(t *testing.T) {
	_, clock := fixture(t)
	at := clock.Now()
	for _, native := range []bool{false, true} {
		p := &process{job: Job{AllocatedDevices: []string{"0"}}}
		member := &nativeMember{}
		if native {
			p.native = &nativeGroup{members: []*nativeMember{member}}
		}
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		w := Worker{SignalEvidenceSource: func(SignalCapture) *SignalEvidence {
			close(entered)
			<-release
			return &SignalEvidence{CoreCapture: "unavailable"}
		}}
		exit := nativeWaitExit(syscall.WaitStatus(syscall.SIGSEGV), at)
		go func() {
			done <- collectPublishedExit(exit, func(e *memberExit) error {
				p.mu.Lock()
				defer p.mu.Unlock()
				if native {
					member.Exit = e
				} else {
					p.outcome.Exit = e
				}
				return nil
			}, func(e *memberExit) { w.captureSignalEvidencePID(42, "", e, false) })
		}()
		<-entered
		times := p.teardownTimes()
		if len(times) != 1 || times[0] != at {
			t.Error("blocked metadata hid exit fact", times)
		}
		c := &processControl{clock: clock, processes: map[attemptKey]*process{{job: "job"}: p}}
		if !c.teardownNear("0", at) {
			t.Error("concurrent fault would quarantine teardown")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSignalEvidenceLongCoreAndLibraryMaps(t *testing.T) {
	_, clock := fixture(t)
	start := clock.Now().Add(800 * time.Millisecond)
	crash := start.Truncate(time.Second)    // Kernel %t rounds a rapid crash down.
	observed := start.Add(20 * time.Second) // Wait includes a slow core writer.
	request := SignalCapture{PID: 42, StartedAt: start, ObservedAt: observed, Birth: "boot:123"}
	maps := strings.Repeat("1000-2000 r--p 00000000 08:01 123 /runtime/libc.so.6\n", 100) + "2000-3000 r-xp 00001000 08:01 123 /runtime/libc.so.6\n"
	if len(maps) <= 4096 {
		t.Fatal("fixture must exceed journalctl's default field limit")
	}
	core, _ := json.Marshal(map[string]string{"COREDUMP_PID": "42", "COREDUMP_SIGNAL": "11", "COREDUMP_TIMESTAMP": fmt.Sprint(crash.UnixMicro()), "COREDUMP_FILENAME": "/operator-cores/core.42", "COREDUMP_PROC_MAPS": maps})
	kernel, _ := json.Marshal(map[string]string{"MESSAGE": "python[42]: segfault at ffffffe0 ip 123", "__REALTIME_TIMESTAMP": fmt.Sprint(crash.UnixMicro())})
	e := &SignalEvidence{}
	calls := 0
	collectSignalJournal(context.Background(), e, request, func(_ context.Context, args ...string) ([]byte, bool, error) {
		calls++
		if !slices.Contains(args, "--all") || !slices.Contains(args, "--reverse") || !slices.Contains(args, "--since=@"+fmt.Sprint(start.Unix())) {
			t.Error("query loses large fields or crash lifetime", args)
		}
		if slices.Contains(args, "-k") {
			return kernel, false, nil
		}
		if !slices.Contains(args, "COREDUMP_PID=42") {
			t.Error("missing PID selector")
		}
		return core, false, nil
	})
	if calls != 2 || e.CorePointer == "" || len(e.Libraries) != 1 || e.Libraries[0].Name != "libc.so.6" || e.FaultAddress != "ffffffe0" {
		t.Fatalf("slow core evidence lost: %+v", e)
	}
	// A same-number PID from before this execution must not supply its core.
	request.StartedAt = observed.Add(-time.Second)
	other := &SignalEvidence{}
	applyCoreMetadataFor(other, core, request)
	if other.CorePointer != "" || len(other.Libraries) != 0 {
		t.Fatal("earlier execution supplied core")
	}
}

func TestSignalEvidenceWrappedAttemptUsesCorePayloadPID(t *testing.T) {
	_, clock := fixture(t)
	request := SignalCapture{StartedAt: clock.Now(), ObservedAt: clock.Now().Add(10 * time.Second), Cgroup: "/worker/attempt-unique/main"}
	core, _ := json.Marshal(map[string]string{"COREDUMP_PID": "43", "COREDUMP_SIGNAL": "11", "COREDUMP_TIMESTAMP": fmt.Sprint(request.StartedAt.Add(time.Second).UnixMicro()), "COREDUMP_CGROUP": request.Cgroup, "COREDUMP_FILENAME": "/operator-cores/core.43"})
	e := &SignalEvidence{}
	collectSignalJournal(context.Background(), e, request, func(_ context.Context, args ...string) ([]byte, bool, error) {
		if slices.Contains(args, "-k") {
			return nil, false, nil
		}
		if !slices.Contains(args, "COREDUMP_CGROUP="+request.Cgroup) || slices.Contains(args, "COREDUMP_PID=42") {
			t.Error("wrapped job selected launcher", args)
		}
		return core, false, nil
	})
	if e.PID != 43 || e.Attribution != "attempt_cgroup" || e.CorePointer == "" {
		t.Fatal("actual core payload identity missing", e)
	}
	wrong := request
	wrong.Cgroup = "/worker/another-attempt/main"
	e = &SignalEvidence{}
	applyCoreMetadataFor(e, core, wrong)
	if e.PID != 0 || e.CorePointer != "" {
		t.Fatal("foreign attempt core attached")
	}
}

func TestFaultEvidenceNoCgroupIncludesOwnedDescendants(t *testing.T) {
	births := map[int]string{10: "root", 11: "child", 12: "grandchild"}
	birth := func(pid int) (string, error) { return births[pid], nil }
	members := func(root int) ([]int, error) { return []int{10, 11, 12}, nil }
	pids, err := faultOwnedPIDs(map[int]string{10: "root"}, birth, members)
	if err != nil || !slices.Equal(pids, []int{10, 11, 12}) {
		t.Fatal("payload descendants missing", pids, err)
	}
	if _, err := faultOwnedPIDs(map[int]string{10: "old-birth"}, birth, members); err == nil {
		t.Fatal("recycled supervisor adopted")
	}
	members = func(root int) ([]int, error) { births[10] = "reused"; return []int{10, 11}, nil }
	if _, err := faultOwnedPIDs(map[int]string{10: "root"}, birth, members); err == nil {
		t.Fatal("reuse during inventory adopted")
	}
}

func TestFaultEvidenceBurstFiltersEachKernelTime(t *testing.T) {
	_, clock := fixture(t)
	assigned := clock.Now().Add(time.Second)
	w := Worker{Clock: clock, FaultHostSource: func() (map[string]string, []string) { return nil, nil }, FaultProcessSource: func(map[int]string) ([]int, error) { return []int{42}, nil }}
	j := Job{ID: "job", Attempt: 1, AssignedAt: &assigned, AllocatedDevices: []string{"0"}}
	w.control = &processControl{clock: clock, processes: map[attemptKey]*process{jobKey(j): {job: j}}}
	d := MachineDevice{ID: "0"}
	base := w.captureFault(d, KernelLogLine{At: clock.Now()}, "one", "xe", "pci")
	first := faultAtLine(base, KernelLogLine{At: clock.Now()}, "one", "xe", "pci")
	second := faultAtLine(base, KernelLogLine{At: assigned.Add(time.Second)}, "two", "xe", "pci")
	if len(first.Attempts) != 0 || len(second.Attempts) != 1 || len(base.Attempts) != 1 {
		t.Fatal("first event filter contaminated burst", first, second)
	}
}
