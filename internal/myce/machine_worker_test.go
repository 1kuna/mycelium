package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// This protocol harness is deliberately independent of queue admission. The
// real queue remains capped at one in M2; every payload is a real CPU process.
type machineHarness struct {
	mu         sync.Mutex
	jobs       []Job
	issued     bool
	reports    []Checkin
	results    []Result
	controls   map[string]AttemptDirective
	blockLog   string
	logEntered chan struct{}
	unblock    chan struct{}
	once       sync.Once
}

func (h *machineHarness) serve(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/workers/checkin":
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		h.mu.Lock()
		h.reports = append(h.reports, c)
		reply := Reply{AttemptProtocol: keyedAttemptProtocol}
		if !h.issued {
			h.issued = true
			for _, j := range h.jobs {
				j.Machine, j.Session = c.Machine.ID, c.Machine.Session
				reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Job: &j})
			}
		} else {
			for _, a := range c.Attempts {
				d := h.controls[a.ID]
				d.Execution, d.Session = a.Execution, a.Session
				reply.Attempts = append(reply.Attempts, d)
			}
		}
		h.mu.Unlock()
		_ = json.NewEncoder(w).Encode(reply)
	case "/api/workers/complete":
		var result Result
		_ = json.NewDecoder(r.Body).Decode(&result)
		h.mu.Lock()
		h.results = append(h.results, result)
		h.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	case "/api/workers/log":
		var chunk LogChunk
		_ = json.NewDecoder(r.Body).Decode(&chunk)
		if chunk.ID == h.blockLog {
			h.once.Do(func() { close(h.logEntered) })
			select {
			case <-h.unblock:
			case <-r.Context().Done():
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	default:
		if strings.HasSuffix(r.URL.Path, "/events") {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			_ = json.NewEncoder(w).Encode(WorkerEvent{})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "start_gate": startGateProtocol})
	}
}
func fakeMachineDevices() []MachineDevice {
	return []MachineDevice{
		{ID: "0", Platform: "cuda", Identity: "GPU-fake-a", Fingerprint: "fixture-driver", Selector: "GPU-fake-a", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8},
		{ID: "1", Platform: "xpu", Identity: "fixture-xpu-uuid", Fingerprint: "fixture-driver", Selector: "3", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8},
	}
}
func harnessWorker(t *testing.T, h *machineHarness) Worker {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(server.Close)
	m := machine("machine", "fake", "machine")
	m.Devices = fakeMachineDevices()
	m.HostMemoryBudgetGB = 32
	return Worker{Machine: m, StateDir: newTestStateRoot(t), Interval: 10 * time.Millisecond, Clock: RealClock{}, Client: Client{URL: server.URL, Token: "harness-token"}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return fakeMachineDevices(), nil }, CPUIsolationProbe: func(context.Context) error { return nil }, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
}
func harnessJob(t *testing.T, id, device, command string) Job {
	t.Helper()
	s := spec("experiment")
	s.Resumable = false
	s.Needs.HostMemoryGB = 1
	s.Run.Command = []string{"sh", "-c", command}
	j := Job{ID: id, Spec: s, Attempt: 1, Checkpoint: filepath.Join(t.TempDir(), "checkpoint")}
	if device != "" {
		j.AllocatedDevices = []string{device}
	} else {
		j.Spec = cpuSpec(1, 1)
		j.Spec.Resumable = false
		j.Spec.Run.Command = s.Run.Command
		if err := normalizeResources(&j.Spec); err != nil {
			t.Fatal(err)
		}
	}
	return j
}
func startHarness(t *testing.T, w Worker, h *machineHarness) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		for _, j := range h.jobs {
			h.stop(j.ID)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("machine worker did not stop")
		}
	})
	return cancel, done
}
func (h *machineHarness) stop(id string) {
	h.mu.Lock()
	h.controls[id] = AttemptDirective{Stop: true, Kill: true}
	h.mu.Unlock()
}
func (h *machineHarness) completed(id string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.results {
		if r.ID == id {
			return true
		}
	}
	return false
}
func (h *machineHarness) result(id string) Result {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.results {
		if r.ID == id {
			return r
		}
	}
	return Result{}
}
func awaitFile(t *testing.T, path string) {
	t.Helper()
	await(t, func() bool { _, err := os.Stat(path); return err == nil })
}

func TestMachineWorkerConcurrentDevicesCPUAndNeighborStop(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "must-not-reach-payload")
	dir := t.TempDir()
	command := func(name, checks string) string {
		return checks + `; echo $$ > ` + filepath.Join(dir, name) + `; echo ready; while :; do sleep 1; done`
	}
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	h.jobs = []Job{
		harnessJob(t, "cuda", "0", command("cuda", `test "$CUDA_VISIBLE_DEVICES" = GPU-fake-a && test "$ZE_AFFINITY_MASK" = 2147483647 && test "$ONEAPI_DEVICE_SELECTOR" = '!*:*' && test "$ZE_ENABLE_PCI_ID_DEVICE_ORDER" = 1 && test -z "$MYCE_TOKEN" || exit 42`)),
		harnessJob(t, "xpu", "1", command("xpu", `test -z "$CUDA_VISIBLE_DEVICES" && test "$ZE_AFFINITY_MASK" = 3 && test "$ONEAPI_DEVICE_SELECTOR" = level_zero:0 && test "$ZE_ENABLE_PCI_ID_DEVICE_ORDER" = 1 && test -z "$MYCE_TOKEN" || exit 42`)),
		harnessJob(t, "cpu", "", `test -z "$CUDA_VISIBLE_DEVICES" && test "$ZE_AFFINITY_MASK" = 2147483647 && test "$ONEAPI_DEVICE_SELECTOR" = '!*:*' && test "$ZE_ENABLE_PCI_ID_DEVICE_ORDER" = 1 && test -z "$MYCE_TOKEN" && touch `+filepath.Join(dir, "cpu")),
	}
	w := harnessWorker(t, h)
	startHarness(t, w, h)
	for _, name := range []string{"cuda", "xpu", "cpu"} {
		awaitFile(t, filepath.Join(dir, name))
	}
	h.mu.Lock()
	seen := false
	for _, c := range h.reports {
		if len(c.Attempts) >= 2 && len(c.Machine.Devices) == 2 {
			seen = true
		}
	}
	h.mu.Unlock()
	if !seen {
		await(t, func() bool {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, c := range h.reports {
				if len(c.Attempts) >= 2 && len(c.Machine.Devices) == 2 {
					return true
				}
			}
			return false
		})
	}
	h.stop("cuda")
	await(t, func() bool { return h.completed("cuda") })
	data, _ := os.ReadFile(filepath.Join(dir, "xpu"))
	var pid int
	_, _ = fmt.Sscan(string(data), &pid)
	if pid <= 0 || syscall.Kill(pid, 0) != nil || h.completed("xpu") {
		t.Fatal("stopping CUDA touched XPU neighbor")
	}
	h.stop("xpu")
	await(t, func() bool { return h.completed("xpu") && h.completed("cpu") })
	if !h.result("cpu").Success {
		t.Fatal("CPU select-none helper failed")
	}
}

func TestMachineWorkerParallelReadinessUploadAndControl(t *testing.T) {
	dir := t.TempDir()
	h := &machineHarness{controls: map[string]AttemptDirective{}, blockLog: "slow", logEntered: make(chan struct{}), unblock: make(chan struct{})}
	h.jobs = []Job{harnessJob(t, "slow", "0", `echo ready; while :; do sleep 1; done`), harnessJob(t, "neighbor", "1", `touch `+filepath.Join(dir, "neighbor")+`; while :; do sleep 1; done`)}
	w := harnessWorker(t, h)
	startHarness(t, w, h)
	t.Cleanup(func() { close(h.unblock) })
	select {
	case <-h.logEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not block")
	}
	awaitFile(t, filepath.Join(dir, "neighbor"))
	h.stop("neighbor")
	await(t, func() bool { return h.completed("neighbor") })
	if h.completed("slow") {
		t.Fatal("blocked neighbor was affected")
	}
	h.stop("slow")
}

func TestMachineWorkerIdentityMismatchAndCPUIsolationRefused(t *testing.T) {
	for _, cpu := range []bool{false, true} {
		t.Run(fmt.Sprint(cpu), func(t *testing.T) {
			h := &machineHarness{controls: map[string]AttemptDirective{}}
			device := "1"
			want := "device_identity_mismatch"
			if cpu {
				device = ""
				want = "cpu_accelerator_isolation_unavailable"
			}
			marker := filepath.Join(t.TempDir(), "ran")
			h.jobs = []Job{harnessJob(t, "refused", device, "touch "+marker)}
			w := harnessWorker(t, h)
			if cpu {
				w.CPUIsolationProbe = func(context.Context) error { return errors.New("visible accelerator") }
			} else {
				w.InventoryProbe = func(context.Context) ([]MachineDevice, error) {
					d := fakeMachineDevices()
					d[1].Identity = "changed"
					return d, nil
				}
			}
			_, err := w.forAttempt(h.jobs[0])
			var rejection *rejectionError
			if !errors.As(err, &rejection) || rejection.code != want {
				t.Fatalf("typed refusal: got %v, want %s", err, want)
			}
			startHarness(t, w, h)
			await(t, func() bool { return h.completed("refused") })
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refused payload executed")
			}
			if !strings.Contains(h.result("refused").Error, map[bool]string{false: "mapping", true: "select-none"}[cpu]) {
				t.Fatalf("missing %s refusal: %s", want, h.result("refused").Error)
			}
		})
	}
}

func TestMachineWorkerSidecarSharesDeviceAndMemberFailureIsLocal(t *testing.T) {
	dir := t.TempDir()
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	check := `test "$ZE_AFFINITY_MASK" = 3 && test "$ONEAPI_DEVICE_SELECTOR" = level_zero:0 && test -z "$CUDA_VISIBLE_DEVICES" && test "$ZE_ENABLE_PCI_ID_DEVICE_ORDER" = 1 && test -z "$MYCE_TOKEN" || exit 42; `
	group := harnessJob(t, "group", "1", check+`touch `+filepath.Join(dir, "main")+`; while :; do sleep 1; done`)
	group.Spec.Run.Sidecars = []NativeSidecar{{Name: "helper", MemoryGB: .1, Command: []string{"sh", "-c", check + `touch ` + filepath.Join(dir, "side") + `; while test ! -f ` + filepath.Join(dir, "fail") + `; do sleep .01; done; exit 7`}}}
	neighbor := harnessJob(t, "neighbor", "0", `echo $$ > `+filepath.Join(dir, "neighbor")+`; while :; do sleep 1; done`)
	h.jobs = []Job{group, neighbor}
	w := harnessWorker(t, h)
	startHarness(t, w, h)
	for _, name := range []string{"main", "side", "neighbor"} {
		awaitFile(t, filepath.Join(dir, name))
	}
	if err := os.WriteFile(filepath.Join(dir, "fail"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return h.completed("group") })
	if h.result("group").Success || h.completed("neighbor") {
		t.Fatal("member failure escaped its attempt")
	}
	h.stop("neighbor")
	await(t, func() bool { return h.completed("neighbor") })
}

func TestMachineWorkerRestartReplaysEveryBufferedResult(t *testing.T) {
	h := &machineHarness{issued: true, controls: map[string]AttemptDirective{}}
	w := harnessWorker(t, h)
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	root.state.Session = "saved-session"
	if err = root.save(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		j := harnessJob(t, fmt.Sprint("saved-", i), fmt.Sprint(i), "exit 0")
		j.Machine, j.Session = w.Machine.ID, root.state.Session
		s, err := newAttemptStore(root, j)
		if err != nil {
			t.Fatal(err)
		}
		r := Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Success: true}
		s.state.Identity = ID()
		if err = saveWorkerJSON(s.dir, s.state.Identity+".result.json", r); err != nil {
			t.Fatal(err)
		}
		if err = s.save(); err != nil {
			t.Fatal(err)
		}
	}
	root.close()
	startHarness(t, w, h)
	await(t, func() bool { return h.completed("saved-0") && h.completed("saved-1") })
	for i := 0; i < 2; i++ {
		r := h.result(fmt.Sprint("saved-", i))
		if !r.Success || r.Session != "saved-session" || r.Attempt != 1 {
			t.Fatal("buffered result lost exact custody")
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !slices.ContainsFunc(h.reports, func(c Checkin) bool { return c.Machine.Session == "saved-session" && len(c.Attempts) == 2 }) {
		t.Fatal("restart omitted saved attempts")
	}
}

func TestMachineWorkerAggregateRefusalAndUnsupportedGPUCount(t *testing.T) {
	w := harnessWorker(t, &machineHarness{controls: map[string]AttemptDirective{}})
	a, b := harnessJob(t, "a", "0", "exit 0"), harnessJob(t, "b", "1", "exit 0")
	w.Machine.HostMemoryBudgetGB = 1.5
	if !errors.Is(localAttemptAdmission(w, []Job{a}, b, nil), errWorkerMemory) {
		t.Fatal("aggregate host RAM oversubscription accepted")
	}
	w.Machine.HostMemoryBudgetGB = 32
	w.Machine.CPUBudgetCores = 1
	if !errors.Is(localAttemptAdmission(w, []Job{a}, b, nil), errWorkerMemory) {
		t.Fatal("aggregate CPU oversubscription accepted")
	}
	for _, platform := range []string{"cuda", "xpu"} {
		_, err := WithModelDevice(Model{}, platform, "0,1")
		var he *rejectionError
		if !errors.As(err, &he) || he.code != "gpu_count_unsupported" {
			t.Fatalf("multi mask accepted: %v", err)
		}
	}
	count := 2
	s := spec("experiment")
	s.Needs.GPU = &GPUSelection{Count: &count}
	var he *rejectionError
	if err := normalizeResources(&s); !errors.As(err, &he) || he.code != "gpu_count_unsupported" {
		t.Fatalf("multi count accepted: %v", err)
	}
}

// Old tests still assert the exact single attempt, now found through the
// machine's collection index. Artifact identities and assertions are unchanged.
func readSingleAttemptState(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var st workerState
	if err = json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Job != nil {
		return data, nil
	}
	if len(st.Attempts) != 1 {
		return nil, fmt.Errorf("expected exactly one attempt, got %d", len(st.Attempts))
	}
	return os.ReadFile(filepath.Join(filepath.Dir(path), st.Attempts[0]))
}

func TestMachineWorkerSlowReadinessNeighborControls(t *testing.T) {
	dir := t.TempDir()
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	slow := harnessJob(t, "slow", "1", "exit 99")
	port := 0
	slow.Spec.Run.Sidecars = []NativeSidecar{{Name: "waiting", MemoryGB: .1, Port: &port, Readiness: &NativeReadiness{Path: "/ready", Timeout: "1m"}, Command: []string{"sh", "-c", "touch " + filepath.Join(dir, "side") + `; while :; do sleep 1; done`}}}
	neighbor := harnessJob(t, "neighbor", "0", "touch "+filepath.Join(dir, "neighbor")+`; while :; do sleep 1; done`)
	h.jobs = []Job{slow, neighbor}
	w := harnessWorker(t, h)
	w.memoryCeilings = fakeCeilings(t)
	startHarness(t, w, h)
	awaitFile(t, filepath.Join(dir, "side"))
	awaitFile(t, filepath.Join(dir, "neighbor"))
	parents, err := filepath.Glob(filepath.Join(w.memoryCeilings.root, "attempt-*"))
	if err != nil || len(parents) != 2 {
		t.Fatalf("attempt cgroup parents: %v %v", parents, err)
	}
	h.stop("slow")
	await(t, func() bool { return h.completed("slow") })
	if h.completed("neighbor") {
		t.Fatal("readiness cancellation touched neighbor")
	}
	parents, err = filepath.Glob(filepath.Join(w.memoryCeilings.root, "attempt-*"))
	if err != nil || len(parents) != 1 {
		t.Fatal("cleanup did not preserve exactly the neighboring subtree", parents, err)
	}
	h.stop("neighbor")
	await(t, func() bool { return h.completed("neighbor") })
}

func TestMachineWorkerInventoryIdentityAndFixedBudget(t *testing.T) {
	old := fakeMachineDevices()
	current := []MachineDevice{old[1], old[0]}
	current[0].ID, current[1].ID = "0", "1"
	stable := reconcileDevices(old, current)
	if stable[0].Identity != old[0].Identity || stable[1].Identity != old[1].Identity || stable[0].Reason != "" || stable[1].Reason != "" {
		t.Fatal("discovery order reassigned identities")
	}
	current = fakeMachineDevices()
	current[1].Selector = "2"
	stable = reconcileDevices(old, current)
	if stable[1].Reason != "device_identity_mismatch" || stable[1].Selector != "3" || stable[0].Reason != "" {
		t.Fatal("XPU ordinal mismatch silently followed")
	}
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	w := harnessWorker(t, h)
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	if err = w.initInventory(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	for _, d := range w.Machine.Devices {
		if d.BudgetGB != 8 || d.FreeMemoryGB != 8 || d.TotalMemoryGB != 8 {
			t.Fatal("device budget replaced with shared host RAM", d)
		}
	}
}

func TestMachineWorkerUnsupportedMasksBeforeLaunch(t *testing.T) {
	var rule *rejectionError
	check := func(err error) {
		t.Helper()
		if !errors.As(err, &rule) || rule.code != "gpu_count_unsupported" {
			t.Fatalf("missing typed multi-device refusal: %v", err)
		}
	}
	config, err := PlanInstall("worker", "/fixture/home", "", "/fixture/nas", "http://queue.invalid", "fixture", Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 8})
	if err != nil {
		t.Fatal(err)
	}
	config.Device = "0,1"
	if err := config.ValidateInstall(); err != nil {
		t.Fatalf("worker inventory allowance rejected: %v", err)
	}
	_, err = (MachineProbe{}).DetectDevice(context.Background(), "0,1")
	check(err)
	c := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Device: "0,1"}}}
	check(validateModels(c))
	c.Models[0].Device = ""
	c.Models[0].DeviceEnv = map[string]string{"ONEAPI_DEVICE_SELECTOR": "level_zero:0,1"}
	check(validateModels(c))
	w := Worker{Machine: Machine{Device: "0,0"}}
	if err := w.Run(context.Background()); !errors.As(err, &rule) || rule.code != "gpu_selector_invalid" {
		t.Fatalf("duplicate inventory selectors accepted: %v", err)
	}
	q, _ := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	count := 2
	s := spec("experiment")
	s.Needs.GPU = &GPUSelection{Count: &count}
	err = (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/jobs", s, nil)
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != 400 || he.Code != "gpu_count_unsupported" {
		t.Fatalf("HTTP multi count refusal: %v", err)
	}
}

func TestMachineWorkerCrashHelper(t *testing.T) {
	if os.Getenv("MYCE_MACHINE_CRASH_HELPER") != "1" {
		return
	}
	m := machine("machine", "fake", "machine")
	m.Devices = fakeMachineDevices()
	m.HostMemoryBudgetGB = 32
	w := Worker{Machine: m, StateDir: os.Getenv("MYCE_MACHINE_CRASH_STATE"), Client: Client{URL: os.Getenv("MYCE_MACHINE_CRASH_URL")}, Interval: 10 * time.Millisecond, Clock: RealClock{}, InventoryProbe: func(context.Context) ([]MachineDevice, error) { return fakeMachineDevices(), nil }, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestMachineWorkerRestartCleansTwoRunningAttempts(t *testing.T) {
	dir := t.TempDir()
	h := &machineHarness{controls: map[string]AttemptDirective{}}
	for i := 0; i < 2; i++ {
		id := fmt.Sprint(i)
		h.jobs = append(h.jobs, harnessJob(t, "running-"+id, id, `touch `+filepath.Join(dir, id)+`; while :; do sleep 1; done`))
	}
	w := harnessWorker(t, h)
	child := exec.Command(os.Args[0], "-test.run=^TestMachineWorkerCrashHelper$")
	child.Env = append(os.Environ(), "MYCE_RECOVERY_TEST_HELPER=1", "MYCE_MACHINE_CRASH_HELPER=1", "MYCE_MACHINE_CRASH_STATE="+w.StateDir, "MYCE_MACHINE_CRASH_URL="+w.Client.URL)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	for i := 0; i < 2; i++ {
		awaitFile(t, filepath.Join(dir, fmt.Sprint(i)))
	}
	var before []workerState
	root, err := w.openStore()
	if err == nil {
		root.close()
		t.Fatal("second controller acquired a live machine spool")
	}
	data, err := os.ReadFile(filepath.Join(w.StateDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state workerState
	if err = json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	for _, name := range state.Attempts {
		data, err = os.ReadFile(filepath.Join(w.StateDir, name))
		if err != nil {
			t.Fatal(err)
		}
		var st workerState
		if err = json.Unmarshal(data, &st); err != nil {
			t.Fatal(err)
		}
		before = append(before, st)
	}
	if len(before) != 2 {
		t.Fatal("machine index lost a running attempt")
	}
	t.Cleanup(func() {
		for _, st := range before {
			if st.PID > 0 {
				_ = syscall.Kill(-st.PID, syscall.SIGKILL)
				_ = waitWorkerGroup(st.PID, RealClock{})
			}
		}
	})
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	startHarness(t, w, h)
	await(t, func() bool { return h.completed("running-0") && h.completed("running-1") })
	for _, st := range before {
		members, err := workerGroupMembers(st.PID)
		if err != nil || len(members) != 0 {
			t.Fatalf("restart left a running group: %v %v", members, err)
		}
		r := h.result(st.Job.ID)
		if !r.Interrupted || r.Session != state.Session || r.Attempt != 1 {
			t.Fatal("restart lost exact interruption custody")
		}
	}
}

func TestMachineInventoryAuthorityKeepsQueueAdmissionCap(t *testing.T) {
	q, clock := fixture(t)
	m := machine("inventory", "fake")
	m.TotalMemoryGB, m.FreeMemoryGB = 1000, 1000
	m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-a", Fingerprint: "fixture", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}, {ID: "1", Platform: "fake", Identity: "fake-b", Fingerprint: "fixture", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}
	huge := spec("experiment")
	huge.Needs.MemoryGB = 9
	oversized := submit(t, q, huge)
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	reply, err := q.Checkin(c)
	if err != nil || len(reply.Attempts) != 0 {
		t.Fatal("scalar capacity bypassed inventory", err)
	}
	if err := q.Cancel(oversized.ID); err != nil {
		t.Fatal(err)
	}
	first := submit(t, q, spec("experiment"))
	clock.Advance(time.Millisecond)
	second := submit(t, q, spec("experiment"))
	reply, err = q.Checkin(c)
	if err != nil || len(reply.Attempts) != 1 || reply.Attempts[0].ID != first.ID {
		t.Fatal("inventory first admission", err)
	}
	assigned := reply.Attempts[0].Job
	if !slices.Equal(assigned.AllocatedDevices, []string{"0"}) || !slices.Equal(assigned.Copies[len(assigned.Copies)-1].AllocatedDevices, []string{"0"}) {
		t.Fatal("assignment/copy lost device selection")
	}
	c.Attempts = []AttemptReport{{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Session: m.Session, Started: true}}
	reply, err = q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range reply.Attempts {
		if d.Job != nil {
			t.Fatal("M2 queue lifted one-attempt cap")
		}
	}
	requireState(t, q, second.ID, "queued")
}
func TestMachineDeviceProjectionPreservesSharedHostFacts(t *testing.T) {
	m := machine("host", "fake")
	m.CapabilityProfile.Availability = &Availability{Available: false}
	d := fakeMachineDevices()[0]
	d.TotalMemoryGB, d.FreeMemoryGB, d.BudgetGB = 32, 32, 2
	d.CapabilityProfile = &CapabilityProfile{LogicalCPUs: 1, SystemMemoryBytes: 1, OS: "wrong", Arch: "wrong"}
	got := d.machine(m)
	if got.FreeMemoryGB != 2 || got.TotalMemoryGB != 2 {
		t.Fatal("physical capacity or telemetry widened the device allowance")
	}
	if got.CapabilityProfile.LogicalCPUs != m.CapabilityProfile.LogicalCPUs || got.CapabilityProfile.SystemMemoryBytes != m.CapabilityProfile.SystemMemoryBytes || got.CapabilityProfile.Availability.Available {
		t.Fatal("device profile replaced shared CPU/RAM or participation facts")
	}
}

func TestMachineInventoryEmptyAndDuplicateOwner(t *testing.T) {
	q, _ := fixture(t)
	m := machine("cpu-machine", "fake", "physical")
	m.Devices = []MachineDevice{}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}); err != nil {
		t.Fatal(err)
	}
	if got := q.s.Machines[m.ID]; got.Devices == nil || got.GPU != "cpu" || got.FreeMemoryGB != 0 {
		t.Fatal("empty inventory restored scalar accelerator capacity")
	}
	m.ID, m.Session = "duplicate-machine", "duplicate-session"
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}); err == nil {
		t.Fatal("second machine controller for one physical host admitted")
	}
	if q.s.Machines[m.ID] != nil {
		t.Fatal("refused duplicate mutated machine ownership")
	}
}
