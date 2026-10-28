package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// The test binary is a real CPU HTTP helper, main command, and grandchild. No
// fake runner or fake HTTP readiness replaces the OS process lifecycle.
func TestNativeCPUHelper(t *testing.T) {
	mode := os.Getenv("NATIVE_TEST_MODE")
	if mode == "" {
		return
	}
	record := func(name string, v any) {
		data, _ := json.Marshal(v)
		file := filepath.Join(os.Getenv("NATIVE_TEST_DIR"), name)
		if err := os.WriteFile(file+".partial", data, 0600); err != nil {
			os.Exit(91)
		}
		if err := os.Rename(file+".partial", file); err != nil {
			os.Exit(91)
		}
	}
	if mode == "child" {
		record("child.pid", os.Getpid())
		for {
			time.Sleep(time.Hour)
		}
	}
	env := map[string]string{}
	for _, entry := range os.Environ() {
		k, v, _ := strings.Cut(entry, "=")
		env[k] = v
	}
	record(mode+".json", struct {
		PID int
		Env map[string]string
	}{os.Getpid(), env})
	fmt.Println("native-helper-output")
	if mode == "main" {
		response, err := http.Get(os.Getenv("MYCE_SIDECAR_HELPER_URL") + "/ready")
		if err != nil || response.StatusCode != 200 {
			os.Exit(92)
		}
		response.Body.Close()
		record("main-ready", true)
		if os.Getenv("NATIVE_TEST_BLOCK") == "1" {
			for {
				if _, err := os.Stat(filepath.Join(os.Getenv("NATIVE_TEST_DIR"), "finish")); err == nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
		return
	}
	exe, _ := os.Executable()
	child := exec.Command(exe, "-test.run=^TestNativeCPUHelper$")
	child.Env = append(os.Environ(), "NATIVE_TEST_MODE=child")
	if err := child.Start(); err != nil {
		os.Exit(93)
	}
	go func() { _ = child.Wait() }()
	listener, err := net.Listen("tcp4", "127.0.0.1:"+os.Getenv("MYCE_SIDECAR_PORT"))
	if err != nil {
		os.Exit(94)
	}
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		_, allowErr := os.Stat(filepath.Join(os.Getenv("NATIVE_TEST_DIR"), "allow-ready"))
		if os.Getenv("NATIVE_TEST_UNREADY") == "1" || os.Getenv("NATIVE_TEST_UNREADY") == "file" && allowErr != nil {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	})
	if err = http.Serve(listener, nil); err != nil {
		os.Exit(95)
	}
}

type nativeTestRun struct {
	p        *process
	w        Worker
	s        *workerStore
	q        *Queue
	j        *Job
	dir      string
	calls    *atomic.Int32
	finished bool
}

func nativeSpec(t *testing.T) Spec {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Resumable = false
	s.Needs.HostMemoryGB = 2
	s.Run.Command = []string{exe, "-test.run=^TestNativeCPUHelper$"}
	s.Run.Env = map[string]string{"NATIVE_TEST_MODE": "main", "NATIVE_TEST_DIR": "{work}", "NATIVE_TEST_BLOCK": "1", "GORACE": "atexit_sleep_ms=0"}
	zero := 0
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: s.Run.Command, MemoryGB: 1, Port: &zero, Readiness: &NativeReadiness{Path: "/ready", Timeout: "5s"}, Env: map[string]string{"NATIVE_TEST_MODE": "side", "NATIVE_TEST_DIR": s.Run.Env["NATIVE_TEST_DIR"], "GORACE": "atexit_sleep_ms=0"}}}
	return s
}
func nativeStart(t *testing.T, spec Spec, platform string, catalogs ...EnvironmentCatalog) *nativeTestRun {
	t.Helper()
	q, _ := fixture(t)
	for _, catalog := range catalogs {
		if err := q.ConfigureEnvironments(catalog); err != nil {
			t.Fatal(err)
		}
	}
	j := submit(t, q, spec)
	m := machine("native", platform, "native-host")
	m.Device = "1"
	m.Capabilities = append(m.Capabilities, "logs_none")
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB = 64
	m.HostAvailableMemoryGB = 64
	reply := check(t, q, m, nil)
	if reply.Job == nil {
		t.Fatal("no assignment", reply)
	}
	calls := &atomic.Int32{}
	handler := API{Queue: q, Token: "fleet-private-test-token"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/launch" {
			calls.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	w := Worker{environmentCache: &EnvironmentCache{Root: t.TempDir(), Limit: 512 << 20, Clock: RealClock{}}, Clock: RealClock{}, StateDir: t.TempDir(), Machine: m, Client: Client{URL: server.URL, Token: "fleet-private-test-token"}, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	// Synthetic lifecycle tests own their helper. Host listener discovery is
	// exercised directly by TestNativeGroupForeignReadinessListener.
	w.nativeListenerProbe = func(string, int) error { return nil }
	if platform == "cuda" || platform == "xpu" {
		w.nativeGPUDeviceProbe = syntheticNativeGPUDeviceProbe(MachineDevice{ID: "1", Platform: platform, Selector: "1"})
	}
	store, err := w.openStore()
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	store.state.Session = m.Session
	var p *process
	// Reuse the pending input copy before exercising the native lifecycle.
	await(t, func() bool {
		p, err = w.start(store, *reply.Job)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		store.close()
		server.Close()
		t.Fatal(err)
	}
	run := &nativeTestRun{p: p, w: w, s: store, q: q, j: j, dir: p.job.Spec.Run.Workdir, calls: calls}
	t.Cleanup(func() {
		if !run.finished {
			p.stop(w.Clock.Now(), 0, true)
			select {
			case <-p.done:
			case <-time.After(10 * time.Second):
				t.Error("native cleanup timed out")
			}
		}
		store.close()
		server.Close()
		nativeNoSurvivors(t, run)
	})
	return run
}
func nativeNoSurvivors(t *testing.T, run *nativeTestRun) {
	t.Helper()
	members, err := workerGroupMembers(run.p.cmd.Process.Pid)
	if err != nil || len(members) > 0 {
		t.Errorf("surviving group: %v %v", members, err)
	}
	// Independent PID inventory checks the actual recorded member/grandchild PIDs.
	out, err := exec.Command("ps", "-eo", "pid=,stat=").Output()
	if err != nil {
		t.Fatal(err)
	}
	pids := map[int]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && !strings.HasPrefix(f[1], "Z") {
			pid, _ := strconv.Atoi(f[0])
			pids[pid] = true
		}
	}
	for _, name := range []string{"side.json", "main.json", "child.pid"} {
		data, err := os.ReadFile(filepath.Join(run.dir, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var pid int
		if name == "child.pid" {
			_ = json.Unmarshal(data, &pid)
		} else {
			var v struct{ PID int }
			_ = json.Unmarshal(data, &v)
			pid = v.PID
		}
		if pids[pid] {
			t.Errorf("surviving helper PID %d (%s)", pid, name)
		}
	}
}

// Resolve the single worker fixture's actual attempt after normal input staging.
func nativeWorkerWork(t *testing.T, stateDir string) string {
	t.Helper()
	var work string
	await(t, func() bool {
		data, err := readSingleAttemptState(filepath.Join(stateDir, "state.json"))
		if err != nil {
			return false
		}
		var state workerState
		if json.Unmarshal(data, &state) != nil || state.Job == nil {
			return false
		}
		work = state.Job.Spec.Run.Workdir
		return filepath.IsAbs(work)
	})
	return work
}

func nativeFile(t *testing.T, dir, name string) {
	t.Helper()
	await(t, func() bool { _, err := os.Stat(filepath.Join(dir, name)); return err == nil })
}
func nativeFinish(t *testing.T, r *nativeTestRun) Result {
	t.Helper()
	select {
	case f := <-r.p.done:
		r.finished = true
		if f.err != nil {
			t.Fatal(f.err)
		}
		if f.result.ReleasePending {
			t.Fatal("cleanup unconfirmed", f.result)
		}
		if !f.result.Success {
			if data, err := os.ReadFile(filepath.Join(r.s.dir, r.s.state.Log)); err == nil {
				t.Logf("native output: %s", data)
			}
		}
		return f.result
	case <-time.After(10 * time.Second):
		t.Fatal("native group did not finish")
		return Result{}
	}
}
func nativeTouch(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
		t.Fatal(err)
	}
}
func nativeEnv(t *testing.T, dir, name string) map[string]string {
	t.Helper()
	nativeFile(t, dir, name+".json")
	data, _ := os.ReadFile(filepath.Join(dir, name+".json"))
	var v struct{ Env map[string]string }
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v.Env
}

func TestNativeGroupReadyEnvironmentAndCleanup(t *testing.T) {
	for _, platform := range []string{"cuda", "xpu"} {
		t.Run(platform, func(t *testing.T) {
			t.Setenv("MYCE_TOKEN", "ambient-private-test-token")
			t.Setenv("CUDA_VISIBLE_DEVICES", "wrong")
			t.Setenv("ZE_AFFINITY_MASK", "wrong")
			t.Setenv("ONEAPI_DEVICE_SELECTOR", "wrong")
			s := nativeSpec(t)
			s.Run.Env["HELPER_URL"] = "{sidecars.helper.url}"
			s.Run.Sidecars[0].Env["PORT_ARG"] = "{port}"
			r := nativeStart(t, s, platform)
			nativeFile(t, r.dir, "main-ready")
			nativeFile(t, r.dir, "child.pid")
			main, side := nativeEnv(t, r.dir, "main"), nativeEnv(t, r.dir, "side")
			for _, env := range []map[string]string{main, side} {
				if _, ok := env["MYCE_TOKEN"]; ok {
					t.Fatal("fleet token inherited")
				}
				for key, value := range selectedDeviceEnv(platform, "1") {
					if env[key] != value {
						t.Fatalf("binding %s=%q", key, env[key])
					}
				}
				if env["MYCE_JOB_ID"] != r.j.ID || env["MYCE_ATTEMPT"] != "1" {
					t.Fatal("attempt env missing")
				}
			}
			if main["HELPER_URL"] != main["MYCE_SIDECAR_HELPER_URL"] || side["MYCE_SIDECAR_PORT"] != main["MYCE_SIDECAR_HELPER_PORT"] || side["PORT_ARG"] != side["MYCE_SIDECAR_PORT"] {
				t.Fatal("endpoint env mismatch")
			}
			if platform == "cuda" && (side["ZE_AFFINITY_MASK"] != "2147483647" || side["ONEAPI_DEVICE_SELECTOR"] != "!*:*") || platform == "xpu" && side["CUDA_VISIBLE_DEVICES"] != "" {
				t.Fatal("irrelevant selector inherited")
			}
			status, err := r.q.HostMaintenanceStatus("native-host")
			if err != nil || status.Sidecars != 1 || status.Quiescent {
				t.Fatal("missing shared work enumeration", status, err)
			}
			nativeTouch(t, r.dir, "finish")
			if result := nativeFinish(t, r); !result.Success {
				t.Fatal(result)
			}
			if r.calls.Load() < 5 {
				t.Fatal("missing per-member authorization", r.calls.Load())
			}
		})
	}
}
func TestNativeGroupReadinessTimeout(t *testing.T) {
	s := nativeSpec(t)
	s.Run.Sidecars[0].Readiness.Timeout = "150ms"
	s.Run.Sidecars[0].Env["NATIVE_TEST_UNREADY"] = "1"
	r := nativeStart(t, s, "fake")
	result := nativeFinish(t, r)
	if result.Success || result.Interrupted || !strings.Contains(result.Error, "timeout") {
		t.Fatal(result)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "main.json")); !os.IsNotExist(err) {
		t.Fatal("main started before readiness")
	}
}
func TestNativeGroupOccupiedPort(t *testing.T) {
	occupied, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	s := nativeSpec(t)
	port := occupied.Addr().(*net.TCPAddr).Port
	s.Run.Sidecars[0].Port = &port
	r := nativeStart(t, s, "fake")
	result := nativeFinish(t, r)
	if result.Success || !result.Interrupted || !strings.Contains(result.Error, "port") {
		t.Fatal(result)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "side.json")); !os.IsNotExist(err) {
		t.Fatal("helper started on occupied port")
	}
}
func TestNativeGroupMemberDeath(t *testing.T) {
	s := nativeSpec(t)
	r := nativeStart(t, s, "fake")
	nativeFile(t, r.dir, "main-ready")
	nativeFile(t, r.dir, "child.pid")
	data, _ := os.ReadFile(filepath.Join(r.dir, "side.json"))
	var v struct{ PID int }
	_ = json.Unmarshal(data, &v)
	if err := syscall.Kill(v.PID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	result := nativeFinish(t, r)
	if result.Success || result.Interrupted || !strings.Contains(result.Error, "sidecar helper exited") {
		t.Fatal(result)
	}
}
func TestNativeGroupCancellationPreparing(t *testing.T) {
	s := nativeSpec(t)
	s.Run.Sidecars[0].Env["NATIVE_TEST_UNREADY"] = "1"
	r := nativeStart(t, s, "fake")
	nativeFile(t, r.dir, "child.pid")
	r.p.stop(r.w.Clock.Now(), time.Second, false)
	result := nativeFinish(t, r)
	if !result.Interrupted || result.Success {
		t.Fatal(result)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "main.json")); !os.IsNotExist(err) {
		t.Fatal("main started after cancellation")
	}
}
func TestNativeGroupDistinctPortsAndLogOptOut(t *testing.T) {
	s1 := nativeSpec(t)
	s1.Logs = "none"
	s2 := nativeSpec(t)
	r1, r2 := nativeStart(t, s1, "fake"), nativeStart(t, s2, "fake")
	nativeFile(t, r1.dir, "main-ready")
	nativeFile(t, r2.dir, "main-ready")
	e1, e2 := nativeEnv(t, r1.dir, "main"), nativeEnv(t, r2.dir, "main")
	if e1["MYCE_SIDECAR_HELPER_PORT"] == e2["MYCE_SIDECAR_HELPER_PORT"] {
		t.Fatal("concurrent port collision")
	}
	for _, r := range []*nativeTestRun{r1, r2} {
		nativeTouch(t, r.dir, "finish")
		if result := nativeFinish(t, r); !result.Success {
			t.Fatal(result)
		}
	}
	files, _ := filepath.Glob(filepath.Join(r1.s.dir, "*.log"))
	if len(files) != 0 || r1.s.state.Log != "" {
		t.Fatal("logs:none persisted payload logs")
	}
}
func TestNativeGroupLateLaunchDenied(t *testing.T) {
	s := nativeSpec(t)
	s.Run.Sidecars[0].Env["NATIVE_TEST_UNREADY"] = "file"
	r := nativeStart(t, s, "fake")
	nativeFile(t, r.dir, "child.pid")
	if err := r.q.Cancel(r.j.ID); err != nil {
		t.Fatal(err)
	}
	nativeTouch(t, r.dir, "allow-ready")
	result := nativeFinish(t, r)
	if !result.Interrupted {
		t.Fatal(result)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "main.json")); !os.IsNotExist(err) {
		t.Fatal("main launched after queue cancellation")
	}
}

func TestNativeGroupNamedEnvironment(t *testing.T) {
	s := nativeSpec(t)
	recipe, err := snapshotEnvironment(EnvironmentRecipe{Binary: s.Run.Command[0]}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	s.Run.Sidecars[0].Environment = "tools"
	s.Run.Sidecars[0].Command = append([]string{"{env.tools.root}/bin/" + filepath.Base(s.Run.Command[0])}, s.Run.Command[1:]...)
	s.Run.Sidecars[0].Env["HELPER_ROOT"] = "{env.tools.root}"
	r := nativeStart(t, s, "fake", EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": recipe}}})
	nativeFile(t, r.dir, "main-ready")
	env := nativeEnv(t, r.dir, "side")
	want := filepath.Join(r.w.environmentCache.Root, recipe.Key)
	if env["MYCE_ENVIRONMENT_DIR"] != want || env["HELPER_ROOT"] != want {
		t.Fatal("helper environment not bound")
	}
	nativeTouch(t, r.dir, "finish")
	if result := nativeFinish(t, r); !result.Success {
		t.Fatal(result)
	}
}
func TestNativeGroupForeignReadinessListener(t *testing.T) {
	s := nativeSpec(t)
	r := nativeStart(t, s, "fake")
	nativeFile(t, r.dir, "main-ready")
	env := nativeEnv(t, r.dir, "main")
	if err := verifyNativeListener(env["MYCE_SIDECAR_HELPER_PORT"], r.p.cmd.Process.Pid); err != nil {
		t.Fatal("refused owned HTTP listener", err)
	}
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }))
	defer foreign.Close()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(foreign.URL, "http://"))
	if err := verifyNativeListener(port, r.p.cmd.Process.Pid); err == nil {
		t.Fatal("accepted another process's HTTP listener")
	}
	nativeTouch(t, r.dir, "finish")
	if result := nativeFinish(t, r); !result.Success {
		t.Fatal(result)
	}
}
func TestNativeWorkerCancellationPreparation(t *testing.T) {
	s := nativeSpec(t)
	s.Run.Sidecars[0].Env["NATIVE_TEST_UNREADY"] = "1"
	q, _ := fixture(t)
	q.clock = &workerFakeClock{now: q.clock.Now()}
	j := submit(t, q, s)
	run := workerFixture(t, q)
	dir := nativeWorkerWork(t, run.state)
	nativeFile(t, dir, "child.pid")
	if value := job(t, q, j.ID); value.StartedAt != nil {
		t.Fatal("readiness preparation marked started")
	}
	data, err := readSingleAttemptState(filepath.Join(run.state, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state workerState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return job(t, q, j.ID).State == "cancelled" && job(t, q, j.ID).ReleaseConfirmed })
	if result := run.result(t); !result.Interrupted || result.ReleasePending {
		t.Fatal(result)
	}
	nativeNoSurvivors(t, &nativeTestRun{p: &process{cmd: &exec.Cmd{Process: &os.Process{Pid: state.PID}}}, dir: dir})
}
func TestNativeGroupValidation(t *testing.T) {
	for _, kind := range []string{"reserved-name", "collision", "ram", "token", "device", "remote-readiness", "invalid-readiness", "docker"} {
		t.Run(kind, func(t *testing.T) {
			s := nativeSpec(t)
			switch kind {
			case "reserved-name":
				s.Run.Sidecars[0].Name = "main"
			case "collision":
				s.Run.Sidecars[0].Name = "a-b"
				other := s.Run.Sidecars[0]
				other.Name = "a.b"
				s.Run.Sidecars = append(s.Run.Sidecars, other)
			case "ram":
				s.Run.Sidecars[0].MemoryGB = 3
			case "token":
				s.Run.Sidecars[0].Env["MYCE_TOKEN"] = "override"
			case "device":
				s.Run.Sidecars[0].Env["CUDA_VISIBLE_DEVICES"] = "other"
			case "remote-readiness":
				s.Run.Sidecars[0].Readiness.Path = "//other/health"
			case "invalid-readiness":
				s.Run.Sidecars[0].Readiness.Path = "/bad%url"
			case "docker":
				s.Run.Docker = true
			}
			if s.Validate() == nil {
				t.Fatal("invalid group accepted")
			}
		})
	}
}

func TestNativeGroupInfrastructureLeaveRequeues(t *testing.T) {
	s := nativeSpec(t)
	s.Resumable = true
	r := nativeStart(t, s, "fake")
	nativeFile(t, r.dir, "main-ready")
	if err := r.q.Leave("native", false); err != nil {
		t.Fatal(err)
	}
	r.p.stop(r.w.Clock.Now(), time.Second, false)
	result := nativeFinish(t, r)
	if !result.Interrupted || result.Success {
		t.Fatal(result)
	}
	if err := r.q.Complete(result); err != nil {
		t.Fatal(err)
	}
	current := job(t, r.q, r.j.ID)
	if current.State != "queued" || current.Losses != 0 {
		t.Fatal("infrastructure stop consumed stage/loss budget", current.State, current.Losses)
	}
}
