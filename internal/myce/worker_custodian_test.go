package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mycelium/internal/probeguard"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The subprocess uses the ordinary worker/runner and exact production custodian
// entry point. Only inventory/host facts are synthetic, as in myce dev.
func TestCustodianServiceHelper(t *testing.T) {
	marker := -1
	for i, a := range os.Args {
		if a == "--" {
			marker = i
			break
		}
	}
	if marker < 0 {
		return
	}
	args := os.Args[marker+1:]
	config, id := "", ""
	for i := 1; i+1 < len(args); i += 2 {
		if args[i] == "--config" {
			config = args[i+1]
		}
		if args[i] == "--attempt-custodian" {
			id = args[i+1]
		}
	}
	var c ServiceConfig
	data, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	unit, err := CurrentWorkerUnit(id)
	if err != nil {
		t.Fatal(err)
	}
	m := machine(c.Name, "fake", c.HostID)
	m.HostMemoryBudgetGB, m.HostMemoryMeasured = 8, true
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	w := Worker{EnvironmentLimit: 1 << 28, Clock: RealClock{}, StateDir: c.State, ServiceConfig: &c, Custodian: id, Unit: unit, Client: Client{URL: c.Queue, Token: "custodian-test"}, Machine: m, Interval: 25 * time.Millisecond, CPUIsolationProbe: func(context.Context) error { return nil }}
	if w.ModelConfig, err = ReadModels(c.ModelCatalog); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "darwin" {
		probeguard.Before = nil // Explicit native RSS sensor tier; inventory stays synthetic.
		w.processMemory = localProcessMemory()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	if id == "" {
		if err := PrepareStateRoots(c.State); err != nil {
			t.Fatal(err)
		}
		_ = os.WriteFile(filepath.Join(c.State, "worker-image"), []byte(BuildVersion), 0600)
		_ = os.WriteFile(filepath.Join(c.State, "worker-pid"), []byte(fmt.Sprint(os.Getpid())), 0600)
	}
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

// Every native mode shares this configuration and its isolated slice.
func nativeCustodianServiceConfig(dir, queue string) ServiceConfig {
	return ServiceConfig{State: filepath.Join(dir, "state"), Binary: filepath.Join(dir, "myce"), Config: filepath.Join(dir, "config.json"), Name: "custodian-test", HostID: "custodian-test-host", Queue: queue, Service: "myce-worker@custodian-test-" + ID() + ".service", Slice: "mycetest" + ID() + ".slice"}
}

func checkNativeFixtureSlice(c ServiceConfig) error {
	id := strings.TrimSuffix(strings.TrimPrefix(c.Slice, "mycetest"), ".slice")
	if c.Slice != "mycetest"+id+".slice" || !validCustodianID(id) {
		return fmt.Errorf("native fixture requires its own unique slice, got %q", c.workerSlice())
	}
	return c.validateWorkerSlice()
}

func nativeCustodianStartArgs(c ServiceConfig) []string {
	return []string{"systemd-run", "--user", "--collect", "--quiet", "--unit=" + c.Service, "--property=Delegate=yes", "--slice=" + c.workerSlice(), "--property=KillMode=mixed"}
}

func TestCustodianNativeSliceIsolation(t *testing.T) {
	seen := map[string]bool{}
	for range 3 {
		c := nativeCustodianServiceConfig(t.TempDir(), "http://queue")
		if err := checkNativeFixtureSlice(c); err != nil || seen[c.Slice] {
			t.Fatalf("fixture slice is not unique: %q %v", c.Slice, err)
		}
		seen[c.Slice] = true
		args := strings.Join(nativeCustodianStartArgs(c), "\n")
		if !strings.Contains(args, "--slice="+c.Slice+"\n") || strings.Contains(args, "--slice=myce.slice") {
			t.Fatal("native fixture command targets the live slice", args)
		}
		for _, slice := range []string{"", "myce.slice", "myce-jobs.slice", "myce-test.slice"} {
			c.Slice = slice
			if checkNativeFixtureSlice(c) == nil {
				t.Fatalf("native fixture could alter the live slice: %q", slice)
			}
		}
	}
}

// Opt-in test tier: invokes only uniquely named, test-owned native manager jobs.
// Unlike fake managers this proves service teardown cannot reach running work.
func TestCustodianNativeWorkerReplacement(t *testing.T) {
	if os.Getenv("MYCE_NATIVE_CUSTODIAN_TEST") != "1" {
		t.Skip("native service-manager tier")
	}
	dir := t.TempDir()
	next := os.Getenv("MYCE_NATIVE_REPLACEMENT_BINARY")
	if next == "" {
		next = filepath.Join(dir, "next-worker")
		build := exec.Command("go", "test", "-c", "-p", "1", "-ldflags", "-X mycelium/internal/myce.BuildVersion=my55-next", "-o", next, ".")
		build.Env = append(os.Environ(), "GOMAXPROCS=2")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build replacement: %v\n%s", err, out)
		}
	}
	for _, mode := range []string{"plain", "native", "cancel", "memory", "preparation", "publication", "model"} {
		t.Run(mode, func(t *testing.T) { custodianNativeReplacement(t, next, mode) })
	}
}

func custodianNativeReplacement(t *testing.T, next, mode string) {
	migration := strings.HasPrefix(mode, "migration")
	modelMode := mode == "model" || mode == "migration-model"
	q, _ := fixture(t)
	q.clock = RealClock{}
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	entered, unblock := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseRequest := func() { releaseOnce.Do(func() { close(unblock) }) }
	handler := API{Queue: q, Token: "custodian-test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mode == "preparation" && r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/files/blobs/") ||
			mode == "publication" && r.Method == "POST" && r.URL.Path == "/api/workers/complete" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-unblock:
			case <-r.Context().Done():
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	defer releaseRequest()
	dir := t.TempDir()

	c := nativeCustodianServiceConfig(dir, server.URL)
	if migration {
		if runtime.GOOS != "linux" {
			t.Skip("systemd migration native tier")
		}
		runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
		if !filepath.IsAbs(runtimeDir) {
			t.Fatal("native fixture requires user manager runtime directory")
		}
		c.Service = filepath.Join(runtimeDir, "systemd", "user", c.Service)
		c.Role, c.Detection = "worker", Detection{OS: "linux"}
	}
	if err := checkNativeFixtureSlice(c); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "linux" {
		// Only reads target the installed slice. Fixture workers and detached
		// scopes share a separate per-run slice, including during cleanup.
		liveProperties := func() string {
			t.Helper()
			out, err := exec.Command("systemctl", "--user", "show", "myce.slice", "--property=MemoryMax", "--property=MemorySwapMax").CombinedOutput()
			if err != nil {
				t.Fatalf("read live slice memory properties: %v: %s", err, out)
			}
			return strings.TrimSpace(string(out))
		}
		before := liveProperties()
		t.Logf("live myce.slice before fixture: %s", before)
		t.Cleanup(func() {
			after := liveProperties()
			t.Logf("live myce.slice after fixture: %s", after)
			if after != before {
				t.Error("native fixture changed live slice memory properties")
			}
		})
		t.Cleanup(func() {
			for _, args := range [][]string{{"stop", c.custodianSlice(), c.workerSlice()}, {"revert", c.workerSlice()}} {
				if out, err := exec.Command("systemctl", append([]string{"--user"}, args...)...).CombinedOutput(); err != nil {
					t.Errorf("remove fixture slice: %v: %s", err, out)
				}
			}
		})
	}
	if modelMode {
		configureNativeModel(t, q, &c)
	}
	if err := os.MkdirAll(c.State, 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(c)
	if err := os.WriteFile(c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	shellQuote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	writeImage := func(binary string) {
		t.Helper()
		script := "#!/bin/sh\nexport MYCE_TEST_ROOT=" + shellQuote(os.Getenv("MYCE_TEST_ROOT")) + "\nexport PATH=/usr/bin:/bin:/usr/sbin:/sbin\nexec " + shellQuote(binary) + " -test.run='^TestCustodianServiceHelper$' -- \"$@\"\n"
		tmp := c.Binary + ".tmp"
		if err := os.WriteFile(tmp, []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, c.Binary); err != nil {
			t.Fatal(err)
		}
	}
	writeImage(os.Args[0])
	command := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput()
		if err != nil {
			t.Fatalf("native manager %s: %v\n%s", args[0], err, out)
		}
	}
	label := "local.mycelium.worker-test." + ID()
	plist := filepath.Join(dir, "worker.plist")
	if runtime.GOOS == "darwin" {
		text := fmt.Sprintf(`<?xml version="1.0"?><plist version="1.0"><dict><key>Label</key><string>%s</string><key>ProgramArguments</key><array><string>%s</string><string>service</string><string>--config</string><string>%s</string></array><key>RunAtLoad</key><true/><key>StandardOutPath</key><string>%s</string><key>StandardErrorPath</key><string>%s</string></dict></plist>`, label, c.Binary, c.Config, filepath.Join(dir, "worker.log"), filepath.Join(dir, "worker.log"))
		if err := os.WriteFile(plist, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	manager := installManager{os: runtime.GOOS, command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, name, args...).CombinedOutput()
	}}
	if migration {
		if err := os.MkdirAll(filepath.Dir(c.Service), 0700); err != nil {
			t.Fatal(err)
		}
		text := "[Service]\nExecStart=" + unitQuote(c.Binary) + " service --config " + unitQuote(c.Config) + "\nSlice=app.slice\nOOMScoreAdjust=200\nDelegate=yes\nKillMode=mixed\nStandardOutput=append:" + filepath.Join(dir, "worker.log") + "\nStandardError=inherit\n"
		f, err := os.OpenFile(c.Service, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString(text)
		err = errors.Join(err, f.Close())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			// Exact unique fixture files only; no installed unit or shared slice.
			// Also cover a failure during initial startup or migration staging,
			// before the ordinary stop defer can complete.
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if out, err := manager.command(cleanup, "systemctl", "--user", "stop", filepath.Base(c.Service)); err != nil {
				t.Errorf("stop owned migration fixture: %v: %s", err, out)
			}
			for _, path := range []string{filepath.Join(c.Service+".d", workerUnitMigrationDropIn), c.Service + ".d", c.Service} {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					t.Error(err)
				}
			}
			command("systemctl", "--user", "daemon-reload")
		})
		command("systemctl", "--user", "daemon-reload")
	}
	unit := installUnit{path: plist, label: label}
	starts := 0
	start := func() {
		t.Helper()
		if migration {
			command("systemctl", "--user", "start", filepath.Base(c.Service))
			want := "unavailable"
			if starts > 0 {
				want = "available"
			}
			await(t, func() bool {
				q.mu.Lock()
				defer q.mu.Unlock()
				m := q.s.Machines[c.Name]
				return m != nil && m.MemoryContainment != nil && m.MemoryContainment.State == want
			})
			if starts > 0 {
				out, err := manager.command(t.Context(), "systemctl", "--user", "show", c.workerSlice(), "--property=MemoryMax", "--value")
				if err != nil || strings.TrimSpace(string(out)) == "infinity" || strings.TrimSpace(string(out)) == "" {
					t.Fatalf("joint limit missing: %q %v", out, err)
				}
				t.Logf("migrated fixture reports available; slice MemoryMax=%s", strings.TrimSpace(string(out)))
				pid, err := os.ReadFile(filepath.Join(c.State, "worker-pid"))
				if err != nil {
					t.Fatal(err)
				}
				score, err := os.ReadFile("/proc/" + strings.TrimSpace(string(pid)) + "/oom_score_adj")
				if err != nil {
					t.Fatal(err)
				}
				setting, err := manager.command(t.Context(), "systemctl", "--user", "show", filepath.Base(c.Service), "--property=OOMScoreAdjust", "--value")
				if err != nil || strings.TrimSpace(string(setting)) != "0" {
					t.Fatalf("replacement unit OOM setting: %q %v", setting, err)
				}
				// A user manager can retain a privileged inherited minimum (100
				// on the native qualification host). setAttemptOOMScore handles
				// that bound; a unit's requested zero is not proof of process zero.
				t.Logf("replacement unit requests OOMScoreAdjust=0; observed worker score=%s", strings.TrimSpace(string(score)))
			}
			starts++
		} else if runtime.GOOS == "darwin" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := manager.registration(ctx, unit, installUnitState{loaded: true}); err != nil {
				t.Fatal(err)
			}
		} else {
			args := nativeCustodianStartArgs(c)
			if modelMode {
				args = append(args, "--property=StandardOutput=append:"+filepath.Join(dir, "worker.log"), "--property=StandardError=inherit")
			}
			command(append(args, c.Binary, "service", "--config", c.Config)...)
		}
	}
	staged := false
	stop := func() {
		t.Helper()
		if migration && !staged {
			pid, err := os.ReadFile(filepath.Join(c.State, "worker-pid"))
			if err != nil {
				t.Fatal(err)
			}
			membership := "/proc/" + strings.TrimSpace(string(pid)) + "/cgroup"
			before, err := os.ReadFile(membership)
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				status, err := StageWorkerUnitMigration(t.Context(), c, manager.command)
				if err != nil || status.Reason != "worker_unit_migration_staged" {
					t.Fatalf("stage fixture migration: %+v %v", status, err)
				}
			}
			if after, err := os.ReadFile(membership); err != nil || string(after) != string(before) {
				t.Fatalf("staging moved live worker: %q %v", after, err)
			}
			staged = true
			t.Log("staged migration twice while running; no service stop or scope move")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := manager.stop(ctx, installUnit{path: c.Service, label: label}); err != nil {
			t.Error(err)
		}
	}

	// Read unique fixture diagnostics even when an assertion interrupts cleanup.
	defer func() {
		if t.Failed() {
			data, _ := os.ReadFile(filepath.Join(dir, "worker.log"))
			t.Logf("worker diagnostics: %s", data)
			logs, _ := filepath.Glob(filepath.Join(custodianDir(c.State), "*.log"))
			for _, p := range logs {
				b, _ := os.ReadFile(p)
				t.Logf("custodian diagnostics: %s", b)
			}
		}
	}()
	defer func() {
		// These labels/units are exclusively owned by this fixture. On failure,
		// cancel through custody first, then remove only its test manager objects.
		requests, _ := filepath.Glob(filepath.Join(custodianDir(c.State), "*.request.json"))
		for _, path := range requests {
			var request custodianRequest
			b, _ := os.ReadFile(path)
			if json.Unmarshal(b, &request) != nil || request.State.Job == nil {
				continue
			}
			var snapshot custodianSnapshot
			if readCustodianJSON(custodianDir(c.State), custodianName(request.ID, "snapshot"), &snapshot) == nil && !snapshot.Finished {
				now := time.Now()
				control := custodianControl{ID: request.ID, Execution: Execution{ID: request.State.Job.ID, Attempt: request.State.Job.Attempt}, Session: request.State.Session, StopAt: &now, Kill: true, UserCancel: true}
				_ = saveWorkerJSON(custodianDir(c.State), custodianName(request.ID, "control"), control)
				await(t, func() bool {
					var v custodianSnapshot
					return readCustodianJSON(custodianDir(c.State), custodianName(request.ID, "snapshot"), &v) == nil && v.Finished
				})
			}
			if runtime.GOOS == "darwin" {
				_ = exec.Command("launchctl", "bootout", fmt.Sprintf("gui/%d/local.mycelium.attempt.%s", os.Getuid(), request.ID)).Run()
			} else {
				_ = exec.Command("systemctl", "--user", "stop", custodianUnit(request.ID)).Run()
			}
		}
	}()
	start()
	defer stop()
	if modelMode {
		custodianNativeModelReplacement(t, q, c, server.URL, next, start, stop, writeImage)
		return
	}
	s := cpuSpec(1, 1)
	s.Resumable = false
	_, input := retentionUpload(t, Client{URL: server.URL, Token: "custodian-test"}, "fixture input")
	s.Files = FileManifest{"input": input}
	s.Outputs = map[string]string{"out.txt": "results"}
	s.Run.Command = []string{"/bin/sh", "-c", `printf 'before\n'; echo $$ > "$MYCE_CHECKPOINT_DIR/payload-pid"; while [ ! -f "$MYCE_CHECKPOINT_DIR/finish" ]; do printf 'tick\n'; sleep 0.05; done; printf 'after\n'; printf 'accepted\n' > "$MYCE_CHECKPOINT_DIR/output"; printf 'accepted\n' > out.txt`}
	if mode == "native" {
		s.Run.Sidecars = []NativeSidecar{{Name: "helper", MemoryGB: 0.25, Command: []string{"/bin/sleep", "600"}}}
	}
	if mode == "memory" {
		limit := 0.125
		s = cpuSpec(1, limit)
		s.MemoryLimitGB = &limit
		s.Resumable = false
		s.Run.Command = []string{os.Args[0], "-test.run=^TestCustodianMemoryPayload$"}
		s.Run.Env = map[string]string{"MY55_MEMORY_PAYLOAD": "1", "MYCE_TEST_ROOT": os.Getenv("MYCE_TEST_ROOT")}
	}

	j := submit(t, q, s)
	var before custodianSnapshot
	if mode == "preparation" {
		await(t, func() bool {
			select {
			case <-entered:
				return true
			default:
				return false
			}
		})
		await(t, func() bool {
			files, _ := filepath.Glob(filepath.Join(custodianDir(c.State), "*.snapshot.json"))
			return len(files) == 1 && readCustodianJSON(custodianDir(c.State), filepath.Base(files[0]), &before) == nil
		})
		if before.Started || before.Finished || job(t, q, j.ID).StartedAt != nil {
			t.Fatal("fixture did not hold input preparation")
		}
		stop()
		// Only the parent proxy publishes the admission ledger. Requiring a
		// new ledger after the old service exits proves the replacement has
		// adopted this preparing custodian before its input is released.
		ledger := filepath.Join(custodianDir(c.State), custodianName(before.ID, "ledger"))
		if err := os.Remove(ledger); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		writeImage(next)
		start()
		await(t, func() bool {
			image, _ := os.ReadFile(filepath.Join(c.State, "worker-image"))
			b, e := readSingleAttemptState(filepath.Join(c.State, "state.json"))
			_, ledgerErr := os.Stat(ledger)
			var st workerState
			return string(image) == "my55-next" && e == nil && json.Unmarshal(b, &st) == nil && st.Custodian == before.ID && ledgerErr == nil
		})
		var after custodianSnapshot
		if err := readCustodianJSON(custodianDir(c.State), custodianName(before.ID, "snapshot"), &after); err != nil || after.PID != before.PID || after.Birth != before.Birth || after.Started || after.Finished {
			t.Fatalf("preparation did not survive replacement: %+v %v", after, err)
		}
		releaseRequest()
	}
	await(t, func() bool {
		files, _ := filepath.Glob(filepath.Join(custodianDir(c.State), "*.snapshot.json"))
		if len(files) != 1 {
			return false
		}
		b, e := os.ReadFile(files[0])
		return e == nil && json.Unmarshal(b, &before) == nil && before.Started
	})
	if runtime.GOOS == "linux" {
		if err := verifyCustodianIsolation(before.ID, before.PID, before.Cgroup); err != nil {
			t.Fatal(err)
		}
		var gate custodianGate
		var receipt custodianGateReceipt
		var ledger custodianLedger
		for kind, value := range map[string]any{"gate": &gate, "gate-receipt": &receipt, "ledger": &ledger} {
			if err := readCustodianJSON(custodianDir(c.State), custodianName(before.ID, kind), value); err != nil {
				t.Fatal(err)
			}
		}
		if gate.Nonce == "" || receipt.Nonce != gate.Nonce || receipt.Error != "" {
			t.Fatalf("launch gate not serviced: %+v %+v", gate, receipt)
		}
		t.Log("real systemd worker service -> scope/worker -> exact gate receipt and ledger -> command started")
	}
	await(t, func() bool { return job(t, q, j.ID).StartedAt != nil })
	assigned := job(t, q, j.ID)
	original := assigned.Attempt
	session := assigned.Session
	checkpoint := before.State.Job.Checkpoint
	if !needsCheckpoint(before.State.Job) {
		checkpoint = filepath.Join(c.State, "checkpoints", j.ID, fmt.Sprintf("attempt-%d", original))
	} else {
		checkpoint = attemptCheckpoint(*before.State.Job)
	}
	var payload int
	await(t, func() bool {
		b, e := os.ReadFile(filepath.Join(checkpoint, "payload-pid"))
		if e != nil {
			return false
		}
		_, e = fmt.Sscan(string(b), &payload)
		return e == nil
	})
	birth, err := processBirth(payload)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "publication" {
		if err := os.WriteFile(filepath.Join(checkpoint, "finish"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(testWaitTimeout):
			t.Fatal("result publication did not reach the blocked queue endpoint")
		}
		var saved custodianSnapshot
		if err := readCustodianJSON(custodianDir(c.State), custodianName(before.ID, "snapshot"), &saved); err != nil || !saved.Finished || saved.State.Result == nil || saved.State.Result.ReleasePending {
			t.Fatalf("attempt has no replayable result: %+v %v", saved, err)
		}
		got := job(t, q, j.ID)
		if got.State != "running" || got.ReleaseConfirmed || got.Attempt != original || got.Session != session {
			t.Fatalf("test did not hold the exact completion: %+v", got)
		}
		stale := *saved.State.Result
		stale.Attempt++
		if err := q.Complete(stale); !errors.Is(err, ErrStale) {
			t.Fatalf("different attempt was accepted: %v", err)
		}
	}
	if mode != "preparation" {
		stop()
	}
	if mode == "memory" {
		if err := os.WriteFile(filepath.Join(checkpoint, "finish"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool {
			var v custodianSnapshot
			return readCustodianJSON(custodianDir(c.State), custodianName(before.ID, "snapshot"), &v) == nil && v.Finished && v.State.Result != nil && v.State.Result.OutOfMemory && v.State.Result.ExecutionCause != nil
		})
	}
	if mode != "memory" && mode != "publication" {
		if current, e := processBirth(payload); e != nil || current != birth {
			t.Fatalf("service teardown interrupted payload: %v", e)
		}
	}
	if mode != "preparation" {
		writeImage(next)
		start()
	}
	await(t, func() bool {
		b, _ := os.ReadFile(filepath.Join(c.State, "worker-image"))
		return string(b) == "my55-next"
	})
	if mode != "memory" {
		await(t, func() bool {
			b, e := readSingleAttemptState(filepath.Join(c.State, "state.json"))
			var st workerState
			if e != nil || json.Unmarshal(b, &st) != nil || st.Custodian != before.ID {
				return false
			}
			if mode == "publication" {
				return st.Result != nil && st.Result.ID == j.ID && st.Result.Attempt == original && st.Result.Session == session
			}
			return st.PID == before.State.PID
		})
	}
	if mode == "publication" {
		releaseRequest()
	} else if mode != "memory" {
		if current, e := processBirth(payload); e != nil || current != birth {
			t.Fatalf("new binary failed to retain payload: %v", e)
		}
	}
	if mode == "cancel" {
		if err := q.Cancel(j.ID); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { v := job(t, q, j.ID); return v.CancelledAt != nil && v.ReleaseConfirmed })
	} else if mode == "memory" {
		await(t, func() bool { v := job(t, q, j.ID); return v.State == "failed" && v.ReleaseConfirmed })
	} else {

		if err := os.WriteFile(filepath.Join(checkpoint, "finish"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { v := job(t, q, j.ID); return v.State == "succeeded" && v.ReleaseConfirmed })
	}
	after := job(t, q, j.ID)
	if after.Attempt != original || after.Session != session || after.Losses != 0 || mode == "publication" && len(after.AttemptHistory) != 1 {
		t.Fatalf("execution identity changed: %+v", after)
	}
	if mode != "memory" && mode != "cancel" {
		accepted := filepath.Join(dir, "accepted.txt")
		client := Client{URL: server.URL, Token: "custodian-test"}
		if err := client.DownloadResult(context.Background(), j.ID, "out.txt", accepted); err != nil {
			t.Fatal(err)
		}
		if b, err := os.ReadFile(accepted); err != nil || string(b) != "accepted\n" {
			t.Fatalf("published artifact lost: %q %v", b, err)
		}
		if b, e := os.ReadFile(filepath.Join(checkpoint, "output")); e != nil || string(b) != "accepted\n" {
			t.Fatalf("output lost: %s %v", b, e)
		}
		data, err = os.ReadFile(filepath.Join(c.State, before.State.Log))
		if err != nil || !strings.Contains(string(data), "before") || !strings.Contains(string(data), "after") {
			t.Fatalf("logs lost: %v %s", err, data)
		}
		logs, e := q.Logs(j.ID, 0)
		if e != nil {
			t.Fatal(e)
		}
		var combined strings.Builder
		for _, line := range logs {
			combined.WriteString(line.Text)
		}
		if strings.Count(combined.String(), "before\n") != 1 || strings.Count(combined.String(), "after\n") != 1 {
			t.Fatal("queue logs lost or duplicated restart output", combined.String())
		}
	}

	if mode == "publication" {
		t.Logf("different worker image published the same finished attempt %d/session exactly once", original)
		return
	}
	t.Logf("different worker image adopted same attempt %d/session; payload birth unchanged; logs, result and release preserved", original)
}

func TestCustodianMemoryPayload(t *testing.T) {
	if os.Getenv("MY55_MEMORY_PAYLOAD") != "1" {
		return
	}
	checkpoint := os.Getenv("MYCE_CHECKPOINT_DIR")
	if err := os.WriteFile(filepath.Join(checkpoint, "payload-pid"), []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Println("memory-ready")
	for {
		if _, err := os.Stat(filepath.Join(checkpoint, "finish")); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	pages := make([]byte, 256<<20)
	for i := 0; i < len(pages); i += 4096 {
		pages[i] = 1
	}
	for {
		runtime.KeepAlive(pages)
		time.Sleep(time.Second)
	}
}
