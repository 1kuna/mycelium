package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt-in because it builds/runs two historical binaries. The private wrapper
// builds their exact git revisions in detached .reports worktrees, plus M.
func TestM4ProductionLineageDrill(t *testing.T) {
	stopOnly := os.Getenv("MYCE_M4_STOP_ONLY") == "1"
	production, compat, bridge, current := os.Getenv("MYCE_M4_PRODUCTION_BINARY"), os.Getenv("MYCE_M4_COMPAT_BINARY"), os.Getenv("MYCE_M4_BRIDGE_BINARY"), os.Getenv("MYCE_M4_CURRENT_BINARY")
	if production == "" {
		t.Skip("run scripts/m4-production-drill.sh to build the historical binaries")
	}
	for _, path := range []string{production, compat, bridge, current} {
		if !filepath.IsAbs(path) {
			t.Fatal("absolute drill binaries required")
		}
	}
	root := t.TempDir()
	stateDir := filepath.Join(root, "queue")
	db := filepath.Join(stateDir, "queue.db")
	checkpoints := filepath.Join(root, "checkpoints")
	hostDir := filepath.Join(stateDir, "dev-host")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	url := "http://" + address
	const token = "synthetic-m4-lineage-token"
	t.Setenv("MYCE_TOKEN", token)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cleanEnv := []string{"HOME=" + root, "PATH=" + os.Getenv("PATH"), "TMPDIR=" + root, "MYCE_TOKEN=" + token, "MYCE_QUEUE=" + url, "MYCE_HOST_STATE_DIR=" + hostDir}
	processLogs := map[*exec.Cmd]string{}
	start := func(binary string, args ...string) (*exec.Cmd, <-chan error) {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = cleanEnv
		cmd.Dir = root
		log, err := os.CreateTemp(root, "process-*.log")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { log.Close() })
		processLogs[cmd] = log.Name()
		cmd.Stdout = log
		cmd.Stderr = log
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		return cmd, done
	}
	stop := func(cmd *exec.Cmd, done <-chan error) {
		t.Helper()
		if err := cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("process stop", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	oldArgs := []string{"dev", "--listen", address, "--db", db, "--checkpoints", checkpoints, "--host-id", "b70", "--workers", "2", "--gpu", "fake", "--memory-gb", "8", "--host-state-dir", hostDir}
	old, oldDone := start(production, oldArgs...)
	oldStopped := false
	defer func() {
		if !oldStopped {
			old.Process.Kill()
			<-oldDone
		}
	}()
	client := Client{URL: url, Token: token}
	wait := func(predicate func() bool) {
		t.Helper()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		for !predicate() {
			select {
			case <-ticker.C:
			case <-deadline.C:
				t.Fatal("drill condition timed out")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	snapshot := func() Snapshot { var s Snapshot; _ = client.Call(ctx, "GET", "/api/status", nil, &s); return s }
	wait(func() bool { return len(snapshot().Machines) == 2 })
	submitAPI := func(s Spec) *Job {
		t.Helper()
		var j Job
		if err := client.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		return &j
	}
	doneJob := func(id string) bool {
		for _, j := range snapshot().Jobs {
			if j.ID == id {
				return j.State == "succeeded" && j.ReleaseConfirmed
			}
		}
		return false
	}
	for _, name := range []string{"fake-1", "fake-2"} {
		s := spec("experiment")
		s.Resumable = false
		s.AllowedMachines = []string{name}
		s.Run.Command = []string{"sh", "-c", "test -z \"$MYCE_TOKEN\" && printf production-ok"}
		j := submitAPI(s)
		wait(func() bool { return doneJob(j.ID) })
	}
	t.Log("57ed4e6 real queue + two split fake-GPU workers: jobs succeeded")
	src, c, _, manager, run := m4InstallFixture(t)
	t.Setenv("MYCE_TOKEN", token)
	// The only synthetic install boundaries are inventory and systemd. Point the
	// source configs/registry at the spools actually written by the old workers.
	for i := range src {
		s := &src[i]
		s.Name = []string{"fake-1", "fake-2"}[i]
		s.HostID = "b70"
		s.Queue = url
		s.State = filepath.Join(stateDir, "workers", s.Name)
		s.ModelCache = filepath.Join(s.State, "model-cache")
		s.EnvironmentCache = filepath.Join(s.State, "environment-cache")
		s.Checkpoints = checkpoints
		s.HostMemoryBudgetGB = 0
		data, _ := json.Marshal(s)
		var raw map[string]any
		json.Unmarshal(data, &raw)
		raw["host_state_dir"] = hostDir
		data, _ = json.Marshal(raw)
		if err := os.WriteFile(s.Config, data, 0600); err != nil {
			t.Fatal(err)
		}
		// dev's foreground registry has no unit/config; model its systemd install.
		paths, _ := filepath.Glob(filepath.Join(hostDir, "worker-*.json"))
		for _, p := range paths {
			data, _ := os.ReadFile(p)
			var r map[string]any
			json.Unmarshal(data, &r)
			if r["name"] == s.Name {
				r["config"] = s.Config
				r["unit"] = filepath.Base(s.Service)
				data, _ = json.Marshal(r)
				if err := os.WriteFile(p, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	openIntent, err := os.ReadFile(filepath.Join(hostDir, "host-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	bridgeCall := func(action string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, bridge, "-test.run=^TestM4ProductionBridge$", "-test.count=1", "-test.v")
		cmd.Env = append(cleanEnv, "MYCE_M4_BRIDGE_DIR="+hostDir, "MYCE_M4_BRIDGE_ACTION="+action)
		out, err := cmd.CombinedOutput()
		t.Logf("production bridge %s: %s", action, out)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !stopOnly {
		bridgeCall("close")
	}
	stop(old, oldDone)
	oldStopped = true
	closedIntent, err := os.ReadFile(filepath.Join(hostDir, "host-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot the stopped database and actual old worker spools together.
	archive := filepath.Join(root, "production.tar")
	if out, err := exec.Command("tar", "-cf", archive, "-C", stateDir, ".").CombinedOutput(); err != nil {
		t.Fatalf("snapshot: %v %s", err, out)
	}
	originalFiles := map[string][]byte{}
	for _, s := range src {
		for _, p := range []string{s.Config, s.Service, s.Binary} {
			originalFiles[p], err = os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("production stopped (stop-only=%t); stopped-state DB/config/unit/binary snapshot retained", stopOnly)
	q, err := Open(db, checkpoints, RealClock{})
	if err != nil {
		t.Fatal("M upgrade", err)
	}
	if !stopOnly && q.s.HostMaintenance["b70"].Mode != "disabled" {
		q.Close()
		t.Fatal("M startup lost production drain fence")
	}
	if stopOnly {
		if len(q.s.ProductionDrains) != 0 {
			t.Fatal("stop-only drill unexpectedly has production drain receipts")
		}
		if _, err := q.ChangeHostMaintenance("b70", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
			t.Fatal(err)
		}
	}
	newListener, err := net.Listen("tcp", address)
	if err != nil {
		q.Close()
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(API{Queue: q, Token: token}.Handler())
	server.Listener.Close()
	server.Listener = newListener
	server.Start()
	queueCtx, stopQueue := context.WithCancel(ctx)
	defer stopQueue()
	queueDone := make(chan error, 1)
	go func() { queueDone <- q.Run(queueCtx) }()
	queueClosed := false
	closeQueue := func() {
		if !queueClosed {
			stopQueue()
			<-queueDone
			server.Close()
			q.Close()
			queueClosed = true
		}
	}
	defer closeQueue()
	// Real 08816cf worker against the upgraded queue, using its isolated open
	// local intent; M's durable disabled policy remains the admission fence.
	compatHost := filepath.Join(root, "compat-host")
	if err := PrepareStateRoots(compatHost); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(compatHost, "host-state.json"), openIntent, 0600); err != nil {
		t.Fatal(err)
	}
	workerArgs := []string{"join", "--queue", url, "--name", "fake-1", "--host-id", "b70", "--gpu", "fake", "--memory-mode", "fixed", "--memory-gb", "8", "--state-dir", src[0].State, "--checkpoint-root", checkpoints, "--host-state-dir", compatHost}
	var legacy *exec.Cmd
	var legacyDone <-chan error
	legacyStopped := true
	defer func() {
		if !legacyStopped {
			legacy.Process.Kill()
			<-legacyDone
		}
	}()

	stopCompat := func() {
		t.Helper()
		if err := writeInstallFile(filepath.Join(compatHost, "host-state.json"), closedIntent, 0600, installFiles{}, nil); err != nil {
			t.Fatal(err)
		}
		stop(legacy, legacyDone)
		legacyStopped = true
	}
	if !stopOnly {
		legacy, legacyDone = start(compat, workerArgs...)
		legacyStopped = false
		beforeContact := q.s.Machines["fake-1"].LastSeen
		wait(func() bool {
			data, _ := os.ReadFile(processLogs[legacy])
			return strings.Contains(string(data), "HTTP 404")
		})
		if !q.s.Machines["fake-1"].LastSeen.Equal(beforeContact) {
			t.Fatal("legacy binary unexpectedly passed removed host-control route")
		}
		stopCompat()
		t.Log("08816cf binary: removed host-control route returns HTTP 404; process retries, no check-in/admission; laptop local close stops it")
	}
	// Leave no live worker while offline conversion runs. Preserve actual source
	// session identity after the compatibility check's clean leave.
	closeQueue()
	qc, err := PlanInstall("queue", root, "", root, url, "queue", Detection{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	qc.DB = db
	qc.Checkpoints = checkpoints
	if err := os.MkdirAll(filepath.Dir(qc.Config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(qc)
	os.WriteFile(qc.Config, data, 0600)
	mapping := []MachineRename{{"fake-1", "b70", "0"}, {"fake-2", "b70", "1"}}
	noop := func(context.Context, string, ...string) ([]byte, error) { return nil, nil }
	var drain []MachineDrainOptions
	if stopOnly {
		options, evidenceRun := m4DrillEmptyEvidence(t, src)
		drain = []MachineDrainOptions{options}
		noop = evidenceRun
	}
	if err := ConvertQueueInstall(ctx, qc, mapping, false, false, RealClock{}, noop, drain...); err != nil {
		t.Fatal("offline production conversion", err)
	}
	q, err = Open(db, checkpoints, RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	if stopOnly && len(q.s.MachineConversion.VerifiedEmptyDrains) != 2 {
		t.Fatal("missing stop-only conversion receipts")
	}
	newListener, err = net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server = httptest.NewUnstartedServer(API{Queue: q, Token: token}.Handler())
	server.Listener.Close()
	server.Listener = newListener
	server.Start()
	queueCtx, stopQueue = context.WithCancel(ctx)
	defer stopQueue()
	queueDone = make(chan error, 1)
	go func() { queueDone <- q.Run(queueCtx) }()
	queueClosed = false
	c, err = PlanMachineInstall(c.Config, []string{src[0].Config, src[1].Config}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Use the actual current executable as the installed artifact, then the real
	// M worker with only its two-GPU inventory probe replaced.
	binary := current
	if err := ConvertWorkerInstall(ctx, c, binary, false, run); err != nil {
		t.Fatal("worker conversion", err)
	}
	w := Worker{StateDir: c.State, Machine: machine("b70", "fake", "b70"), Client: client, Clock: RealClock{}, Interval: 10 * time.Millisecond, CheckpointRoot: checkpoints, ModelConfig: q.models, HostMemorySource: func(context.Context) (float64, error) { return 128, nil }, InventoryProbe: func(context.Context) ([]MachineDevice, error) {
		return []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-0", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}, {ID: "1", Platform: "fake", Identity: "fake-1", Selector: "1", MemoryMode: "fixed", TotalMemoryGB: 8, BudgetGB: 8, FreeMemoryGB: 8}}, nil
	}}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.Run(workerCtx) }()
	workerStopped := false
	defer func() {
		if !workerStopped {
			stopWorker()
			<-workerDone
		}
	}()
	wait(func() bool { s, err := q.HostMaintenanceStatus("b70"); return err == nil && s.Healthy && s.Quiescent })
	if _, err := q.ChangeHostMaintenance("b70", HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.AllowedMachines = []string{"fake-1"}
	var typed *HTTPError
	if _, err := q.Submit(s); !errors.As(err, &typed) || typed.Code != "machine_renamed" {
		t.Fatal("machine_renamed", err)
	}
	jobs := []*Job{}
	for _, d := range []string{"0", "1"} {
		s := m3Spec("experiment", d, 8, 1)
		s.Resumable = false
		s.AllowedMachines = []string{"b70"}
		s.Run.Command = []string{"sh", "-c", `while [ ! -f "$MYCE_CHECKPOINT_DIR/finish" ]; do sleep .02; done`}
		jobs = append(jobs, submitAPI(s))
	}
	wait(func() bool {
		snap := snapshot()
		count := 0
		for _, j := range snap.Jobs {
			if j.Machine == "b70" && j.State == "running" && j.StartedAt != nil {
				count++
			}
		}
		return count == 2
	})
	for _, j := range jobs {
		active := job(t, q, j.ID)
		path := filepath.Join(checkpoints, j.ID, "attempt-"+strconv.Itoa(active.Attempt), "finish")
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, j := range jobs {
		wait(func() bool { return doneJob(j.ID) })
	}
	t.Log("M conversion: two concurrent real commands on distinct components; machine_renamed verified")
	if _, err := q.ChangeHostMaintenance("b70", HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	workerStopped = true
	rollbackRun := func(ctx context.Context, name string, args ...string) ([]byte, error) { return run(ctx, name, args...) }
	if err := RollbackMachineInstall(ctx, c.Config, false, rollbackRun); err != nil {
		t.Fatal(err)
	}
	// During the worker rollback interval queue names are still retired: observe
	// the actual old process rather than assuming systemd's 78 policy is enough.
	if err := writeInstallFile(filepath.Join(compatHost, "host-state.json"), openIntent, 0600, installFiles{}, nil); err != nil {
		t.Fatal(err)
	}
	if !stopOnly {
		legacy, legacyDone = start(compat, workerArgs...)
		legacyStopped = false
	}
	var reply Reply
	err = client.Call(ctx, "POST", "/api/workers/checkin", Checkin{Machine: *q.s.MachineConversion.Machines["fake-1"]}, &reply)
	if !errors.As(err, &typed) || typed.Code != "machine_renamed" {
		t.Fatal("rollback interval must refuse retired checkin", err)
	}
	if !stopOnly {
		wait(func() bool {
			data, _ := os.ReadFile(processLogs[legacy])
			return strings.Contains(string(data), "HTTP 404")
		})
		ticker := time.NewTimer(100 * time.Millisecond)
		select {
		case err := <-legacyDone:
			legacyStopped = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal("unexpected legacy exit", err)
			}
			t.Logf("rollback interval: legacy process exit=%d; RestartPreventExitStatus=78 applies only to exit 78", exit.ExitCode())
		case <-ticker.C:
			t.Log("rollback interval: retired check-ins refused; actual 08816cf process stays alive/retries (78 does not suppress retries)")
			stopCompat()
		}
		ticker.Stop()
	}
	serviceContract := src[0]
	serviceContract.RequireMemoryCeilings = true
	if !strings.Contains(serviceContract.ServiceText(), "RestartPreventExitStatus=78") {
		t.Fatal("missing service restart fence")
	}
	if manager.states[filepath.Base(c.Service)].active {
		t.Fatal("both generations active")
	}
	closeQueue()
	// All M jobs are disposable drill probes, no new business intent was accepted.
	// Archive their final state before restoring the exact frozen production DB.
	if out, err := exec.Command("tar", "-cf", filepath.Join(root, "post-M.tar"), "-C", stateDir, ".").CombinedOutput(); err != nil {
		t.Fatalf("archive M evidence: %v %s", err, out)
	}
	moved := stateDir + ".post-M"
	if err := os.Rename(stateDir, moved); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(stateDir, 0700)
	if out, err := exec.Command("tar", "-xf", archive, "-C", stateDir).CombinedOutput(); err != nil {
		t.Fatalf("restore: %v %s", err, out)
	}
	for p, data := range originalFiles {
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	old, oldDone = start(production, oldArgs...)
	oldStopped = false
	wait(func() bool { return len(snapshot().Machines) == 2 })
	if !stopOnly {
		bridgeCall("open")
	}
	for _, name := range []string{"fake-1", "fake-2"} {
		s := spec("experiment")
		s.Resumable = false
		s.AllowedMachines = []string{name}
		s.Run.Command = []string{"sh", "-c", "printf restored-production-ok"}
		j := submitAPI(s)
		wait(func() bool { return doneJob(j.ID) })
	}
	stop(old, oldDone)
	oldStopped = true
	t.Log("snapshot abort: exact 57ed4e6 database and split-worker spools restored; both workers complete jobs again")
}
