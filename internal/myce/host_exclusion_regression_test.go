package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func TestExclusionNativeAndContainerDescendants(t *testing.T) {
	for _, mode := range []string{"native", "container_descendant", "partial_docker", "late_manual_start", "inventory_churn", "missing_devices"} {
		t.Run(mode, func(t *testing.T) {
			in, o, k, data, devices := exclusionFixture(t)
			want := "FAIL"
			call := emptyDocker
			switch mode {
			case "native":
				in.Native = []HostNativeIdentity{{Identity: "owned"}}
				data["proc/100/environ"] = &fstest.MapFile{Data: []byte("MYCE_PROCESS_ID=owned\x00")}
				data["proc/100/cmdline"] = &fstest.MapFile{}
			case "container_descendant":
				data["proc/100/cgroup"].Data = []byte("0::/docker/owned/child\n")
				data["proc/200/cgroup"] = &fstest.MapFile{Data: []byte("0::/docker/owned\n")}
				data["proc/200/stat"] = &fstest.MapFile{Data: data["proc/100/stat"].Data}
				data["proc/200/fd"] = &fstest.MapFile{Mode: fs.ModeDir}
				data["proc/100/fd/3"] = &fstest.MapFile{}
				devices["/proc/100/fd/3"] = "195:0"
				c := ownedDockerObject("exited", "no")
				c.State.Pid = 200
				call = func(_ context.Context, _ string, args ...string) ([]byte, error) {
					if args[0] == "ps" {
						return []byte("container"), nil
					}
					return json.Marshal([]dockerInspection{c})
				}
			case "partial_docker":
				c := ownedDockerObject("running", "no")
				call = func(_ context.Context, _ string, args ...string) ([]byte, error) {
					if args[0] == "ps" {
						return []byte("container missing"), nil
					}
					return json.Marshal([]dockerInspection{c})
				}
			case "late_manual_start":
				scans := 0
				call = func(_ context.Context, _ string, args ...string) ([]byte, error) {
					if args[0] == "ps" {
						scans++
						if scans == 1 {
							return nil, nil
						}
						return []byte("container"), nil
					}
					return json.Marshal([]dockerInspection{ownedDockerObject("running", "no")})
				}
			case "inventory_churn":
				want = "UNKNOWN"
				read := k.ReadDir
				n := 0
				k.ReadDir = func(path string) ([]os.DirEntry, error) {
					if path == "/proc" {
						n++
						if n == 2 {
							data["proc/300/stat"] = &fstest.MapFile{}
						}
					}
					return read(path)
				}
			case "missing_devices":
				want = "UNKNOWN"
				read := k.ReadFile
				k.ReadFile = func(path string) ([]byte, error) {
					if path == "/proc/devices" {
						return nil, os.ErrPermission
					}
					return read(path)
				}
			}
			r := ObserveHostExclusion(context.Background(), o, in, RealClock{}, k, call)
			if r.Verdict != want {
				t.Fatal(want, r)
			}
			if mode == "container_descendant" && !slices.Contains(exclusionCodes(r), "owned_gpu_holder") {
				t.Fatal("missed descendant holder", r)
			}
			if mode == "partial_docker" && (!slices.Contains(exclusionCodes(r), "owned_container_live") || !slices.Contains(exclusionCodes(r), "inventory_incomplete")) {
				t.Fatal("lost mixed causes", r)
			}
		})
	}
}

func TestExclusionPendingOperationNeedsPositiveDisposition(t *testing.T) {
	for _, op := range []string{"create", "start", "network-create"} {
		t.Run(op, func(t *testing.T) {
			clock := &fakeClock{now: time.Unix(1000, 0)}
			dir := t.TempDir()
			d := &dockerAttempt{ctx: context.Background(), dir: dir, clock: clock, state: dockerExecution{Identity: "attempt", Endpoint: "fake", Pending: true, Operation: op, PendingName: "expected", PendingContainer: "container", Container: "container", Network: "expected", Containers: map[string]dockerMember{}}, command: func(_ context.Context, args ...string) ([]byte, error) {
				if args[0] == "ps" || args[0] == "network" && args[1] == "ls" {
					return nil, nil
				}
				return nil, errors.New("unexpected operation")
			}}
			if released, err := d.reconcile(); err != nil || released || !d.state.Pending || d.state.Released {
				t.Fatal("empty scan settled accepted operation", released, err, d.state)
			}
			// A wrong-name object cannot settle a pending create.
			if op == "create" {
				d.command = func(_ context.Context, args ...string) ([]byte, error) {
					if args[0] == "ps" {
						return []byte("container"), nil
					}
					if args[0] == "inspect" {
						return json.Marshal([]dockerInspection{ownedDockerObject("exited", "no")})
					}
					return nil, nil
				}
				if released, err := d.reconcile(); err != nil || released || !d.state.Pending {
					t.Fatal("wrong object settled create", released, err)
				}
			}
		})
	}
}

type exclusionDeadlineService struct {
	t         *testing.T
	dir       string
	clock     *fakeClock
	deadlines []time.Time
	grace     []time.Duration
}

func (s *exclusionDeadlineService) Stop(_ context.Context, grace time.Duration) error {
	state, err := ReadHostIntent(s.dir)
	if err != nil || state.StopDeadline == nil {
		s.t.Fatal("TERM before durable deadline", state, err)
	}
	if grace != max(0, state.StopDeadline.Sub(s.clock.Now())) {
		s.t.Fatal("renewed grace", grace, state.StopDeadline)
	}
	s.deadlines = append(s.deadlines, *state.StopDeadline)
	s.grace = append(s.grace, grace)
	return nil
}
func (*exclusionDeadlineService) Empty(context.Context) (bool, error) { return true, nil }
func TestExclusionDeadlineSurvivesRestartAndExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	service := &exclusionDeadlineService{t: t, dir: dir, clock: clock}
	h := HostControl{Dir: dir, Clock: clock, Service: service, Observe: testUnknownExclusion}
	_, _ = h.Disable(context.Background(), "close", 10*time.Second)
	clock.Advance(11 * time.Second)
	// A new controller models restart; a repeated close cannot renew TERM grace.
	h = HostControl{Dir: dir, Clock: clock, Service: service, Observe: testUnknownExclusion}
	_, _ = h.Disable(context.Background(), "retry", time.Minute)
	if len(service.grace) != 2 || service.grace[0] != 10*time.Second || service.grace[1] != 0 || !service.deadlines[0].Equal(service.deadlines[1]) {
		t.Fatal(service.grace, service.deadlines)
	}
}

func TestExclusionConfiguredDockerEndpoint(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///configured.sock")
	home := t.TempDir()
	config, err := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "dedicated"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var restored ServiceConfig
	if json.Unmarshal(b, &restored) != nil || restored.DockerEndpoint != "unix:///configured.sock" {
		t.Fatal(string(b))
	}
	args := restored.Args()
	i := slices.Index(args, "--docker-host")
	if i < 0 || args[i+1] != restored.DockerEndpoint {
		t.Fatal(args)
	}
}

func TestExclusionInputIncludesStandaloneCustody(t *testing.T) {
	_, v := hostIdentityFixture(t)
	v.Privileged = false
	dir := hostCommandDirectory(v.StateDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(dir, "attempt-command-fixture.json", workerState{Identity: "standalone", PID: 123}); err != nil {
		t.Fatal(err)
	}
	in, err := readHostExclusionInput(v)
	if err != nil || len(in.Native) != 1 || in.Native[0].Identity != "standalone" {
		t.Fatal(in, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "attempt-command-broken.json"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	in, err = readHostExclusionInput(v)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(in.Problems)
	if !strings.Contains(string(b), "native_custody_unreadable") {
		t.Fatal(string(b))
	}
}

func TestExclusionRejectsLegacyOperatorAssertion(t *testing.T) {
	d := &dockerAttempt{state: dockerExecution{Identity: "attempt", Released: true, OperatorConfirmed: true}, command: func(context.Context, ...string) ([]byte, error) {
		t.Fatal("legacy assertion must not reach release")
		return nil, nil
	}}
	if released, err := d.reconcile(); released || err == nil {
		t.Fatal("old operator assertion accepted", released, err)
	}
}

func TestExclusionProductionStatusRejectsPersistedSimulation(t *testing.T) {
	_, v := hostIdentityFixture(t)
	v.Privileged = false
	v.Config.DockerEndpoint = "unix:///example.sock"
	v.Config.Service = filepath.Join(v.Account.Home, "myce-worker.service")
	if err := closeTestHost(v.StateDir); err != nil {
		t.Fatal(err)
	}
	state, err := ReadHostIntent(v.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	report := testHostExclusion(context.Background(), state)
	if err = mutateHostIntent(v.StateDir, state.HostRevision, func(s *HostIntent) error {
		s.LocalExclusion = &HostLocalExclusion{Simulated: true, Confirmed: true, Evidence: &report, HostObservation: HostObservation{HostRevision: state.HostRevision, Boot: report.Boot}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	in, err := readHostExclusionInput(v)
	if err != nil {
		t.Fatal(err)
	}
	_, o, k, data, _ := exclusionFixture(t)
	data["proc/sys/kernel/random/boot_id"].Data = []byte(in.Boot)
	result := ObserveHostExclusion(context.Background(), o, *in, RealClock{}, k, emptyDocker)
	if result.Verdict != "UNKNOWN" || !slices.Contains(exclusionCodes(result), "simulated_exclusion_unknown") {
		t.Fatal("status trusted simulation", result)
	}
}

func TestExclusionExactDockerReconciliationVerifiesRelease(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	j := dockerJob()
	s.state.Job = &j
	s.state.Identity = "owned"
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	s.close()
	if err := closeTestHost(w.HostStateDir); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"created": "id1", "label": "owned"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveWorkerJSON(s.dir, "owned.docker.json", dockerExecution{Owner: &Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, Identity: "owned", Endpoint: "unix:///example.sock", Container: "id1", Operation: "start"}); err != nil {
		t.Fatal(err)
	}
	invocation := HostInvocation{StateDir: w.HostStateDir, Config: ServiceConfig{State: w.StateDir}, Account: WorkerAccount{UID: uint32(os.Geteuid())}}
	executor := HostMutationExecutor{}
	if _, err := executor.Execute(context.Background(), invocation, HostMutation{Operation: "resolve-docker", SpoolDir: t.TempDir(), AttemptID: "owned"}); !errors.Is(err, ErrHostTarget) {
		t.Fatal("unregistered spool accepted", err)
	}
	if _, err := executor.Execute(context.Background(), invocation, HostMutation{Operation: "resolve-docker", SpoolDir: w.StateDir, AttemptID: "owned"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(s.dir, "owned.docker.json"))
	var saved dockerExecution
	if err != nil || json.Unmarshal(b, &saved) != nil || !saved.Released || saved.Pending || saved.OperatorConfirmed {
		t.Fatal(saved, err)
	}
	calls, _ := os.ReadFile(filepath.Join(root, "calls"))
	if !strings.Contains(string(calls), "--filter label=myce.attempt=owned") || strings.Contains(string(calls), "create ") || strings.Contains(string(calls), "start ") {
		t.Fatal("resolution lacked exact custody", string(calls))
	}
}

func TestExclusionDisabledRetentionUsesScopedInventory(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	for name, value := range map[string]string{"created": "id1", "label": "owned"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveWorkerJSON(s.dir, "owned.docker.json", dockerExecution{Owner: &Result{Machine: w.Machine.ID, Session: s.state.Session, Execution: Execution{ID: "job", Attempt: 1}}, Identity: "owned", Endpoint: "unix:///example.sock", Released: true}); err != nil {
		t.Fatal(err)
	}
	retention := time.Duration(0)
	h := HostControl{Clock: clock, ContainerRetention: &retention}
	workers := []HostWorker{{Name: w.Machine.ID, State: s.dir}}
	if err := h.maintainDocker(context.Background(), workers); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "created")); err != nil {
		t.Fatal("unacknowledged evidence removed", err)
	}
	if err := saveWorkerJSON(s.dir, "owned.docker-ack.json", dockerAcknowledgement{Worker: w.Machine.ID, At: clock.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := h.maintainDocker(context.Background(), workers); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "removed")); err != nil {
		t.Fatal("expired evidence not maintained", err)
	}
	b, _ := os.ReadFile(filepath.Join(root, "calls"))
	calls := string(b)
	if !strings.Contains(calls, "--filter label=myce.attempt=owned --filter label=myce.worker="+w.Machine.ID) || !strings.Contains(calls, "rm id1") || strings.Contains(calls, "prune") {
		t.Fatal("unscoped maintenance", calls)
	}
}

func TestExclusionWorkerRepairsRestartPolicyAndUnpauses(t *testing.T) {
	c := ownedDockerObject("paused", "always")
	clock := &fakeClock{now: time.Unix(1000, 0)}
	calls := []string{}
	d := &dockerAttempt{ctx: context.Background(), clock: clock, state: dockerExecution{Identity: "attempt", Endpoint: "fake", Stopped: true, Forced: true}}
	d.command = func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, strings.Join(args, " "))
		switch args[0] {
		case "ps":
			return []byte("container"), nil
		case "inspect":
			return json.Marshal([]dockerInspection{c})
		case "update":
			c.HostConfig.RestartPolicy.Name = "no"
		case "unpause":
			c.State.Paused = false
			c.State.Status = "running"
		case "kill":
			running := false
			c.State.Running = &running
			c.State.Status = "exited"
		case "network":
			return nil, nil
		default:
			t.Fatal("unexpected mutation", args)
		}
		return nil, nil
	}
	for i := 0; i < 3; i++ {
		if _, err := d.reconcile(); err != nil {
			t.Fatal(err)
		}
	}
	if !d.state.Released || !slices.Contains(calls, "update --restart=no container") || !slices.Contains(calls, "unpause container") || !slices.Contains(calls, "kill --signal=KILL container") {
		t.Fatal(d.state, calls)
	}
}

func TestExclusionLateTerminalStartAndNetworkAcceptance(t *testing.T) {
	for _, op := range []string{"start", "network-create"} {
		t.Run(op, func(t *testing.T) {
			clock := &fakeClock{now: time.Unix(1000, 0)}
			at := clock.Now()
			d := &dockerAttempt{ctx: context.Background(), clock: clock, state: dockerExecution{Identity: "attempt", Endpoint: "fake", Operation: op, Pending: true, PendingContainer: "container", Container: "container", Network: "expected", Stopped: true, Forced: true, DispatchedAt: &at}}
			removed := false
			d.command = func(_ context.Context, args ...string) ([]byte, error) {
				if args[0] == "ps" {
					if op == "start" {
						return []byte("container"), nil
					}
					return nil, nil
				}
				if args[0] == "inspect" {
					c := ownedDockerObject("exited", "no")
					c.State.StartedAt = at.Add(time.Second)
					return json.Marshal([]dockerInspection{c})
				}
				if args[0] == "network" {
					switch args[1] {
					case "ls":
						if op == "network-create" && !removed {
							return []byte("network"), nil
						}
						return nil, nil
					case "inspect":
						return json.Marshal([]dockerNetworkInspection{{ID: "network", Name: "expected", Driver: "bridge", Internal: true, Labels: map[string]string{"myce.attempt": "attempt"}}})
					case "rm":
						removed = true
						return nil, nil
					}
				}
				t.Fatal("unexpected operation", args)
				return nil, nil
			}
			released, err := d.reconcile()
			if err != nil || !released || d.state.Pending || !d.state.Released {
				t.Fatal("positive accepted-operation evidence did not settle", released, err, d.state)
			}
		})
	}
}

func TestExclusionReadsEveryRegisteredEndpointInChild(t *testing.T) {
	_, v := hostIdentityFixture(t)
	v.Privileged = false
	v.Config.DockerEndpoint = "unix:///primary.sock"
	state, err := ReadHostIntent(v.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	spool := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.json")
	config := ServiceConfig{Role: "worker", HostID: state.Host, State: spool, DockerEndpoint: "unix:///secondary.sock"}
	b, _ := json.Marshal(config)
	if err = os.WriteFile(configPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = AdoptHostWorker(v.StateDir, HostWorker{Host: state.Host, State: spool, Name: "second", Session: "session", Unit: "myce-worker@second.service", Config: configPath}); err != nil {
		t.Fatal(err)
	}
	input, err := readHostExclusionInput(v)
	if err != nil || !slices.Contains(input.Endpoints, config.DockerEndpoint) {
		t.Fatal("configured empty-spool endpoint omitted", input, err)
	}
	// The collected endpoint set drives both read-only scans, even with no objects.
	in, o, k, _, _ := exclusionFixture(t)
	in.Endpoints = input.Endpoints
	seen := map[string]int{}
	r := ObserveHostExclusion(context.Background(), o, in, RealClock{}, k, func(ctx context.Context, endpoint string, args ...string) ([]byte, error) {
		seen[endpoint]++
		return emptyDocker(ctx, endpoint, args...)
	})
	if r.Verdict != "PASS" || seen[v.Config.DockerEndpoint] != 2 || seen[config.DockerEndpoint] != 2 {
		t.Fatal("incomplete configured endpoint observation", r, seen)
	}
	config.DockerEndpoint = ""
	b, _ = json.Marshal(config)
	if err = os.WriteFile(configPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	input, err = readHostExclusionInput(v)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, problem := range input.Problems {
		found = found || problem.Code == "worker_endpoint_unconfigured"
	}
	if !found {
		t.Fatal("unknown registered endpoint silently omitted", input)
	}
}

func TestExclusionCleansEveryConfiguredEndpoint(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	// Log the selected endpoint before the existing fake CLI strips its host flag.
	launcher := filepath.Join(root, "docker")
	b, err := os.ReadFile(launcher)
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.Replace(string(b), "#!/bin/sh\n", "#!/bin/sh\nprintf '%s\\n' \"$2\" >> \"$MYCE_TEST_DOCKER_ROOT/endpoints\"\n", 1))
	if err = os.WriteFile(launcher, b, 0700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	config := ServiceConfig{Role: "worker", HostID: "example", State: s.dir, DockerEndpoint: "unix:///secondary.sock"}
	b, _ = json.Marshal(config)
	if err = os.WriteFile(configPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Clock: clock, DockerEndpoint: "unix:///primary.sock", dockerProgress: &dockerDisableProgress{}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workers := []HostWorker{{Host: config.HostID, Name: w.Machine.ID, State: s.dir, Config: configPath}}
	if err = h.confirmDocker(ctx, workers, clock.Now()); err != nil {
		t.Fatal(err)
	}
	b, err = os.ReadFile(filepath.Join(root, "endpoints"))
	if err != nil || !slices.Contains(strings.Fields(string(b)), config.DockerEndpoint) {
		t.Fatal("cleanup omitted configured empty-spool endpoint", string(b), err)
	}
	if !slices.Contains(strings.Fields(string(b)), h.DockerEndpoint) {
		t.Fatal("cleanup omitted primary endpoint", string(b))
	}
}

func TestExclusionCleanupCannotCertifyIncompleteObservation(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	h := HostControl{Dir: dir, Clock: clock, Service: &fakeHostService{}, Observe: func(ctx context.Context, s HostIntent) HostExclusionReport {
		r := testHostExclusion(ctx, s)
		r.Sources["gpu_holders"] = nil
		return r
	}}
	r, err := h.Disable(context.Background(), "close", 0)
	if err == nil || r.Confirmed || r.Exclusion == nil || r.Exclusion.Verdict != "UNKNOWN" {
		t.Fatal("incomplete observation certified exclusion", r, err)
	}
}

func TestExclusionAcceptedCreateSurvivesClientDeath(t *testing.T) {
	w, s, clock, root := dockerFixture(t)
	if err := os.WriteFile(filepath.Join(root, "mode"), []byte("late-create"), 0600); err != nil {
		t.Fatal(err)
	}
	j := dockerJob()
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed: create") })
	p.docker.close() // Client/worker loop dies; the accepted daemon operation survives.
	s.close()
	path := filepath.Join(s.dir, s.state.Identity+".docker.json")
	b, err := os.ReadFile(path)
	var saved dockerExecution
	if err != nil || json.Unmarshal(b, &saved) != nil || !saved.Pending || saved.PendingName == "" {
		t.Fatal("lost durable create identity", saved, err)
	}
	d := &dockerAttempt{ctx: context.Background(), dir: s.dir, binary: p.docker.binary, env: p.docker.env, clock: clock, timeout: p.docker.timeout, state: saved}
	if done, err := d.reconcile(); err != nil || done || !d.state.Pending {
		t.Fatal("empty inventory settled dead client's create", done, err)
	}
	if err = os.WriteFile(filepath.Join(root, "release-late"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
	await(t, func() bool {
		done, err := d.reconcile()
		if err != nil {
			t.Fatal(err)
		}
		return done
	})
	if d.state.Pending || !d.state.Released {
		t.Fatal("positive late create did not settle after cleanup", d.state)
	}
	b, _ = os.ReadFile(filepath.Join(root, "calls"))
	if strings.Count(string(b), "create --restart=no ") != 1 || strings.Contains(string(b), "\nstart ") {
		t.Fatal("recovery replayed create/start", string(b))
	}
}

func TestExclusionDeadlineEscalatesAfterRestart(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	dir := t.TempDir()
	testHostOpen(t, dir, clock)
	var deadline time.Time
	var calls []string
	service := func() *SystemdHostService {
		stopped := false
		return &SystemdHostService{Unit: "myce-worker.service", Clock: clock,
			Command: func(_ context.Context, _ string, args ...string) ([]byte, error) {
				call := strings.Join(args, " ")
				if args[1] == "show" {
					state := "active"
					if stopped {
						state = "inactive"
					}
					return []byte("LoadState=loaded\nKillMode=control-group\nSendSIGKILL=yes\nActiveState=" + state + "\nControlGroup=/user.slice/myce-worker.service\n"), nil
				}
				intent, err := ReadHostIntent(dir)
				if err != nil || intent.StopDeadline == nil {
					t.Fatal("signal before durable deadline", intent, err)
				}
				if deadline.IsZero() {
					deadline = *intent.StopDeadline
				} else if !deadline.Equal(*intent.StopDeadline) {
					t.Fatal("restart renewed grace", deadline, intent.StopDeadline)
				}
				calls = append(calls, call)
				stopped = true
				return nil, nil
			}, ReadFile: func(path string) ([]byte, error) {
				if filepath.Base(path) == "cgroup.events" {
					return []byte("populated 0\n"), nil
				}
				return nil, nil
			}}

	}
	r, err := (HostControl{Dir: dir, Clock: clock, Service: service(), Observe: testUnknownExclusion}).Disable(context.Background(), "close", 10*time.Second)
	if err == nil || !r.cleanupComplete {
		t.Fatal("initial cleanup incomplete", r, err)
	}
	if len(calls) != 1 || !strings.Contains(calls[0], "stop --no-block") {
		t.Fatal("initial TERM cleanup", calls)
	}
	clock.Advance(11 * time.Second)
	r, err = (HostControl{Dir: dir, Clock: clock, Service: service(), Observe: testUnknownExclusion}).Disable(context.Background(), "restart", time.Minute)
	if err == nil || !r.cleanupComplete {
		t.Fatal("restart cleanup incomplete", r, err)
	}
	if len(calls) != 3 || !strings.Contains(calls[1], "--signal=SIGKILL") || !strings.Contains(calls[2], "stop --no-block") {
		t.Fatal("expired restart did not escalate before cleanup", calls)
	}
}

func TestExclusionStagedInstallKeepsUnknownProof(t *testing.T) {
	home := t.TempDir()
	nas := filepath.Join(home, "nas")
	if err := os.Mkdir(nas, 0700); err != nil {
		t.Fatal(err)
	}
	c, err := PlanInstall("worker", home, "", nas, "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	if err != nil {
		t.Fatal(err)
	}
	c.HostStateDir, c.StartDisabled, c.AllowDeviceless = filepath.Join(home, "host"), true, true
	c.DockerEndpoint = "unix:///configured.sock"
	c.WorkerInstance("first")
	if err = os.MkdirAll(c.HostStateDir, 0700); err != nil {
		t.Fatal(err)
	}
	initial, err := InitializeHostIntent(c.HostStateDir, c.HostID, RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	// Installation preserves an existing UNKNOWN observation; it no longer
	// performs a new close/observation merely to stage a binary. Fresh null
	// observations are separately covered by the install-preservation tests.
	report := testUnknownExclusion(context.Background(), initial)
	initial.LocalExclusion = &HostLocalExclusion{Evidence: &report, HostObservation: HostObservation{HostRevision: initial.HostRevision, Boot: report.Boot}}
	if err = atomicHostJSON(c.HostStateDir, hostStateFile, initial, nil); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(c.HostStateDir, hostStateFile))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(home, "binary")
	if err = os.WriteFile(binary, []byte("fixture"), 0700); err != nil {
		t.Fatal(err)
	}
	command := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		text := strings.Join(args, " ")
		if strings.HasSuffix(text, filepath.Base(c.Service)) && (strings.Contains(text, "enable --now ") || strings.Contains(text, "restart ")) {
			t.Fatal("staged install activated worker", args)
		}
		if strings.Contains(text, "--property=UnitFileState") {
			return []byte("disabled"), nil
		}
		if strings.Contains(text, "--property=DropInPaths") {
			return nil, nil
		}
		if strings.Contains(text, "show ") {
			return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
		}
		return nil, nil
	}
	for i := 0; i < 2; i++ {
		if err = Install(context.Background(), c, binary, "fixture-token", command); err != nil {
			t.Fatal("disabled install/rerun", i, err)
		}
		after, err := os.ReadFile(filepath.Join(c.HostStateDir, hostStateFile))
		if err != nil || string(before) != string(after) {
			t.Fatal("install changed stored exclusion evidence", err)
		}
		state, err := ReadHostIntent(c.HostStateDir)
		if err != nil || state.Desired != "closed" || state.HostRevision != initial.HostRevision || !state.StopDeadline.Equal(*initial.StopDeadline) {
			t.Fatal("install changed the accepted close", state, err)
		}
		if state.LocalExclusion == nil || state.LocalExclusion.Confirmed || state.LocalExclusion.Evidence == nil || state.LocalExclusion.Evidence.Verdict != "UNKNOWN" {
			t.Fatal("install manufactured privileged proof", state)
		}
		installed, err := ReadServiceConfig(c.Config)
		if err != nil || installed.DockerEndpoint != c.DockerEndpoint {
			t.Fatal("configured endpoint not persisted", installed.DockerEndpoint, err)
		}
	}
}
