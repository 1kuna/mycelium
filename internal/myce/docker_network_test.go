package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestDockerPrivateNetworkContract(t *testing.T) {
	for _, tc := range []struct {
		fields string
		valid  bool
	}{
		{`"docker_network":"private"`, true},
		{`"docker_network":"host"`, false},
		{`"docker_network":"private","docker_memory_gb":2,"docker_sidecars":[{"name":"naming","memory_gb":2,"command":["docker","run","image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}]`, true},
		{`"docker_network":"private","docker_memory_gb":4,"docker_sidecars":[{"name":"naming","memory_gb":4,"command":["docker","run","image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}]`, true},
		{`"docker_network":"private","docker_memory_gb":4,"docker_sidecars":[{"name":"naming","memory_gb":7,"command":["docker","run","image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}]`, false},
	} {
		s := dockerJob().Spec
		s.Needs.HostMemoryGB = 6
		data, _ := json.Marshal(s)
		var obj map[string]any
		_ = json.Unmarshal(data, &obj)
		var fields map[string]any
		_ = json.Unmarshal([]byte("{"+tc.fields+"}"), &fields)
		for k, v := range fields {
			obj["run"].(map[string]any)[k] = v
		}
		data, _ = json.Marshal(obj)
		_ = json.Unmarshal(data, &s)
		if err := s.Validate(); (err == nil) != tc.valid {
			t.Fatalf("%s valid=%v: %v", tc.fields, tc.valid, err)
		}
	}
}

func fakeDockerNetwork(root string, args []string, mode string) bool {
	if args[0] != "network" {
		return false
	}
	path := filepath.Join(root, "network")
	switch args[1] {
	case "create":
		label := ""
		for i, a := range args {
			if a == "--label" && strings.HasPrefix(args[i+1], "myce.attempt=") {
				label = strings.TrimPrefix(args[i+1], "myce.attempt=")
			}
		}
		data, _ := json.Marshal(map[string]any{"Id": "net1", "Name": args[len(args)-1], "Driver": "bridge", "Internal": true, "Labels": map[string]string{"myce.attempt": label}, "Containers": map[string]any{}})
		_ = os.WriteFile(path, data, 0600)
		fmt.Println("net1")
	case "ls":
		if mode == "hidden-network" {
			return true
		}
		if _, err := os.Stat(path); err == nil {
			fmt.Println("net1")
		}
		if !strings.Contains(strings.Join(args, " "), "label=myce.attempt") {
			if _, e := os.Stat(filepath.Join(root, "unlabeled-network")); e == nil {
				fmt.Println("legacy-net")
			}
		}
	case "inspect":
		data, _ := os.ReadFile(path)
		if len(data) == 0 {
			fmt.Fprintln(os.Stderr, "Error: No such network: net1")
			os.Exit(1)
		}
		if mode == "hidden-network" {
			var obj map[string]any
			_ = json.Unmarshal(data, &obj)
			obj["Labels"] = map[string]string{}
			data, _ = json.Marshal(obj)
		}
		fmt.Printf("[%s]", data)
	case "disconnect":
		data, _ := os.ReadFile(path)
		var obj map[string]any
		_ = json.Unmarshal(data, &obj)
		delete(obj["Containers"].(map[string]any), args[len(args)-1])
		data, _ = json.Marshal(obj)
		_ = os.WriteFile(path, data, 0600)
		_ = os.WriteFile(filepath.Join(root, "disconnected-"+args[len(args)-1]), nil, 0600)
	case "rm":
		if mode == "refuse-network-rm" {
			fmt.Fprintln(os.Stderr, "Error response from daemon: network removal refused")
			os.Exit(1)
		}
		// A late labeled container races the first removal.
		if mode == "late-network-container" {
			if _, e := os.Stat(filepath.Join(root, "late-added")); e != nil {
				_ = os.WriteFile(filepath.Join(root, "extra"), nil, 0600)
				_ = os.WriteFile(filepath.Join(root, "late-added"), nil, 0600)
				os.Exit(1)
			}
		}
		data, _ := os.ReadFile(path)
		var obj map[string]any
		_ = json.Unmarshal(data, &obj)
		if len(obj["Containers"].(map[string]any)) > 0 {
			fmt.Fprintln(os.Stderr, "Error response from daemon: active endpoints")
			os.Exit(1)
		}
		_ = os.Rename(path, filepath.Join(root, "network-removed"))
		fmt.Println("net1")
	}
	return true
}
func fakeDockerGroupCreate(root string, args []string) {
	id := "id1"
	alias := "main"
	label := ""
	net := ""
	memory, swap := int64(0), int64(0)
	for i, a := range args {
		switch a {
		case "--network-alias":
			alias = args[i+1]
		case "--network":
			net = args[i+1]
		case "--label":
			if strings.HasPrefix(args[i+1], "myce.attempt=") {
				label = strings.TrimPrefix(args[i+1], "myce.attempt=")
			}
		case "--memory":
			memory, _ = strconv.ParseInt(args[i+1], 10, 64)
		case "--memory-swap":
			swap, _ = strconv.ParseInt(args[i+1], 10, 64)
		}
	}
	if alias != "main" {
		id = "sidecar-" + alias
	}
	obj := map[string]any{"Id": id, "Config": map[string]any{"Labels": map[string]string{"myce.attempt": label}}, "HostConfig": map[string]any{"Memory": memory, "MemorySwap": swap, "NetworkMode": net}, "NetworkSettings": map[string]any{"Networks": map[string]any{net: map[string]any{"IPAddress": "127.0.0.1"}}}}
	data, _ := json.Marshal(obj)
	_ = os.WriteFile(filepath.Join(root, "object-"+id+".json"), data, 0600)
	netdata, _ := os.ReadFile(filepath.Join(root, "network"))
	var n map[string]any
	_ = json.Unmarshal(netdata, &n)
	n["Containers"].(map[string]any)[id] = map[string]any{}
	netdata, _ = json.Marshal(n)
	_ = os.WriteFile(filepath.Join(root, "network"), netdata, 0600)
	if id == "id1" {
		_ = os.WriteFile(filepath.Join(root, "created"), nil, 0600)
		_ = os.WriteFile(filepath.Join(root, "label"), []byte(label), 0600)
	}
	fmt.Println(id)
}
func fakeDockerGroupInspect(root string, ids []string, mode string) {
	objects := []map[string]any{}
	for _, id := range ids {
		data, _ := os.ReadFile(filepath.Join(root, "object-"+id+".json"))
		var obj map[string]any
		_ = json.Unmarshal(data, &obj)
		if obj == nil {
			data, _ = os.ReadFile(filepath.Join(root, "object-id1.json"))
			_ = json.Unmarshal(data, &obj)
			obj["Id"] = id
		}
		file := "running-" + id
		if id == "id1" {
			file = "running"
		}
		if id == "id2" {
			file = "extra"
		}
		_, e := os.Stat(filepath.Join(root, file))
		running := e == nil
		status := "exited"
		if running {
			status = "running"
		}
		obj["State"] = map[string]any{"Running": running, "Status": status, "ExitCode": 0}
		if mode == "other-network" {
			obj["HostConfig"].(map[string]any)["NetworkMode"] = "host"
		}
		if mode == "socket-mount" {
			obj["Mounts"] = []any{map[string]string{"Source": "/var/run/docker.sock", "Destination": "/run/docker.sock"}}
		}
		if mode == "published-port" {
			obj["HostConfig"].(map[string]any)["PortBindings"] = map[string]any{"80/tcp": []any{map[string]string{"HostPort": "8080"}}}
		}
		objects = append(objects, obj)
	}
	_ = json.NewEncoder(os.Stdout).Encode(objects)
}

func TestDockerPrivateNetworkLifecycle(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerJob()
	j.Checkpoint = t.TempDir()
	j.Spec.Run.DockerNetwork = "private"
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.stop(c.Now(), 0, true)
	select {
	case f := <-p.done:
		if !f.result.Forced {
			t.Fatal("not forced")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("cleanup stuck")
	}
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	calls := string(data)
	if !strings.Contains(calls, "network create --driver bridge --internal") || !strings.Contains(calls, "MYCE_DOCKER_NETWORK=myce-") {
		t.Fatalf("network contract missing: %s", calls)
	}
	if !strings.Contains(calls, "network disconnect --force net1 id1") || strings.Index(calls, "network rm") < strings.Index(calls, "kill --signal=KILL") {
		t.Fatal("network removed before containers stopped")
	}
	if _, e := os.Stat(filepath.Join(root, "network-removed")); e != nil {
		t.Fatal("network not removed")
	}
}

func dockerGroupJob(t *testing.T) Job {
	j := dockerJob()
	j.Checkpoint = t.TempDir()
	j.Spec.Run.DockerNetwork = "private"
	j.Spec.Run.DockerMemoryGB = 2
	j.Spec.Needs.HostMemoryGB = 6
	j.Spec.Run.DockerSidecars = []DockerSidecar{{Name: "sortformer", MemoryGB: 2, Command: append([]string(nil), j.Spec.Run.Command...)}, {Name: "naming", MemoryGB: 2, Command: append([]string(nil), j.Spec.Run.Command...)}}
	return j
}
func TestDockerSidecarsMainExitAndReadiness(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerGroupJob(t)
	var ready atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			rw.WriteHeader(503)
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(u.Port())
	j.Spec.Run.DockerSidecars[0].Readiness = &DockerReadiness{Port: port, Path: "/ready", Timeout: "10s"}
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running-sidecar-sortformer")); return e == nil })
	if _, e := os.Stat(filepath.Join(root, "running")); e == nil {
		t.Fatal("main started before ready")
	}
	ready.Store(true)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	_ = os.Rename(filepath.Join(root, "running"), filepath.Join(root, "main-exited"))
	await(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Contains(string(data), "kill --signal=TERM sidecar-")
	})
	c.advance(StopGrace)
	select {
	case f := <-p.done:
		if !f.result.Success || f.result.Interrupted {
			t.Fatalf("main success became interruption: %+v", f.result)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("main exit did not end job")
	}
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	calls := string(data)
	if strings.Contains(calls, "--memory-swap") {
		t.Fatal("default swap forcibly limited")
	}
	if strings.Index(calls, "start sidecar-sortformer") > strings.Index(calls, "start sidecar-naming") || strings.Index(calls, "start sidecar-naming") > strings.Index(calls, "start id1") {
		t.Fatal("sidecars not first")
	}
	for _, name := range []string{"main", "sortformer", "naming"} {
		if !strings.Contains(calls, "--network-alias "+name) || !strings.Contains(calls, "--memory 2147483648") {
			t.Fatalf("missing network/memory for %s", name)
		}
	}
	if _, e := os.Stat(filepath.Join(root, "network-removed")); e != nil {
		t.Fatal("network survives")
	}
}
func TestDockerSidecarCrashFailsJob(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	j := dockerGroupJob(t)
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	_ = os.Rename(filepath.Join(root, "running-sidecar-naming"), filepath.Join(root, "crashed"))
	select {
	case f := <-p.done:
		if f.result.Success || f.result.Interrupted || !strings.Contains(f.result.Error, "sidecar naming exited") {
			t.Fatalf("crash: %+v", f.result)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("sidecar crash did not stop job")
	}
}

func TestDockerNetworkViolationsAndRemovalHold(t *testing.T) {
	for _, mode := range []string{"published-port", "other-network", "socket-mount", "refuse-network-rm", "late-network-container"} {
		t.Run(mode, func(t *testing.T) {
			w, s, c, root := dockerFixture(t)
			j := dockerGroupJob(t)
			p, e := startDockerFixture(w, s, j)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(p.docker.close)
			await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
			_ = os.WriteFile(filepath.Join(root, "mode"), []byte(mode), 0600)
			_ = os.WriteFile(filepath.Join(root, "unlabeled-network"), nil, 0600)
			if mode == "refuse-network-rm" || mode == "late-network-container" {
				p.stop(c.Now(), 0, true)
			}
			if mode == "refuse-network-rm" {
				await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
				select {
				case <-p.done:
					t.Fatal("network removal failure released memory")
				default:
				}
				_ = os.WriteFile(filepath.Join(root, "mode"), nil, 0600)
			}
			select {
			case f := <-p.done:
				if mode == "published-port" || mode == "other-network" || mode == "socket-mount" {
					if f.result.Success || f.result.Interrupted || !strings.Contains(f.result.Error, "containment violation") {
						t.Fatalf("violation: %+v", f.result)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup stuck")
			}
			if _, e := os.Stat(filepath.Join(root, "unlabeled-network")); e != nil {
				t.Fatal("unlabeled network touched")
			}
			if mode == "late-network-container" {
				if _, e := os.Stat(filepath.Join(root, "extra-stopped")); e != nil {
					t.Fatal("late container survived")
				}
			}
		})
	}
}
func TestDisableDockerPrivateNetwork(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerGroupJob(t)
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.docker.close() // worker disappeared; daemon and spool survive
	s.close()
	if e = saveWorkerJSON(w.HostStateDir, "worker-test.json", HostWorker{State: s.dir, Host: "docker-host"}); e != nil {
		t.Fatal(e)
	}
	h := HostControl{Dir: w.HostStateDir, Clock: c, Docker: true, Processes: HostProcesses{Owned: func(workerState) (bool, error) { return false, nil }, Signal: func(workerState, syscall.Signal) error { return nil }, Pause: func(d time.Duration) { c.advance(d) }}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	report, e := h.Disable(ctx, "network test", 0)
	if e != nil || len(report.Unconfirmed) > 0 {
		t.Fatalf("disable: %+v %v", report, e)
	}
	if _, e := os.Stat(filepath.Join(root, "network-removed")); e != nil {
		t.Fatal("disable left network")
	}
}

func TestDisableOrphanNetworkWithoutContainer(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	s.close()
	fakeDockerNetwork(root, []string{"network", "create", "--label", "myce.attempt=orphan", "myce-orphan"}, "")
	h := HostControl{Dir: w.HostStateDir, Clock: c, Docker: true, Service: &fakeHostService{}, Timeout: 2 * time.Second}
	report, e := h.Disable(context.Background(), "orphan network", 0)
	if e != nil || !report.Confirmed {
		t.Fatalf("disable: %+v %v", report, e)
	}
	if _, e := os.Stat(filepath.Join(root, "network-removed")); e != nil {
		t.Fatal("labeled orphan network survived disable")
	}
}
func TestDockerReadinessTimeoutAndStop(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprint(stop), func(t *testing.T) {
			w, s, c, root := dockerFixture(t)
			j := dockerGroupJob(t)
			var checked atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { checked.Store(true); rw.WriteHeader(503) }))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			port, _ := strconv.Atoi(u.Port())
			j.Spec.Run.DockerSidecars[0].Readiness = &DockerReadiness{Port: port, Path: "/ready", Timeout: "1s"}
			p, e := startDockerFixture(w, s, j)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(p.docker.close)
			await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running-sidecar-sortformer")); return e == nil })
			await(t, func() bool { return checked.Load() })
			if stop {
				p.stop(c.Now(), 0, true)
			} else {
				c.advance(time.Second)
			}
			select {
			case f := <-p.done:
				if f.result.Success {
					t.Fatal("failed readiness succeeded")
				}
				if !stop && !strings.Contains(f.result.Error, "readiness timeout") {
					t.Fatalf("wrong reason: %+v", f.result)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("readiness cleanup stuck")
			}
			if _, e := os.Stat(filepath.Join(root, "running")); e == nil {
				t.Fatal("main started after readiness refusal")
			}
		})
	}
}

func TestDockerExplicitSwapAndHiddenNetworkCustody(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerGroupJob(t)
	swap := 3.0
	j.Spec.Run.DockerMemorySwap = &swap
	j.Spec.Run.DockerSidecars[0].MemorySwap = &swap
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	if strings.Count(string(data), "--memory-swap 3221225472") != 2 {
		t.Fatalf("explicit swap missing: %s", data)
	}
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("hidden-network"), 0600)
	p.stop(c.Now(), 0, true)
	select {
	case <-p.done:
		t.Fatal("saved network disappeared from filtered scan; custody released")
	case <-time.After(700 * time.Millisecond):
	}
	_ = os.WriteFile(filepath.Join(root, "mode"), nil, 0600)
	select {
	case <-p.done:
	case <-time.After(4 * time.Second):
		t.Fatal("cleanup did not recover")
	}
}

func TestDockerGroupAdmissionUsesSimultaneousJobMemory(t *testing.T) {
	q, _ := fixture(t)
	m := machine("group", "fake")
	m.HostID = "group-host"
	m.HostMemoryMeasured = true
	m.HostAvailableMemoryGB = 32
	m.HostMemoryBudgetGB = 6
	check(t, q, m, nil)
	s := dockerGroupJob(t).Spec
	s.Needs.HostMemoryGB = 4
	s.Run.DockerMemoryGB = 3
	for i := range s.Run.DockerSidecars {
		s.Run.DockerSidecars[i].MemoryGB = 3
	}
	j := submit(t, q, s)
	r := check(t, q, m, nil).Job
	if r == nil || r.ID != j.ID {
		t.Fatal("summed container peaks prevented admission")
	}
	snap, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if snap.Machines[0].FreeHostMemoryGB != 2 {
		t.Fatalf("admission did not charge job memory: %g", snap.Machines[0].FreeHostMemoryGB)
	}
}

func TestDockerReadinessRejectsSuccessAtDeadline(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerGroupJob(t)
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { c.advance(time.Second); rw.WriteHeader(200) }))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(u.Port())
	j.Spec.Run.DockerSidecars[0].Readiness = &DockerReadiness{Port: port, Path: "/ready", Timeout: "1s"}
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	select {
	case f := <-p.done:
		if f.result.Success || !strings.Contains(f.result.Error, "readiness timeout") {
			t.Fatalf("late readiness: %+v", f.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deadline response admitted main")
	}
	if _, e := os.Stat(filepath.Join(root, "running")); e == nil {
		t.Fatal("main started at expired readiness boundary")
	}
}

func TestDockerReadinessFailureIsCommandFailureNotLoss(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	j := dockerGroupJob(t)
	var checked atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { checked.Store(true); rw.WriteHeader(503) }))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	port, _ := strconv.Atoi(u.Port())
	j.Spec.Run.DockerSidecars[0].Readiness = &DockerReadiness{Port: port, Path: "/ready", Timeout: "1s"}
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(p.docker.close)
	await(t, func() bool { return checked.Load() })
	c.advance(time.Second)
	select {
	case f := <-p.done:
		if f.result.Interrupted || f.result.Success {
			t.Fatalf("readiness failure became loss: %+v", f.result)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("cleanup stuck")
	}
	if _, e := os.Stat(filepath.Join(root, "running")); e == nil {
		t.Fatal("main started")
	}
}

func TestDockerGroupRestartCleansSidecarsAndNetwork(t *testing.T) {
	w, s, _, root := dockerFixture(t)
	j := dockerGroupJob(t)
	p, e := startDockerFixture(w, s, j)
	if e != nil {
		t.Fatal(e)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	p.docker.close()
	restarted, e := w.restoreDocker(s)
	if e != nil || restarted == nil {
		t.Fatalf("restore: %v", e)
	}
	t.Cleanup(restarted.docker.close)
	select {
	case f := <-restarted.done:
		if !f.result.Interrupted || !f.result.Forced {
			t.Fatalf("restart: %+v", f.result)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("restart cleanup stuck")
	}
	for _, name := range []string{"sortformer", "naming"} {
		if _, e := os.Stat(filepath.Join(root, "stopped-sidecar-"+name)); e != nil {
			t.Fatal("sidecar survived restart")
		}
	}
	if _, e := os.Stat(filepath.Join(root, "network-removed")); e != nil {
		t.Fatal("network survived restart")
	}
}

func TestDockerNetworkRemovalEscalatesThroughQueue(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	s.close()
	q, _ := fixture(t)
	q.clock = c
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	w.Client = Client{URL: server.URL, Token: "test"}
	w.Interval = time.Millisecond
	w.Machine.HostID = "network-host"
	w.CheckpointRoot = q.root
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	j := submit(t, q, dockerGroupJob(t).Spec)
	await(t, func() bool {
		c.advance(time.Millisecond)
		_, e := os.Stat(filepath.Join(root, "running"))
		return e == nil
	})
	_ = os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-network-rm"), 0600)
	if e := q.Cancel(j.ID); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		c.advance(time.Second)
		data, _ := os.ReadFile(filepath.Join(root, "calls"))
		return strings.Contains(string(data), "network rm")
	})
	c.advance(2 * time.Minute)
	snap, e := q.Snapshot()
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(strings.Join(snap.HostHolds, " "), "operator action required") {
		t.Fatalf("network hold not escalated: %v", snap.HostHolds)
	}
	requireState(t, q, j.ID, "stopping")
	_ = os.WriteFile(filepath.Join(root, "mode"), nil, 0600)
	await(t, func() bool { c.advance(time.Millisecond); return job(t, q, j.ID).State == "cancelled" })
	cancel()
	deadline := time.After(6 * time.Second)
	for {
		c.advance(time.Millisecond)
		select {
		case e := <-done:
			if e != nil {
				t.Fatal(e)
			}
			return
		case <-deadline:
			t.Fatal("worker did not finish")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
