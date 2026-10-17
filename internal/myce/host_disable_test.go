package myce

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDisableOfflineFlagAndOwnedProcessGrace(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	root, state := t.TempDir(), t.TempDir()
	if err := saveWorkerJSON(root, "worker-test.json", HostWorker{State: state, Host: "b70", Queue: "http://offline"}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(state, "state.json", workerState{PID: 42, Identity: "owned"}); err != nil {
		t.Fatal(err)
	}
	live := true
	signals := []syscall.Signal{}
	h := HostControl{Dir: root, Clock: clock, Processes: HostProcesses{
		Owned: func(st workerState) (bool, error) { return st.Identity == "owned" && live, nil },
		Signal: func(st workerState, sig syscall.Signal) error {
			signals = append(signals, sig)
			if sig == syscall.SIGKILL {
				live = false
			}
			return nil
		},
		Pause: func(d time.Duration) { clock.Advance(d) },
	}}
	ensureTestHost(t, h.Dir, h.Clock)
	r, err := h.Disable(context.Background(), "Verso drill", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if live || len(signals) != 2 || signals[0] != syscall.SIGTERM || signals[1] != syscall.SIGKILL {
		t.Fatalf("cleanup signals %v, live=%v", signals, live)
	}
	if r.Stopped != 1 {
		t.Fatalf("report %+v", r)
	}
	disabled, err := HostDisabled(root)
	if err != nil || disabled == "" {
		t.Fatalf("flag missing: %q %v", disabled, err)
	}
	w := Worker{HostStateDir: root}
	if err := w.Run(context.Background()); err != nil {
		t.Fatalf("disabled restart did not exit cleanly: %v", err)
	}
	current, e := ReadHostIntent(root)
	if e != nil {
		t.Fatal(e)
	}
	h.Expected = current.HostRevision
	h.Settled = func(HostIntent) error { return nil }
	h.Expected = readTestHostRevision(t, root)
	if err := h.Enable(context.Background()); err == nil {
		t.Fatal("offline close opened without queue settlement")
	}
	if disabled, err = HostDisabled(root); err != nil || disabled == "" {
		t.Fatalf("refused open changed close flag %q %v", disabled, err)
	}
}

func TestDisableNowNeverSignalsUnownedAndReportsSurvivor(t *testing.T) {
	for _, survives := range []bool{false, true} {
		t.Run(fmt.Sprint(survives), func(t *testing.T) {
			root, state := t.TempDir(), t.TempDir()
			clock := &fakeClock{now: time.Unix(1000, 0)}
			saveWorkerJSON(root, "worker-test.json", HostWorker{State: state})
			saveWorkerJSON(state, "state.json", workerState{PID: 42, Identity: "owned"})
			saveWorkerJSON(state, "engine-unrelated.json", workerState{PID: 43, Identity: "similar-name"})
			live := true
			h := HostControl{Dir: root, Clock: clock, Processes: HostProcesses{
				Owned: func(st workerState) (bool, error) { return st.Identity == "owned" && live, nil },
				Signal: func(st workerState, sig syscall.Signal) error {
					if st.Identity != "owned" || sig != syscall.SIGKILL {
						t.Fatalf("wrong target/signal: %+v %v", st, sig)
					}
					if !survives {
						live = false
					}
					return nil
				}, Pause: func(d time.Duration) { clock.Advance(d) },
			}}
			ensureTestHost(t, h.Dir, h.Clock)
			r, err := h.Disable(context.Background(), "now", 0)
			if (err != nil) != survives || (len(r.Unconfirmed) > 0) != survives {
				t.Fatalf("cleanup result %+v %v", r, err)
			}
		})
	}
}

func TestDisableRealOwnedGroupsAndSimilarUnownedProcess(t *testing.T) {
	root, state, env := t.TempDir(), t.TempDir(), t.TempDir()
	saveWorkerJSON(root, "worker-test.json", HostWorker{State: state, Environments: env, Host: "test"})
	start := func(identity string) *exec.Cmd {
		cmd := exec.Command("sh", "-c", `trap '' TERM; echo ready; while :; do sleep 1; done`, "myce-supervisor-"+identity)
		cmd.Env = append(os.Environ(), "MYCE_PROCESS_ID="+identity)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdout, e := cmd.StdoutPipe()
		if e != nil {
			t.Fatal(e)
		}
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		if _, e = bufio.NewReader(stdout).ReadString('\n'); e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() })
		return cmd
	}
	for i, path := range []string{filepath.Join(state, "state.json"), filepath.Join(state, "engine-test.json"), filepath.Join(env, "build-state.json")} {
		identity := fmt.Sprintf("owned-real-%d-%s", i, ID())
		cmd := start(identity)
		if err := saveWorkerJSON(filepath.Dir(path), filepath.Base(path), workerState{PID: cmd.Process.Pid, Identity: identity}); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := start("similar-but-not-owned")
	if err := saveWorkerJSON(state, "attempt-stale.json", workerState{PID: unrelated.Process.Pid, Identity: "similar-but-not-owned-extra"}); err != nil {
		t.Fatal(err)
	}
	h := HostControl{Dir: root}
	ensureTestHost(t, h.Dir, h.Clock)
	r, err := h.Disable(context.Background(), "drill", 100*time.Millisecond)
	requireNativeMarkerDisableReport(t, h, r, err)
	if r.Stopped != 3 {
		t.Fatalf("stopped %+v", r)
	}
	if e := syscall.Kill(unrelated.Process.Pid, 0); e != nil {
		t.Fatalf("unrelated process was touched: %v", e)
	}
}

func TestHostDisablePendingDoesNotPrematurelyRequeueDuringGrace(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	j := submit(t, q, spec("production"))
	running := check(t, q, m, nil).Job
	if err := q.Started(result(running, true)); err != nil {
		t.Fatal(err)
	}
	if err := closeQueueHost(q, "host", "checkpoint grace", nil); err != nil {
		t.Fatal(err)
	}
	clock.Advance(3 * time.Minute)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "stopping" || got.Losses != 0 {
		t.Fatalf("released without cleanup: %+v", got)
	}
	if r := check(t, q, m, running); r.Kill {
		t.Fatal("queue bypassed local checkpoint grace")
	}
}

func TestHostDisableQueueAndPagePersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Unix(1000, 0)}
	path := filepath.Join(dir, "queue.db")
	q, err := Open(path, dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	m.HostID = "host"
	check(t, q, m, nil)
	if err = closeQueueHost(q, "host", "operator drill", nil); err != nil {
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, dir, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if reply := check(t, q, m, nil); !reply.Leave || reply.Job != nil {
		t.Fatalf("restart lost fencing: %+v", reply)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	req, _ := http.NewRequest("GET", server.URL, nil)
	req.Header.Set("Authorization", "Bearer test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "disabled by operator") {
		t.Fatal("disabled host not shown on status page")
	}
	if strings.Contains(string(b), "operator drill") || q.s.HostIntents["host"].Reason != "operator drill" {
		t.Fatal("status must retain the private operator reason internally without displaying it")
	}
}

func TestDisableRecordedWorkerLifetimeAndPIDReuse(t *testing.T) {
	root := readyTestHost(t, t.TempDir())
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	saveWorkerJSON(root, "worker-test.json", HostWorker{State: t.TempDir(), PID: cmd.Process.Pid, Birth: birth + "-stale"})
	if _, err = (HostControl{Dir: root}).Disable(context.Background(), "stale PID", 0); err != nil {
		t.Fatal(err)
	}
	if err = syscall.Kill(cmd.Process.Pid, 0); err != nil {
		t.Fatal("reused unrelated worker PID was killed")
	}
	saveWorkerJSON(root, "worker-test.json", HostWorker{State: t.TempDir(), PID: cmd.Process.Pid, Birth: birth})
	if _, err = (HostControl{Dir: root}).Disable(context.Background(), "real worker", 0); err != nil {
		t.Fatal(err)
	}
	live, err := ownedHostProcess(workerState{PID: cmd.Process.Pid, Birth: birth})
	if err != nil || live {
		t.Fatalf("worker not gone %v %v", live, err)
	}
}
