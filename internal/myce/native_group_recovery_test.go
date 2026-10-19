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

func TestNativeGroupWorkerCrashHelper(t *testing.T) {
	if os.Getenv("NATIVE_CRASH_WORKER") != "1" {
		return
	}
	w := Worker{Clock: RealClock{}, StateDir: os.Getenv("NATIVE_CRASH_STATE"), Client: Client{URL: os.Getenv("NATIVE_CRASH_URL"), Token: "review-synthetic"}, Machine: machine("review-worker", "fake"), Interval: 5 * time.Millisecond, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	if err := w.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNativeGroupSIGKILLRestart(t *testing.T) {
	for _, scrub := range []bool{false, true} {
		t.Run(fmt.Sprintf("scrub=%v", scrub), func(t *testing.T) {
			q, _ := fixture(t)
			q.clock = RealClock{}
			s := spec("experiment")
			s.Resumable = false
			s.Needs.HostMemoryGB = 2
			cmd := []string{"/bin/sleep", "600"}
			if scrub {
				cmd = []string{"/usr/bin/env", "-i", "/bin/sleep", "600"}
			}
			s.Run.Command = cmd
			s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: cmd, MemoryGB: 1}}
			j := submit(t, q, s)
			server := httptest.NewServer(API{Queue: q, Token: "review-synthetic"}.Handler())
			defer server.Close()
			stateDir := t.TempDir()
			child := exec.Command(os.Args[0], "-test.run=^TestNativeGroupWorkerCrashHelper$")
			child.Env = append(os.Environ(), "NATIVE_CRASH_WORKER=1", "NATIVE_CRASH_URL="+server.URL, "NATIVE_CRASH_STATE="+stateDir)
			child.Stderr = os.Stderr
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
			var before workerState
			await(t, func() bool {
				b, e := os.ReadFile(filepath.Join(stateDir, "state.json"))
				if e != nil {
					return false
				}
				_ = json.Unmarshal(b, &before)
				if before.PID <= 0 {
					return false
				}
				b, e = os.ReadFile(filepath.Join(stateDir, "native-"+before.Identity+".json"))
				if e != nil {
					return false
				}
				var v struct{ Members []nativeMember }
				_ = json.Unmarshal(b, &v)
				return len(v.Members) == 2 && v.Members[0].Phase == "running" && v.Members[1].Phase == "running"
			})
			defer func() { _ = syscall.Kill(-before.PID, syscall.SIGKILL); _ = waitWorkerGroup(before.PID, RealClock{}) }()
			// Prove the exec gate has completed, including env -i, before killing the worker.
			await(t, func() bool {
				b, _ := os.ReadFile(filepath.Join(stateDir, "native-"+before.Identity+".json"))
				var v struct{ Members []nativeMember }
				_ = json.Unmarshal(b, &v)
				for _, m := range v.Members {
					o, _ := exec.Command("ps", "-p", strconv.Itoa(m.PID), "-o", "comm=").Output()
					if !strings.Contains(string(o), "sleep") {
						return false
					}
				}
				return true
			})
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = child.Wait()
			await(t, func() bool {
				p, _ := workerGroupMembers(before.PID)
				for _, pid := range p {
					if pid == before.PID {
						return false
					}
				}
				return len(p) > 0
			})
			old, _ := workerGroupMembers(before.PID)
			t.Logf("after SIGKILL: old group live count=%d", len(old))
			if err := q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			w := Worker{Clock: RealClock{}, StateDir: stateDir, Client: Client{URL: server.URL, Token: "review-synthetic"}, Machine: machine("review-worker", "fake"), Interval: 5 * time.Millisecond, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
			go func() { done <- w.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case e := <-done:
					if e != nil {
						t.Error(e)
					}
				case <-time.After(10 * time.Second):
					t.Error("restart worker failed to stop")
				}
			}()
			await(t, func() bool { return job(t, q, j.ID).ReleaseConfirmed })
			left, e := workerGroupMembers(before.PID)
			t.Logf("after restart and release confirmation: survivors=%d err=%v", len(left), e)
			if e != nil || len(left) > 0 {
				t.Fatalf("restart falsely released custody with %d original members still live", len(left))
			}
		})
	}
}

func TestNativeGroupPortHandoff(t *testing.T) {
	for _, collisions := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(collisions), func(t *testing.T) { nativePortHandoff(t, collisions) })
	}
}

func nativePortHandoff(t *testing.T, collisions int) {
	t.Helper()
	q, _ := fixture(t)
	s := nativeSpec(t)
	s.Run.Env["NATIVE_TEST_BLOCK"] = "0"
	s.Priority = "production"
	s.SubmissionKey = "review-port-handoff"
	s.Run.Sidecars[0].Command = append(append([]string{}, s.Run.Sidecars[0].Command...), "--", "{port}")
	j := submit(t, q, s)
	m := machine("native", "fake", "native-host")
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB = 64
	m.HostAvailableMemoryGB = 64
	reply := check(t, q, m, nil)
	if reply.Job == nil {
		t.Fatal("no assignment")
	}
	stateDir := t.TempDir()
	ceilings := fakeCeilings(t)
	var n atomic.Int32
	var stolen []net.Listener
	var stealErr error
	api := API{Queue: q, Token: "review-synthetic"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/launch" {
			call := n.Add(1)
			if call >= 3 && call <= int32(2*collisions+1) && call%2 == 1 {
				b, _ := os.ReadFile(filepath.Join(stateDir, "state.json"))
				var st workerState
				_ = json.Unmarshal(b, &st)
				b, _ = os.ReadFile(filepath.Join(stateDir, "native-"+st.Identity+".json"))
				var v struct{ Members []nativeMember }
				_ = json.Unmarshal(b, &v)
				if len(v.Members) > 0 {
					var custody workerState
					b, e := os.ReadFile(filepath.Join(stateDir, "attempt-"+st.Identity+".json"))
					if e != nil || json.Unmarshal(b, &custody) != nil {
						t.Error("missing durable attempt custody", e)
					}
					member := v.Members[0]
					birth, e := processBirth(member.PID)
					if e != nil || custody.Groups[st.PID][member.PID] != birth || birth == "" || custody.Cgroup != filepath.Join(ceilings.root, "attempt-"+st.Identity) {
						t.Error("member reached exec gate without durable birth/group/cgroup custody", e)
					}
					out, e := exec.Command("ps", "-p", strconv.Itoa(v.Members[0].PID), "-o", "command=").Output()
					stealErr = e
					fields := strings.Fields(string(out))
					if len(fields) > 0 {
						var listener net.Listener
						listener, stealErr = net.Listen("tcp4", "127.0.0.1:"+fields[len(fields)-1])
						if stealErr == nil {
							stolen = append(stolen, listener)
						}
					}
				}
			}
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	defer func() {
		for _, l := range stolen {
			l.Close()
		}
	}()
	w := Worker{Clock: RealClock{}, StateDir: stateDir, Machine: m, Client: Client{URL: server.URL, Token: "review-synthetic"}, HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	w.memoryCeilings = ceilings
	store, e := w.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Session = m.Session
	p, e := w.start(store, *reply.Job)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { p.stop(w.Clock.Now(), 0, true) }()
	select {
	case finish := <-p.done:
		if len(stolen) != collisions || stealErr != nil {
			t.Fatalf("port theft not exercised: got %d want %d: %v", len(stolen), collisions, stealErr)
		}
		if e := q.Complete(finish.result); e != nil {
			t.Fatal(e)
		}
		cur := job(t, q, j.ID)
		t.Logf("raced allocation: success=%v interrupted=%v state=%s retries=%d error=%s", finish.result.Success, finish.result.Interrupted, cur.State, cur.InfrastructureRetries, finish.result.Error)
		if finish.result.ReleasePending {
			t.Fatal("group release unconfirmed", finish.result)
		}
		if collisions < nativeBindAttempts {
			if !finish.result.Success || finish.result.Interrupted || cur.State != "succeeded" || cur.InfrastructureRetries != 0 {
				t.Fatal("bind collision did not recover within attempt", finish.result)
			}
			env := nativeEnv(t, s.Run.Env["NATIVE_TEST_DIR"], "main")
			for _, l := range stolen {
				if env["MYCE_SIDECAR_HELPER_PORT"] == strconv.Itoa(l.Addr().(*net.TCPAddr).Port) {
					t.Fatal("main used a stolen endpoint")
				}
			}
		} else {
			if finish.result.Success || !finish.result.Interrupted || cur.State != "queued" || cur.InfrastructureRetries != 1 {
				t.Fatal("exhausted collisions did not use infrastructure requeue", finish.result)
			}
			if _, err := os.Stat(filepath.Join(s.Run.Env["NATIVE_TEST_DIR"], "main.json")); !os.IsNotExist(err) {
				t.Fatal("main started before readiness")
			}
		}
		nativeNoSurvivors(t, &nativeTestRun{p: p, dir: s.Run.Env["NATIVE_TEST_DIR"]})
	case <-time.After(30 * time.Second):
		p.stop(w.Clock.Now(), 0, true)
		<-p.done
		t.Fatal("unexpected run timeout")
	}
}

func TestNativeGroupProductionOutcomes(t *testing.T) {
	for _, mode := range []string{"timeout", "self-exit", "infrastructure"} {
		t.Run(mode, func(t *testing.T) {
			s := nativeSpec(t)
			s.Priority = "production"
			s.SubmissionKey = "review-" + mode
			if mode == "timeout" {
				s.Run.Sidecars[0].Readiness.Timeout = "150ms"
				s.Run.Sidecars[0].Env["NATIVE_TEST_UNREADY"] = "1"
			}
			r := nativeStart(t, s, "fake")
			if mode != "timeout" {
				nativeFile(t, r.dir, "main-ready")
				if mode == "infrastructure" {
					r.p.stop(r.w.Clock.Now(), time.Second, false)
				} else {
					data, _ := os.ReadFile(filepath.Join(r.dir, "side.json"))
					var v struct{ PID int }
					_ = json.Unmarshal(data, &v)
					if err := syscall.Kill(v.PID, syscall.SIGTERM); err != nil {
						t.Fatal(err)
					}
				}
			}
			result := nativeFinish(t, r)
			if err := r.q.Complete(result); err != nil {
				t.Fatal(err)
			}
			cur := job(t, r.q, r.j.ID)
			wantState, wantRetries := "failed", 0
			if mode == "infrastructure" {
				wantState, wantRetries = "queued", 1
			}
			t.Logf("%s: interrupted=%v state=%s infrastructure_retries=%d", mode, result.Interrupted, cur.State, cur.InfrastructureRetries)
			if cur.State != wantState || cur.InfrastructureRetries != wantRetries {
				t.Fatalf("expected %s/%d", wantState, wantRetries)
			}
		})
	}
}

func TestNativeGroupZeroSelfExit(t *testing.T) {
	s := nativeSpec(t)
	s.Priority = "production"
	s.SubmissionKey = "review-zero-exit"
	dir := s.Run.Env["NATIVE_TEST_DIR"]
	s.Run.Command = []string{"/bin/sleep", "600"}
	s.Run.Sidecars[0].Port = nil
	s.Run.Sidecars[0].Readiness = nil
	s.Run.Sidecars[0].Command = []string{"sh", "-c", `while [ ! -f "$REVIEW_EXIT" ]; do sleep 0.01; done; exit 0`}
	s.Run.Sidecars[0].Env = map[string]string{"REVIEW_EXIT": filepath.Join(dir, "self-exit")}
	r := nativeStart(t, s, "fake")
	await(t, func() bool { return r.p.native.started() })
	nativeTouch(t, dir, "self-exit")
	result := nativeFinish(t, r)
	if err := r.q.Complete(result); err != nil {
		t.Fatal(err)
	}
	cur := job(t, r.q, r.j.ID)
	t.Logf("zero self-exit: success=%v interrupted=%v state=%s retries=%d", result.Success, result.Interrupted, cur.State, cur.InfrastructureRetries)
	if result.Success || result.Interrupted || cur.State != "failed" || cur.InfrastructureRetries != 0 {
		t.Fatal("successful helper exit did not fail stage")
	}
}

// A birth mismatch must neither signal an unrelated live process nor clear
// custody. Use a real isolated process group, rather than a fake kill function.
func TestNativeGroupRecoveryBirthMismatch(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "600")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	st := workerState{Identity: ID(), PID: cmd.Process.Pid, Groups: map[int]map[int]string{cmd.Process.Pid: {cmd.Process.Pid: birth + "-different"}}}
	if err := saveWorkerJSON(dir, "attempt-"+st.Identity+".json", st); err != nil {
		t.Fatal(err)
	}
	store := workerStore{dir: dir, state: workerState{Identity: st.Identity, PID: st.PID, Job: &Job{}}}
	if err := store.cleanupOrphan(RealClock{}); err == nil {
		t.Fatal("birth mismatch released live group")
	}
	if now, err := processBirth(cmd.Process.Pid); err != nil || now != birth {
		t.Fatal("unrelated process was killed", err)
	}
	st.Groups[st.PID][st.PID] = birth
	if err := saveWorkerJSON(dir, "attempt-"+st.Identity+".json", st); err != nil {
		t.Fatal(err)
	}
	if err := store.cleanupOrphan(RealClock{}); err != nil {
		t.Fatal(err)
	}
	if _, err := processBirth(st.PID); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("authenticated group survived recovery", err)
	}
}
