package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

// Use the real worker in an owned subprocess so stopping it cannot deliver the
// outstanding attempt as a side effect of a graceful queue check-out.
func startReconcileWorker(t *testing.T, endpoint, spool, hostDir string) func() {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerRecoveryCrashHelper$")
	cmd.Env = append(os.Environ(), "MYCE_RECOVERY_TEST_HELPER=1", "MYCE_RECOVERY_TEST_URL="+endpoint, "MYCE_RECOVERY_TEST_STATE="+spool, "MYCE_HOST_STATE_DIR="+hostDir)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if err := cmd.Process.Kill(); err != nil {
				t.Errorf("stop owned worker: %v", err)
			}
			_ = cmd.Wait()
		})
	}
	t.Cleanup(func() {
		stop()
		s, err := (Worker{StateDir: spool}).openStore()
		if err != nil {
			t.Error(err)
			return
		}
		defer s.close()
		if err = s.cleanupOrphan(RealClock{}); err != nil {
			t.Error(err)
		}
		if t.Failed() {
			t.Logf("owned worker output: %s", output.String())
		}
	})
	return stop
}

func readReconcileWorkerState(spool string) (workerState, error) {
	var s workerState
	b, err := os.ReadFile(filepath.Join(spool, "state.json"))
	if err == nil {
		err = json.Unmarshal(b, &s)
	}
	return s, err
}

func TestHostReconcileRotatedWorkerSessionSettles(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(stringBool(recovery), func(t *testing.T) {
			q, clock := fixture(t)
			hostDir := readyTestHost(t, t.TempDir())
			spool := t.TempDir()
			var unregistered atomic.Bool
			api := API{Queue: q, Token: "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/workers/checkin" {
					b, err := io.ReadAll(r.Body)
					var c Checkin
					if err != nil || json.Unmarshal(b, &c) != nil {
						unregistered.Store(true)
					}
					r.Body = io.NopCloser(bytes.NewReader(b))
					workers, err := HostWorkers(hostDir)
					if err != nil || len(workers) != 1 || workers[0].Session != c.Machine.Session || c.Machine.Session == "" {
						unregistered.Store(true)
					}
				}
				api.ServeHTTP(w, r)
			}))
			defer server.Close()
			spec := spec("experiment")
			spec.Run.Command = []string{"sh", "-c", `trap 'exit 0' TERM; echo ready-$MYCE_ATTEMPT; while :; do sleep 0.05; done`}
			var assigned *Job
			if recovery {
				assigned = submit(t, q, spec)
			}
			stop := startReconcileWorker(t, server.URL, spool, hostDir)
			var previous string
			await(t, func() bool {
				st, err := readReconcileWorkerState(spool)
				if err != nil || st.Session == "" || st.Fresh {
					return false
				}
				previous = st.Session
				if recovery {
					logs, _ := q.Logs(assigned.ID, 0)
					return len(logs) > 0
				}
				return true
			})
			stop()
			if !recovery {
				assigned = submit(t, q, spec)
			}
			stop = startReconcileWorker(t, server.URL, spool, hostDir)
			await(t, func() bool {
				got := job(t, q, assigned.ID)
				if got.Session == "" || got.Session == previous || got.State != "running" || got.StartedAt == nil {
					return false
				}
				// Started authorizes the payload before releasing its launch gate.
				// Crash an executing payload, not the in-flight authorization call.
				logs, _ := q.Logs(assigned.ID, 0)
				for _, line := range logs {
					if line.Attempt == got.Attempt {
						return true
					}
				}
				return false
			})
			before := job(t, q, assigned.ID)
			stop()
			saved, err := (Worker{StateDir: spool}).openStore()
			if err != nil {
				t.Fatal(err)
			}
			err = saved.cleanupOrphan(RealClock{})
			saved.close()
			if err != nil {
				t.Fatal(err)
			}
			if unregistered.Load() {
				t.Error("a worker checked in before its session was durably registered")
			}
			// The worker and recorded command really stopped. Only the Linux
			// service/cgroup observation is a fake on this laptop.
			if r, err := (HostControl{Observe: testHostExclusion, Dir: hostDir, Clock: clock, Service: &fakeHostService{}}).Disable(context.Background(), "stopped worker", 0); err != nil || !r.Confirmed {
				t.Fatal(r, err)
			}
			if err := ReconcileHostLocal(context.Background(), hostDir, clock, func(endpoint string) (Client, error) { return Client{URL: endpoint, Token: "test"}, nil }); err != nil {
				t.Fatal(err)
			}
			s, err := ReadHostIntent(hostDir)
			got := job(t, q, assigned.ID)
			if err != nil || s.HostSettlement() != "confirmed" || len(s.Pending) != 0 || got.State != "queued" || got.Losses != before.Losses {
				t.Fatal("rotated session did not settle", s, got, err)
			}
		})
	}
}

func TestHostReconcileCloseSupersedesUndeliveredAttempts(t *testing.T) {
	for _, mode := range []string{"experiment", "production", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("one", "fake")
			m.HostID = "host"
			check(t, q, m, nil)
			s := spec("experiment")
			want := "queued"
			if mode == "production" {
				s = spec("production")
				s.AllowedMachines = []string{m.ID}
				want = "failed"
			}
			j := submit(t, q, s)
			j = check(t, q, m, nil).Job
			if err := q.Started(result(j, false)); err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				cancelAck(t, q, j.ID)
				want = "cancelled"
			}
			request := closeExchange(m.HostID, clock)
			request.Sessions = []HostSession{{Machine: m.ID, Session: m.Session}}
			if ack, err := q.ReconcileHost(request); err != nil || len(ack.Pending) != 0 {
				t.Fatal(ack, err)
			}
			if got := job(t, q, j.ID); got.State != want || got.Losses != 0 {
				t.Fatal(got)
			}
			if err := q.Complete(result(j, true)); !errors.Is(err, ErrStale) {
				t.Fatal("superseded result was not stale", err)
			}
		})
	}
}

func TestHostReconcileUndeliveredFileOutputsDoNotBlockRelease(t *testing.T) {
	q, clock := fixture(t)
	store := t.TempDir()
	if err := q.ConfigureFileStore(store, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	var database string
	if err := q.db.QueryRow("PRAGMA database_list").Scan(new(int), new(string), &database); err != nil {
		t.Fatal(err)
	}
	hostDir, spool := readyTestHost(t, t.TempDir()), t.TempDir()
	var mu sync.RWMutex
	active := q
	var offline atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		if offline.Load() {
			http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
			return
		}
		API{Queue: active, Token: "test"}.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	input := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(input, []byte("input"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.UploadFile(context.Background(), input, ref); err != nil {
		t.Fatal(err)
	}
	finish := filepath.Join(t.TempDir(), "finish")
	spec := spec("experiment")
	spec.Files = FileManifest{"input": ref}
	spec.Run.Command = []string{"sh", "-c", `echo ready; while [ ! -f "$FINISH" ]; do sleep 0.02; done; echo preserved-output > output`}
	spec.Run.Env = map[string]string{"FINISH": finish}
	spec.Outputs = map[string]string{"output": "results"}
	j := submit(t, q, spec)
	stop := startReconcileWorker(t, server.URL, spool, hostDir)
	await(t, func() bool { logs, _ := q.Logs(j.ID, 0); return len(logs) > 0 })
	offline.Store(true)
	mu.Lock()
	err = q.Close()
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(finish, nil, 0600); err != nil {
		t.Fatal(err)
	}
	var saved Result
	var resultPath string
	await(t, func() bool {
		st, err := readReconcileWorkerState(spool)
		if err != nil || st.Identity == "" {
			return false
		}
		resultPath = filepath.Join(spool, st.Identity+".result.json")
		b, err := os.ReadFile(resultPath)
		return err == nil && json.Unmarshal(b, &saved) == nil && saved.Success
	})
	stop()
	st, err := readReconcileWorkerState(spool)
	if err != nil || st.Job == nil || !st.Job.FileTransport || len(saved.OutputRefs) != 0 {
		t.Fatal(st, saved, err)
	}
	output := filepath.Join(st.Job.Spec.Run.Workdir, "output")
	outputBefore, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	resultBefore, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatal(err)
	}
	if r, err := (HostControl{Observe: testHostExclusion, Dir: hostDir, Clock: clock, Service: &fakeHostService{}}).Disable(context.Background(), "undelivered output", 0); err != nil || !r.Confirmed {
		t.Fatal(r, err)
	}
	mu.Lock()
	active, err = Open(database, q.root, clock)
	if err == nil {
		err = active.ConfigureFileStore(store, 1<<20, true)
	}
	mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	offline.Store(false)
	if err = ReconcileHostLocal(context.Background(), hostDir, clock, func(string) (Client, error) { return client, nil }); err != nil {
		t.Fatal("delivery vetoed session release", err)
	}
	s, err := ReadHostIntent(hostDir)
	got := job(t, active, j.ID)
	if err != nil || s.HostSettlement() != "confirmed" || len(s.Pending) != 0 || !slices.Contains(s.Holds, "diagnostic:undelivered_result") || got.State != "queued" || got.Losses != 0 || len(got.OutputRefs) != 0 {
		t.Fatal(s, got, err)
	}
	for path, expected := range map[string][]byte{output: outputBefore, resultPath: resultBefore} {
		b, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(b, expected) {
			t.Fatal("undelivered evidence changed", path, err)
		}
	}
	// Missing delivery is stale before manifest validation; even a later complete
	// manifest cannot publish the superseded attempt.
	for _, delivered := range []bool{false, true} {
		if delivered {
			ref, err := IdentifyFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if err = client.UploadFile(context.Background(), output, ref); err != nil {
				t.Fatal(err)
			}
			saved.OutputRefs = FileManifest{"output": ref}
		}
		err = client.Call(context.Background(), "POST", "/api/workers/complete", saved, nil)
		var conflict *HTTPError
		if !errors.As(err, &conflict) || conflict.Status != http.StatusConflict {
			t.Fatal("superseded attempt was not stale", delivered, err)
		}
		got = job(t, active, j.ID)
		if got.State != "queued" || len(got.OutputRefs) != 0 || got.Losses != 0 {
			t.Fatal("superseded output published", got)
		}
	}
}
