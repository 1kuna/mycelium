package myce

import (
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the real adapter, durable custody and queue through the fake daemon.
func TestDockerProductionOutcomeCause(t *testing.T) {
	for _, cause := range []string{"stop-main", "stop-sidecar", "stop-readiness", "exit-one", "oom", "oom-with-stop", "sidecar-readiness-exit", "readiness-timeout", "ambiguous-main", "ambiguous-sidecar", "worker-recovery", "daemon-recovery", "exhaustion"} {
		t.Run(cause, func(t *testing.T) {
			w, store, clock, root := dockerFixture(t)
			q, _ := fixture(t)
			q.clock = clock
			w.CheckpointRoot = q.root
			m := w.Machine
			m.HostMemoryMeasured, m.HostAvailableMemoryGB, m.FreeHostMemoryGB = true, 32, 32
			j := dockerGroupJob(t)
			j.Spec.Priority, j.Spec.SubmissionKey = "production", "docker-stage"
			var probed atomic.Bool
			if strings.Contains(cause, "readiness") {
				server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
					probed.Store(true)
					rw.WriteHeader(http.StatusServiceUnavailable)
				}))
				defer server.Close()
				u, _ := url.Parse(server.URL)
				port, _ := strconv.Atoi(u.Port())
				j.Spec.Run.DockerSidecars[0].Readiness = &DockerReadiness{Port: port, Path: "/ready", Timeout: "10s"}
			}
			submitted := submit(t, q, j.Spec)
			// A configured value above two must not expand the agreed budget.
			if err := q.SetProductionInfrastructureRetries(4); err != nil {
				t.Fatal(err)
			}
			attempts := 1
			if cause == "exhaustion" {
				attempts = 3
			}
			for attempt := 1; attempt <= attempts; attempt++ {
				if attempt > 1 {
					// Each fake daemon uses fixed container IDs. A fresh fixture
					// models the next attempt without inheriting old daemon objects.
					w, store, _, root = dockerFixture(t)
					w.Clock, w.CheckpointRoot = clock, q.root
				}
				running := check(t, q, m, nil).Job
				if running == nil || running.ID != submitted.ID || running.Attempt != attempt {
					t.Fatalf("assignment %d: %+v", attempt, running)
				}
				if err := q.Started(result(running, false)); err != nil {
					t.Fatal(err)
				}
				p, err := startDockerFixture(w, store, *running)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(p.docker.close)
				if strings.Contains(cause, "readiness") {
					await(t, probed.Load)
				} else {
					await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
				}
				writeMode := func(mode string) {
					t.Helper()
					if err := os.WriteFile(filepath.Join(root, "mode"), []byte(mode), 0600); err != nil {
						t.Fatal(err)
					}
				}
				exitContainer := func(file string) {
					t.Helper()
					if err := os.Remove(filepath.Join(root, file)); err != nil {
						t.Fatal(err)
					}
				}
				switch cause {
				case "stop-main", "stop-sidecar", "exhaustion":
					// Record intent first; the daemon then reports a killed member.
					p.stop(clock.Now(), StopGrace, false)
					if cause == "stop-sidecar" {
						writeMode("sidecar-killed")
						exitContainer("running-sidecar-sortformer")
					} else {
						writeMode("main-killed")
						exitContainer("running")
					}
					clock.advance(StopGrace)
				case "stop-readiness":
					if err := q.Leave(m.ID, true); err != nil {
						t.Fatal(err)
					}
					p.stop(clock.Now(), 0, true)
				case "exit-one", "oom", "oom-with-stop", "ambiguous-main":
					mode := map[string]string{"exit-one": "main-exit-one", "oom": "main-oom", "oom-with-stop": "main-oom", "ambiguous-main": "main-killed"}[cause]
					writeMode(mode)
					if cause == "oom-with-stop" {
						p.stop(clock.Now(), 0, true)
					} else {
						exitContainer("running")
					}
					await(t, func() bool { p.docker.mu.Lock(); defer p.docker.mu.Unlock(); return p.docker.state.Stopped })
					clock.advance(StopGrace)
				case "sidecar-readiness-exit", "ambiguous-sidecar":
					if cause == "ambiguous-sidecar" {
						writeMode("sidecar-killed")
					}
					exitContainer("running-sidecar-sortformer")
				case "readiness-timeout":
					clock.advance(10 * time.Second)
				case "worker-recovery":
					p.docker.close()
					p, err = w.restoreDocker(store)
					if err != nil || p == nil {
						t.Fatal("restore", err)
					}
					t.Cleanup(p.docker.close)
				case "daemon-recovery":
					writeMode("down")
					await(t, func() bool { return strings.Contains(p.docker.status(), "unconfirmed") })
					exitContainer("running")
					writeMode("")
				}
				select {
				case finish := <-p.done:
					if finish.err != nil {
						t.Fatal(finish.err)
					}
					if err := q.Complete(finish.result); err != nil {
						t.Fatal(err)
					}
					got := job(t, q, submitted.ID)
					infra := strings.HasPrefix(cause, "stop-") || cause == "exhaustion" || strings.HasSuffix(cause, "recovery")
					want, retries := "failed", 0
					if infra {
						want, retries = "queued", min(attempt, 2)
						if attempt == 3 {
							want = "failed"
						}
					}
					if got.State != want || got.InfrastructureRetries != retries || finish.result.Interrupted != infra {
						t.Fatalf("result=%+v state=%s retries=%d, want %s/%d", finish.result, got.State, got.InfrastructureRetries, want, retries)
					}
					if strings.HasPrefix(cause, "ambiguous") && !strings.Contains(got.Reason, "SIGKILL without an infrastructure stop") {
						t.Fatal("status hid ambiguous kill cause", got.Reason)
					}
					if attempt == 3 && got.Reason != "infrastructure_retries_exhausted" {
						t.Fatal("missing exhaustion reason", got.Reason)
					}
					t.Logf("attempt=%d interrupted=%v state=%s retries=%d reason=%s", attempt, finish.result.Interrupted, got.State, got.InfrastructureRetries, got.Reason)
				case <-time.After(5 * time.Second):
					t.Fatal("Docker outcome did not settle")
				}
			}
		})
	}
}

func TestDockerReleasedStageFailureDoesNotRequeue(t *testing.T) {
	for _, mode := range []string{"main-nonzero", "main-oom"} {
		t.Run(mode, func(t *testing.T) {
			w, store, d, clock, root := restartResultFixture(t)
			if err := os.WriteFile(filepath.Join(root, "mode"), []byte(mode), 0600); err != nil {
				t.Fatal(err)
			}
			restartMainExit(t, d, root)
			clock.advance(StopGrace)
			for i := 0; i < 3 && !d.state.Released; i++ {
				restartTick(t, d)
			}
			if !d.state.Released {
				t.Fatal("cleanup did not release custody")
			}
			r := restartReleasedResult(t, w, store)
			if r.Interrupted || r.Success || r.OutOfMemory != (mode == "main-oom") {
				t.Fatal("recovery erased an observed stage failure", r)
			}
			q := restartQueue(t, store, w)
			q.s.Jobs[r.ID].Spec.Priority = "production"
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, r.ID)
			if got.State != "failed" || got.InfrastructureRetries != 0 {
				t.Fatal(got)
			}
		})
	}
}
