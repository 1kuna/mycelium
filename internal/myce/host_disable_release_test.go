package myce

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNotifiedDisableDockerRelease(t *testing.T) {
	for _, mode := range []string{"graceful", "now", "unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			w, s, dockerClock, root := dockerFixture(t)
			m := machine(w.Machine.ID, "fake")
			m.HostID = "host"
			check(t, q, m, nil)
			j := submit(t, q, dockerJob().Spec)
			running := check(t, q, m, nil).Job
			if running == nil {
				t.Fatal("not assigned")
			}
			// Last heartbeat during cleanup must not survive confirmed host cleanup.
			m.DockerCleanup = "stopping"
			check(t, q, m, running)
			s.state = workerState{Session: m.Session, Job: running, Identity: "owned"}
			if err := s.save(); err != nil {
				t.Fatal(err)
			}
			s.close()
			for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": "term"} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			custody := dockerExecution{Identity: "owned", Endpoint: "unix:///var/run/docker.sock", Container: "id1", Operation: "start"}
			if mode == "unconfirmed" {
				custody.Pending = true
				custody.Operation = "create"
				custody.Container = ""
				if err := os.Remove(filepath.Join(root, "created")); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(root, "running")); err != nil {
					t.Fatal(err)
				}
			}
			if err := saveWorkerJSON(s.dir, "owned.docker.json", custody); err != nil {
				t.Fatal(err)
			}
			var acknowledgements atomic.Int32
			handler := (API{Queue: q, Token: "test"}).Handler()
			server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/api/workers/complete" {
					acknowledgements.Add(1)
				}
				handler.ServeHTTP(out, req)
			}))
			defer server.Close()
			if err := AdoptHostWorker(w.HostStateDir, HostWorker{State: s.dir, Host: m.HostID, Name: m.ID, Queue: server.URL}); err != nil {
				t.Fatal(err)
			}
			h := HostControl{Dir: w.HostStateDir, Service: &fakeHostService{}, Clock: dockerClock, Docker: true, Timeout: 3 * time.Second, Notify: NotifyHost(Client{Token: "test"})}
			grace := 10 * time.Second
			if mode == "now" {
				grace = 0
			}
			if mode == "unconfirmed" {
				h.Timeout = 500 * time.Millisecond
			}
			report, err := h.Disable(context.Background(), "operator test", grace)
			if mode == "unconfirmed" {
				if err == nil || report.Confirmed {
					t.Fatalf("unconfirmed accepted: %+v %v", report, err)
				}
				clock.Advance(2 * time.Minute)
				snap, err := q.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if acknowledgements.Load() != 0 {
					t.Fatal("acknowledged unconfirmed cleanup")
				}
				if len(q.s.HostReleases) != 1 || !strings.Contains(strings.Join(snap.HostHolds, " "), "operator action required") {
					t.Fatalf("custody lost: %+v", snap.HostHolds)
				}
				return
			}
			if err != nil || !report.Confirmed || len(report.QueueErrors) != 0 {
				t.Fatalf("disable: %+v %v", report, err)
			}
			if acknowledgements.Load() != 1 {
				t.Fatal("exact release acknowledgement not delivered before return")
			}
			calls, err := os.ReadFile(filepath.Join(root, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			signal := "--signal=TERM"
			if mode == "now" {
				signal = "--signal=KILL"
			}
			if !strings.Contains(string(calls), signal) {
				t.Fatalf("missing disable signal %s", signal)
			}
			if got := job(t, q, j.ID); got.State != "queued" || got.Losses != 0 || !got.RequeuedByOperator {
				t.Fatalf("operator policy: %+v", got)
			}
			clock.Advance(2 * time.Minute)
			snap, err := q.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if len(snap.HostHolds) != 0 || len(q.s.HostReleases) != 0 {
				t.Fatalf("hold after cleanup: %+v", snap.HostHolds)
			}
			// Rejoin's same release-only acknowledgement is safe to replay.
			ack := Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: running.Attempt}, ReleaseOnly: true}
			if err := q.Complete(ack); err != nil {
				t.Fatal(err)
			}
			if err := h.Enable(context.Background()); err != nil {
				t.Fatal(err)
			}
			m.DockerCleanup = ""
			next := check(t, q, m, nil).Job
			if next == nil || next.Attempt != running.Attempt+1 {
				t.Fatal("not reassigned")
			}
			for _, change := range []func(*Result){func(r *Result) {}, func(r *Result) { r.Attempt = next.Attempt; r.Session = "foreign" }, func(r *Result) { r.Attempt = next.Attempt; r.Machine = "foreign" }, func(r *Result) { r.Attempt = next.Attempt; r.ID = "foreign" }} {
				stale := ack
				change(&stale)
				if err := q.Complete(stale); err != nil {
					t.Fatal(err)
				}
				if got := job(t, q, j.ID); got.Attempt != next.Attempt || got.State != "running" || got.ReleaseConfirmed {
					t.Fatalf("stale ack changed replacement: %+v", got)
				}
				if len(q.s.HostReleases) != 1 {
					t.Fatal("stale ack settled replacement custody")
				}
			}
		})
	}
}
