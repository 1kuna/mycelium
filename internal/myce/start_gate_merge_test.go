package myce

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProductionStartGateMixedVersions(t *testing.T) {
	q, _ := fixture(t)
	handler := API{Queue: q, Token: "test"}.Handler()
	req := httptest.NewRequest("POST", "/api/workers/checkin", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer test")
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	if rw.Code != http.StatusUpgradeRequired || !strings.Contains(rw.Body.String(), "upgrade queue and workers together") {
		t.Fatalf("old worker accepted: %d %s", rw.Code, rw.Body.String())
	}
	for _, body := range []string{`{"ok":true}`, `{"ok":false,"start_gate":"durable-start-v1"}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write([]byte(body))
			}))
			defer server.Close()
			w := recoveryWorker(t)
			w.HostStateDir = readyTestHost(t, t.TempDir())
			w.Client = Client{URL: server.URL}
			now := time.Now()
			j := Job{ID: "test", Spec: spec("production")}
			j.Spec.StartBy = &now
			if err := w.authorizeStart(j, "test"); err == nil || !strings.Contains(err.Error(), "protocol mismatch") {
				t.Fatalf("old queue accepted: %v", err)
			}
		})
	}
	w := Worker{}
	if _, err := w.start(nil, Job{Spec: spec("production")}); err == nil || !strings.Contains(err.Error(), "no start_by") {
		t.Fatalf("legacy assignment accepted: %v", err)
	}
}

func TestProductionStartGateDisableInFlight(t *testing.T) {
	for _, scope := range []string{"host", "instance"} {
		t.Run(scope, func(t *testing.T) {
			q, _ := fixture(t)
			s := spec("production")
			s.Resumable = false
			marker := filepath.Join(t.TempDir(), "ran")
			s.Run.Command = []string{"/bin/sh", "-c", "echo ran > " + marker}
			submit(t, q, s)
			a := check(t, q, machine("b70", "xpu"), nil).Job
			root := readyTestHost(t, t.TempDir())
			unit, err := WorkerUnitDirectory(root, "myce-worker@gpu1.service")
			if err != nil {
				t.Fatal(err)
			}
			if err = os.MkdirAll(unit, 0700); err != nil {
				t.Fatal(err)
			}
			flagDir := root
			if scope == "instance" {
				flagDir = unit
			}
			handler := API{Queue: q, Token: "test"}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(rw, r)
				if r.URL.Path == "/api/workers/started" {
					// The durable answer is accepted, but disable wins before it reaches the worker.
					if e := closeTestHost(flagDir); e != nil {
						t.Error(e)
					}
				}
			}))
			defer server.Close()
			w := recoveryWorker(t)
			w.HostStateDir = unit
			w.Machine = machine("b70", "xpu")
			w.Client = Client{URL: server.URL, Token: "test"}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = a.Session
			p, err := w.start(store, *a)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case f := <-p.done:
				if f.result.Success || f.result.ReleasePending || f.err != nil {
					t.Fatalf("unsafe finish: %+v", f)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("start cleanup stalled")
			}
			if _, err = os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("disabled payload executed: %v", err)
			}
			if job(t, q, a.ID).StartedAt == nil {
				t.Fatal("accepted start lost custody")
			}
			if err = w.authorizeStart(*a, a.Session); err == nil {
				t.Fatal("disabled worker requested another gate")
			}
		})
	}
}

func TestProductionStartGateQueueHostDisable(t *testing.T) {
	q, _ := fixture(t)
	submit(t, q, spec("production"))
	a := check(t, q, machine("b70", "xpu"), nil).Job
	if err := q.DisableHost("b70", "test disable", false); err != nil {
		t.Fatal(err)
	}
	if err := q.Started(result(a, true)); err == nil {
		t.Fatal("disabled host accepted start")
	}
	if job(t, q, a.ID).StartedAt != nil {
		t.Fatal("disabled attempt recorded started")
	}
}

func TestProductionStartGatePlannedStopDoesNotRenewOnLoss(t *testing.T) {
	for _, path := range []string{"quiet", "restart", "completion"} {
		t.Run(path, func(t *testing.T) {
			q, c := fixture(t)
			if err := q.SetLossTimeouts(5*time.Second, 20*time.Second, 60*time.Second); err != nil {
				t.Fatal(err)
			}
			s := spec("production")
			s.AllowedMachines = []string{"b70"}
			j := submit(t, q, s)
			m := machine("b70", "xpu")
			a := check(t, q, m, nil).Job
			check(t, q, machine("witness", "fake"), nil)
			if err := q.Started(result(a, true)); err != nil {
				t.Fatal(err)
			}
			if err := q.Leave(m.ID, true); err != nil {
				t.Fatal(err)
			}
			requireState(t, q, j.ID, "stopping")
			c.Advance(5 * time.Second)
			switch path {
			case "quiet":
				check(t, q, machine("witness", "fake"), nil)
			case "restart":
				m.Session = "new-session"
				if _, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: a.Session}); err != nil {
					t.Fatal(err)
				}
			case "completion":
				r := result(a, false)
				r.Interrupted = true
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
			}
			got := job(t, q, j.ID)
			if !got.Spec.StartBy.Equal(*j.Spec.StartBy) {
				t.Fatalf("planned stop renewed deadline: %s, want %s", got.Spec.StartBy, j.Spec.StartBy)
			}
		})
	}
}
