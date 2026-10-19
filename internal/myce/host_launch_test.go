package myce

import (
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenancePendingEnvironmentPlan(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"late": {"fake": r}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "late"
	submit(t, q, s)
	m := machine("builder", "fake", "host")
	m.EnvironmentBuilder = true
	reply := check(t, q, m, nil)
	if len(reply.EnvironmentPlan) != 1 {
		t.Fatal("missing plan", reply)
	}
	if len(q.hostObligations("host")) == 0 {
		t.Fatal("issued environment plan is absent from shared work enumeration")
	}
	status := maintenanceChange(t, q, "drain", false)
	if !status.Quiescent {
		t.Fatal("unstarted plan was not cancelled", status)
	}
}

// Deliver the exact checkin reply after drain, including a build already waiting
// behind cache/preparation work. The final supervisor gate must deny the payload.
func TestMaintenanceDelayedEnvironmentLaunch(t *testing.T) {
	for _, delayed := range []string{"reply", "command"} {
		for _, action := range []string{"drain", "disable", "disable-stop"} {
			t.Run(delayed+"/"+action, func(t *testing.T) {
				q, _ := fixture(t)
				marker := filepath.Join(t.TempDir(), "payload")
				r, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"/bin/sh", "-c", `printf started > "$1"; printf '#!/bin/sh\nexit 0\n' > "$2/bin/tool"; chmod +x "$2/bin/tool"`, "fixture", marker, "{cache}"}}, "fake")
				if err != nil {
					t.Fatal(err)
				}
				if err = q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"late": {"fake": r}}}); err != nil {
					t.Fatal(err)
				}
				s := spec("experiment")
				s.Environment.Name = "late"
				submit(t, q, s)
				m := machine("builder", "fake", "host")
				m.EnvironmentBuilder = true
				plan := check(t, q, m, nil).EnvironmentPlan
				if len(plan) != 1 || plan[0].Launch == nil {
					t.Fatal("missing exact plan")
				}
				server := httptest.NewServer(API{q, "test"}.Handler())
				defer server.Close()
				worker := Worker{Machine: m, Client: Client{URL: server.URL, Token: "test"}}
				entered, release := make(chan struct{}), make(chan struct{})
				calls := 0
				authorize := func(l HostLaunch) error {
					calls++
					if delayed == "command" && calls == 2 {
						close(entered)
						<-release
					}
					return worker.authorizeLaunch(l)
				}
				rt := &environmentRuntime{authorize: authorize, dir: t.TempDir(), cache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}, reports: map[string]EnvironmentReport{}}
				defer rt.stop()
				change := func() HostMaintenanceStatus {
					name := action
					if name == "disable-stop" {
						name = "disable"
					}
					return maintenanceChange(t, q, name, action == "disable-stop")
				}
				if delayed == "reply" {
					if st := change(); !st.Quiescent {
						t.Fatal("pending plan not cancelled", st)
					}
				}
				rt.sync(plan, false)
				if delayed == "command" {
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("did not reach final command gate")
					}
					st := change()
					if st.Quiescent || st.Preparations != 1 {
						t.Fatal("authorized preparation custody lost", st)
					}
					close(release)
				}
				rt.mu.Lock()
				done := rt.done
				rt.mu.Unlock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("build did not settle")
				}
				if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("payload ran after maintenance", err)
				}
				_, reports, _ := rt.report()
				m.EnvironmentReports = reports
				check(t, q, m, nil)
				st, err := q.HostMaintenanceStatus("host")
				if err != nil || !st.Quiescent || st.Preparations != 0 {
					t.Fatal("exact cancellation did not settle custody", st, err)
				}
				maintenanceChange(t, q, "resume", false)
				if err := q.AuthorizeLaunch(*plan[0].Launch); !errors.Is(err, ErrStale) {
					t.Fatal("resume resurrected cached plan", err)
				}
			})
		}
	}
}

func TestMaintenancePreparationExactCompletion(t *testing.T) {
	q, _ := fixture(t)
	m := machine("builder", "fake", "host")
	check(t, q, m, nil)
	for _, kind := range []string{"environment", "copy", "benchmark"} {
		l := q.issuePreparation(&m, kind, "fixture")
		if err := q.AuthorizeLaunch(l); err != nil {
			t.Fatal(err)
		}
		st := maintenanceChange(t, q, "drain", false)
		if st.Quiescent || st.Preparations != 1 {
			t.Fatal("missing started custody", st)
		}
		m.EnvironmentReports = []EnvironmentReport{{Key: l.Key, Attempt: "wrong", State: "ready"}}
		m.CopyReports = []CopyReport{{Model: l.Key, Attempt: "wrong", State: "ready"}}
		if _, err := q.Checkin(Checkin{Machine: m, BenchmarkDone: "wrong"}); err != nil {
			t.Fatal(err)
		}
		st, _ = q.HostMaintenanceStatus("host")
		if st.Quiescent {
			t.Fatal("wrong attempt discharged preparation")
		}
		m.EnvironmentReports[0].Attempt = l.Attempt
		m.CopyReports[0].Attempt = l.Attempt
		if _, err := q.Checkin(Checkin{Machine: m, BenchmarkDone: l.Attempt}); err != nil {
			t.Fatal(err)
		}
		st, _ = q.HostMaintenanceStatus("host")
		if !st.Quiescent {
			t.Fatal("exact completion did not discharge", st)
		}
		maintenanceChange(t, q, "resume", false)
	}
}

func TestMaintenancePreparationSurvivesQueueRestart(t *testing.T) {
	q, clock := fixture(t)
	m := machine("builder", "fake", "host")
	check(t, q, m, nil)
	l := q.issuePreparation(&m, "environment", "fixture")
	if err := q.AuthorizeLaunch(l); err != nil {
		t.Fatal(err)
	}
	maintenanceChange(t, q, "drain", false)
	path, root := "", q.root
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	st, err := q.HostMaintenanceStatus("host")
	if err != nil || st.Quiescent || st.Preparations != 1 {
		t.Fatal("restart lost preparation custody", st, err)
	}
	check(t, q, m, nil)
	st, _ = q.HostMaintenanceStatus("host")
	if st.Quiescent {
		t.Fatal("missing report discharged started preparation")
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: l.Key, Attempt: l.Attempt, State: "paused"}}
	check(t, q, m, nil)
	st, _ = q.HostMaintenanceStatus("host")
	if !st.Quiescent {
		t.Fatal("exact completion did not settle persisted custody", st)
	}
}
