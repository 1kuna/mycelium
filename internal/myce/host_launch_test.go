package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
				authorize := func(ctx context.Context, l HostLaunch) error {
					calls++
					if delayed == "command" && calls == 2 {
						close(entered)
						<-release
					}
					return worker.authorizeLaunchContext(ctx, l)
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
					case <-time.After(testWaitTimeout):
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
				case <-time.After(testWaitTimeout):
					t.Fatal("build did not settle")
				}
				if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("payload ran after maintenance", err)
				}
				_, reports, _, _ := rt.report()
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
	for _, kind := range []string{"environment", "copy"} {
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
		if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		st, _ = q.HostMaintenanceStatus("host")
		if st.Quiescent {
			t.Fatal("wrong attempt discharged preparation")
		}
		m.EnvironmentReports[0].Attempt = l.Attempt
		m.CopyReports[0].Attempt = l.Attempt
		if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
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

func TestNativeLaunchCustodyFences(t *testing.T) {
	for _, boundary := range []string{"restart-replay", "maintenance", "cancel", "deadline", "stale"} {
		t.Run(boundary, func(t *testing.T) {
			path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
			clock := &fakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
			open := func() *Queue {
				t.Helper()
				q, err := Open(path, root, clock)
				if err != nil {
					t.Fatal(err)
				}
				fakeCheckpointAvailability(t, q, true)
				return q
			}
			q := open()
			defer func() { _ = q.Close() }()
			restart := func() {
				t.Helper()
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				q = open()
			}
			m := machine("worker", "fake", "host")
			priority := "production"
			if boundary == "stale" {
				priority = "experiment"
			}
			s := spec(priority)
			s.Resumable = false
			by := clock.Now().Add(time.Minute)
			if priority == "production" {
				s.StartBy = &by
			}
			s.WallTime = "10m"
			submit(t, q, s)
			a := check(t, q, m, nil).Job
			if a == nil {
				t.Fatal("missing assignment")
			}
			r := result(a, true)
			custody := nativeLaunchCustody{ProcessID: "process", CustodyID: "generation", Phase: "reserved"}
			launch := workerLaunchRequest{HostLaunch: HostLaunch{Machine: m.ID, Session: m.Session, Kind: "batch", Key: a.ID, Attempt: fmt.Sprint(a.Attempt)}, Custody: &custody}
			supervisor := custody
			supervisor.Phase, supervisor.SupervisorPID, supervisor.SupervisorBirth = "started", 1, "birth"
			start := workerStartedRequest{Result: r, Custody: &supervisor}
			if _, err := q.startedWithCustody(t.Context(), start); !errors.Is(err, ErrStale) {
				t.Fatal("Started authorized with no reservation", err)
			}
			if err := q.authorizeLaunchWithCustody(t.Context(), launch); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, a.ID); got.ReleaseConfirmed || got.StartedAt != nil || got.Copies[0].ProcessID != custody.ProcessID {
				t.Fatalf("reservation changed startup or release state: %+v", got)
			}
			wrongSession := start
			wrongSession.Session = "previous-session"
			if _, err := q.startedWithCustody(t.Context(), wrongSession); !errors.Is(err, ErrStale) {
				t.Fatal("Started accepted a different worker session", err)
			}
			switch boundary {
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(m.HostID, HostMaintenanceRequest{Action: "drain"}); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				if err := q.Cancel(a.ID); err != nil {
					t.Fatal(err)
				}
			case "deadline":
				clock.Advance(by.Sub(clock.Now()))
			case "stale":
				// Replace the attempt in the same worker session before a
				// delayed gate or result from the first attempt arrives.
				stopped := classifyAttemptOutcome(result(a, false), attemptCause{memberOutcome: memberOutcome{ObservationLost: true}})
				stopped.OperatorStop = true
				if err := q.Complete(stopped); err != nil {
					t.Fatal(err)
				}
				next := check(t, q, m, nil).Job
				if next == nil || next.Attempt != a.Attempt+1 || next.Session != a.Session {
					t.Fatal("missing replacement attempt", next)
				}
				if err := q.authorizeLaunchWithCustody(t.Context(), launch); !errors.Is(err, ErrStale) {
					t.Fatal("accepted stale launch", err)
				}
				if _, err := q.startedWithCustody(t.Context(), start); !errors.Is(err, ErrStale) {
					t.Fatal("accepted stale Started", err)
				}
				if err := q.Complete(start.Result); !errors.Is(err, ErrStale) {
					t.Fatal("accepted stale result", err)
				}
				if got := job(t, q, a.ID); got.Attempt != next.Attempt || got.StartedAt != nil || got.ReleaseConfirmed {
					t.Fatal("stale report changed replacement execution", got)
				}
				return
			case "restart-replay":
				// Simulate a lost reply after each durable write. Reopening the
				// queue preserves custody but requires the same worker to report.
				restart()
				var waiting *HTTPError
				if err := q.authorizeLaunchWithCustody(t.Context(), launch); !errors.As(err, &waiting) || waiting.Status != http.StatusServiceUnavailable || waiting.Code != "shared_queue_control_retry" {
					t.Fatal("launch did not wait for the unreported session", err)
				}
				check(t, q, m, a)
				if err := q.authorizeLaunchWithCustody(t.Context(), launch); err != nil {
					t.Fatal("reservation replay failed after reconnect", err)
				}
				left, err := q.startedWithCustody(t.Context(), start)
				if err != nil || left == nil || *left != 600 {
					t.Fatal("durable Started failed", left, err)
				}
				startedAt := *job(t, q, a.ID).StartedAt
				restart()
				clock.Advance(time.Second)
				check(t, q, m, a)
				left, err = q.startedWithCustody(t.Context(), start)
				if err != nil || left == nil || *left != 599 {
					t.Fatal("replay reset wall time or lost custody", left, err)
				}
				wrongBirth := supervisor
				wrongBirth.SupervisorBirth = "other-birth"
				if _, err := q.startedWithCustody(t.Context(), workerStartedRequest{Result: r, Custody: &wrongBirth}); !errors.Is(err, ErrStale) {
					t.Fatal("replay accepted different supervisor", err)
				}
				got := job(t, q, a.ID)
				count := map[string]int{}
				for _, e := range got.LedgerEntries {
					count[e.Kind]++
				}
				if count["custody_reserved"] != 1 || count["custody_started"] != 1 || count["started"] != 1 || !got.StartedAt.Equal(startedAt) || got.ReleaseConfirmed {
					t.Fatal("restart replay changed ledger or release", count, got.StartedAt)
				}
				return
			}
			if _, err := q.startedWithCustody(t.Context(), start); !errors.Is(err, ErrStale) {
				t.Fatal("Started crossed live fence", err)
			}
			got := job(t, q, a.ID)
			for _, e := range got.LedgerEntries {
				if e.Kind == "custody_started" || e.Kind == "started" {
					t.Fatal("fenced request partially recorded start", e)
				}
			}
			if got.ReleaseConfirmed || got.Copies[0].ReleaseConfirmed {
				t.Fatal("refusal invented release")
			}
		})
	}
}
