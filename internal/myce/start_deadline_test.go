package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProductionStartDeadlineActualStart(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, boundary := range []bool{false, true} {
			t.Run(map[bool]string{false: "before", true: "at"}[boundary], func(t *testing.T) {
				q, c := fixture(t)
				m := machine("b70", "xpu")
				j := submit(t, q, spec("production"))
				assigned := check(t, q, m, nil).Job
				if assigned == nil {
					t.Fatal("not assigned")
				}
				c.Advance(ProductionStartTimeout - time.Second)
				if boundary {
					c.Advance(time.Second)
				}
				err := q.Started(result(assigned, true))
				if boundary {
					if err == nil {
						t.Fatal("late start accepted")
					}
					failed := job(t, q, j.ID)
					if failed.State != "failed" || !strings.Contains(failed.Reason, "command start on b70") {
						t.Fatalf("failed: %+v", failed)
					}
					if reply := check(t, q, m, assigned); !reply.Stop || !reply.Kill {
						t.Fatalf("no stop: %+v", reply)
					}
					next := submit(t, q, spec("experiment"))
					if check(t, q, m, nil).Job != nil {
						t.Fatal("unreleased expired attempt reused")
					}
					finish(t, q, assigned, true)
					requireState(t, q, j.ID, "failed")
					if check(t, q, m, nil).Job == nil {
						t.Fatalf("released host not reused for %s", next.ID)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					c.Advance(2 * time.Second)
					requireState(t, q, j.ID, "running")
					if got := job(t, q, j.ID); strings.Contains(got.Reason, "left to start") {
						t.Fatalf("started job still waiting: %s", got.Reason)
					}
					finish(t, q, assigned, true)
					requireState(t, q, j.ID, "succeeded")
				}
			})
		}

	})

	t.Run("ProductionStartDeadlineQualifiedRouteDiagnostic", func(t *testing.T) {
		q, clock, m, s := observationFixture(t)
		decoy := cpuMachine("a-incompatible", "a-incompatible", "cpu", 8, 32)
		decoy.CapabilityProfile.OS = "darwin"
		check(t, q, decoy, nil)
		if err := setMaintenance(t, q, decoy.ID, "drain"); err != nil {
			t.Fatal(err)
		}
		// The only qualified route lacks RAM; the drained host cannot execute
		// this extension and must not supply its startup failure explanation.
		m.HostAvailableMemoryGB = .05
		check(t, q, m, nil)
		s.Priority = "production"
		// This diagnostic tests caller-owned expiry. Generated startup windows
		// now wait through real capacity shortages under #1011.
		deadline := clock.Now().Add(ProductionStartTimeout)
		s.StartBy = &deadline
		j := submit(t, q, s)
		var logs bytes.Buffer
		previous := log.Writer()
		log.SetOutput(&logs)
		t.Cleanup(func() { log.SetOutput(previous) })
		clock.Advance(ProductionStartTimeout)
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		failed := job(t, q, j.ID)
		if failed.State != "failed" || failed.Attempt != 0 || !strings.Contains(failed.Reason, "waiting for RAM envelope: "+m.ID) || strings.Contains(failed.Reason, decoy.ID) {
			t.Errorf("wrong startup diagnostic: %+v", failed)
		}
		if got := statusJobValue(failed); got.Reason != "production start deadline exceeded" {
			t.Errorf("public reason leaked internal diagnostics: %q", got.Reason)
		}
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		if got := logs.String(); strings.Count(got, "event=production_start_failed") != 1 || !strings.Contains(got, "job="+j.ID) || !strings.Contains(got, "attempt=0") || !strings.Contains(got, "waiting for RAM envelope: "+m.ID) || strings.Contains(got, decoy.ID) {
			t.Errorf("missing or repeated deadline evidence: %s", got)
		}

	})

	t.Run("ProductionStartDeadlineDefault", func(t *testing.T) {
		q, c := fixture(t)
		s := spec("production")
		s.Needs.Platform = Platform{Required: []string{"xpu"}}
		j := submit(t, q, s)
		c.Advance(149 * time.Second)
		waiting := job(t, q, j.ID)
		if waiting.State != "queued" || !strings.Contains(waiting.Reason, "needs xpu") || !strings.Contains(waiting.Reason, "1s left to start") {
			t.Fatalf("waiting: %+v", waiting)
		}
		c.Advance(time.Second)
		failed := job(t, q, j.ID)
		if failed.State != "failed" || !strings.Contains(failed.Reason, "production start deadline exceeded") || !strings.Contains(failed.Reason, "needs xpu") {
			t.Fatalf("expired: %+v", failed)
		}
		if check(t, q, machine("b70", "xpu"), nil).Job != nil {
			t.Fatal("expired production was assigned")
		}

	})

	t.Run("ProductionStartDeadlineBlockers", func(t *testing.T) {
		for _, blocker := range []string{"experiments", "Docker"} {
			t.Run(blocker, func(t *testing.T) {
				q, c := fixture(t)
				m := machine("b70", "xpu")
				if blocker == "Docker" {
					m.Capabilities = append(m.Capabilities, "docker")
					m.CapabilityProfile.OS = "linux"
				}
				m.TotalMemoryGB = 8
				check(t, q, m, nil)
				var experiment *Job
				if blocker == "experiments" || blocker == "Docker" {
					if err := q.ConfigureHosts(map[string]HostPolicy{"b70": {ProductionHold: true}}); err != nil {
						t.Fatal(err)
					}
					s := spec("experiment")
					if blocker == "Docker" {
						s.Run.Docker = true
						s.Run.Command = []string{"docker", "run", "image@sha256:" + strings.Repeat("a", 64)}
					}
					submit(t, q, s)
					experiment = check(t, q, m, nil).Job
				} else if err := setMaintenance(t, q, "b70", "disable"); err != nil {
					t.Fatal(err)
				}
				s := spec("production")
				deadline := c.Now().Add(6 * time.Second)
				s.StartBy = &deadline
				j := submit(t, q, s)
				if experiment != nil {
					check(t, q, m, experiment)
				}
				waiting := job(t, q, j.ID)
				want := "stopping 1 attempts"
				if blocker == "Docker" {
					want = "Docker cleanup"
				}
				if blocker == "disabled" {
					want = "host b70 disabled by operator"
				}
				if !strings.Contains(waiting.Reason, want) || !strings.Contains(waiting.Reason, "6s left to start") {
					t.Fatalf("waiting: %s", waiting.Reason)
				}
				c.Advance(6 * time.Second)
				failed := job(t, q, j.ID)
				if failed.State != "failed" || !strings.Contains(failed.Reason, want) {
					t.Fatalf("failed: %+v", failed)
				}
				if experiment != nil {
					requireState(t, q, experiment.ID, "stopping")
					if len(q.s.HostReleases) == 0 {
						t.Fatal("release custody cleared")
					}
					// Room arriving at the deadline cannot resurrect production.
					finish(t, q, experiment, false)
					if reply := check(t, q, m, nil); reply.Job != nil && reply.Job.ID == j.ID {
						t.Fatal("zombie start")
					}
				}
			})
		}

	})

	t.Run("ProductionStartDeadlineRestart", func(t *testing.T) {
		q, c := fixture(t)
		j := submit(t, q, spec("production"))
		deadline := *j.Spec.StartBy
		c.Advance(149 * time.Second)
		path := strings.TrimSuffix(q.lock.Name(), ".lock")
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(path, q.root, c)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		if got := job(t, reopened, j.ID); got.State != "queued" || !got.Spec.StartBy.Equal(deadline) {
			t.Fatalf("reopened: %+v", got)
		}
		c.Advance(time.Second)
		requireState(t, reopened, j.ID, "failed")

	})

	t.Run("ProductionStartDeadlineRetryAndGiveUp", func(t *testing.T) {
		q, c := fixture(t)
		j := submit(t, q, spec("production"))
		assigned := check(t, q, machine("b70", "xpu"), nil).Job
		if err := q.Started(result(assigned, true)); err != nil {
			t.Fatal(err)
		}
		c.Advance(ProductionStartTimeout)
		r := result(assigned, false)
		r.Interrupted = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		requeued := job(t, q, j.ID)
		if requeued.State != "queued" || !requeued.Spec.StartBy.Equal(c.Now().Add(ProductionStartTimeout)) {
			t.Fatalf("started attempt did not get a recovery window: %+v", requeued)
		}
		s := spec("production")
		giveUp := c.Now().Add(time.Second)
		s.GiveUpAt = &giveUp
		earlier := submit(t, q, s)
		c.Advance(2 * time.Second)
		if got := job(t, q, earlier.ID); got.State != "failed" || got.Reason != "give-up time reached" {
			t.Fatalf("give-up: %+v", got)
		}

	})

	t.Run("ProductionStartDeadlinePastDoesNotPreempt", func(t *testing.T) {
		q, c := fixture(t)
		submit(t, q, spec("experiment"))
		m := machine("b70", "xpu")
		experiment := check(t, q, m, nil).Job
		s := spec("production")
		now := c.Now()
		s.StartBy = &now
		j := submit(t, q, s)
		requireState(t, q, j.ID, "failed")
		if reply := check(t, q, m, experiment); reply.Stop {
			t.Fatal("past-deadline job stopped experiment")
		}

	})

	t.Run("ProductionStartDeadlineUnconfirmedHost", func(t *testing.T) {
		q, c := fixture(t)
		m := machine("b70", "xpu")
		m.DockerCleanup = "cleanup unconfirmed"
		check(t, q, m, nil)
		j := submit(t, q, spec("production"))
		if reply := check(t, q, m, nil); reply.Job != nil {
			t.Fatal("unconfirmed Docker host admitted production")
		}
		waiting := job(t, q, j.ID)
		if !strings.Contains(waiting.Reason, "Docker cleanup") {
			t.Fatal(waiting.Reason)
		}
		c.Advance(ProductionStartTimeout)
		requireState(t, q, j.ID, "failed")

	})

	t.Run("ProductionStartDeadlineRoomAtBoundary", func(t *testing.T) {
		q, c := fixture(t)
		m := machine("b70", "xpu")
		submit(t, q, spec("experiment"))
		experiment := check(t, q, m, nil).Job
		s := spec("production")
		deadline := c.Now().Add(6 * time.Second)
		s.StartBy = &deadline
		j := submit(t, q, s)
		check(t, q, m, experiment)
		c.Advance(6 * time.Second)
		finish(t, q, experiment, false) // First queue mutation at the boundary makes room.
		if reply := check(t, q, m, nil); reply.Job != nil && reply.Job.ID == j.ID {
			t.Fatal("deadline-boundary assignment")
		}
		requireState(t, q, j.ID, "failed")

	})

	t.Run("ProductionStartDeadlineReleaseOnlyClearsPendingStatus", func(t *testing.T) {
		q, c := fixture(t)
		submit(t, q, spec("production"))
		assigned := check(t, q, machine("b70", "xpu"), nil).Job
		c.Advance(ProductionStartTimeout)
		requireState(t, q, assigned.ID, "failed")
		r := result(assigned, false)
		r.ReleaseOnly = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, assigned.ID)
		if !got.ReleaseConfirmed || strings.Contains(got.Reason, "release pending") {
			t.Fatalf("settled cleanup still pending: %+v", got)
		}

	})
}

func TestProductionStartDeadlinePermit(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, timely := range []bool{true, false} {
			t.Run(fmt.Sprintf("timely=%t", timely), func(t *testing.T) {
				q, c := fixture(t)
				s := spec("production")
				s.WallTime = "10m"
				j := submit(t, q, s)
				assigned := check(t, q, machine("b70", "xpu"), nil).Job
				if assigned == nil {
					t.Fatal("not assigned")
				}
				custody := NativeCustodyReport{Execution: Execution{ID: assigned.ID, Attempt: assigned.Attempt}, Machine: assigned.Machine, Session: assigned.Session, ProcessID: "original-process", CustodyID: "original-group", Phase: "reserved"}
				if err := q.NativeCustody(custody); err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
				defer server.Close()
				var permit map[string]any
				client := Client{URL: server.URL, Token: "test"}
				if timely {
					if err := client.Call(context.Background(), "POST", "/api/workers/started", result(assigned, true), &permit); err != nil {
						t.Fatalf("in-time permit: %v", err)
					}
					if permit["start_gate"] != startGateProtocol {
						t.Fatalf("permit: %+v", permit)
					}
				}
				before := job(t, q, j.ID)
				c.Advance(ProductionStartTimeout)
				err := client.Call(context.Background(), "POST", "/api/workers/started", result(assigned, true), &permit)
				if timely {
					// The reply might have been lost: acknowledge the original durable
					// start, without minting a new start, budget or process identity.
					got := job(t, q, j.ID)
					starts := 0
					for _, e := range got.LedgerEntries {
						if e.Kind == "started" {
							starts++
						}
					}
					if err != nil || got.State != "running" || got.StartedAt == nil || !got.StartedAt.Equal(*before.StartedAt) || starts != 1 || len(q.s.ProductionStarts) != 1 || got.Starts != 1 || permit["wall_time_remaining_seconds"] != (10*time.Minute-ProductionStartTimeout).Seconds() {
						t.Fatal("replay changed original authorization", got, permit, err)
					}
					custody.ProcessID, custody.CustodyID = "second-process", "second-group"
					if err := q.NativeCustody(custody); !errors.Is(err, ErrStale) {
						t.Fatal("replay authorized a duplicate process", err)
					}
				} else {
					var refusal *HTTPError
					if !errors.As(err, &refusal) || refusal.Code != "request_stale" {
						t.Fatal("late first start did not retain its typed refusal", err)
					}
					requireState(t, q, j.ID, "failed")
				}
			})
		}

	})

	t.Run("ProductionStartDeadlineOwnDockerPreparationPermit", func(t *testing.T) {
		q, _ := fixture(t)
		m := dockerMachine("b70", "xpu")
		s := dockerJob().Spec
		s.Priority = "production"
		submit(t, q, s)
		assigned := check(t, q, m, nil).Job
		if assigned == nil {
			t.Fatal("not assigned")
		}
		m.DockerCleanup = "creating"
		check(t, q, m, assigned)
		if err := q.Started(result(assigned, true)); err != nil {
			t.Fatalf("own preparation rejected: %v", err)
		}
		sibling := machine("sibling", "xpu")
		sibling.HostID = "b70"
		sibling.DockerCleanup = "cleanup unconfirmed"
		check(t, q, sibling, nil)
		if err := q.Started(result(assigned, true)); err == nil {
			t.Fatal("unknown sibling cleanup ignored")
		}

	})

	t.Run("ProductionStartGateSingleDurableWinner", func(t *testing.T) {
		for _, boundary := range []bool{false, true} {
			t.Run(map[bool]string{false: "before", true: "exactly"}[boundary], func(t *testing.T) {
				q, c := fixture(t)
				submit(t, q, spec("production"))
				a := check(t, q, machine("b70", "xpu"), nil).Job
				c.Advance(ProductionStartTimeout - time.Second)
				if boundary {
					c.Advance(time.Second)
				}
				var wg sync.WaitGroup
				accepted := make(chan bool, 2)
				for i := 0; i < 2; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); accepted <- q.Started(result(a, true)) == nil }()
				}
				wg.Wait()
				close(accepted)
				wins := 0
				for yes := range accepted {
					if yes {
						wins++
					}
				}
				want := 2
				if boundary {
					want = 0
				}
				if wins != want {
					t.Fatalf("acknowledged %d replays, want %d", wins, want)
				}
				path := strings.TrimSuffix(q.lock.Name(), ".lock")
				if e := q.Close(); e != nil {
					t.Fatal(e)
				}
				reopened, e := Open(path, q.root, c)
				if e != nil {
					t.Fatal(e)
				}
				defer reopened.Close()
				got := job(t, reopened, a.ID)
				if boundary {
					if got.State != "failed" || got.StartedAt != nil {
						t.Fatalf("boundary: %+v", got)
					}
				} else {
					if got.StartedAt == nil || got.State != "running" {
						t.Fatalf("lost durable start: %+v", got)
					}
					// Reconnect restores the exact custody before replaying its lost reply.
					check(t, reopened, machine("b70", "xpu"), a)
					if e := reopened.Started(result(a, true)); e != nil {
						t.Fatal("restarted queue refused exact replay", e)
					}
					replayed := job(t, reopened, a.ID)
					starts := 0
					for _, e := range replayed.LedgerEntries {
						if e.Kind == "started" {
							starts++
						}
					}
					if starts != 1 || !replayed.StartedAt.Equal(*got.StartedAt) || len(reopened.s.ProductionStarts) != 1 {
						t.Fatal("replay duplicated durable start", replayed)
					}
				}
			})
		}

	})

	t.Run("ProductionStartGateQueueHostDisable", func(t *testing.T) {
		q, _ := fixture(t)
		submit(t, q, spec("production"))
		a := check(t, q, machine("b70", "xpu"), nil).Job
		if err := setMaintenance(t, q, "b70", "disable"); err != nil {
			t.Fatal(err)
		}
		if err := q.Started(result(a, true)); err == nil {
			t.Fatal("disabled host accepted start")
		}
		if job(t, q, a.ID).StartedAt != nil {
			t.Fatal("disabled attempt recorded started")
		}

	})
}

func TestProductionStartDeadlineDisabledFleetFailsPromptly(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, alternate := range []string{"", "cuda", "xpu"} {
			t.Run("alternate-"+alternate, func(t *testing.T) {
				q, _ := fixture(t)
				check(t, q, machine("b70", "xpu"), nil)
				if alternate != "" {
					check(t, q, machine("other", alternate), nil)
				}
				if err := setMaintenance(t, q, "b70", "disable"); err != nil {
					t.Fatal(err)
				}
				s := spec("production")
				s.Needs.Platform = Platform{Required: []string{"xpu"}}
				j := submit(t, q, s)
				got := job(t, q, j.ID)
				if alternate == "xpu" {
					if got.State != "queued" {
						t.Fatalf("enabled alternate rejected: %+v", got)
					}
				} else if got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") || got.Attempt != 0 {
					t.Fatalf("disabled-only fleet did not fail immediately: %+v", got)
				}
			})
		}

	})

	t.Run("StartWindowFanOutDisabledFleetFailsPromptly", func(t *testing.T) {
		q, clock, m, s := fanOutFixture(t, 1)
		if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
			t.Fatal(err)
		}
		s.Priority = "production"
		parent := submit(t, q, s)
		got := job(t, q, parent.ID)
		if got.State != "failed" || !strings.Contains(got.Reason, "no eligible host is enabled") || got.Attempt != 0 || !got.FinishedAt.Equal(clock.Now()) {
			t.Fatalf("disabled-only fleet did not reject unassigned fan-out immediately: %+v", got)
		}

	})
}

// #991: a fatal assertion must not wait forever in Server.Close while the
// worker retries a blocked completion. Use the real fixture in a subprocess so
// its intentional assertion failure can be checked without failing this test.
