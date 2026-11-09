package myce

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func idleSignals() OwnerSignals {
	return OwnerSignals{IdleKnown: true, IdleSeconds: 300, Power: "ac", Laptop: true, Heat: "nominal"}
}

func profileAvailability(m Machine, a Availability) Machine {
	profile := *m.CapabilityProfile
	profile.Availability = &a
	m.CapabilityProfile = &profile
	return m
}
func TestPoliteQueueStopAndResume(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, signal := range []string{"owner"} {
			t.Run(signal, func(t *testing.T) {
				q, clock := fixture(t)
				p := availabilityPolicy{clock: clock, daily: true}
				m := profileAvailability(machine("daily", "fake"), p.evaluate(idleSignals()))
				j := submit(t, q, spec("experiment"))
				r := check(t, q, m, nil)
				if r.Job == nil {
					t.Fatal("not assigned")
				}
				s := idleSignals()
				switch signal {
				case "owner":
					s.IdleSeconds = 0
				}
				m = profileAvailability(m, p.evaluate(s))
				r2 := check(t, q, m, r.Job)
				if !r2.Stop || r2.Leave || r2.GraceSeconds != 10 || !r2.AdmissionBlocked {
					t.Fatal(r2)
				}
				got := job(t, q, j.ID)
				if got.State != "stopping" || got.Losses != 0 {
					t.Fatal(got)
				}
				finish(t, q, r.Job, true)
				if check(t, q, m, nil).Job != nil {
					t.Fatal("withholding worker got job")
				}
				other := machine("elsewhere", "fake")
				r3 := check(t, q, other, nil)
				if r3.Job == nil || r3.Job.ID != j.ID || r3.Job.Attempt != 2 {
					t.Fatal("didn't resume elsewhere", r3)
				}
				if job(t, q, j.ID).Losses != 0 {
					t.Fatal("planned stop consumed retry")
				}
			})
		}

	})

	t.Run("DedicatedAvailabilityAndStaleness", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)}
		monitor := newAvailabilityMonitor(clock, true, false, false)
		monitor.sample(idleSignals())
		clock.Advance(signalMaxAge + time.Second)
		if a := monitor.snapshot(); a.Available || a.Reason != "owner signals stale" {
			t.Fatal(a)
		}

	})

	t.Run("PoliteOfflineResultDoesNotChargeLoss", func(t *testing.T) {
		q, _ := fixture(t)
		m := machine("daily", "fake")
		j := submit(t, q, spec("experiment"))
		r := check(t, q, m, nil)
		res := result(r.Job, true)
		res.Interrupted = true
		res.PoliteStop = true
		if err := q.Complete(res); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, j.ID)
		if got.State != "queued" || got.Losses != 0 {
			t.Fatal(got)
		}
		if err := q.Complete(res); !errors.Is(err, ErrStale) {
			t.Fatal("late result accepted", err)
		}

	})
}

func TestPerMachineJobLimit(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock := fixture(t)
		a, b := machine("card1", "fake"), machine("card2", "fake")
		a.HostID = "same"
		b.HostID = "same"
		a.MaxJobs = 1
		b.MaxJobs = 1
		check(t, q, a, nil)
		check(t, q, b, nil)
		first, second := submit(t, q, spec("production")), submit(t, q, spec("experiment"))
		r := check(t, q, a, nil)
		if r.Job == nil || r.Job.ID != first.ID {
			t.Fatal(r)
		}
		if err := q.Started(result(r.Job, true)); err != nil {
			t.Fatal("own admission counted against its start gate", err)
		}
		clock.Advance(time.Second)
		if got := check(t, q, b, nil); got.Job != nil {
			t.Fatal("limit bypassed across GPUs", got)
		}
		finish(t, q, r.Job, true)
		if got := check(t, q, b, nil); got.Job == nil || got.Job.ID != second.ID {
			t.Fatal("released job didn't free limit", got)
		}
		// Absence of the optional limit preserves simultaneous per-resource filling.
		q2, _ := fixture(t)
		a.MaxJobs = 0
		b.MaxJobs = 0
		submit(t, q2, spec("experiment"))
		submit(t, q2, spec("experiment"))
		if check(t, q2, a, nil).Job == nil || check(t, q2, b, nil).Job == nil {
			t.Fatal("default filling changed")
		}

	})

	t.Run("MachineLimitRetainsUnacknowledgedStop", func(t *testing.T) {
		q, clock := fixture(t)
		a, b := machine("limited-a", "fake"), machine("limited-b", "fake")
		a.HostID = "limited-host"
		b.HostID = "limited-host"
		a.MaxJobs = 1
		b.MaxJobs = 1
		check(t, q, a, nil)
		check(t, q, b, nil)
		j := submit(t, q, spec("experiment"))
		assigned := check(t, q, a, nil).Job
		if assigned == nil {
			t.Fatal("no initial assignment")
		}
		a = profileAvailability(a, Availability{Mode: "daily", Reason: "owner active"})
		check(t, q, a, assigned)
		clock.Advance(11 * time.Second)
		q.Tick()
		if q.machineLimitReason(&b) == "" {
			t.Fatal("expired grace credited unacknowledged physical release")
		}
		if r := check(t, q, b, nil); r.Job != nil {
			t.Fatal("limit admitted over an unacknowledged attempt")
		}
		ack := result(assigned, true)
		ack.ReleaseOnly = true
		if err := q.Complete(ack); err != nil {
			t.Fatal(err)
		}
		if r := check(t, q, b, nil); r.Job == nil || r.Job.ID != j.ID || r.Job.Attempt != 2 {
			t.Fatal("confirmed release didn't free machine limit", r)
		}

	})
}

func TestPoliteWorkerCheckpointResumesElsewhere(t *testing.T) {
	q, _ := fixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	api := API{Queue: q, Token: "test"}.Handler()
	server := httptest.NewServer(api)
	defer server.Close()
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `
 test -z "$MYCE_TOKEN" || exit 99
 if [ "$MYCE_ATTEMPT" -gt 1 ]; then
  test "$(cat "$MYCE_CHECKPOINT_DIR/saved")" = saved || exit 98
  echo "resumed=$MYCE_MACHINE"; exit 0
 fi
 mkfifo -m 600 "$TMPDIR/gate" || exit 1
 exec 3<> "$TMPDIR/gate"
 trap 'echo saved > "$MYCE_CHECKPOINT_DIR/saved"; exit 0' TERM
 echo ready
 while :; do read line <&3; done`}
	j := submit(t, q, s)
	var active atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := Worker{EnvironmentLimit: 1 << 20, DailyUse: true, OwnerSignals: func(context.Context) OwnerSignals {
		s := idleSignals()
		if active.Load() {
			s.IdleSeconds = 0
		}
		return s
	}, Clock: clock, Interval: time.Millisecond, Machine: machine("daily", "fake", "fixture-daily-host"), StateDir: newTestStateRoot(t), Client: Client{URL: server.URL, Token: "test"}}
	w.checkpointPrepare = prepareCheckpoint
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				return true
			default:
				return false
			}
		})
	}()
	wait := func(f func() bool) { t.Helper(); await(t, func() bool { clock.Advance(time.Millisecond); return f() }) }
	wait(func() bool {
		logs, _ := q.Logs(j.ID, 0)
		for _, l := range logs {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	active.Store(true)
	clock.Advance(signalRefresh)
	wait(func() bool { return job(t, q, j.ID).State == "queued" })
	data, err := os.ReadFile(filepath.Join(j.Checkpoint, "attempt-1", "saved"))
	if err != nil || strings.TrimSpace(string(data)) != "saved" {
		t.Fatal("checkpoint not saved", err)
	}
	got := job(t, q, j.ID)
	if got.Losses != 0 {
		t.Fatal("planned local stop charged loss")
	}
	workerFixture(t, q, prepareCheckpoint)
	wait(func() bool { return job(t, q, j.ID).State == "succeeded" })
	logs, _ := q.Logs(j.ID, 0)
	found := false
	for _, l := range logs {
		if strings.Contains(l.Text, "resumed=worker") {
			found = true
		}
	}
	if !found || job(t, q, j.ID).Attempt != 2 {
		t.Fatal("different hypha didn't consume checkpoint")
	}
	snap, _ := q.Snapshot()
	for _, m := range snap.Machines {
		if m.ID == "daily" && (machineAvailability(m) == nil || machineAvailability(m).Available) {
			t.Fatal("status lost withholding availability")
		}
	}
}

func TestMachineLimitIncludesModelCalls(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, clock, m := modelFixture(t, "shared")
		m.MaxJobs = 1
		check(t, q, m, nil)
		first := addModelCall(t, q, "experiment")
		reportModel(t, q, m, "ready")
		if first.Machine != m.ID {
			t.Fatal("first model call not routed")
		}
		second := addModelCall(t, q, "production")
		if second.Machine != "" {
			t.Fatal("model call exceeded machine limit")
		}
		other := machine("second-card", "fake")
		other.HostID = "shared"
		other.MaxJobs = 1
		check(t, q, other, nil)
		j := submit(t, q, spec("experiment"))
		if check(t, q, other, nil).Job != nil {
			t.Fatal("batch exceeded model call limit")
		}
		clock.Advance(time.Second)
		if err := q.finishCall(first.ID); err != nil {
			t.Fatal(err)
		}
		if second.Machine != m.ID {
			t.Fatal("finishing call didn't free limit")
		}
		// A withholding hypha may not serve its ready pinned model.
		m = profileAvailability(m, Availability{Mode: "daily", Reason: "on battery"})
		reportModel(t, q, m, "ready")
		if second.Error == "" {
			t.Fatal("running model call wasn't withdrawn")
		}
		third := addModelCall(t, q, "production")
		if third.Machine != "" {
			t.Fatal("withholding hypha routed a model call")
		}
		if job(t, q, j.ID).State != "queued" {
			t.Fatal("blocked job changed")
		}

	})

	t.Run("AvailabilityCancelsModelWorkAndRecovers", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)}
		monitor := newAvailabilityMonitor(clock, true, false, false)
		monitor.sample(idleSignals())
		s := idleSignals()
		s.Power = "battery"
		monitor.sample(s)
		rt := &modelRuntime{availability: monitor, clock: clock, models: map[string]*workerModel{"test": {model: Model{Name: "test"}, engine: &FakeEngine{}, state: "ready", queueFlight: 1}}}
		rt.withdrawLocalAvailability()
		rt.wg.Wait()
		if rt.models["test"].state != "unloaded" {
			t.Fatal("offline model engine not released")
		}
		monitor.sample(idleSignals())
		if monitor.workContext().Err() != nil {
			t.Fatal("fresh eligibility didn't reopen model admission")
		}
	})
}

func TestPoliteOwnerStopDuringBlockedCheckin(t *testing.T) {
	q, _ := fixture(t)
	clock := &workerFakeClock{now: q.clock.Now()}
	q.clock = clock
	entered, release := make(chan struct{}), make(chan struct{})
	var block atomic.Bool
	var blocked atomic.Bool
	api := API{Queue: q, Token: "test"}.Handler()
	server := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() && r.URL.Path == "/api/workers/checkin" && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		api.ServeHTTP(w, r)
	}))
	defer server.Close()
	s := spec("experiment")
	s.Run.Command = []string{"sh", "-c", `mkfifo -m 600 "$TMPDIR/gate" || exit 1; exec 3<> "$TMPDIR/gate"; trap 'echo saved > "$MYCE_CHECKPOINT_DIR/saved"; exit 0' TERM; echo ready; while :; do read line <&3; done`}
	j := submit(t, q, s)
	var active atomic.Bool
	state := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	worker := Worker{EnvironmentLimit: 1 << 20, Clock: clock, DailyUse: true, OwnerSignals: func(context.Context) OwnerSignals {
		s := idleSignals()
		if active.Load() {
			s.IdleSeconds = 0
		}
		return s
	}, Interval: time.Millisecond, StateDir: state, Machine: machine("daily", "fake"), Client: Client{URL: server.URL, Token: "test"}}
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		close(release)
		cancel()
		await(t, func() bool {
			clock.Advance(time.Millisecond)
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				return true
			default:
				return false
			}
		})
	}()
	wait := func(f func() bool) { t.Helper(); await(t, func() bool { clock.Advance(time.Millisecond); return f() }) }
	wait(func() bool {
		logs, _ := q.Logs(j.ID, 0)
		for _, l := range logs {
			if strings.Contains(l.Text, "ready") {
				return true
			}
		}
		return false
	})
	block.Store(true)
	wait(func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	active.Store(true)
	clock.Advance(signalRefresh)
	wait(func() bool { _, err := os.Stat(filepath.Join(j.Checkpoint, "attempt-1", "saved")); return err == nil })
	// Result persisted independently while the queue response is still held.
	wait(func() bool {
		matches, _ := filepath.Glob(filepath.Join(state, "*.result.json"))
		return len(matches) == 1
	})
}

func TestDailyDesktopCPUThermalWaiver(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		desktop, laptop, detectedLaptop bool
		power, gpu                      string
		idle                            float64
		known, available                bool
		reason                          string
	}{
		{"desktop", true, false, false, "ac", "nominal", 300, true, true, "CPU thermal coverage waived for desktop"},
		{"detected laptop", true, false, true, "ac", "nominal", 300, true, false, "host CPU thermal coverage unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)}
			p := availabilityPolicy{clock: clock, daily: true, desktop: tc.desktop, laptop: tc.laptop}
			s := OwnerSignals{IdleKnown: tc.known, IdleSeconds: tc.idle, Laptop: tc.detectedLaptop, Power: tc.power, Heat: "unknown", GPUHeat: tc.gpu, HeatCoverage: "gpu-only"}
			a := p.evaluate(s)
			if a.Available != tc.available || a.Reason != tc.reason {
				t.Fatal(a)
			}
			if a.Signals.Heat != "unknown" {
				t.Fatal("waiver fabricated CPU heat", a)
			}
			if a.Available {
				m := profileAvailability(machine("desktop", "fake"), a)
				if !strings.Contains(HyphaAvailability(&m), tc.reason) {
					t.Fatal(HyphaAvailability(&m))
				}
				clock.Advance(signalRefresh)
				s.IdleSeconds = 0
				if p.evaluate(s).Available {
					t.Fatal("owner return did not withdraw availability")
				}
			}
		})
	}
}
