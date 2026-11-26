package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Exact launch and Started gates for an assigned attempt on a host with
// waiting work. Hold is time inside q.mu; latency is the whole call.
// Maintain and health are the body's maintenance and transaction-tail health
// phases inside that hold.
type launchGateSample struct{ hold, latency, maintain, health time.Duration }

func measureLaunchGates(t testing.TB, q *Queue, j *Job) (launch, started launchGateSample) {
	t.Helper()
	gate := func(call func(context.Context) error) launchGateSample {
		var sample launchGateSample
		q.observeBodyPhase = func(name string, elapsed time.Duration) {
			switch name {
			case "maintain":
				sample.maintain += elapsed
			case "health":
				sample.health += elapsed
			}
		}
		defer func() { q.observeBodyPhase = nil }()
		span := &requestSpan{}
		begin := time.Now()
		if err := call(context.WithValue(context.Background(), requestSpanKey{}, span)); err != nil {
			t.Fatal(err)
		}
		sample.hold, sample.latency = span.hold, time.Since(begin)
		return sample
	}
	l := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)}
	launch = gate(func(ctx context.Context) error { return q.authorizeLaunchContext(ctx, l) })
	r := Result{Machine: j.Machine, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}
	started = gate(func(ctx context.Context) error { return q.Started(r, ctx) })
	return launch, started
}

// go test ./internal/myce -run '^$' -bench BenchmarkLaunchWaitingScale -benchtime 20x
//
// The #2179 shape (12,000 retained jobs, 13,600 storage observations) with a
// waiting set pinned behind an alone pool holder or ordinary capacity. Each
// iteration repeats the exact launch and Started gates of an assigned attempt.
func BenchmarkLaunchWaitingScale(b *testing.B) {
	for _, alone := range []bool{false, true} {
		for _, waiting := range []int{30, 150, 500, 2000} {
			b.Run(fmt.Sprintf("alone=%t/waiting=%d", alone, waiting), func(b *testing.B) {
				q, workers := checkinWaitingScaleFixture(b, waiting, alone, false)
				runCheckinWaitingRounds(b, q, workers, 1) // Settle first offers, as in production.
				j := q.s.Jobs["running-2"]
				measureLaunchGates(b, q, j)
				var launches, starts []launchGateSample
				b.ResetTimer()
				for range b.N {
					l, s := measureLaunchGates(b, q, j)
					launches, starts = append(launches, l), append(starts, s)
				}
				b.StopTimer()
				median := func(samples []launchGateSample, field func(launchGateSample) time.Duration) float64 {
					values := make([]time.Duration, len(samples))
					for i, s := range samples {
						values[i] = field(s)
					}
					slices.Sort(values)
					return holdMS(values[len(values)/2])
				}
				for _, metric := range []struct {
					name  string
					field func(launchGateSample) time.Duration
				}{
					{"hold-ms", func(s launchGateSample) time.Duration { return s.hold }},
					{"ms", func(s launchGateSample) time.Duration { return s.latency }},
					{"maintain-ms", func(s launchGateSample) time.Duration { return s.maintain }},
					{"health-ms", func(s launchGateSample) time.Duration { return s.health }},
				} {
					b.ReportMetric(median(launches, metric.field), "launch-"+metric.name)
					b.ReportMetric(median(starts, metric.field), "started-"+metric.name)
				}
			})
		}
	}
}

// A representative launch workload on the planning fixture: five workers take
// assignments from check-ins, then each attempt passes the exact launch gate
// and Started some time later. Production work arrives mid-run and preempts
// experiments; a cancellation, stale and foreign-session requests, unknown
// preparation and engine custody, and a quiet worker past its loss timeout
// exercise each refusal fence. Ticks run on cadence between rounds.
func runLaunchWorkload(t *testing.T, q *Queue, workers []Machine) []string {
	t.Helper()
	clock := q.clock.(*fakeClock)
	type held struct {
		execution Execution
		session   string
		started   bool
		since     int
	}
	holding := make([]map[string]*held, len(workers))
	for i := range holding {
		holding[i] = map[string]*held{}
	}
	var transcript []string
	var quiet *HostLaunch
	contact := make([]time.Time, len(workers))
	note := func(format string, args ...any) { transcript = append(transcript, fmt.Sprintf(format, args...)) }
	outcome := func(err error) string {
		var conflict *batchLaunchConflict
		switch {
		case err == nil:
			return "ok"
		case errors.As(err, &conflict):
			return "conflict:" + conflict.boundary
		case errors.Is(err, ErrStale):
			return "stale"
		}
		return "error:" + err.Error()
	}
	launch := func(l HostLaunch) error { return q.authorizeLaunchContext(context.Background(), l) }
	checkin := func(round, i int) []HostLaunch {
		m := workers[i]
		c := Checkin{Machine: m, period: time.Second, AttemptProtocol: keyedAttemptProtocol}
		ids := make([]string, 0, len(holding[i]))
		for id := range holding[i] {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		for _, id := range ids {
			h := holding[i][id]
			c.Attempts = append(c.Attempts, AttemptReport{Execution: h.execution, Session: h.session, Started: h.started})
		}
		r, err := q.CheckinContext(context.Background(), c)
		if err != nil {
			t.Fatalf("round %d worker %d check-in: %v", round, i, err)
		}
		contact[i] = clock.Now()
		var pending []HostLaunch
		for _, a := range r.Attempts {
			note("r%d w%d directive %s/%d job=%t stop=%t hold=%t", round, i, a.ID, a.Attempt, a.Job != nil, a.Stop, a.Hold)
			h := holding[i][a.ID]
			switch {
			case a.Stop || a.Kill:
				// The worker stops the attempt, or never forked a refused one.
				note("r%d w%d release %s %s", round, i, a.ID, outcome(q.Complete(Result{Machine: m.ID, Session: a.Session, Execution: a.Execution, Interrupted: true})))
				delete(holding[i], a.ID)
			case a.Job != nil && h == nil:
				holding[i][a.ID] = &held{execution: a.Execution, session: a.Session, since: round}
				pending = append(pending, HostLaunch{Machine: m.ID, Session: a.Session, Kind: "batch", Key: a.ID, Attempt: strconv.Itoa(a.Attempt)})
			}
		}
		return pending
	}
	start := func(round, i int, pending []HostLaunch) {
		for _, l := range pending {
			clock.Advance(150 * time.Millisecond)
			err := launch(l)
			note("r%d w%d launch %s/%s %s", round, i, l.Key, l.Attempt, outcome(err))
			if err != nil {
				delete(holding[i], l.Key)
				continue
			}
			clock.Advance(50 * time.Millisecond)
			h := holding[i][l.Key]
			err = q.Started(Result{Machine: l.Machine, Session: l.Session, Execution: h.execution})
			note("r%d w%d started %s/%s %s", round, i, l.Key, l.Attempt, outcome(err))
			h.started = err == nil
			if err != nil {
				delete(holding[i], l.Key)
			}
		}
	}
	finish := func(round, i int) {
		ids := []string{}
		for id, h := range holding[i] {
			if h.started && round-h.since >= 4 {
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)
		for _, id := range ids {
			h := holding[i][id]
			note("r%d w%d complete %s %s", round, i, id, outcome(q.Complete(Result{Machine: workers[i].ID, Session: h.session, Execution: h.execution, Success: true})))
			delete(holding[i], id)
		}
	}
	tick := func(round int) {
		if err := q.Tick(); err != nil {
			t.Fatalf("round %d tick: %v", round, err)
		}
	}
	arrive := func(first, last int) {
		// Production work arrives for every host, including the card. Each
		// needs an experiment's room.
		if err := q.update(func() error {
			for k := first; k < last; k++ {
				s := cpuSpec(8, 0)
				s.Project, s.Name, s.Priority = "urgent", fmt.Sprintf("urgent-%d", k%2), "production"
				s.Run.Command = []string{"echo", s.Name}
				s.Needs.Memory = "auto"
				s.Needs.Platform = Platform{Required: []string{"cpu"}}
				if k%3 == 0 {
					s.Needs.CPUCores = 2
					s.Needs.Platform = Platform{Required: []string{"cuda"}}
					s.Needs.MemoryGB = 46
				}
				if err := s.Validate(); err != nil {
					return err
				}
				id := fmt.Sprintf("urgent-%d", k)
				j := &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
				if !s.Needs.cpuOnly() {
					j.MemoryFallback = &SizeHint{MemoryGB: 46, HostMemoryGB: 4}
				}
				submitted(q, j)
				q.putJob(id, j)
			}
			q.maintain()
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	probe := func() {
		// Refusal fences: stale attempt, malformed attempt, foreign session,
		// unknown preparation and engine custody, and a Started replay of a
		// stale attempt.
		var h *held
		for i := range workers {
			ids := []string{}
			for id := range holding[i] {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			for _, id := range ids {
				if h == nil && holding[i][id].started {
					h = holding[i][id]
				}
			}
		}
		if h == nil {
			t.Fatal("no started attempt to probe")
		}
		j := q.s.Jobs[h.execution.ID]
		base := HostLaunch{Machine: j.Machine, Session: h.session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)}
		probes := []struct {
			name string
			l    HostLaunch
		}{
			{"repeat", base},
			{"stale-attempt", HostLaunch{Machine: base.Machine, Session: base.Session, Kind: "batch", Key: base.Key, Attempt: strconv.Itoa(j.Attempt + 1)}},
			{"bad-attempt", HostLaunch{Machine: base.Machine, Session: base.Session, Kind: "batch", Key: base.Key, Attempt: "x"}},
			{"foreign-session", HostLaunch{Machine: base.Machine, Session: "other-session", Kind: "batch", Key: base.Key, Attempt: base.Attempt}},
			{"preparation", HostLaunch{Machine: base.Machine, Session: base.Session, Kind: "environment", Key: "env", Attempt: "unknown"}},
			{"engine", HostLaunch{Machine: base.Machine, Session: base.Session, Kind: "engine", Key: "model", Attempt: "unknown", Device: "0"}},
		}
		for _, p := range probes {
			note("probe %s %s", p.name, outcome(launch(p.l)))
		}
		stale := Result{Machine: base.Machine, Session: base.Session, Execution: Execution{ID: base.Key, Attempt: j.Attempt + 1}}
		note("probe started-stale %s", outcome(q.Started(stale)))
	}
	for round := range 12 {
		if quiet != nil {
			break
		}
		clock.Advance(time.Second)
		tick(round)
		pending := make([][]HostLaunch, len(workers))
		for i := range workers {
			finish(round, i)
			pending[i] = checkin(round, i)
		}
		// Offers wait for their launch while other control traffic lands.
		switch round {
		case 0:
			if len(pending[1]) > 0 {
				note("r%d cancel %s %s", round, pending[1][0].Key, outcome(q.Cancel(pending[1][0].Key)))
			}
			// Two offers lose their room before their launch arrives.
			arrive(0, 2)
		case 2:
			arrive(2, 6)
		case 3:
			probe()
		}
		if quiet == nil && round >= 6 && len(pending[4]) > 0 {
			// This offer's launch arrives only after worker 4 is quiet.
			quiet = &pending[4][0]
			pending[4] = pending[4][1:]
		}
		for i := range workers {
			start(round, i, pending[i])
		}
	}
	// Worker 4 stays quiet past its loss timeout. The other workers keep
	// reporting, so the fleet is not quiet. No tick or check-in runs between
	// crossing the timeout and the late launch, so only the launch's own
	// fences can see the loss.
	q.mu.Lock()
	timeout := q.machineLossTimeout(workers[4].ID)
	q.mu.Unlock()
	note("loss timeout %s", timeout)
	lost := contact[4].Add(timeout)
	for clock.Now().Add(time.Second).Before(lost.Add(-100 * time.Millisecond)) {
		clock.Advance(time.Second)
		for i := range 4 {
			checkin(99, i)
		}
	}
	clock.Advance(lost.Sub(clock.Now()) + 50*time.Millisecond)
	if quiet == nil {
		t.Logf("%s", strings.Join(transcript, "\n"))
		t.Fatal("worker 4 had no offer to launch late")
	}
	note("quiet launch %s/%s %s", quiet.Key, quiet.Attempt, outcome(launch(*quiet)))
	return transcript
}

// Fixture jobs bypass Submit for stable identities; give them the custody
// fields Submit derives.
func submitted(q *Queue, j *Job) {
	j.Checkpoint, j.WaitingSince, j.PriorityPolicy = q.checkpointPath(j.ID), j.Queued, q.priorityMatch(j.Spec)
}

type launchWorkloadJob struct {
	ID, State, Machine, Session, Reason string
	Attempt                             int
	Placement                           *PreemptionPlacement
	Refusal                             string
}

func launchWorkloadDigest(t *testing.T, q *Queue, transcript []string) (string, []byte) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	var jobs []launchWorkloadJob
	for _, j := range q.s.Jobs {
		if strings.HasPrefix(j.ID, "retained-") {
			continue
		}
		jobs = append(jobs, launchWorkloadJob{j.ID, j.State, j.Machine, j.Session, j.Reason, j.Attempt, j.PreemptionPlacement, recordedStartRefusal(j)})
	}
	slices.SortFunc(jobs, func(a, b launchWorkloadJob) int { return strings.Compare(a.ID, b.ID) })
	var picks []string
	scan := q.placementScan()
	for _, m := range scan.machines {
		id := ""
		if j := q.plannedPick(m, scan); j != nil {
			id = j.ID
		}
		picks = append(picks, targetKey(m)+"="+id)
	}
	data, err := json.Marshal(struct {
		Transcript []string
		Jobs       []launchWorkloadJob
		Picks      []string
	}{transcript, jobs, picks})
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), data
}

// Frozen on main before launch gates stopped running a full maintenance pass.
// Every check-in directive, launch and Started outcome, refusal fence, final
// job state and next pick must stay identical.
func TestLaunchGateDecisionEquivalence(t *testing.T) {
	q, workers := candidateCostFixture(t)
	if err := q.update(func() error {
		for i := range workers {
			q.deleteJob(fmt.Sprintf("running-%d", i))
		}
		// Experiments fill each host's CPUs, or share worker 0's card.
		for _, j := range q.liveJobs() {
			j = q.touchJob(j.ID)
			j.Spec.Priority, j.Spec.Needs.CPUCores = "experiment", 12
			if !j.Spec.Needs.cpuOnly() {
				j.Spec.Needs.CPUCores = 2
			}
			submitted(q, j)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	transcript := runLaunchWorkload(t, q, workers)
	got, data := launchWorkloadDigest(t, q, transcript)
	counts := map[string]int{}
	for _, line := range transcript {
		for _, kind := range []string{" launch ", " started ", " release ", " complete ", "probe ", " cancel ", "quiet launch"} {
			if strings.Contains(line, kind) {
				counts[strings.TrimSpace(kind)]++
			}
		}
	}
	t.Logf("events=%d %v digest=%s", len(transcript), counts, got)
	const want = "5bcaaa8d5b16772f4e589476a851f0a6ce52cdc8b51ee9465341f115f2bda00c"
	if got != want {
		t.Logf("workload records: %s", data)
		t.Fatalf("launch outcomes %s, want %s", got, want)
	}
}
