package myce

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// Synthetic #2179 shape: about 12,000 retained jobs with submission bindings,
// five occupied workers and a waiting set of identical fan-out pieces pinned
// to one host. With alone, the pinned host's running attempt holds its memory
// pool alone after a pressure stop, so every waiting piece meets that custody.
// The gpu shape pins accelerator pieces behind a busy card, as in the incident;
// otherwise CPU pieces wait behind the host's CPU allowance.
func checkinWaitingScaleFixture(t testing.TB, waiting int, alone, gpu bool) (*Queue, []Machine) {
	t.Helper()
	q, workers := largeRetainedHistoryFixture(t, 12000)
	for id, j := range q.s.Jobs {
		if j.State == "queued" {
			delete(q.s.Jobs, id)
		}
	}
	for i := range workers {
		m := q.s.Machines[workers[i].ID]
		m.Capabilities = append(m.Capabilities, pooledMemoryCapability, sampledMemoryCapability)
		workers[i].Capabilities = m.Capabilities
	}
	one := 1
	pinned, holder := workers[1], q.s.Jobs["running-1"]
	holder.Spec.Needs.CPUCores, holder.Spec.Needs.HostMemoryGB = 4, 8
	if !alone && !gpu {
		holder.Spec.Needs.CPUCores = 16 // The baseline waits on ordinary CPU capacity.
	}
	if gpu {
		pinned, holder = workers[0], q.s.Jobs["running-0"]
		holder.Spec = Spec{Name: "holder", Project: "holder", Priority: "experiment", Run: RunSpec{Command: []string{"echo", "holder"}},
			Needs: ResourceNeeds{CPUCores: 4, HostMemoryGB: 8, MemoryGB: 20, GPU: &GPUSelection{Count: &one}, Platform: Platform{Required: []string{"cuda"}}}}
		holder.AllocatedDevices = []string{"0"}
	}
	holder.Memory = &MemoryPools{RAM: memoryPoolEstimate{TypicalGB: 8, ChargeGB: 8, Alone: alone}}
	// Running attempts hold exact host custody, as live executions do.
	for i := range workers {
		j := q.s.Jobs[fmt.Sprintf("running-%d", i)]
		j.Copies = []Copy{{Machine: j.Machine, Session: j.Session, Attempt: j.Attempt, StartedAt: j.StartedAt, Memory: j.Memory, AllocatedDevices: j.AllocatedDevices}}
	}
	clock := q.clock.(*fakeClock)
	parentSpec := cpuSpec(4, 0)
	parentSpec.Project, parentSpec.Name, parentSpec.Priority = "fanout", "heldout-candidate", "experiment"
	parentSpec.Run.Command = []string{"echo", "heldout"}
	parentSpec.Needs.Memory, parentSpec.Needs.HostMemoryGB = "auto", 11
	parentSpec.Needs.Platform = Platform{Required: []string{"cpu"}}
	if gpu {
		parentSpec.Needs.GPU, parentSpec.Needs.MemoryGB = &GPUSelection{Count: &one}, 30
		parentSpec.Needs.Platform = Platform{Required: []string{"cuda"}}
	}
	parentSpec.AllowedMachines = []string{pinned.ID}
	if err := parentSpec.Validate(); err != nil {
		t.Fatal(err)
	}
	parentSpec.FanOut = &FanOut{}
	parent := &Job{ID: "heldout", Spec: parentSpec, State: "queued", Queued: clock.Now(), FanOut: &FanOutProgress{Next: waiting}}
	for i := range waiting {
		piece := FanOutPiece{Key: fmt.Sprintf("s%04d", i), Args: []string{fmt.Sprintf("S%04d", i)}}
		parentSpec.FanOut.Pieces = append(parentSpec.FanOut.Pieces, piece)
		child := *parent
		child.ID, child.FanOut, child.Spec.FanOut = fmt.Sprintf("piece-%04d", i), nil, nil
		child.Queued = clock.Now().Add(time.Duration(i) * time.Millisecond)
		child.Batch = &BatchWork{Parent: parent.ID, Pieces: []FanOutPiece{piece}, Concurrency: 1}
		q.s.Jobs[child.ID] = &child
		parent.FanOut.Children = append(parent.FanOut.Children, child.ID)
	}
	q.s.Jobs[parent.ID] = parent
	// Production retained 13,600 storage observations. A fifth share the
	// pieces' program, so their disk need derives storage identities.
	q.s.StorageObservations = map[string]StorageObservation{}
	for i := range 13600 {
		program := fmt.Sprintf("profile:program-%d", i%97)
		if i%5 == 0 {
			program = "profile:" + parentSpec.Name
		}
		key := StorageKey{Program: program, Configuration: fmt.Sprint(i), Shape: "synthetic"}
		used := StorageRange{Samples: 2, Min: 1 << 20, Peak: 2 << 20}
		q.s.StorageObservations[identityHash(key)] = StorageObservation{Key: key, Family: fmt.Sprintf("family-%d", i%97),
			Managed: used, Output: used, RecordedAt: clock.Now().Add(-time.Hour)}
	}
	q.s.Submissions = map[string]*submissionRecord{}
	for id, j := range q.s.Jobs {
		j.Spec.SubmissionKey = id
		q.s.Submissions[submissionDigest(j.Spec.Project, id)] = &submissionRecord{Kind: submissionBinding, Project: j.Spec.Project, Key: id, JobID: id, Job: j}
	}
	if err := q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	return q, workers
}

type checkinWaitingSample struct {
	hold, maintain time.Duration
	attempts       []string
}

// Runs rounds of heartbeats from every worker, advancing the clock by one
// second per round. It returns the per-check-in hold and maintenance time and
// the reply's attempt identities, which are the placement decisions.
func runCheckinWaitingRounds(t testing.TB, q *Queue, workers []Machine, rounds int) []checkinWaitingSample {
	t.Helper()
	clock := q.clock.(*fakeClock)
	var maintain time.Duration
	q.observeBodyPhase = func(name string, elapsed time.Duration) {
		if name == "maintain" {
			maintain += elapsed
		}
	}
	defer func() { q.observeBodyPhase = nil }()
	var out []checkinWaitingSample
	for range rounds {
		clock.Advance(time.Second)
		for i, m := range workers {
			maintain = 0
			span := &requestSpan{}
			r, err := q.CheckinContext(context.WithValue(context.Background(), requestSpanKey{}, span), backlogCheckin(i, m))
			if err != nil {
				t.Fatal(err)
			}
			sample := checkinWaitingSample{hold: span.hold, maintain: maintain}
			for _, a := range r.Attempts {
				sample.attempts = append(sample.attempts, fmt.Sprintf("%s/%d stop=%t", a.ID, a.Attempt, a.Stop))
			}
			out = append(out, sample)
		}
	}
	return out
}

// go test ./internal/myce -run '^$' -bench BenchmarkCheckinWaitingScale -benchtime 1x
func BenchmarkCheckinWaitingScale(b *testing.B) {
	for _, shape := range []struct{ alone, gpu bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		for _, waiting := range []int{30, 150, 500, 2000} {
			b.Run(fmt.Sprintf("gpu=%t/alone=%t/waiting=%d", shape.gpu, shape.alone, waiting), func(b *testing.B) {
				q, workers := checkinWaitingScaleFixture(b, waiting, shape.alone, shape.gpu)
				runCheckinWaitingRounds(b, q, workers, 1) // Warm caches and settle first offers.
				b.ResetTimer()
				samples := runCheckinWaitingRounds(b, q, workers, max(1, b.N))
				b.StopTimer()
				holds := make([]time.Duration, len(samples))
				var maintain time.Duration
				for i, s := range samples {
					holds[i], maintain = s.hold, maintain+s.maintain
				}
				slices.Sort(holds)
				b.ReportMetric(holdMS(holds[len(holds)/2]), "median-hold-ms")
				b.ReportMetric(holdMS(maintain)/float64(len(samples)), "maintain-ms/checkin")
			})
		}
	}
}

// Frozen on the pre-#2179 planner. Every check-in reply, final job outcome
// and planned pick across the waiting-set shapes: CPU and accelerator pieces
// behind an alone pool holder, and behind ordinary capacity.
func TestCheckinWaitingPlacementEquivalence(t *testing.T) {
	for _, shape := range []struct {
		alone, gpu bool
		want       string
	}{
		{false, false, "5a038c4b4bcde8f16c0bf65991bea558479bb7fde42bbaeb8df4c2a4855dd515"},
		{true, false, "fcfb5a6696cba769d24c9e80ea39a335536ee9e1734695e6fee1c158fcf024ea"},
		{true, true, "fd1b2d1c9558066cdb244e457644c7e74eb05b3cbf991b31fd49b2a5f745f273"},
	} {
		t.Run(fmt.Sprintf("gpu=%t/alone=%t", shape.gpu, shape.alone), func(t *testing.T) {
			q, workers := checkinWaitingScaleFixture(t, 40, shape.alone, shape.gpu)
			// Production pieces may plan releases of the experiment holder.
			for i := 0; i < 40; i += 8 {
				q.s.Jobs[fmt.Sprintf("piece-%04d", i)].Spec.Priority = "production"
			}
			var replies [][]string
			for _, s := range runCheckinWaitingRounds(t, q, workers, 3) {
				replies = append(replies, s.attempts)
			}
			q.mu.Lock()
			defer q.mu.Unlock()
			q.maintain()
			type jobOutcome struct {
				ID, State, Machine, Session, Reason string
				Attempt                             int
				Placement                           *PreemptionPlacement
			}
			var jobs []jobOutcome
			for _, j := range q.s.Jobs {
				if !terminal(j.State) {
					jobs = append(jobs, jobOutcome{j.ID, j.State, j.Machine, j.Session, j.Reason, j.Attempt, j.PreemptionPlacement})
				}
			}
			var preemptions []string
			for id, targets := range q.admissionWaitDecisions {
				for target, d := range targets {
					preemptions = append(preemptions, fmt.Sprintf("%s@%s=%s/%t", id, target, d.preemption.Reason, d.preemption.Planned))
				}
			}
			slices.Sort(preemptions)
			slices.SortFunc(jobs, func(a, b jobOutcome) int { return strings.Compare(a.ID, b.ID) })
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
				Replies     [][]string
				Jobs        []jobOutcome
				Picks       []string
				Preemptions []string
			}{replies, jobs, picks, preemptions})
			if err != nil {
				t.Fatal(err)
			}
			got := fmt.Sprintf("%x", sha256.Sum256(data))
			t.Logf("jobs=%d picks=%d preemptions=%d digest=%s", len(jobs), len(picks), len(preemptions), got)
			if got != shape.want {
				t.Fatalf("placement outcomes %s, want %s", got, shape.want)
			}
		})
	}
}
