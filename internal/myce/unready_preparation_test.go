package myce

import (
	"strings"
	"testing"
	"time"
)

func TestUnreadyDeadlineBackfillsReadyCPUAndPrepares(t *testing.T) {
	for _, resumable := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite-command", true: "cooperative-yield"}[resumable], func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 4, 8)
			m.EnvironmentBuilder = true
			r := binaryRecipe(t, "cpu")
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
				t.Fatal(err)
			}
			highSpec := cpuSpec(2, 1)
			highSpec.Priority = "deadline"
			due := clock.Now().Add(time.Hour)
			highSpec.Deadline, highSpec.Environment.Name = &due, "tools"
			high := submit(t, q, highSpec)
			lowSpec := cpuSpec(2, 1)
			lowSpec.Resumable = resumable
			low := submit(t, q, lowSpec)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != low.ID {
				t.Fatal("unready deadline left ready CPU work idle", run)
			}
			if resumable {
				// Use the existing deadline stop and exact release, even while
				// the higher tier needs the worker for preparation first.
				stop := job(t, q, low.ID)
				if stop.State != "stopping" || priorityStop(stop) == nil || priorityStop(stop).Demand != high.ID {
					t.Fatal("backfill did not yield through priority preemption", stop.State)
				}
				clock.Advance(time.Second)
				finishInterrupted(t, q, run)
			} else {
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
				finish(t, q, run, true)
			}
			reply := check(t, q, m, nil)
			if reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch == nil {
				t.Fatal("missing preparation after backfill released", reply)
			}
			launch := *reply.EnvironmentPlan[0].Launch
			if resumable {
				got, err := q.jobSnapshot(low.ID)
				if err != nil || !strings.Contains(got.Reason, high.ID) || !strings.Contains(got.Reason, "waiting for builder report") {
					t.Fatal("offered preparation blocker hidden", got, err)
				}
			}
			if err := q.AuthorizeLaunch(launch); err != nil {
				t.Fatal(err)
			}
			m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, Attempt: launch.Attempt, State: "building"}}
			clock.Advance(time.Second)
			if got := check(t, q, m, nil); got.Job != nil || len(got.EnvironmentPlan) != 1 || got.EnvironmentPlan[0].Launch.Attempt != launch.Attempt {
				t.Fatal("started preparation was displaced or lost", got)
			}
			if !q.s.PreparationPlans[launch.Attempt].Started {
				t.Fatal("preparation made no progress")
			}
			m.Environments = []string{r.Key}
			m.EnvironmentReports[0].State = "ready"
			clock.Advance(time.Second)
			if got := check(t, q, m, nil).Job; got == nil || got.ID != high.ID {
				t.Fatal("ready deadline did not beat lower-tier work", got)
			}
			if _, held := q.s.PreparationPlans[launch.Attempt]; held {
				t.Fatal("exact preparation completion did not discharge custody")
			}
		})
	}
}

// A terminal receipt for an older launch cannot satisfy a repeated offer when
// the prefix is absent. Exercise the real runtime/cache, not just queue reports.
func TestRepeatedCPUPreparationWithOldReadyReceipt(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 8)
	m.EnvironmentBuilder = true
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(2, 1)
	s.Environment.Name = "tools"
	wanted := submit(t, q, s)
	first := check(t, q, m, nil).EnvironmentPlan[0]
	old := EnvironmentReport{Key: r.Key, Attempt: first.Launch.Attempt, State: "ready"}
	m.EnvironmentReports = []EnvironmentReport{old}
	// No ready prefix is advertised: the old terminal receipt clears only its
	// exact custody and the queue must offer a new launch for this cache key.
	reply := check(t, q, m, nil)
	if reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Attempt == old.Attempt {
		t.Fatal("missing replacement preparation", reply)
	}
	next := reply.EnvironmentPlan[0].Launch.Attempt
	rt := &environmentRuntime{dir: t.TempDir(), cache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}, reports: map[string]EnvironmentReport{r.Key: old}, authorize: q.authorizeLaunchContext}
	defer rt.stop()
	rt.sync(reply.EnvironmentPlan, false)
	rt.mu.Lock()
	done := rt.done
	rt.mu.Unlock()
	if done == nil {
		t.Fatal("old ready receipt suppressed replacement build")
	}
	<-done
	m.Environments, m.EnvironmentReports, _, _ = rt.report()
	if len(m.EnvironmentReports) != 1 || m.EnvironmentReports[0].State != "ready" || m.EnvironmentReports[0].Attempt != next {
		t.Fatal("replacement preparation did not complete", m.EnvironmentReports)
	}
	if got := check(t, q, m, nil).Job; got == nil || got.ID != wanted.ID {
		t.Fatal("replacement ready receipt did not release preparation and start work", got)
	}
}

func TestPreparationBlockerInJobGET(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 8)
	m.EnvironmentBuilder = true
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(2, 1)
	s.Priority, s.Deadline, s.Environment.Name = "deadline", spec("deadline").Deadline, "tools"
	high := submit(t, q, s)
	reply := check(t, q, m, nil)
	launch := *reply.EnvironmentPlan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, Attempt: launch.Attempt, State: "building"}}
	check(t, q, m, nil)
	low := submit(t, q, cpuSpec(2, 1))
	got, err := q.jobSnapshot(low.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Reason, "preparation") || !strings.Contains(got.Reason, high.ID) || !strings.Contains(got.Reason, r.Key) {
		t.Fatalf("job GET hid the actual preparation blocker: %q", got.Reason)
	}
	if got := job(t, q, low.ID); !strings.Contains(got.Reason, high.ID) || !strings.Contains(got.Reason, r.Key) {
		t.Fatalf("status hid the actual preparation blocker: %q", got.Reason)
	}
}

func TestBlockedPreparationDemandDoesNotWithholdReadyCPU(t *testing.T) {
	for _, blocker := range []string{"host-memory", "project-cap"} {
		t.Run(blocker, func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 4, 8)
			m.EnvironmentBuilder = true
			r := binaryRecipe(t, "cpu")
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
				t.Fatal(err)
			}
			lowSpec := cpuSpec(2, 1)
			lowSpec.Resumable = true
			lowSpec.AllowedMachines = []string{m.ID}
			low := submit(t, q, lowSpec)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != low.ID {
				t.Fatal("missing initial experiment", run)
			}
			highSpec := cpuSpec(2, 6)
			due := clock.Now().Add(time.Hour)
			highSpec.Project, highSpec.Priority, highSpec.Deadline = "urgent", "deadline", &due
			highSpec.Environment.Name, highSpec.AllowedMachines = "tools", []string{m.ID}
			high := submit(t, q, highSpec)
			if stop := job(t, q, low.ID); stop.State != "stopping" || priorityStop(stop) == nil {
				t.Fatal("higher tier did not request release", stop.State)
			}
			finishInterrupted(t, q, run)
			if p := q.s.Jobs[high.ID].PreemptionPlacement; p == nil || len(p.Releases) == 0 {
				t.Fatal("missing paid target commitment")
			}
			if blocker == "host-memory" {
				m.HostAvailableMemoryGB = 4
			} else {
				peer := cpuMachine("peer", "other-host", "cpu", 4, 8)
				occupant := cpuSpec(1, 1)
				occupant.Project, occupant.AllowedMachines = "urgent", []string{peer.ID}
				other := submit(t, q, occupant)
				if assigned := check(t, q, peer, nil).Job; assigned == nil || assigned.ID != other.ID {
					t.Fatal("missing concurrent project work")
				}
				if err := q.ConfigureProjects(map[string]ProjectPolicy{"urgent": {Weight: 1, MaxConcurrent: 1}}); err != nil {
					t.Fatal(err)
				}
				if reason := q.capReason(q.s.Jobs[high.ID]); reason == "" {
					t.Fatal("fixture did not establish the project cap")
				}
			}
			got := check(t, q, m, nil)
			if got.Job == nil || got.Job.ID != low.ID || len(got.EnvironmentPlan) != 0 {
				t.Fatalf("ineligible preparation demand withheld fitting ready work: job=%v offers=%d high-reason=%s", got.Job, len(got.EnvironmentPlan), q.capReason(q.s.Jobs[high.ID]))
			}
			if q.s.Jobs[high.ID].PreemptionPlacement == nil {
				t.Fatal("eligibility change discarded exact target commitment")
			}
		})
	}
}

func TestPreparationStatusKeepsReadyAlternativeWorker(t *testing.T) {
	q, _ := fixture(t)
	a := cpuMachine("worker-a", "host-a", "cpu", 4, 8)
	b := cpuMachine("worker-b", "host-b", "cpu", 4, 8)
	a.EnvironmentBuilder, b.EnvironmentBuilder = true, true
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := cpuSpec(2, 1)
	s.Priority, s.Environment.Name = "production", "tools"
	submit(t, q, s)
	if reply := check(t, q, a, nil); reply.Job != nil || len(reply.EnvironmentPlan) != 1 {
		t.Fatal("missing production preparation offer", reply)
	}
	low := submit(t, q, cpuSpec(2, 1))
	before, err := q.jobSnapshot(low.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(before.Reason, "waiting for software preparation for job ") {
		t.Fatal("status hid the ready alternative worker", before.Reason)
	}
	if got := check(t, q, b, nil).Job; got == nil || got.ID != low.ID {
		t.Fatal("ready alternative worker did not dispatch", got)
	}
}
