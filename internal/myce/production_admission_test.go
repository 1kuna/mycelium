package myce

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestProductionAdmissionRepeatedPreemptionBackoff(t *testing.T) {
	for _, next := range []string{"expiry", "other-host", "outside-window"} {
		t.Run(next, func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
				t.Fatal(err)
			}
			a := machine("a", "fake", "host-a")
			if err := q.ConfigureHosts(map[string]HostPolicy{"host-a": {ProductionHold: false}}); err != nil {
				t.Fatal(err)
			}
			victim := submit(t, q, spec("experiment"))
			var first *Job
			for i := range 2 {
				run := check(t, q, a, nil).Job
				if run == nil || run.ID != victim.ID {
					t.Fatal("retry before threshold withheld", run)
				}
				if i == 0 {
					first = run
				}
				if err := q.Started(result(run, false)); err != nil {
					t.Fatal(err)
				}
				if next == "outside-window" && i == 1 {
					clock.Advance(2*ProductionStartTimeout + time.Second)
				}
				p := spec("production")
				p.AllowedMachines = []string{a.ID}
				prod := submit(t, q, p)
				if !check(t, q, a, run).Stop {
					t.Fatal("production did not preempt experiment")
				}
				finishInterrupted(t, q, run)
				run = check(t, q, a, nil).Job
				if run == nil || run.ID != prod.ID {
					t.Fatal("production withheld", run)
				}
				finish(t, q, run, true)
			}
			if next == "outside-window" {
				if got := check(t, q, a, nil).Job; got == nil || got.ID != victim.ID {
					t.Fatal("unrelated old preemption counted", got)
				}
				return
			}
			if got := check(t, q, a, nil).Job; got != nil {
				t.Fatal("admitted third churn cycle", got.ID)
			}
			if reason := runtimeJob(t, q, victim.ID).Reason; !strings.Contains(reason, "production preemption back-off") || !strings.Contains(reason, "150s") {
				t.Fatal("missing status reason", reason)
			}
			if q.held("host-a") {
				t.Fatal("disabled production hold activated")
			}
			if err := q.Complete(result(first, true)); !errors.Is(err, ErrStale) {
				t.Fatal("stale attempt accepted", err)
			}
			q = reopenCancellationQueue(t, q, clock)
			if got := check(t, q, a, nil).Job; got != nil {
				t.Fatal("back-off lost on restart", got.ID)
			}
			if next == "other-host" {
				sibling := machine("a-sibling", "fake", "host-a")
				if got := check(t, q, sibling, nil).Job; got != nil {
					t.Fatal("sibling worker bypassed physical-host back-off", got.ID)
				}
				other := machine("b", "fake", "host-b")
				if got := check(t, q, other, nil).Job; got == nil || got.ID != victim.ID {
					t.Fatal("other host withheld", got)
				}
				return
			}
			clock.Advance(ProductionStartTimeout - time.Second)
			if got := check(t, q, a, nil).Job; got != nil {
				t.Fatal("back-off expired early", got.ID)
			}
			clock.Advance(time.Second)
			if got := check(t, q, a, nil).Job; got == nil || got.ID != victim.ID {
				t.Fatal("back-off did not expire", got)
			}
		})
	}
}

// A real declared producer, a qualified destination and a cold experiment with
// matching preparation history; all timing and custody use the queue fake clock.
func productionAdmissionFixture(t *testing.T) (*Queue, *workerFakeClock, Machine, Machine, Spec, Spec) {
	t.Helper()
	q, clock, source, p := fanOutFixture(t, 2)
	if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	for key, point := range q.s.WorkObservations {
		point.Rate /= 100 // Each measured wave takes 100 seconds.
		q.s.WorkObservations[key] = point
	}
	target := cloneMachine(&source)
	target.ID, target.HostID, target.Session = "destination", "destination-host", "destination-session"
	e := registeredExtension(t, q, p.Extension)
	r := newExtensionReport(e, &target, p.workType())
	r.Setup, r.Qualified, r.CheckID = true, true, "destination-check"
	target.ExtensionReports = []ExtensionReport{r}
	target.EnvironmentBuilder = true
	recipe := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"cold": {"cpu": recipe}}}); err != nil {
		t.Fatal(err)
	}
	target.EnvironmentBuildSeconds = map[string]float64{recipe.Key: 200}
	check(t, q, target, nil)
	p.Priority, p.AllowedMachines = "production", []string{source.ID}
	p.Outputs = map[string]string{"out": "results"}
	x := cpuSpec(1, 1)
	x.Project, x.Priority, x.Environment.Name, x.AllowedMachines = p.Project, "experiment", "cold", []string{target.ID}
	return q, clock, source, target, p, x
}

func TestProductionAdmissionPredictableDependentDemand(t *testing.T) {
	for _, edge := range []string{"piece", "whole-parent", "fan-out-stage", "producer-overrun"} {
		t.Run(edge, func(t *testing.T) {
			q, clock, source, target, p, x := productionAdmissionFixture(t)
			parent := submit(t, q, p)
			c := cpuSpec(1, 1)
			c.Project, c.Priority, c.AllowedMachines = p.Project, "production", []string{target.ID}
			if edge == "fan-out-stage" {
				c = p
				c.AllowedMachines = []string{target.ID}
			}
			c.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "item-0/out"}}
			if edge == "piece" {
				c.After.Piece, c.After.Outputs["input"] = "item-0", "out"
			}
			consumer := submit(t, q, c)
			wave := check(t, q, source, nil).Job
			if wave == nil || wave.Batch == nil || !wave.Batch.Run {
				t.Fatal("no measured producer wave", wave)
			}
			if err := q.Started(result(wave, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(10 * time.Second)
			experiment := submit(t, q, x)
			reply := check(t, q, target, nil)
			if reply.Job != nil || len(reply.EnvironmentPlan) != 0 {
				t.Fatal("admitted bootstrap before imminent production", reply)
			}
			if reason := runtimeJob(t, q, experiment.ID).Reason; !strings.Contains(reason, "predictable production demand") || !strings.Contains(reason, "~90s") || !strings.Contains(reason, "setup 200s") {
				t.Fatal("missing measured status", reason)
			}
			if q.held(hostID(&target)) {
				t.Fatal("forecast enabled a production hold")
			}
			if edge == "producer-overrun" {
				clock.Advance(91 * time.Second)
				if reply := check(t, q, target, nil); len(reply.EnvironmentPlan) != 1 {
					t.Fatal("forecast hold did not expire when production never arrived", reply)
				}
				return
			}
			// Withdrawing this demand immediately restores ordinary preparation.
			if err := q.Cancel(consumer.ID); err != nil {
				t.Fatal(err)
			}
			if reply := check(t, q, target, nil); len(reply.EnvironmentPlan) != 1 {
				t.Fatal("demand withdrawal did not restore preparation", reply)
			}
		})
	}
}

func TestProductionAdmissionNoDemandOrUnknownEstimate(t *testing.T) {
	for _, mode := range []string{"no-demand", "unknown-setup", "unknown-producer", "producer-overrun", "restricted-destination", "setup-fits"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, source, target, p, x := productionAdmissionFixture(t)
			parent := submit(t, q, p)
			if mode != "no-demand" {
				c := cpuSpec(1, 1)
				c.Project, c.Priority, c.AllowedMachines = p.Project, "production", []string{target.ID}
				if mode == "restricted-destination" {
					c = p
					c.AllowedMachines = []string{source.ID}
				}
				c.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "item-0/out"}}
				submit(t, q, c)
			}
			wave := check(t, q, source, nil).Job
			if wave == nil {
				t.Fatal("missing producer")
			}
			if mode != "unknown-producer" {
				if err := q.Started(result(wave, false)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "unknown-setup" {
				target.EnvironmentBuildSeconds = nil
			}
			if mode == "setup-fits" {
				for key := range target.EnvironmentBuildSeconds {
					target.EnvironmentBuildSeconds[key] = 10
				}
			}
			if mode == "producer-overrun" {
				clock.Advance(101 * time.Second)
			}
			submit(t, q, x)
			if reply := check(t, q, target, nil); len(reply.EnvironmentPlan) != 1 || reply.Job != nil {
				t.Fatal("ordinary cold admission changed", reply)
			}
		})
	}
}

func TestProductionAdmissionCachedPreparationAndQueuedDemand(t *testing.T) {
	for _, component := range []bool{false, true} {
		t.Run(fmt.Sprint("component=", component), func(t *testing.T) {
			q, _, source, target, p, x := productionAdmissionFixture(t)
			if component {
				target.GPU = "fake"
				target.Devices = m3Machine().Devices
				target.CPUExtensionFingerprint = target.ExtensionFingerprint
				check(t, q, target, nil)
			}
			parent := submit(t, q, p)
			wave := check(t, q, source, nil).Job
			if wave == nil {
				t.Fatal("missing producer")
			}
			if err := q.Started(result(wave, false)); err != nil {
				t.Fatal(err)
			}
			experiment := submit(t, q, x)
			offer := check(t, q, target, nil).EnvironmentPlan
			if len(offer) != 1 || offer[0].Launch == nil {
				t.Fatal("missing initial offer", offer)
			}
			c := cpuSpec(1, 1)
			c.Project, c.Priority, c.AllowedMachines = p.Project, "production", []string{target.ID}
			c.After = &PieceDependency{Job: parent.ID, Outputs: map[string]string{"input": "item-0/out"}}
			submit(t, q, c)
			if err := q.AuthorizeLaunch(*offer[0].Launch); !errors.Is(err, ErrStale) {
				t.Fatal("cached preparation bypassed new predictable demand", err)
			}
			// Readiness makes the expected arrival immediate, without enabling a hold.
			finish(t, q, wave, true)
			if reason := runtimeJob(t, q, experiment.ID).Reason; !strings.Contains(reason, "predictable production demand") || !strings.Contains(reason, "~0s") {
				t.Fatal("ready queued production not explained", reason)
			}
		})
	}
}

func TestProductionAdmissionPreparationPreemptionUsesMeasuredBackoff(t *testing.T) {
	for _, component := range []bool{false, true} {
		t.Run(fmt.Sprint("component=", component), func(t *testing.T) {
			q, clock, _, target, _, x := productionAdmissionFixture(t)
			if component {
				target.GPU = "fake"
				target.Devices = m3Machine().Devices
				target.CPUExtensionFingerprint = target.ExtensionFingerprint
				check(t, q, target, nil)
			}
			experiment := submit(t, q, x)
			var old HostLaunch
			for range 2 {
				plan := check(t, q, target, nil).EnvironmentPlan
				if len(plan) != 1 || plan[0].Launch == nil {
					t.Fatal("missing preparation", plan)
				}
				old = *plan[0].Launch
				if err := q.AuthorizeLaunch(old); err != nil {
					t.Fatal(err)
				}
				c := cpuSpec(1, 1)
				c.Project, c.Priority, c.AllowedMachines = x.Project, "production", []string{target.ID}
				prod := submit(t, q, c)
				if stopped := q.s.PreparationPlans[old.Attempt]; stopped.PriorityStop == nil {
					t.Fatal("production did not preempt preparation", stopped)
				}
				target.EnvironmentReports = []EnvironmentReport{{Key: old.Key, Attempt: old.Attempt, State: "paused"}}
				var run *Job
				if component {
					run = assignedJob(machineCheck(t, q, target))
				} else {
					run = check(t, q, target, nil).Job
				}
				if run == nil || run.ID != prod.ID {
					t.Fatal("settled preparation blocked production", run)
				}
				finish(t, q, run, true)
			}
			if reply := check(t, q, target, nil); len(reply.EnvironmentPlan) != 0 {
				t.Fatal("third preparation admitted", reply)
			}
			if reason := runtimeJob(t, q, experiment.ID).Reason; !strings.Contains(reason, "back-off") || !strings.Contains(reason, "200s") {
				t.Fatal("measured setup did not size back-off", reason)
			}
			if err := q.AuthorizeLaunch(old); !errors.Is(err, ErrStale) {
				t.Fatal("stale preparation accepted", err)
			}
			clock.Advance(200 * time.Second)
			if reply := check(t, q, target, nil); len(reply.EnvironmentPlan) != 1 {
				t.Fatal("measured back-off did not expire", reply)
			}
		})
	}
}

func TestProductionAdmissionPreservesBoundReservation(t *testing.T) {
	q, _, _, target, _, x := productionAdmissionFixture(t)
	experiment := submit(t, q, x)
	if _, err := q.CreateReservation(reservationRequest("validation", "host="+hostID(&target), experiment.ID)); err != nil {
		t.Fatal(err)
	}
	c := cpuSpec(1, 1)
	c.Project, c.Priority, c.AllowedMachines = x.Project, "production", []string{target.ID}
	submit(t, q, c)
	if reply := check(t, q, target, nil); len(reply.EnvironmentPlan) != 1 {
		t.Fatal("unrelated production demand blocked bound validation", reply)
	}
}
