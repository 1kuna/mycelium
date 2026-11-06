package myce

import (
	"context"
	"testing"
	"time"
)

func discreteFanOutFixture(t *testing.T) (*Queue, *fakeClock, Machine, Spec) {
	t.Helper()
	q, clock := fixture(t)
	e := compositionExtension(t, false)
	e.Kinds = nil
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "xpu"}
	v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 3, HostMemoryGB: 2}
	delete(v.Baselines, "kind:sample")
	v.References = map[string]WorkReference{"extension:sample": {InputShape: "small-v1", Unit: "records", Units: 10}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := sharedDiscreteMachine("xpu")
	m.Devices[0].ExtensionFingerprint = "synthetic-xpu"
	free, active := 8.0, 20.0
	m.Devices[0].Telemetry = &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active, Source: "synthetic"}
	card := m.Devices[0].machine(m)
	r := newExtensionReport(e, &card, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "device-check"
	m.Devices[0].ExtensionReports = []ExtensionReport{r}
	s := Spec{Name: "piece", Project: "synthetic", Priority: "production", Extension: extensionKey(e),
		Targets: []ExecutionTarget{v.Target}, Workload: &Workload{InputShape: "small-v1", Units: 10},
		FanOut: &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "piece"}}}}
	return q, clock, m, s
}

func discreteFanOutCheck(t *testing.T, q *Queue, m Machine, runs ...*Job) Reply {
	t.Helper()
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	for _, j := range runs {
		gb, host := j.Spec.Needs.MemoryGB, j.Spec.Needs.HostMemoryGB
		c.Attempts = append(c.Attempts, AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session,
			Started: true, CurrentHostMemory: &CurrentHostMemory{DeviceGB: &gb, NonreclaimableGB: host, FootprintGB: &host, GrowthObserved: true}})
	}
	r, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestDiscreteFanOutSharingHeadroom(t *testing.T) {
	for _, condition := range []string{"present", "missing activity", "missing memory", "stale telemetry", "saturated", "missing host", "host allowance"} {
		t.Run(condition, func(t *testing.T) {
			q, clock, m, s := discreteFanOutFixture(t)
			parent := submit(t, q, s)
			a := assignedJob(discreteFanOutCheck(t, q, m))
			if a == nil || a.Batch == nil || a.Batch.Parent != parent.ID {
				t.Fatal("exclusive first piece missing", job(t, q, parent.ID).Reason)
			}
			if err := q.Started(result(a, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			free, active := 5.0, 100.0
			m.Devices[0].Telemetry.FreeGB = &free
			switch condition {
			case "missing activity":
				m.Devices[0].Telemetry.ActivePercent = nil
			case "missing memory":
				m.Devices[0].Telemetry.FreeGB = nil
			case "stale telemetry":
				m.Devices[0].Telemetry.AgeSeconds = deviceMemorySampleFreshFor.Seconds()
			case "saturated":
				m.Devices[0].Telemetry.ActivePercent = &active
			case "missing host":
				m.HostMemoryMeasured = false
			case "host allowance":
				m.HostMemoryBudgetGB = 3
			}
			other := submit(t, q, s)
			b := assignedJob(discreteFanOutCheck(t, q, m, a))
			if condition != "present" {
				if b != nil {
					t.Fatal("unproven headroom admitted a second piece", b.ID)
				}
				requireState(t, q, other.ID, "queued")
				return
			}
			if b == nil || b.Batch.Parent != other.ID || b.AllocatedDevices[0] != a.AllocatedDevices[0] || b.Spec.FanOut != nil {
				t.Fatal("ready parent did not materialize on the occupied card", b, job(t, q, other.ID).Reason)
			}
			w := Worker{Machine: m, Clock: clock, hostMemorySamples: q.hostMemorySamples[m.ID],
				HostMemorySource: func(context.Context) (float64, error) { return 32, nil }}
			if err := localAttemptAdmission(w, []Job{*job(t, q, a.ID)}, *b, nil); err != nil {
				t.Fatal("worker rejected measured queue placement", err)
			}
			// Existing #625 growth handling must stop a grower and retain exact
			// release custody, without charging an infrastructure retry.
			if err := q.Started(result(b, false)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			growthCheck(t, q, m, []*Job{a, b}, []float64{7, 3}, 0)
			requireState(t, q, a.ID, "stopping")
			requireState(t, q, b.ID, "running")
			if priorityStop(job(t, q, a.ID)).Cause != sharedGPUPressureCause {
				t.Fatal("growth did not use the existing planned-stop path")
			}
			finishInterrupted(t, q, a)
			if got := job(t, q, a.ID); got.InfrastructureRetries != 0 || got.OOMRetries != 0 || got.DeviceMemoryFloorGB < 7 {
				t.Fatal("growth lost its retry floor or spent a failure budget", got)
			}
		})
	}
}

func TestDiscreteFanOutWaitsForIncumbentSample(t *testing.T) {
	q, clock, m, s := discreteFanOutFixture(t)
	submit(t, q, s)
	a := assignedJob(discreteFanOutCheck(t, q, m))
	if a == nil {
		t.Fatal("first piece missing")
	}
	if err := q.Started(result(a, false)); err != nil {
		t.Fatal(err)
	}
	other := submit(t, q, s)
	clock.Advance(time.Second)
	r, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol,
		Attempts: []AttemptReport{{Execution: Execution{ID: a.ID, Attempt: a.Attempt}, Session: a.Session, Started: true}}})
	if err != nil || assignedJob(r) != nil {
		t.Fatal("startup without a sensor reading admitted another tenant", err)
	}
	requireState(t, q, other.ID, "queued")
	if got := assignedJob(discreteFanOutCheck(t, q, m, a)); got == nil || got.Batch.Parent != other.ID {
		t.Fatal("fresh incumbent evidence did not unblock the same parent", job(t, q, other.ID).Reason)
	}
}
