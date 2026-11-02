package myce

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func stopped(r Reply, id string) bool {
	for _, a := range r.Attempts {
		if a.ID == id && a.Stop {
			return true
		}
	}
	return false
}

func holdMachine(t *testing.T, scope string) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, c := fixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: true, ProductionHoldScope: scope}}); err != nil {
		t.Fatal(err)
	}
	return q, c, m3Machine()
}

// One physical host with CPU and two fake cards. Device scope packs the sibling
// card and the CPU beside production; host scope keeps today's exclusive fence.
func TestProductionHoldScopeOnMachineWorker(t *testing.T) {
	t.Parallel()
	t.Run("device", func(t *testing.T) {
		t.Parallel()
		q, c, m := holdMachine(t, "device")
		first := submit(t, q, spec("production"))
		p1 := assignedJob(machineCheck(t, q, m))
		if p1 == nil || p1.ID != first.ID || !slices.Equal(p1.AllocatedDevices, []string{"0"}) {
			t.Fatal("production did not start on card 0", p1)
		}
		submit(t, q, spec("experiment"))
		submit(t, q, cpuSpec(1, 1))
		a := assignedJob(machineCheck(t, q, m, p1))
		if a == nil {
			t.Fatal("no lower tier fit beside production")
		}
		b := assignedJob(machineCheck(t, q, m, p1, a))
		if b == nil {
			t.Fatal("second lower tier did not fit beside production")
		}
		x, cpu := a, b
		if x.Spec.Needs.cpuOnly() {
			x, cpu = b, a
		}
		if !slices.Equal(x.AllocatedDevices, []string{"1"}) || !cpu.Spec.Needs.cpuOnly() || len(cpu.AllocatedDevices) != 0 {
			t.Fatal("experiments not on the sibling card and the CPU", x.AllocatedDevices, cpu.AllocatedDevices)
		}

		second := submit(t, q, spec("production"))
		r := machineCheck(t, q, m, p1, x, cpu)
		if !stopped(r, x.ID) || stopped(r, cpu.ID) || stopped(r, p1.ID) || assignedJob(r) != nil {
			t.Fatalf("production must reclaim exactly card 1: %+v", r.Attempts)
		}
		// Elapsed grace is not release: card 1 stays charged until the exact
		// attempt acknowledges its stop.
		c.Advance(time.Minute)
		if got := assignedJob(machineCheck(t, q, m, p1, x, cpu)); got != nil {
			t.Fatal("production started before exact release", got.ID)
		}
		requireState(t, q, cpu.ID, "running")
		finishInterrupted(t, q, x)
		p2 := assignedJob(machineCheck(t, q, m, p1, cpu))
		if p2 == nil || p2.ID != second.ID || !slices.Equal(p2.AllocatedDevices, []string{"1"}) {
			t.Fatal("production did not take the released card", p2)
		}
		requireState(t, q, x.ID, "queued")
		requireState(t, q, cpu.ID, "running")
		requireState(t, q, p1.ID, "running")
	})
	t.Run("host", func(t *testing.T) {
		t.Parallel()
		q, _, m := holdMachine(t, "")
		first := submit(t, q, spec("production"))
		p1 := assignedJob(machineCheck(t, q, m))
		if p1 == nil || p1.ID != first.ID {
			t.Fatal("production did not start", p1)
		}
		x := submit(t, q, spec("experiment"))
		cpu := submit(t, q, cpuSpec(1, 1))
		if got := assignedJob(machineCheck(t, q, m, p1)); got != nil {
			t.Fatal("host-wide hold admitted lower tier", got.ID)
		}
		if reason := job(t, q, x.ID).Reason; !strings.Contains(reason, "reserved for production") {
			t.Fatal(reason)
		}
		finish(t, q, p1, true)
		a := assignedJob(machineCheck(t, q, m))
		if a == nil {
			t.Fatal("lower tier did not return after production")
		}
		b := assignedJob(machineCheck(t, q, m, a))
		if b == nil {
			t.Fatal("second lower tier did not return after production")
		}
		// A card is free, yet the exclusive hold still stops every lower tier.
		second := submit(t, q, spec("production"))
		r := machineCheck(t, q, m, a, b)
		if !stopped(r, x.ID) || !stopped(r, cpu.ID) || assignedJob(r) != nil {
			t.Fatalf("host-wide hold must stop the whole host first: %+v", r.Attempts)
		}
		finishInterrupted(t, q, a)
		if got := assignedJob(machineCheck(t, q, m, b)); got != nil {
			t.Fatal("production started before every host release", got.ID)
		}
		finishInterrupted(t, q, b)
		if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != second.ID {
			t.Fatal("production did not start after host release", got)
		}
	})
}

// An empty startup file holds nothing, so a later file can still add a hold.
func TestProductionHoldEmptyStartupFileStillSeeds(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureHosts(map[string]HostPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: true}}); err != nil {
		t.Fatal("adding a hold after an empty startup file refused:", err)
	}
	if !q.PriorityPolicy().Policy.Hosts["machine"].ProductionHold {
		t.Fatal("hold not seeded")
	}
}

// The policy API changes persisted host scope; restart keeps it, and the
// startup file that seeded the old policy no longer blocks or replaces it.
func TestProductionHoldScopeTransitionSurvivesRestart(t *testing.T) {
	t.Parallel()
	q, c := fixture(t)
	startup := map[string]HostPolicy{"machine": {ProductionHold: true}}
	if err := q.ConfigureHosts(startup); err != nil {
		t.Fatal(err)
	}
	record := q.PriorityPolicy()
	for _, bad := range []HostPolicy{{ProductionHold: true, ProductionHoldScope: "card"}, {ProductionHoldScope: "device"}} {
		record.Policy.Hosts = map[string]HostPolicy{"machine": bad}
		if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err == nil {
			t.Fatalf("invalid host policy accepted: %+v", bad)
		}
	}
	if got := q.PriorityPolicy(); got.Revision != record.Revision || got.Policy.Hosts["machine"] != startup["machine"] {
		t.Fatalf("rejected change altered policy: %+v", got)
	}
	record.Policy.Hosts = map[string]HostPolicy{"machine": {ProductionHold: true, ProductionHoldScope: "device"}}
	record, err := q.SetPriorityPolicy(record.Revision, record.Policy)
	if err != nil {
		t.Fatal(err)
	}
	// Omitting hosts (an older client) preserves them.
	record.Policy.Hosts = nil
	if record, err = q.SetPriorityPolicy(record.Revision, record.Policy); err != nil || record.Policy.Hosts["machine"].ProductionHoldScope != "device" {
		t.Fatalf("omitted hosts replaced policy: %+v %v", record, err)
	}

	path, root := q.dbPathForTest(t), q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, c)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err := q.ConfigureHosts(startup); err != nil {
		t.Fatal("unchanged startup file refused after policy transition:", err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{"machine": {ProductionHold: false}}); err == nil || !strings.Contains(err.Error(), "saved policy") {
		t.Fatalf("edited startup file accepted: %v", err)
	}
	if got := q.PriorityPolicy().Policy.Hosts["machine"]; got.ProductionHoldScope != "device" {
		t.Fatalf("restart lost scope: %+v", got)
	}

	m := m3Machine()
	submit(t, q, spec("production"))
	p := assignedJob(machineCheck(t, q, m))
	if p == nil {
		t.Fatal("production did not start after restart")
	}
	submit(t, q, spec("experiment"))
	x := assignedJob(machineCheck(t, q, m, p))
	if x == nil || slices.Equal(p.AllocatedDevices, x.AllocatedDevices) {
		t.Fatal("device scope not applied after restart", x)
	}
	// The transition is reversible: host scope fences the host again.
	record = q.PriorityPolicy()
	record.Policy.Hosts["machine"] = HostPolicy{ProductionHold: true, ProductionHoldScope: "host"}
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal(err)
	}
	if r := machineCheck(t, q, m, p, x); !stopped(r, x.ID) {
		t.Fatalf("host scope did not resume the exclusive hold: %+v", r.Attempts)
	}
}
