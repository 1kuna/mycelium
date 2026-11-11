package myce

import (
	"fmt"
	"testing"
	"time"
)

func TestHostHygieneQueueDrainPreservesRunningAndOperatorPolicy(t *testing.T) {
	for _, hosts := range []int{1, 32} {
		t.Run(fmt.Sprint(hosts), func(t *testing.T) {
			q, c := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 4, 16)
			for i := 1; i < hosts; i++ {
				other := cpuMachine(fmt.Sprintf("peer-%d", i), fmt.Sprintf("peer-%d", i), "cpu", 4, 16)
				check(t, q, other, nil)
			}
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.AllowedMachines = []string{m.ID}
			first := submit(t, q, s)
			run := check(t, q, m, nil).Job
			if run == nil || run.ID != first.ID {
				t.Fatal("missing initial assignment")
			}
			if err := q.Started(result(run, false)); err != nil {
				t.Fatal(err)
			}
			m.HostHealth = &HostHealth{DrainReason: "host_memory_pressure"}
			c.Advance(time.Second)
			reply := check(t, q, m, run)
			if reply.Stop || reply.Kill {
				t.Fatal("drain killed running custody")
			}
			if q.machineLimitReason(q.s.Machines[m.ID]) != "host_memory_pressure" {
				t.Fatal("missing typed reason")
			}
			if len(alertsOf(q, "host_degraded")) != 1 || alertsOf(q, "host_degraded")[0].Reason != "host_memory_pressure" {
				t.Fatal("missing causal alert")
			}
			finish(t, q, run, true)
			second := submit(t, q, s)
			if check(t, q, m, nil).Job != nil {
				t.Fatal("degraded worker admitted")
			}
			if q.hostLaunchAllowed(q.s.Machines[m.ID], m.Session) {
				t.Fatal("degraded worker authorized preparation")
			}
			m.HostHealth = &HostHealth{}
			recovered := check(t, q, m, nil).Job
			if recovered == nil || recovered.ID != second.ID {
				t.Fatal("recovered worker did not resume")
			}
			finish(t, q, recovered, true)
			if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "drain", DrainOwner: "operator"}); err != nil {
				t.Fatal(err)
			}
			m.HostHealth = &HostHealth{DrainReason: "host_memory_pressure"}
			check(t, q, m, nil)
			m.HostHealth = &HostHealth{}
			check(t, q, m, nil)
			if q.s.HostMaintenance["host"].DrainOwner != "operator" || !q.maintenanceBlocked("host") {
				t.Fatal("recovery overrode operator drain")
			}
		})
	}
}

// One wake per foreign decline that has run as long as its remaining runway;
// owned growth and a recovered or vanished volume clear it.
func TestHostHygieneDiskExhaustionForecastAlert(t *testing.T) {
	q, c := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 4, 16)
	check(t, q, m, nil)
	volume := func(declining, exhaustion float64, cause string) *WorkerHygieneCollection {
		return &WorkerHygieneCollection{DiskBudget: []DiskBudgetVolume{{ID: identityHash("v"), DecliningSeconds: declining, ExhaustionSeconds: &exhaustion, DeclineCause: cause}}}
	}
	report := func(r *WorkerHygieneCollection) {
		t.Helper()
		c.Advance(time.Second)
		q.mu.Lock()
		q.s.Machines[m.ID].HygieneCollection = r
		q.detectHostHygiene()
		q.mu.Unlock()
	}
	report(volume(3600, 7200, "foreign_disk_growth"))
	report(volume(7200, 3600, "owned_disk_growth"))
	if n := len(alertsOf(q, "host_disk_exhaustion_forecast")); n != 0 {
		t.Fatal("early or owned decline alerted", n)
	}
	report(volume(7200, 3600, "foreign_disk_growth"))
	report(volume(7300, 3500, "foreign_disk_growth"))
	alerts := alertsOf(q, "host_disk_exhaustion_forecast")
	if len(alerts) != 1 || alerts[0].Host != "host" || alerts[0].Reason != "foreign_disk_growth" {
		t.Fatal("want one causal alert", alerts)
	}
	report(&WorkerHygieneCollection{})
	report(volume(7200, 3600, "foreign_disk_growth"))
	if len(alertsOf(q, "host_disk_exhaustion_forecast")) != 2 {
		t.Fatal("a vanished volume did not clear, or a new episode did not wake")
	}
}
