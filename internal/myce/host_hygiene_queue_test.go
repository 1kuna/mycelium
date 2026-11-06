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
