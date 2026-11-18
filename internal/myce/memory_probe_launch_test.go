package myce

import (
	"context"
	"math"
	"testing"
	"time"
)

// The queue's page-sized gap cannot absorb the observed inventory-to-launch
// drop. Use this host's idle high water, without adding its reserve twice.
func TestMemoryProbeLaunchNearMiss(t *testing.T) {
	for _, history := range []bool{true, false} {
		t.Run(map[bool]string{true: "idle history", false: "prelaunch evidence"}[history], func(t *testing.T) {
			q, clock, m, s := gpuHostFixture(t)
			const available = 16.94316864013672
			const launch = 16.929885864257812
			const total = 33507364864.0 / (1 << 30)
			const reserve = 4.0
			m.CapabilityProfile.SystemMemoryBytes = uint64(total * (1 << 30))
			m.HostAvailableMemoryGB = available
			m.MemoryObservation = &HostMemoryObservation{BaselineVersion: 1, Quiescent: true,
				TotalGB: total, UsedGB: total - launch - reserve, ReserveGB: reserve}
			if history {
				machineCheck(t, q, m)
				clock.Advance(time.Second)
			}
			observation := *m.MemoryObservation
			observation.UsedGB = total - available - reserve
			m.MemoryObservation = &observation
			machineCheck(t, q, m)
			queued := submit(t, q, s)
			if !history {
				// Older hosts can lack qualified idle history. An exact-session
				// refused launch measures the same gap without guessing a size.
				if err := q.update(func() error {
					j := q.touchJob(queued.ID)
					launchAvailable := launch
					d := &AdmissionDetail{Subcheck: "live_host_growth", Required: available - minimumHostMemoryGB, Available: &launchAvailable, EnvelopeHostGB: available - minimumHostMemoryGB}
					j.LaunchCapacityWait = map[string]LaunchCapacityWait{m.ID + "/0": {Machine: m.ID, Session: m.Session, Admission: d, Evidence: "previous-offer"}}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			run := assignedJob(machineCheck(t, q, m))
			if run == nil || run.ID != queued.ID || len(run.AllocatedDevices) != 1 {
				t.Fatalf("cold qualified work did not get one card: %+v; %s", run, job(t, q, queued.ID).Reason)
			}
			if e := run.MemoryEnvelope; e.Refusal != "" || e.Basis != "exploratory" || e.HostGB > launch || e.HostGB < launch-2*minimumHostMemoryGB {
				t.Fatalf("probe did not fit the measured launch room: %+v", e)
			}
			w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return launch + reserve, nil }, HostMemoryReserveGB: reserve}
			if err := localAttemptAdmission(w, nil, *run, nil); err != nil {
				t.Fatalf("near-miss still refused at worker launch: %v", err)
			}
			// Containment is still strict, even one byte below the offer.
			w.HostMemorySource = func(context.Context) (float64, error) { return run.MemoryEnvelope.HostGB + reserve - 1.0/(1<<30), nil }
			if err := localAttemptAdmission(w, nil, *run, nil); err == nil {
				t.Fatal("launch accepted a physical deficit")
			}
			if math.Abs(q.s.Machines[m.ID].HostMemoryBudgetGB-24) > 1e-9 {
				t.Fatal("probe changed the host allowance")
			}
		})
	}
}
