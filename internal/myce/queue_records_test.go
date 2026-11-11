package myce

import (
	"errors"
	"testing"
)

// Rollback must restore both a keyed answer and nested job state. Exercise cold
// and live records, including entering through the key before touching the job.
func TestQueueRecordsKeyedRollback(t *testing.T) {
	for _, cold := range []bool{false, true} {
		for _, fromKey := range []bool{false, true} {
			t.Run(map[bool]string{false: "live", true: "cold"}[cold]+"/"+map[bool]string{false: "job", true: "key"}[fromKey], func(t *testing.T) {
				q, _ := fixture(t)
				s := spec("production")
				s.SubmissionKey = "undo"
				j := submit(t, q, s)
				if cold {
					run := check(t, q, machine("worker", "fake"), nil).Job
					finish(t, q, run, true)
				}
				drainAdmissionWait(t, q)
				before, err := marshalQueueState(t, q)
				if err != nil {
					t.Fatal(err)
				}
				rejected := errors.New("rejected")
				if err := q.update(func() error {
					var live *Job
					if fromKey {
						live = q.touchSubmission(submissionDigest(s.Project, s.SubmissionKey)).Job
					} else {
						live = q.touchJob(j.ID)
					}
					live.Spec.Run.Command[0] = "mutated"
					live.Reason = "mutated"
					return rejected
				}); !errors.Is(err, rejected) {
					t.Fatal(err)
				}
				after, err := marshalQueueState(t, q)
				if err != nil {
					t.Fatal(err)
				}
				if string(before) != string(after) {
					t.Fatal("job or retained key changed after rollback")
				}
				if _, err := q.CancelJob(j.ID); err != nil {
					t.Fatal(err)
				}
				replay := submit(t, q, s)
				if replay.CancelledAt == nil || replay.ID != j.ID {
					t.Fatal("rollback broke key alias", replay)
				}
			})
		}
	}
}

// A heartbeat enrolls its inventory, never the entire runnable backlog.
func TestQueueRecordsExplicitDirtySet(t *testing.T) {
	q, clock := fixture(t)
	check(t, q, machine("one", "fake"), nil)
	check(t, q, machine("two", "fake"), nil)
	s := spec("experiment")
	s.AllowedMachines = []string{"unavailable"}
	for range 12 {
		submit(t, q, s)
	}
	drainAdmissionWait(t, q)
	before := durableQueueRecord(t, q.db, "machines/two")
	if err := q.update(func() error {
		if len(q.records.jobs) != 0 || len(q.records.catalogUndo) != 0 || len(q.records.machines) != 0 {
			t.Fatal("transaction pre-touched unchanged records")
		}
		q.touchMachine("one").Leaving = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if after := durableQueueRecord(t, q.db, "machines/two"); string(before) != string(after) {
		t.Fatal("unrelated machine rewritten")
	}
	q = reopenQueue(t, q, clock)
	if !q.s.Machines["one"].Leaving || q.s.Machines["two"].Leaving {
		t.Fatal("restart lost the isolated machine decision")
	}
}

// Diagnostic and decision journals may enroll the same record in either order.
func TestQueueRecordsViewAndDecisionRollback(t *testing.T) {
	for _, viewFirst := range []bool{false, true} {
		t.Run(map[bool]string{true: "view-first", false: "decision-first"}[viewFirst], func(t *testing.T) {
			q, _ := fixture(t)
			j := submit(t, q, spec("experiment"))
			if err := q.update(func() error {
				q.putWorkObservation("curve", WorkObservation{StaleReason: "original identity changed"})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			drainAdmissionWait(t, q)
			reason := q.s.Jobs[j.ID].Reason
			rejected := errors.New("reject mixed journals")
			if err := q.update(func() error {
				decision := func() {
					q.touchJob(j.ID).DiagnosticCode = "decision"
					q.touchCatalog("work_observations/curve")
				}
				view := func() {
					q.touchJobReason(q.s.Jobs[j.ID])
					q.s.Jobs[j.ID].Reason = "diagnostic"
					q.touchCurveLabel("curve")
					p := q.s.WorkObservations["curve"]
					p.StaleReason = "component unavailable"
					q.s.WorkObservations["curve"] = p
				}
				if viewFirst {
					view()
					decision()
				} else {
					decision()
					view()
				}
				return rejected
			}); !errors.Is(err, rejected) {
				t.Fatal(err)
			}
			if q.s.Jobs[j.ID].Reason != reason || q.s.Jobs[j.ID].DiagnosticCode != "" || q.s.WorkObservations["curve"].StaleReason != "original identity changed" {
				t.Fatal("mixed diagnostic and decision rollback lost the original")
			}
		})
	}
	t.Run("shared-host-derived-view", func(t *testing.T) {
		q, clock := fixture(t)
		m := sharedHostFixture(t, q)
		before := q.s.Machines[m.ID].SharedHostReason
		if before == "" {
			t.Fatal("missing unacknowledged-profile view")
		}
		row := durableQueueRecord(t, q.db, "machines/"+m.ID)
		removeProfile := func() {
			policy := q.s.HostMaintenance[hostID(&m)]
			policy.SharedHost = nil
			q.s.HostMaintenance[hostID(&m)] = policy
			q.refreshHostMemory()
			if q.s.Machines[m.ID].SharedHostReason != "" || len(q.records.machines) != 0 {
				t.Fatal("derived view enrolled an unchanged machine")
			}
		}
		abort := errors.New("reject policy change")
		if err := q.update(func() error { removeProfile(); return abort }); !errors.Is(err, abort) {
			t.Fatal(err)
		}
		if q.s.Machines[m.ID].SharedHostReason != before {
			t.Fatal("rollback failed to recompute shared-host view")
		}
		q = reopenQueue(t, q, clock)
		status, err := q.HostMaintenanceStatus(hostID(&m))
		if err != nil || status.Policy.SharedHost == nil || len(status.Workers) != 1 || status.Workers[0].Machine.SharedHostReason != before {
			t.Fatalf("reopen lost original policy or its recomputed view: %+v %v", status, err)
		}
		if err := q.update(func() error { removeProfile(); return nil }); err != nil {
			t.Fatal(err)
		}
		if string(row) != string(durableQueueRecord(t, q.db, "machines/"+m.ID)) {
			t.Fatal("policy view rewrote inventory")
		}
		q = reopenQueue(t, q, clock)
		status, err = q.HostMaintenanceStatus(hostID(&m))
		if err != nil || status.Policy.SharedHost != nil || len(status.Workers) != 1 || status.Workers[0].Machine.SharedHostReason != "" {
			t.Fatalf("reopen lost committed policy or its recomputed view: %+v %v", status, err)
		}
	})

}
