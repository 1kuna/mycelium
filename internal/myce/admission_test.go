package myce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func admissionCalendar(t *testing.T, q *Queue, m Machine, beneficiary string) PriorityPolicy {
	t.Helper()
	now := q.clock.Now()
	end := now.Add(time.Hour)
	p := defaultPriorityPolicy()
	p.ScheduledHolds = []ScheduledHold{{ID: "owner", Enabled: true, Beneficiary: beneficiary, Resource: ComponentExclusion{Selector: "host=" + hostID(&m)}, Reason: "owner window", Timezone: "America/New_York", Windows: []HoldWindow{{StartAt: &now, EndAt: &end}}}}
	if _, err := q.SetPriorityPolicy(q.PriorityPolicy().Revision, p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAdmissionColdPreparationKeepsUsefulDemand(t *testing.T) {
	for _, action := range []string{"pause", "cancel", "shared demand"} {
		t.Run(action, func(t *testing.T) {
			e := compositionExtension(t, false)
			q, w := prepareTestWorker(t, e)
			first := submit(t, q, Spec{Name: "first", Project: "test", Extension: extensionKey(e)})
			cold := check(t, q, w.Machine, nil)
			if len(cold.EnvironmentPlan) != 1 {
				t.Fatal("missing cold preparation", cold)
			}
			launch := cold.EnvironmentPlan[0].Launch
			if launch == nil || launch.Demand != first.ID || launch.Spec == nil || launch.Spec.Project != "test" {
				t.Fatal("lost useful identity", launch)
			}
			if action == "cancel" {
				if err := q.Cancel(first.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				changeHold(t, q, first.ID, 0, true, nil)
			}
			var second *Job
			if action == "shared demand" {
				second = submit(t, q, Spec{Name: "second", Project: "test", Extension: extensionKey(e)})
			}
			if err := q.AuthorizeLaunch(*launch); !errors.Is(err, ErrStale) {
				t.Fatal("stale useful demand authorized", err)
			}
			if second != nil {
				reply := check(t, q, w.Machine, nil)
				if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Demand != second.ID || reply.EnvironmentPlan[0].Launch.Attempt == launch.Attempt {
					t.Fatal("shared preparation borrowed stale authority", reply)
				}
				if err := q.AuthorizeLaunch(*reply.EnvironmentPlan[0].Launch); err != nil {
					t.Fatal("fresh shared demand blocked", err)
				}
			}
		})
	}
}

func TestAdmissionScheduledColdBeneficiaryPreparesAndQualifies(t *testing.T) {
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	w.Machine.Device = "0"
	w.Machine.HostMemoryMeasured, w.Machine.HostAvailableMemoryGB = true, 32
	check(t, q, w.Machine, nil)
	admissionCalendar(t, q, w.Machine, "test/*")
	demand := submit(t, q, Spec{Name: "beneficiary", Project: "test", Extension: extensionKey(e)})
	competitor := submit(t, q, Spec{Name: "peer", Project: "peer", Extension: extensionKey(e)})
	cold := check(t, q, w.Machine, nil)
	if cold.Job != nil || len(cold.EnvironmentPlan) != 1 {
		t.Fatal("beneficiary cold offer missing", cold)
	}
	launch := cold.EnvironmentPlan[0].Launch
	if launch.Demand != demand.ID {
		t.Fatal("wrong preparation beneficiary", launch)
	}
	if err := q.AuthorizeLaunch(*launch); err != nil {
		t.Fatal("beneficiary preparation rejected", err)
	}
	if err := w.prepareRegisteredExtension(context.Background(), cold.EnvironmentPlan[0]); err != nil {
		t.Fatal(err)
	}
	w.Machine.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "ready"}}
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	ready := check(t, q, w.Machine, nil)
	if len(ready.ExtensionQualificationPlan) != len(extensionWorks(e)) {
		t.Fatal("qualification offer missing", ready)
	}
	for _, plan := range ready.ExtensionQualificationPlan {
		if plan.Demand != demand.ID {
			t.Fatal("wrong qualification beneficiary", plan)
		}
		reconcileQualification(t, w, plan)
		w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
		assigned := check(t, q, w.Machine, nil).Job
		if assigned == nil || assigned.Spec.ExtensionCheck == nil || assigned.Spec.ExtensionCheck.Demand != demand.ID {
			t.Fatal("beneficiary check not admitted", plan.Report.Work)
		}
		if err := q.Started(result(assigned, false)); err != nil {
			t.Fatal("beneficiary check Started rejected", err)
		}
		q.clock.(*fakeClock).Advance(time.Second)
		finish(t, q, assigned, true)
	}
	for _, plan := range ready.ExtensionQualificationPlan {
		reconcileQualification(t, w, plan)
	}
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	got := check(t, q, w.Machine, nil).Job
	if got == nil || got.ID != demand.ID {
		t.Fatal("qualified beneficiary not admitted", got)
	}
	if stored := q.s.Jobs[competitor.ID]; stored.Attempt != 0 {
		t.Fatal("unrelated demand borrowed beneficiary", stored)
	}
}

func TestAdmissionQualificationRevalidatesBeneficiary(t *testing.T) {
	for _, stage := range []string{"submission", "launch", "Started"} {
		t.Run(stage, func(t *testing.T) {
			q, w, _, demand, _, plans := qualificationFixture(t)
			admissionCalendar(t, q, w.Machine, "test/*")
			plan := plans[0]
			if stage == "submission" {
				changeHold(t, q, demand.ID, 0, true, nil)
				if err := w.reconcileExtensionQualification(context.Background(), plan); err == nil {
					t.Fatal("paused beneficiary submitted check")
				}
				return
			}
			reconcileQualification(t, w, plan)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil {
				t.Fatal("check not assigned")
			}
			// The check's extension-check identity cannot substitute for current owner
			// authority when another window excludes its useful demand.
			admissionCalendar(t, q, w.Machine, "other/*")
			var err error
			if stage == "Started" {
				err = q.Started(result(assigned, false))
			} else {
				err = q.AuthorizeLaunch(HostLaunch{Machine: assigned.Machine, Session: assigned.Session, Kind: "batch", Key: assigned.ID, Attempt: "1"})
			}
			if !errors.Is(err, ErrStale) {
				t.Fatal("changed window authorized old beneficiary", err)
			}
		})
	}
}

func TestAdmissionStartedQualificationDrainsAcrossHolds(t *testing.T) {
	q, w, _, demand, _, plans := qualificationFixture(t)
	reconcileQualification(t, w, plans[0])
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	assigned := check(t, q, w.Machine, nil).Job
	if assigned == nil {
		t.Fatal("check not assigned")
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal(err)
	}
	changeHold(t, q, demand.ID, 0, true, nil)
	admissionCalendar(t, q, w.Machine, "other/*")
	if reply := check(t, q, w.Machine, assigned); reply.Stop || reply.Kill {
		t.Fatal("admission hold stopped Started qualification")
	}
	current := job(t, q, assigned.ID)
	if current.State != "running" || cancelled(current) || current.StopAt != nil {
		t.Fatal("admission hold cancelled Started check", current.State, current.Reason)
	}
	finish(t, q, assigned, true)
}

func TestAdmissionWeeklyCalendarReopensWithPolicy(t *testing.T) {
	q, c := fixture(t)
	m := machine("one", "fake", "host-one")
	check(t, q, m, nil)
	p := defaultPriorityPolicy()
	p.ScheduledHolds = []ScheduledHold{{ID: "local", Enabled: true, Beneficiary: "owner/*", Resource: ComponentExclusion{Selector: "host=host-one"}, Reason: "local calendar", Timezone: "America/New_York", Windows: []HoldWindow{{Weekdays: []int{int(c.Now().Weekday())}, Start: "08:00", End: "08:02"}}}}
	if _, err := q.SetPriorityPolicy(0, p); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("experiment"))
	// Both live transactions and detached status use the admitted calendar.
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("calendar did not withhold work")
	}
	q = reopenCancellationQueue(t, q, c)
	if h := holdStatus(t, q, "local"); h.State != "exclusive_after_release" {
		t.Fatal("reopened local calendar inactive", h)
	}
	if got := check(t, q, m, nil).Job; got != nil {
		t.Fatal("reopen lost calendar")
	}
	c.Advance(2 * time.Minute)
	if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("calendar end failed to release admission")
	}
}

func TestAdmissionQualificationUsesBeneficiaryClass(t *testing.T) {
	q, w, _, demand, _, plans := qualificationFixture(t)
	admissionCalendar(t, q, w.Machine, "test/"+jobClass(demand.Spec))
	reconcileQualification(t, w, plans[0])
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	assigned := check(t, q, w.Machine, nil).Job
	if assigned == nil || automaticExtensionCheck(assigned) == nil {
		t.Fatal("internal check lost beneficiary class")
	}
	if err := q.Started(result(assigned, false)); err != nil {
		t.Fatal("beneficiary class rejected at Started", err)
	}
}
