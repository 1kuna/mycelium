package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func qualificationFixture(t *testing.T) (*Queue, Worker, Extension, *Job, EnvironmentRecipe, []ExtensionQualificationPlan) {
	t.Helper()
	e := compositionExtension(t, false)
	q, w := prepareTestWorker(t, e)
	w.Machine.Device = "0"
	w.Machine.HostMemoryMeasured, w.Machine.HostAvailableMemoryGB = true, 32
	demand := submit(t, q, Spec{Name: "demand", Project: "test", Extension: extensionKey(e)})
	cold := check(t, q, w.Machine, nil)
	if cold.Job != nil || len(cold.ExtensionQualificationPlan) != 0 || len(cold.EnvironmentPlan) != 1 {
		t.Fatal("cold demand bypassed preparation", cold)
	}
	prepare := cold.EnvironmentPlan[0]
	if err := w.prepareRegisteredExtension(context.Background(), prepare); err != nil {
		t.Fatal(err)
	}
	w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
	ready := check(t, q, w.Machine, nil)
	if ready.Job != nil || len(ready.ExtensionQualificationPlan) != len(extensionWorks(e)) {
		t.Fatal("missing work-type qualification offers", ready)
	}
	return q, w, e, demand, prepare, ready.ExtensionQualificationPlan
}

func qualificationJob(t *testing.T, q *Queue, key string) *Job {
	t.Helper()
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range snapshot.Jobs {
		if j.Spec.SubmissionKey == key {
			return j
		}
	}
	t.Fatal("missing internal check", key)
	return nil
}

func reconcileQualification(t *testing.T, w Worker, plan ExtensionQualificationPlan) {
	t.Helper()
	if err := w.reconcileExtensionQualification(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
}

func TestExtensionFirstDemandQualification(t *testing.T) {
	t.Run("cold adoption accepted comparison and warm evidence", func(t *testing.T) {
		q, w, e, demand, prepare, plans := qualificationFixture(t)
		for _, plan := range plans {
			reconcileQualification(t, w, plan)
			first := qualificationJob(t, q, plan.Report.CheckID)
			// Repeated demand and preparation retain the persisted invocation.
			reconcileQualification(t, w, plan)
			if err := w.prepareRegisteredExtension(context.Background(), prepare); err != nil {
				t.Fatal(err)
			}
			if replay := qualificationJob(t, q, plan.Report.CheckID); replay.ID != first.ID {
				t.Fatal("repeated offer created another invocation")
			}
			w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil || assigned.ID != first.ID || assigned.Spec.ExtensionCheck == nil {
				t.Fatalf("exact check not assigned: assignment=%+v check=%+v machine=%+v", assigned, qualificationJob(t, q, plan.Report.CheckID), q.s.Machines[w.Machine.ID])
			}
			if err := q.Started(result(assigned, false)); err != nil {
				t.Fatal(err)
			}
			scratch := assigned.Spec.ExtensionCheck.Directory
			if err := os.WriteFile(filepath.Join(scratch, "actual"), []byte("[99]"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := w.validateExtensionCheckOutput(*assigned); err == nil {
				t.Fatal("ordinary runner comparison accepted incorrect output")
			}
			if err := os.WriteFile(filepath.Join(scratch, "actual"), e.Files["baseline/output"], 0600); err != nil {
				t.Fatal(err)
			}
			if err := w.validateExtensionCheckOutput(*assigned); err != nil {
				t.Fatal(err)
			}
			q.clock.(*fakeClock).Advance(time.Second)
			finish(t, q, assigned, true)
		}
		for _, plan := range plans {
			reconcileQualification(t, w, plan)
			scratch := filepath.Join(extensionCache(w.ExtensionRoot, e, "fake"), plan.Report.CheckID)
			if _, err := os.Stat(scratch); !os.IsNotExist(err) {
				t.Fatal("released scratch retained", err)
			}
		}
		w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
		if qualifiedExtension(demand, &w.Machine) == nil {
			t.Fatal("accepted check did not qualify useful work")
		}
		warm := check(t, q, w.Machine, nil)
		if warm.Job == nil || warm.Job.ID != demand.ID || len(warm.ExtensionQualificationPlan) != 0 {
			t.Fatal("warm demand failed to reuse evidence", warm)
		}
		finish(t, q, warm.Job, true)
		submit(t, q, Spec{Name: "warm-repeat", Project: "test", Extension: extensionKey(e)})
		if repeat := check(t, q, w.Machine, nil); repeat.Job == nil || repeat.Job.Spec.ExtensionCheck != nil || len(repeat.ExtensionQualificationPlan) != 0 {
			t.Fatal("warm repeat scheduled another native check", repeat)
		}
		changed := w.Machine
		changed.ExtensionFingerprint = "new-device-fingerprint"
		if qualifiedExtension(demand, &changed) != nil {
			t.Fatal("warm evidence survived a changed device identity")
		}
	})

	t.Run("reconnect and persisted queue adopt pending check", func(t *testing.T) {
		q, w, _, _, _, plans := qualificationFixture(t)
		plan := plans[0]
		reconcileQualification(t, w, plan)
		first := qualificationJob(t, q, plan.Report.CheckID)
		previous := w.Machine.Session
		w.Machine.Session = "reconnected"
		w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
		if _, err := q.Checkin(Checkin{Machine: w.Machine, Fresh: true, PreviousSession: previous}); err != nil {
			t.Fatal(err)
		}
		reconcileQualification(t, w, plan)
		if replay := qualificationJob(t, q, plan.Report.CheckID); replay.ID != first.ID {
			t.Fatal("reconnect duplicated check")
		}
		var marker []byte
		if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err != nil {
			t.Fatal(err)
		}
		data, err := readQueueState(q.db, marker)
		if err != nil {
			t.Fatal(err)
		}
		var restored state
		if err := json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
		record := restored.Submissions[submissionDigest("extension-check", plan.Report.CheckID)]
		if record == nil || record.JobID != first.ID || automaticExtensionCheck(restored.Jobs[first.ID]) == nil {
			t.Fatal("SQLite lost invocation identity")
		}
	})

	for _, outcome := range []string{"failed", "cancelled", "stale", "maintenance", "demand cancelled", "identity changed"} {
		t.Run(outcome, func(t *testing.T) {
			q, w, e, demand, _, plans := qualificationFixture(t)
			plan := plans[0]
			reconcileQualification(t, w, plan)
			w.Machine.ExtensionReports = LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
			assigned := check(t, q, w.Machine, nil).Job
			if assigned == nil || assigned.Spec.ExtensionCheck == nil {
				t.Fatalf("check not admitted: check=%+v machine=%+v", qualificationJob(t, q, plan.Report.CheckID), q.s.Machines[w.Machine.ID])
			}
			clock := q.clock.(*fakeClock)
			clock.Advance(time.Second)
			switch outcome {
			case "failed":
				finish(t, q, assigned, false)
			case "cancelled":
				if err := q.Cancel(assigned.ID); err != nil {
					t.Fatal(err)
				}
				finish(t, q, assigned, true)
			case "stale":
				stale := result(assigned, true)
				stale.Attempt++
				if err := q.Complete(stale); !errors.Is(err, ErrStale) {
					t.Fatal("stale completion accepted", err)
				}
			case "maintenance":
				if _, err := q.ChangeHostMaintenance(hostID(&w.Machine), HostMaintenanceRequest{Action: "disable", Stop: true}); err != nil {
					t.Fatal(err)
				}
				if err := q.AuthorizeLaunch(HostLaunch{Machine: assigned.Machine, Session: assigned.Session, Kind: "batch", Key: assigned.ID, Attempt: "1"}); !errors.Is(err, ErrStale) {
					t.Fatal("maintenance allowed check launch", err)
				}
				finish(t, q, assigned, true)
			case "demand cancelled":
				if err := q.Cancel(demand.ID); err != nil {
					t.Fatal(err)
				}
				if !cancelled(q.s.Jobs[assigned.ID]) {
					t.Fatal("demand cancellation did not fence its in-flight check")
				}
				finish(t, q, assigned, true)
			case "identity changed":
				w.Machine.ExtensionFingerprint = "changed-native-identity"
				check(t, q, w.Machine, assigned)
				finish(t, q, assigned, true)
			}
			// Even a forged success report cannot qualify without an accepted
			// successful invocation for this exact native identity.
			forged := plan.Report
			forged.Qualified, forged.CheckState = true, "succeeded"
			incoming := w.Machine
			incoming.ExtensionReports = []ExtensionReport{forged}
			check(t, q, incoming, assigned)
			if qualifiedExtension(demand, q.s.Machines[incoming.ID]) != nil {
				t.Fatal("unsuccessful or obsolete check granted qualification")
			}
			if outcome == "failed" || outcome == "cancelled" {
				reconcileQualification(t, w, plan)
				before := qualificationJob(t, q, plan.Report.CheckID)
				reconcileQualification(t, w, plan)
				if after := qualificationJob(t, q, plan.Report.CheckID); after.ID != before.ID || after.Attempt != before.Attempt || !terminal(after.State) {
					t.Fatal("negative cache retried failed check")
				}
				local := LocalExtensionReports(w.ExtensionRoot, "fake", w.extensionFingerprint, &w.Machine)
				for _, report := range local {
					if report.CheckID == plan.Report.CheckID && (report.Qualified || !terminal(report.CheckState)) {
						t.Fatal("failure was not visible", report)
					}
				}
				w.Machine.ExtensionReports = local
				for _, offered := range check(t, q, w.Machine, nil).ExtensionQualificationPlan {
					if offered.Report.CheckID == plan.Report.CheckID {
						t.Fatal("terminal failure was offered for automatic retry")
					}
				}
			}
			if outcome == "identity changed" {
				fresh := newExtensionReport(e, &w.Machine, plan.Report.Work)
				if automaticExtensionCheckID(w.Machine.ID, fresh) == plan.Report.CheckID {
					t.Fatal("changed identity reused old check key")
				}
			}
		})
	}
}

func TestExtensionQualificationMixedBacklog(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
	m := machine("worker", "fake")
	m.ExtensionFingerprint = "synthetic-native-identity"
	m.LastSeen, m.DiskReceivedAt = clock.Now(), clock.Now()
	m.DiskVolumes = []DiskVolume{{ID: "local", Roots: []string{"data"}, TotalBytes: 1 << 30, FreeBytes: 512 << 20, SampledAt: clock.Now()}}
	e := fakeExtension(t)
	other := fakeExtension(t)
	other.Files["extension.yaml"] = []byte(strings.ReplaceAll(strings.ReplaceAll(string(other.Files["extension.yaml"]), "version: '1'", "version: '2'"), "resumable: true", "resumable: false"))
	var err error
	other, err = extensionFromFiles(other.Files)
	if err != nil {
		t.Fatal(err)
	}
	for _, extension := range []Extension{e, other} {
		for _, work := range extensionWorks(extension) {
			r := newExtensionReport(extension, &m, work)
			r.Setup = true
			m.ExtensionReports = append(m.ExtensionReports, r)
		}
	}
	job := func(id string, extensions ...Extension) *Job {
		return &Job{ID: id, State: "queued", Spec: Spec{Project: "test", Kind: "sample"}, Extensions: extensions}
	}
	host, disk, device := job("host", e), job("disk", e), job("device", e)
	host.Spec.AllowedMachines = []string{"other-worker"}
	disk.Spec.Disk = &DiskNeeds{InputBytes: 1 << 30}
	device.Spec.Needs.GPU = &GPUSelection{Devices: []string{"other-device"}}
	resume := job("resume", other)
	resume.Spec.Resumable = true
	useful := job("useful", other)
	plain := &Job{ID: "plain", State: "queued", Spec: spec("experiment")}
	q := &Queue{clock: clock, s: state{Jobs: map[string]*Job{}, Extensions: map[string]Extension{extensionKey(e): e, extensionKey(other): other}, Machines: map[string]*Machine{m.ID: &m}}}
	for _, j := range []*Job{host, disk, device, resume, useful, plain} {
		q.s.Jobs[j.ID] = j
	}
	assertOffers := func(want ...Extension) {
		t.Helper()
		expected := map[string]bool{}
		for _, extension := range want {
			for _, work := range extensionWorks(extension) {
				expected[extension.Digest+"/"+work] = true
			}
		}
		for _, plan := range q.extensionQualificationPlan(&m) {
			key := plan.Report.Digest + "/" + plan.Report.Work
			if !expected[key] || plan.Machine != m.ID {
				t.Fatalf("unexpected qualification offer: %+v", plan)
			}
			delete(expected, key)
		}
		if len(expected) != 0 {
			t.Fatal("missing qualification offers", expected)
		}
	}
	// Ineligible jobs must not lend their eligibility to another frozen digest.
	assertOffers(other)
	host.Spec.AllowedMachines = []string{m.ID}
	assertOffers(e, other)
	useful.State = "cancelled"
	assertOffers(e) // The resumable job cannot keep the non-resumable check alive.
	for i := range m.ExtensionReports {
		m.ExtensionReports[i].Qualified = true
	}
	assertOffers() // A new pass immediately observes newly accepted qualification.
}

// #459's post-restart queue had 149 registered extensions and 194 queued
// plain commands, each with three shared inputs (608, 637 and 4063 bytes).
// Qualification demand used to rehash those inputs for every catalogue entry
// on every target, despite none of these commands using an extension. Keep
// this fixture synthetic: no deployment identities, paths or payloads.
func BenchmarkExtensionQualificationPlainBacklog(b *testing.B) {
	clock := &fakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
	m := machine("worker", "fake")
	m.LastSeen, m.DiskReceivedAt = clock.Now(), clock.Now()
	m.DiskVolumes = []DiskVolume{{ID: "local", Roots: []string{"data", "file-cache", "checkpoint"}, TotalBytes: 1 << 40, FreeBytes: 1 << 39, SampledAt: clock.Now()}}
	for i := range 48 {
		m.CachedSharedInputs = append(m.CachedSharedInputs, fmt.Sprintf("%064x", i))
	}
	q := &Queue{clock: clock, s: state{Jobs: map[string]*Job{}, Extensions: map[string]Extension{}, Machines: map[string]*Machine{m.ID: &m}}}
	for i := range 149 {
		id := fmt.Sprint(i)
		q.s.Extensions[id] = Extension{Name: "extension-" + id, Version: "1", Digest: fmt.Sprintf("%064x", i), Resumable: true}
	}
	for i := range 194 {
		id := fmt.Sprint(i)
		s := spec("experiment")
		s.SharedInputs = map[string]SharedInput{}
		for k, size := range []int64{608, 637, 4063} {
			s.SharedInputs[fmt.Sprint(k)] = SharedInput{SHA256: strings.Repeat(fmt.Sprint(k+1), 64), SizeBytes: size}
		}
		q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
	}
	b.ReportAllocs()
	for b.Loop() {
		if plans := q.extensionQualificationPlan(&m); len(plans) != 0 {
			b.Fatal("plain commands manufactured qualification demand", plans)
		}
	}
}
