package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func failEnvironmentBuild(t *testing.T, q *Queue, m *Machine) EnvironmentRecipe {
	t.Helper()
	reply := machineCheck(t, q, *m)
	if len(reply.EnvironmentPlan) != 1 {
		t.Fatalf("missing build: %+v", reply.EnvironmentPlan)
	}
	r := reply.EnvironmentPlan[0]
	if err := q.AuthorizeLaunch(*r.Launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: r.Key, Attempt: r.Launch.Attempt, State: "failed", FailureKind: "deterministic", Error: "No matching distribution", Log: "compiler diagnostic"}}
	if got := machineCheck(t, q, *m); len(got.EnvironmentPlan) != 0 || assignedJob(got) != nil {
		t.Fatal("failed worker reused", got)
	}
	return r
}

func TestEnvironmentExclusionReroutesAndStartsOnPeer(t *testing.T) {
	q, clock := fixture(t)
	j, a := failedEnvironmentDemand(t, q, clock)
	r := failEnvironmentBuild(t, q, &a)
	if got := job(t, q, j.ID); got.State != "queued" || got.FinishedAt != nil {
		t.Fatal(got.State)
	}
	b := a
	b.ID, b.HostID, b.Session, b.EnvironmentReports = "peer", "peer", "peer-session", nil
	reply := machineCheck(t, q, b)
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("peer not offered build", reply)
	}
	p := reply.EnvironmentPlan[0]
	if err := q.AuthorizeLaunch(*p.Launch); err != nil {
		t.Fatal(err)
	}
	b.Environments = []string{r.Key}
	b.EnvironmentReports = []EnvironmentReport{{Key: r.Key, Attempt: p.Launch.Attempt, State: "ready"}}
	running := assignedJob(machineCheck(t, q, b))
	if running == nil || running.ID != j.ID || running.Machine != b.ID {
		t.Fatal("job did not start on peer", running)
	}
	finish(t, q, running, true)
	requireState(t, q, j.ID, "succeeded")
}

func TestEnvironmentExclusionAllFailedUntilCancellationOrDeadline(t *testing.T) {
	for _, end := range []string{"cancel", "deadline"} {
		t.Run(end, func(t *testing.T) {
			q, clock := fixture(t)
			j, a := failedEnvironmentDemand(t, q, clock)
			if err := q.update(func() error {
				q.touchJob(j.ID)
				due := clock.Now().Add(time.Hour)
				q.s.Jobs[j.ID].Spec.GiveUpAt = &due
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			failEnvironmentBuild(t, q, &a)
			b := a
			b.ID, b.HostID, b.Session, b.EnvironmentReports = "peer", "peer", "peer-session", nil
			failEnvironmentBuild(t, q, &b)
			// An undersized fresh worker is not a compatible unfailed candidate.
			tiny := cpuMachine("tiny", "tiny", "cpu", 1, 32)
			tiny.CPUReserveCores, tiny.EnvironmentBuilder = 1, true
			machineCheck(t, q, tiny)
			// An offline, never-built worker is neither positive nor negative evidence.
			offline := b
			offline.ID, offline.HostID, offline.EnvironmentReports = "offline", "offline", nil
			if err := q.update(func() error {
				q.touchMachine(offline.ID)
				offline.Gone = true
				q.s.Machines[offline.ID] = &offline
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(10 * time.Minute)
			machineCheck(t, q, a)
			machineCheck(t, q, b)
			got, err := q.jobSnapshot(j.ID)
			if err != nil || got.State != "queued" || got.FinishedAt != nil || got.Hold == nil || got.Hold.Kind != "no_buildable_worker" || !strings.Contains(got.Reason, "no-buildable-worker") {
				t.Fatalf("state=%s reason=%s hold=%+v err=%v", got.State, got.Reason, got.Hold, err)
			}
			if end == "cancel" {
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
				requireState(t, q, j.ID, "cancelled")
			} else {
				clock.Advance(time.Hour)
				machineCheck(t, q, a)
				requireState(t, q, j.ID, "failed")
				if !strings.Contains(job(t, q, j.ID).Reason, "give-up time reached") {
					t.Fatal(job(t, q, j.ID).Reason)
				}
			}
		})
	}
}

func TestEnvironmentExclusionRestartAndRecovery(t *testing.T) {
	for _, repair := range []string{"retry", "recipe", "fingerprint", "host", "target", "session"} {
		t.Run(repair, func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
			db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
			q, err := Open(db, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { q.Close() }()
			j, a := failedEnvironmentDemand(t, q, clock)
			r := failEnvironmentBuild(t, q, &a)
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err = Open(db, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			// Omitted receipts do not erase durable failure evidence.
			a.EnvironmentReports = nil
			if reply := machineCheck(t, q, a); len(reply.EnvironmentPlan) != 0 {
				t.Fatal("restart forgot exclusion", reply)
			}
			target := machineTarget(cpuMachineView(&a))
			switch repair {
			case "retry":
				server := httptest.NewServer((API{Queue: q, Token: "fixture"}).Handler())
				defer server.Close()
				client := Client{URL: server.URL, Token: "fixture"}
				request := map[string]any{"worker": a.ID, "target": target}
				if err = client.Call(context.Background(), "POST", "/api/environments/"+r.Key+"/retry", request, nil); err != nil {
					t.Fatal(err)
				}
				// Retry intent also survives restart and a delayed old failure receipt.
				q.Close()
				q, err = Open(db, root, clock)
				if err != nil {
					t.Fatal(err)
				}
				a.EnvironmentReports = []EnvironmentReport{{Key: r.Key, Attempt: r.Launch.Attempt, State: "failed", FailureKind: "deterministic"}}
			case "recipe":
				s := j.Spec
				s.Environment.Files = map[string][]byte{"requirements.txt": []byte("example==2.0\n")}
				j = submit(t, q, s)
			case "fingerprint":
				a.CPUExtensionFingerprint = "changed-toolchain"
			case "target":
				a.CapabilityProfile.OS, a.CapabilityProfile.Arch = "darwin", "arm64"
			case "host":
				a.ID, a.HostID = "replacement-worker", "replacement-host"
			case "session":
				// A process session is not a changed host identity.
				if err = q.update(func() error { q.touchMachine(a.ID).Gone = true; return nil }); err != nil {
					t.Fatal(err)
				}
				a.Session = "new-session"
			}
			reply := machineCheck(t, q, a)
			if repair == "session" {
				if len(reply.EnvironmentPlan) != 0 {
					t.Fatal("process restart cleared exclusion")
				}
				return
			}
			if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Attempt == r.Launch.Attempt {
				t.Fatal("repair did not offer fresh build", reply)
			}
			if repair == "recipe" && reply.EnvironmentPlan[0].Key == r.Key {
				t.Fatal("recipe identity unchanged")
			}
			p := reply.EnvironmentPlan[0]
			if err = q.AuthorizeLaunch(*p.Launch); err != nil {
				t.Fatal(err)
			}
			a.Environments = []string{p.Key}
			a.EnvironmentReports = []EnvironmentReport{{Key: p.Key, Attempt: p.Launch.Attempt, State: "ready"}}
			running := assignedJob(machineCheck(t, q, a))
			if running == nil || running.ID != j.ID {
				t.Fatal("repaired job did not start", running)
			}
		})
	}
}

func TestEnvironmentExclusionTemporaryHoldsAndStatus(t *testing.T) {
	for _, failure := range []string{"connection reset by peer", "environment cache needs 100 bytes; limit 50 (in-use entries protected)", "environment build disk space below floor", "paused"} {
		t.Run(failure, func(t *testing.T) {
			q, clock := fixture(t)
			j, m := failedEnvironmentDemand(t, q, clock)
			r := machineCheck(t, q, m).EnvironmentPlan[0]
			if err := q.AuthorizeLaunch(*r.Launch); err != nil {
				t.Fatal(err)
			}
			report := EnvironmentReport{Key: r.Key, Attempt: r.Launch.Attempt, State: "failed", Error: failure}
			if failure == "paused" {
				report.State = "paused"
			}
			m.EnvironmentReports = []EnvironmentReport{report}
			machineCheck(t, q, m)
			got := job(t, q, j.ID)
			if got.State != "queued" || strings.Contains(got.Reason, "no-buildable-worker") {
				t.Fatal(got.State, got.Reason)
			}
			clock.Advance(30 * time.Second)
			if reply := machineCheck(t, q, m); len(reply.EnvironmentPlan) != 1 {
				t.Fatal("hold never resumed", reply)
			}
		})
	}
	q, clock := fixture(t)
	_, m := failedEnvironmentDemand(t, q, clock)
	r := failEnvironmentBuild(t, q, &m)
	server := httptest.NewServer((API{Queue: q, Token: "fixture"}).Handler())
	defer server.Close()
	req, _ := http.NewRequest("GET", server.URL+"/api/status?sections=machines", nil)
	req.Header.Set("Authorization", "Bearer fixture")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var status Snapshot
	if err = json.NewDecoder(res.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	var report *EnvironmentReport
	for _, w := range status.Machines {
		if w.ID == m.ID {
			for i := range w.EnvironmentReports {
				if w.EnvironmentReports[i].Key == r.Key {
					report = &w.EnvironmentReports[i]
				}
			}
		}
	}
	if report == nil || report.Target == nil || report.Target.Platform != "cpu" || report.Error == "" || report.LogReference != "myce logs "+r.Launch.Demand {
		t.Fatalf("status omitted evidence: %+v", report)
	}
}

func TestEnvironmentExclusionAuthorizedRuntimeRetry(t *testing.T) {
	q, clock := fixture(t)
	_, m := failedEnvironmentDemand(t, q, clock)
	r := failEnvironmentBuild(t, q, &m)
	target := machineTarget(cpuMachineView(&m))
	if err := q.RetryEnvironment(m.ID, r.Key, target); err != nil {
		t.Fatal(err)
	}
	retry := machineCheck(t, q, m).EnvironmentPlan[0]
	rt := &environmentRuntime{dir: t.TempDir(), cache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}, reports: map[string]EnvironmentReport{r.Key: m.EnvironmentReports[0]}}
	rt.authorize = func(_ context.Context, l HostLaunch) error { return q.AuthorizeLaunch(l) }
	calls := 0
	rt.prepare = func(context.Context, EnvironmentRecipe) error { calls++; return nil }
	retry.Preparation = &ExtensionSelection{}
	rt.sync([]EnvironmentRecipe{retry}, false)
	<-rt.done
	if calls != 1 || rt.reports[r.Key].State != "ready" || rt.reports[r.Key].Attempt != retry.Launch.Attempt {
		t.Fatal("retry not executed", calls, rt.reports)
	}
	rt.sync([]EnvironmentRecipe{retry}, false)
	if calls != 1 {
		t.Fatal("completed retry reexecuted")
	}
}
