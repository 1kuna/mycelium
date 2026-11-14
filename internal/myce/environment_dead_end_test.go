package myce

import (
	"strings"
	"testing"
	"time"
)

func TestEnvironmentDeadEndNoBuilder(t *testing.T) {
	q, clock := fixture(t)
	j, m := failedEnvironmentDemand(t, q, clock)
	m.EnvironmentBuilder = false
	machineCheck(t, q, m)
	clock.Advance(time.Second)
	machineCheck(t, q, m)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.State != "failed" || got.Attempt != 0 || got.Finding == nil || got.Finding.Code != deadEndWait || strings.Count(JobReason(got), "Next:") != 1 {
		t.Fatalf("missing immediate preparation finding: state=%s reason=%s finding=%+v err=%v", got.State, got.Reason, got.Finding, err)
	}
}

func TestEnvironmentDeadEndEveryBuilderFailed(t *testing.T) {
	q, clock := fixture(t)
	j, m := failedEnvironmentDemand(t, q, clock)
	failEnvironmentBuild(t, q, &m)
	clock.Advance(time.Second)
	machineCheck(t, q, m)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.State != "failed" || got.Finding == nil || !strings.Contains(got.Finding.What, "every compatible builder") {
		t.Fatalf("deterministic failure left dead wait: state=%s reason=%s err=%v", got.State, got.Reason, err)
	}
}

func TestEnvironmentDeadEndUnknownAndOfflineCapacity(t *testing.T) {
	for _, mode := range []string{"empty", "offline", "builder"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			j, m := failedEnvironmentDemand(t, q, clock)
			if mode != "empty" {
				machineCheck(t, q, m)
				if mode == "offline" {
					if err := q.update(func() error { q.touchMachine(m.ID).Gone = true; return nil }); err != nil {
						t.Fatal(err)
					}
				}
			}
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if got := job(t, q, j.ID); got.State != "queued" {
				t.Fatal(mode, got.State, got.Reason)
			}
		})
	}
}

func TestEnvironmentDeadEndKeepsCompatiblePeer(t *testing.T) {
	q, clock := fixture(t)
	j, a := failedEnvironmentDemand(t, q, clock)
	b := a
	b.ID, b.HostID, b.Session = "peer", "peer", "peer-session"
	machineCheck(t, q, b)
	failEnvironmentBuild(t, q, &a)
	clock.Advance(time.Second)
	machineCheck(t, q, a)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, j.ID); got.State != "queued" {
		t.Fatal("failed host hid capable peer", got.State, got.Reason)
	}
}
