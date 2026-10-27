package myce

import (
	"testing"
	"time"
)

func TestAttemptProtocolCollectionReports(t *testing.T) {
	for _, currentFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "current-last", true: "current-first"}[currentFirst], func(t *testing.T) {
			q, clock := fixture(t)
			m := machine("collection", "fake")
			submit(t, q, spec("experiment"))
			c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
			reply, err := q.Checkin(c)
			if err != nil {
				t.Fatal(err)
			}
			j := reply.Attempts[0].Job
			neighbor := submit(t, q, spec("experiment"))
			current := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: 9}, Session: m.Session, Started: true}
			stale := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt + 1, Progress: 99}, Session: m.Session}
			unknown := AttemptReport{Execution: Execution{ID: "unknown", Attempt: 1}, Session: m.Session}
			c.Attempts = []AttemptReport{stale, unknown, current}
			if currentFirst {
				c.Attempts = []AttemptReport{current, unknown, stale}
			}
			clock.Advance(time.Second)
			reply, err = q.Checkin(c)
			if err != nil || len(reply.Attempts) != 3 {
				t.Fatalf("collection rejected/lost reports: %+v %v", reply, err)
			}
			seen := map[Execution]bool{}
			for _, a := range reply.Attempts {
				seen[a.Execution] = true
				if a.Session != m.Session || a.Job != nil {
					t.Fatalf("bad key or extra admission: %+v", a)
				}
				if a.ID == j.ID && a.Attempt == j.Attempt {
					if a.Stop || a.Kill || a.Hold {
						t.Fatalf("neighbor control reached current: %+v", a)
					}
				} else if !a.Stop || !a.Kill {
					t.Fatalf("stale report not stopped by key: %+v", a)
				}
			}
			if len(seen) != 3 {
				t.Fatalf("duplicate/lost directives: %+v", reply)
			}
			got := job(t, q, j.ID)
			if got.Progress != 9 || got.StartedAt == nil || got.State != "running" {
				t.Fatalf("current report lost: %+v", got)
			}
			requireState(t, q, neighbor.ID, "queued")
			if err = q.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			// The current key is omitted while other keys remain: still deliver its stop,
			// without treating the collection as proof that it has released custody.
			c.Attempts = []AttemptReport{unknown, stale}
			reply, err = q.Checkin(c)
			if err != nil || len(reply.Attempts) != 3 {
				t.Fatalf("omitted current stop lost: %+v %v", reply, err)
			}
			found := false
			for _, a := range reply.Attempts {
				if a.ID == j.ID && a.Attempt == j.Attempt {
					found = a.Stop
				}
			}
			if !found || job(t, q, j.ID).ReleaseConfirmed {
				t.Fatal("omission lost keyed stop or settled custody")
			}
			requireState(t, q, neighbor.ID, "queued")
		})
	}
}

func TestAttemptProtocolCollectionControls(t *testing.T) {
	key := AttemptReport{Execution: Execution{ID: "current", Attempt: 2}, Session: "session"}
	c := Checkin{Machine: Machine{ID: "worker", Session: key.Session}, Attempts: []AttemptReport{key}}
	other := AttemptDirective{Execution: Execution{ID: "neighbor", Attempt: 1}, Session: key.Session, Stop: true, Kill: true, Hold: true}
	owned := AttemptDirective{Execution: key.Execution, Session: key.Session, Stop: true, GraceSeconds: 7}
	for _, decisions := range [][]AttemptDirective{{other, owned}, {owned, other}, {other}} {
		reply, err := workerAttemptReply(c, Reply{AttemptProtocol: keyedAttemptProtocol, Attempts: decisions})
		want := len(decisions) == 2
		if err != nil || reply.Stop != want || reply.Kill || reply.Hold || want && reply.GraceSeconds != 7 {
			t.Fatalf("control collection misrouted: %+v %v", reply, err)
		}
	}
	for _, decisions := range [][]AttemptDirective{{owned, owned}, {owned, {Execution: other.Execution}}} {
		if _, err := workerAttemptReply(c, Reply{AttemptProtocol: keyedAttemptProtocol, Attempts: decisions}); err == nil {
			t.Fatal("invalid collection accepted")
		}
	}
}

func TestAttemptProtocolCollectionRecoveryAdmission(t *testing.T) {
	q, _ := fixture(t)
	m := machine("recovery", "fake")
	first := submit(t, q, spec("experiment"))
	second := submit(t, q, spec("experiment"))
	// Retained copies from earlier executions may both return in one heartbeat.
	// Recovery must obey the same one-attempt admission rule as a fresh pick.
	if err := q.update(func() error {
		for _, id := range []string{first.ID, second.ID} {
			j := q.s.Jobs[id]
			j.Attempt, j.Starts = 1, 1
			j.Copies = []Copy{{Attempt: 1, Machine: m.ID, Session: m.Session, Recoverable: true}}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{
		{Execution: Execution{ID: first.ID, Attempt: 1}, Session: m.Session},
		{Execution: Execution{ID: second.ID, Attempt: 1}, Session: m.Session},
	}}
	reply, err := q.Checkin(c)
	if err != nil || len(reply.Attempts) != 2 {
		t.Fatalf("recovery collection: %+v %v", reply, err)
	}
	requireState(t, q, first.ID, "running")
	requireState(t, q, second.ID, "queued")
	if reply.Attempts[0].Stop || !reply.Attempts[1].Stop || !reply.Attempts[1].Kill {
		t.Fatalf("recovery admitted a neighbor: %+v", reply)
	}
}

func TestAttemptProtocolCollectionValidationAtomic(t *testing.T) {
	q, _ := fixture(t)
	m := machine("atomic", "fake")
	submit(t, q, spec("experiment"))
	c := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
	reply, err := q.Checkin(c)
	if err != nil {
		t.Fatal(err)
	}
	j := reply.Attempts[0].Job
	c.Attempts = []AttemptReport{
		{Execution: Execution{ID: j.ID, Attempt: j.Attempt, Progress: 77}, Session: m.Session, Started: true},
		{Execution: Execution{ID: "bad", Attempt: 1}, Session: "wrong-session"},
	}
	if _, err = q.Checkin(c); err == nil {
		t.Fatal("invalid later entry accepted")
	}
	got := job(t, q, j.ID)
	if got.Progress != 0 || got.StartedAt != nil {
		t.Fatal("invalid collection partially mutated the first attempt")
	}
}

func TestAttemptProtocolRecoveryAdmissionSession(t *testing.T) {
	for _, state := range []string{"running", "stopping"} {
		for _, stale := range []bool{false, true} {
			t.Run(state+"/stale-session="+stringBool(stale), func(t *testing.T) {
				q, clock := fixture(t)
				m := machine("recovery-session", "fake")
				m.Devices = m3Machine().Devices
				check(t, q, m, nil)
				returning := submit(t, q, spec("experiment"))
				other := submit(t, q, spec("experiment"))
				if err := q.update(func() error {
					j := q.s.Jobs[returning.ID]
					j.Attempt, j.Starts = 1, 1
					j.Copies = []Copy{{Attempt: 1, Machine: m.ID, Session: m.Session, Recoverable: true, AllocatedDevices: []string{"0"}}}
					q.retainRelease(&Job{ID: j.ID, Spec: j.Spec, Machine: m.ID, Session: m.Session, Attempt: 1, AllocatedDevices: []string{"0"}})
					blocker := q.s.Jobs[other.ID]
					blocker.State, blocker.Attempt, blocker.Starts = state, 1, 1
					blocker.Machine, blocker.Session = m.ID, m.Session
					blocker.AllocatedDevices = []string{"0"}
					if stale {
						blocker.Session = "stale-session"
						// #180 forbids legacy same-device recovery despite a stale session.
						// Exercise session admission plus distinct-resource recovery while
						// the old blocker still owns its device and release obligation.
						blocker.AllocatedDevices = []string{"1"}
					}
					if state == "stopping" {
						stopAt := clock.Now().Add(StopGrace)
						blocker.StopAt = &stopAt
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{
					{Execution: Execution{ID: returning.ID, Attempt: 1}, Session: m.Session},
				}})
				if err != nil {
					t.Fatal(err)
				}
				var directive *AttemptDirective
				for i := range reply.Attempts {
					if reply.Attempts[i].ID == returning.ID {
						directive = &reply.Attempts[i]
					}
				}
				if directive == nil || directive.Stop != !stale || directive.Kill != !stale || directive.Hold || directive.Job != nil {
					t.Fatalf("recovery session fence: %+v", reply)
				}
				want := "queued"
				if stale {
					want = "running"
				}
				requireState(t, q, returning.ID, want)
				requireState(t, q, other.ID, state)
			})
		}
	}
}
