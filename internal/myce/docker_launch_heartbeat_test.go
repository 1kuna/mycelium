package myce

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// A heartbeat can publish a pending operation before its daemon receipt arrives.
// The next operation must authorize against that lagging, healthy report. The
// fake durable Docker store and command seam force this ordering without sleeps.
func TestDockerLaunchHeartbeatInterleaving(t *testing.T) {
	for _, operation := range []string{"network-create", "create", "start"} {
		for _, inventory := range []string{"legacy", "machine"} {
			t.Run(inventory+"/"+operation, func(t *testing.T) {
				q, clock := fixture(t)
				m := dockerMachine("worker", "fake")
				if inventory == "machine" {
					m = m3Machine()
					m.Capabilities = []string{"docker"}
				}
				submit(t, q, dockerJob().Spec)
				var j *Job
				if inventory == "machine" {
					j = assignedJob(machineCheck(t, q, m))
				} else {
					j = check(t, q, m, nil).Job
				}
				if j == nil {
					t.Fatal("missing assignment")
				}
				launch := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)}
				var saved dockerExecution
				dispatches := 0
				d := &dockerAttempt{clock: clock, dir: "fake-store", timeout: time.Second,
					persist: func(s dockerExecution) error { saved = s; return nil },
					admit:   func() error { return q.AuthorizeLaunch(launch) },
				}
				d.command = func(context.Context, ...string) ([]byte, error) {
					dispatches++
					if dispatches == 1 {
						if !saved.Pending || saved.Operation != operation {
							t.Fatal("operation was not durable before dispatch", saved)
						}
						clock.Advance(100 * time.Millisecond)
						m.DockerCleanup = d.status()
						c := Checkin{Machine: m}
						execution := Execution{ID: j.ID, Attempt: j.Attempt}
						if inventory == "machine" {
							c.AttemptProtocol = keyedAttemptProtocol
							c.Attempts = []AttemptReport{{Execution: execution, Session: j.Session}}
						} else {
							c.Running = &execution
						}
						if _, err := q.Checkin(c); err != nil {
							t.Fatal(err)
						}
					}
					return []byte("receipt"), nil
				}
				if _, err := d.operation(operation, operation); err != nil {
					t.Fatal("first operation", err)
				}
				before := cloneMachine(q.s.Machines[m.ID])
				if _, err := d.operation("create", "create"); err != nil {
					t.Fatalf("live attempt refused after %s heartbeat: %v", operation, err)
				}
				if !reflect.DeepEqual(before, cloneMachine(q.s.Machines[m.ID])) {
					t.Fatal("authorization rewrote heartbeat diagnostics")
				}
				if err := q.Started(result(j, true)); err != nil {
					t.Fatalf("start acknowledgement refused after healthy heartbeat: %v", err)
				}
				current := job(t, q, j.ID)
				if dispatches != 2 || current.Attempt != 1 || current.InfrastructureRetries != 0 || current.Losses != 0 {
					t.Fatal("healthy launch consumed a retry", current)
				}
			})
		}
	}
}

func TestDockerLaunchHeartbeatRetainsFences(t *testing.T) {
	for _, fence := range []string{"cleanup", "stopping", "unknown", "sibling", "old-copy", "attempt", "session", "cancelled", "maintenance"} {
		t.Run(fence, func(t *testing.T) {
			q, _ := fixture(t)
			m := m3Machine()
			m.Capabilities = []string{"docker"}
			submit(t, q, dockerJob().Spec)
			j := assignedJob(machineCheck(t, q, m))
			if j == nil {
				t.Fatal("missing assignment")
			}
			owner := q.s.Machines[m.ID]
			owner.DockerCleanup = "start pending"
			launch := HostLaunch{Machine: j.Machine, Session: j.Session, Kind: "batch", Key: j.ID, Attempt: strconv.Itoa(j.Attempt)}
			switch fence {
			case "cleanup":
				owner.DockerCleanup = "cleanup unconfirmed: inspect"
			case "stopping":
				owner.DockerCleanup = "stop pending"
			case "unknown":
				owner.DockerCleanup = "unknown operation"
			case "sibling":
				sibling := dockerMachine("sibling", "fake")
				sibling.HostID = hostID(owner)
				sibling.DockerCleanup = "start pending"
				q.s.Machines[sibling.ID] = &sibling
			case "old-copy":
				r := HostRelease{Host: hostID(owner), Machine: j.Machine, Session: "old-session", Job: j.ID, Attempt: 1, Docker: true}
				q.s.HostReleases[releaseKey(r)] = r
			case "attempt":
				launch.Attempt = "2"
			case "session":
				launch.Session = "old-session"
			case "cancelled":
				if err := q.Cancel(j.ID); err != nil {
					t.Fatal(err)
				}
			case "maintenance":
				setMaintenance(t, q, hostID(owner), "drain")
			}
			if err := q.AuthorizeLaunch(launch); !errors.Is(err, ErrStale) {
				t.Fatalf("%s fence bypassed: %v", fence, err)
			}
			attempt, _ := strconv.Atoi(launch.Attempt)
			if err := q.Started(Result{Machine: launch.Machine, Session: launch.Session, Execution: Execution{ID: launch.Key, Attempt: attempt}}); !errors.Is(err, ErrStale) {
				t.Fatalf("%s start acknowledgement fence bypassed: %v", fence, err)
			}
		})
	}
}
