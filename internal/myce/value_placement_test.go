package myce

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func valueMachine(id, backend string, ram float64) Machine {
	m := cpuMachine(id, id, backend, 4, ram)
	m.CapabilityProfile.SystemMemoryBytes = uint64(ram * (1 << 30))
	m.TotalMemoryGB = 8
	if backend == "cpu" {
		m.CapabilityProfile.Accelerators = nil
	} else {
		m.CapabilityProfile.Accelerators = []AcceleratorProfile{{Type: backend, MemoryBytes: 8 << 30}}
	}
	return m
}
func valueFleet(t *testing.T) (*Queue, *fakeClock, Machine, Machine) {
	t.Helper()
	q, c := fixture(t)
	small, big := valueMachine("z-small", "cpu", 16), valueMachine("a-accelerator", "fake", 64)
	check(t, q, small, nil)
	check(t, q, big, nil)
	return q, c, small, big
}

func lastResortPolicy(t *testing.T, q *Queue, host string, enabled bool) {
	t.Helper()
	if _, err := q.ChangeHostMaintenance(host, HostMaintenanceRequest{Action: "placement", LastResort: &enabled}); err != nil {
		t.Fatal(err)
	}
}

func TestValueLastResortImmediateFallbackAndStatus(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, mode := range []string{"busy", "lost"} {
			t.Run(mode, func(t *testing.T) {
				q, clock, small, ordinary := valueFleet(t)
				lastResortPolicy(t, q, small.HostID, true)
				var running *Job
				if mode == "busy" {
					submit(t, q, cpuSpec(4, 1))
					running = check(t, q, ordinary, nil).Job
					if running == nil {
						t.Fatal("ordinary host did not take initial work")
					}
				} else {
					clock.Advance(LossAfter + time.Second)
				}
				wanted := submit(t, q, cpuSpec(1, 1))
				if mode == "busy" {
					snap, err := q.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					for _, j := range snap.Jobs {
						if j.ID == wanted.ID && !strings.Contains(j.Reason, "last-resort fallback") {
							t.Fatal("queued fallback reason hidden", j.Reason)
						}
					}
				}
				before := clock.Now()
				got := check(t, q, small, nil).Job
				if got == nil || got.ID != wanted.ID || !clock.Now().Equal(before) {
					t.Fatal("fallback did not start immediately", got)
				}
				if running != nil && job(t, q, running.ID).State != "running" {
					t.Fatal("fallback preempted equal-tier work")
				}
				if !strings.Contains(JobTiming(got), "last-resort fallback: no ordinary host can admit") {
					t.Fatal("missing fallback explanation", JobTiming(got))
				}
				snap, err := q.publicSnapshot()
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range snap.Machines {
					if m.ID == small.ID && !m.LastResort {
						t.Fatal("host policy hidden in status")
					}
				}
				var html bytes.Buffer
				if err := statusPage.Execute(&html, snap); err != nil || !strings.Contains(html.String(), "Last-resort placement") || !strings.Contains(html.String(), "last-resort fallback") {
					t.Fatal("policy or reason hidden in status page", err)
				}
			})
		}

	})

	t.Run("ValueLastResortCPU", func(t *testing.T) {
		q, _ := fixture(t)
		small, ordinary := valueMachine("small", "cpu", 8), valueMachine("ordinary", "metal", 24)
		check(t, q, small, nil)
		check(t, q, ordinary, nil)
		lastResortPolicy(t, q, small.HostID, true)
		wanted := submit(t, q, cpuSpec(1, 1))
		if got := check(t, q, small, nil).Job; got != nil {
			t.Fatal("last-resort host won by pulling first", got)
		}
		if got := check(t, q, ordinary, nil).Job; got == nil || got.ID != wanted.ID {
			t.Fatal("ordinary accelerator host did not get CPU work", got)
		}
		// A fresh policy pass must immediately restore the original MY-18 ordering.
		lastResortPolicy(t, q, small.HostID, false)
		wanted = submit(t, q, cpuSpec(1, 1))
		if got := check(t, q, small, nil).Job; got == nil || got.ID != wanted.ID {
			t.Fatal("clearing last-resort did not restore admission", got)
		}

	})

	t.Run("ValueLastResortFairnessAndHigherTier", func(t *testing.T) {
		t.Run("fairness", func(t *testing.T) {
			q, clock, small, ordinary := valueFleet(t)
			lastResortPolicy(t, q, small.HostID, true)
			a := cpuSpec(1, 1)
			a.Project, a.AllowedMachines = "first", []string{ordinary.ID}
			first := submit(t, q, a)
			clock.Advance(time.Millisecond)
			b := cpuSpec(1, 1)
			b.Project = "second"
			second := submit(t, q, b)
			if got := check(t, q, small, nil).Job; got == nil || got.ID != second.ID {
				t.Fatal("last-resort policy waited behind another project's fair turn", got)
			}
			if got := check(t, q, ordinary, nil).Job; got == nil || got.ID != first.ID {
				t.Fatal("policy changed ordinary host's fair winner", got)
			}
		})
		t.Run("higher tier and custody", func(t *testing.T) {
			q, _, small, ordinary := valueFleet(t)
			lastResortPolicy(t, q, small.HostID, true)
			preemptionPolicy(t, q)
			low := submit(t, q, cpuSpec(4, 1))
			run := check(t, q, ordinary, nil).Job
			if run == nil {
				t.Fatal("ordinary host did not take low tier")
			}
			highSpec := cpuSpec(4, 1)
			highSpec.Project = "urgent"
			high := submit(t, q, highSpec)
			if job(t, q, low.ID).State != "running" {
				t.Fatal("preempted ordinary host despite free fallback")
			}
			if got := check(t, q, small, nil).Job; got == nil || got.ID != high.ID {
				t.Fatal("high tier did not use immediate fallback", got)
			}
			high2 := submit(t, q, highSpec)
			if job(t, q, low.ID).State != "stopping" || priorityStop(job(t, q, low.ID)) == nil {
				t.Fatal("last-resort policy blocked higher-tier preemption")
			}
			if got := check(t, q, ordinary, run).Job; got != nil {
				t.Fatal("preemption credited capacity before exact release")
			}
			finishInterrupted(t, q, run)
			if got := check(t, q, ordinary, nil).Job; got == nil || got.ID != high2.ID {
				t.Fatal("higher tier lost after release", got)
			}
		})

	})

	t.Run("ValueFairnessAndHigherTier", func(t *testing.T) {
		q, _, small, big := valueFleet(t)
		preemptionPolicy(t, q)
		low := submit(t, q, cpuSpec(4, 1))
		run := check(t, q, small, nil).Job
		highSpec := cpuSpec(4, 1)
		highSpec.Project = "urgent"
		high := submit(t, q, highSpec)
		if job(t, q, low.ID).State != "running" {
			t.Fatal("value stopped cheap work despite free scarce host")
		}
		urgent := check(t, q, big, nil).Job
		if urgent == nil || urgent.ID != high.ID {
			t.Fatal("higher tier did not use free host", urgent)
		}
		high2 := submit(t, q, highSpec)
		if job(t, q, low.ID).State != "stopping" || priorityStop(job(t, q, low.ID)) == nil {
			t.Fatal("higher tier failed to preempt")
		}
		if got := check(t, q, small, run).Job; got != nil {
			t.Fatal("preemption credited before release")
		}
		finishInterrupted(t, q, run)
		if got := check(t, q, small, nil).Job; got == nil || got.ID != high2.ID {
			t.Fatal("higher tier lost after release", got)
		}

	})
}

func TestValueCPUAndAccelerator(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		q, _, small, big := valueFleet(t)
		cpu := submit(t, q, cpuSpec(1, 1))
		if got := check(t, q, big, nil).Job; got != nil {
			t.Fatal("accelerator won by pulling first", got.ID)
		}
		run := check(t, q, small, nil).Job
		if run == nil || run.ID != cpu.ID {
			t.Fatal("small CPU host did not win", run)
		}
		gpu := submit(t, q, spec("experiment"))
		if got := check(t, q, big, nil).Job; got == nil || got.ID != gpu.ID {
			t.Fatal("accelerator job lost its accelerator", got)
		}
		snap, err := q.publicSnapshot()
		if err != nil {
			t.Fatal(err)
		}
		var html bytes.Buffer
		if err := statusPage.Execute(&html, snap); err != nil || !strings.Contains(html.String(), "preserve scarce machines") {
			t.Fatal("ranking hidden in status", err)
		}
		if !strings.Contains(JobTiming(job(t, q, cpu.ID)), "usable RAM 16 GiB") {
			t.Fatal("missing chosen RAM/ranking")
		}

	})

	t.Run("ValueNoWaitingForUnavailableCheapHost", func(t *testing.T) {
		for _, mode := range []string{"busy", "lost", "owner", "maintenance", "recovering"} {
			t.Run(mode, func(t *testing.T) {
				q, clock, small, big := valueFleet(t)
				var running *Job
				switch mode {
				case "busy":
					submit(t, q, cpuSpec(4, 1))
					running = check(t, q, small, nil).Job
				case "lost":
					clock.Advance(LossAfter + time.Second)
				case "owner":
					small.CapabilityProfile.Availability = &Availability{Mode: "daily", Available: false, Reason: "owner returned", CheckedAt: clock.Now(), Signals: idleSignals()}
					check(t, q, small, nil)
				case "maintenance":
					if _, err := q.ChangeHostMaintenance(small.HostID, HostMaintenanceRequest{Action: "disable"}); err != nil {
						t.Fatal(err)
					}
				case "recovering":
					if _, err := q.Checkin(Checkin{Machine: small, Recovering: true}); err != nil {
						t.Fatal(err)
					}
				}
				wanted := submit(t, q, cpuSpec(1, 1))
				if mode == "busy" {
					snap, err := q.Snapshot()
					if err != nil {
						t.Fatal(err)
					}
					for _, j := range snap.Jobs {
						if j.ID == wanted.ID && !strings.Contains(j.Reason, "host "+big.HostID+" scarcity") {
							t.Fatal("status ranked an inadmissible cheap host", j.Reason)
						}
					}
				}
				got := check(t, q, big, nil).Job
				if got == nil || got.ID != wanted.ID {
					t.Fatal("waited for cheap host", got)
				}
				if running != nil && job(t, q, running.ID).State != "running" {
					t.Fatal("equal tier was preempted")
				}
			})
		}

	})

	t.Run("ValueOwnerReturnRetainsCustody", func(t *testing.T) {
		q, clock, small, big := valueFleet(t)
		s := cpuSpec(1, 1)
		s.Resumable = true
		submit(t, q, s)
		run := check(t, q, small, nil).Job
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		small.CapabilityProfile.Availability = &Availability{Mode: "daily", Available: false, Reason: "owner returned", CheckedAt: clock.Now(), Signals: idleSignals()}
		reply := check(t, q, small, run)
		if !reply.Stop || job(t, q, run.ID).State != "stopping" {
			t.Fatal("owner did not stop command", reply)
		}
		if got := check(t, q, big, nil).Job; got != nil {
			t.Fatal("retried before cleanup", got)
		}
		finishInterrupted(t, q, run)
		if got := check(t, q, big, nil).Job; got == nil || got.ID != run.ID || got.Attempt <= run.Attempt {
			t.Fatal("no immediate retry on admissible host", got)
		} else if strings.Contains(JobTiming(got), "host "+small.HostID+" scarcity") {
			t.Fatal("retry kept previous attempt ranking", JobTiming(got))
		}

	})

	t.Run("ValueRestartCustody", func(t *testing.T) {
		clock := &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}
		db, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
		q, err := Open(db, root, clock)
		if err != nil {
			t.Fatal(err)
		}
		fakeCheckpointAvailability(t, q, true)
		small, big := valueMachine("z-small", "cpu", 16), valueMachine("a-accelerator", "fake", 64)
		check(t, q, small, nil)
		check(t, q, big, nil)
		wanted := submit(t, q, cpuSpec(1, 1))
		run := check(t, q, small, nil).Job
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(db, root, clock)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		fakeCheckpointAvailability(t, reopened, true)
		if got := check(t, reopened, big, nil).Job; got != nil {
			t.Fatal("restart replayed charged attempt", got)
		}
		reply := check(t, reopened, small, run)
		current := job(t, reopened, wanted.ID)
		if reply.Stop || current.State != "running" || current.Machine != small.ID || current.Attempt != run.Attempt {
			t.Fatal("restart failed adoption", reply, current.State)
		}

	})
}

// Both cases used to recurse through candidate -> betterPlacement -> candidate.

// Round 2 steered only J1: every CPU peer independently claimed the same head,
// so the accelerator took J2 while four CPU hosts remained idle.
