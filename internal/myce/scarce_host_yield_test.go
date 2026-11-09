package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func yieldCPUContract() *ResumeCompatibility {
	return &ResumeCompatibility{CheckpointFormat: "counter-v1", ResultContract: "counter-result-v1", Targets: []ResumeTarget{{Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}, Environment: "none", ResultCheck: "counter-resume-test"}}}
}
func saveYieldGeneration(t *testing.T, j *Job, g int64) {
	t.Helper()
	dir := attemptCheckpoint(*j)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":{"step":%d}}`, g, g)
	if err := os.WriteFile(filepath.Join(dir, "save.json"), []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func yieldFixture(t *testing.T, disposable bool) (*Queue, *fakeClock, Machine, Machine, *Job) {
	return yieldFixtureWithStore(t, disposable, false)
}
func yieldFixtureWithStore(t *testing.T, disposable, files bool) (*Queue, *fakeClock, Machine, Machine, *Job) {
	t.Helper()
	q, clock := fixture(t)
	if files {
		if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
			t.Fatal(err)
		}
	}
	a := m3Machine()
	a.Capabilities = append(a.Capabilities, scarceHostYieldCapability)
	a.ID = "scarce"
	a.HostID = "scarce"
	s := cpuSpec(8, 1)
	s.Resumable = !disposable
	s.Preemptible = disposable
	s.ResumeCompatibility = yieldCPUContract()
	s.SubmissionKey = "portable-counter"
	j := submit(t, q, s)
	run := assignedJob(machineCheck(t, q, a))
	if run == nil || run.ID != j.ID {
		t.Fatal("missing portable attempt", run)
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	if !disposable {
		if files {
			if err := q.publishCheckpoint(context.Background(), yieldPublication(t, q, run, 1)); err != nil {
				t.Fatal(err)
			}
		} else {
			saveYieldGeneration(t, job(t, q, j.ID), 1)
		}
		machineCheck(t, q, a, run)
	}
	b := cpuMachine("general", "general", "cpu", 8, 32)
	b.Devices = []MachineDevice{}
	b.Capabilities = append(b.Capabilities, scarceHostYieldCapability)
	machineCheck(t, q, b)
	return q, clock, a, b, run
}
func yieldDemand(t *testing.T, q *Queue) *Job {
	t.Helper()
	s := spec("experiment")
	s.Resumable = false
	s.Needs.CPUCores = 1
	s.Needs.HostMemoryGB = 1
	s.Needs.Platform = Platform{Required: []string{"fake"}}
	s.RequirementReasons = map[string]string{"backend:fake": "requires the simulated accelerator"}
	return submit(t, q, s)
}
func authorizeYield(t *testing.T, q *Queue, j *Job) {
	t.Helper()
	ok, err := q.authorizeYieldStop(context.Background(), result(j, false))
	if err != nil || !ok {
		t.Fatal("stop not authorized", ok, err, job(t, q, j.ID).Reason)
	}
}

func TestScarceHostYieldRequiresRealUniquenessAndCompatibility(t *testing.T) {
	for _, gap := range []string{"busy-peer", "stale-peer", "maintained-peer", "unavailable", "unqualified", "incompatible", "no-checkpoint", "production", "not-opted-in", "protected"} {
		t.Run(gap, func(t *testing.T) {
			q, clock, a, b, run := yieldFixture(t, false)
			switch gap {
			case "busy-peer", "stale-peer", "maintained-peer":
				peer := m3Machine()
				peer.ID = "peer"
				peer.HostID = "peer"
				machineCheck(t, q, peer)
				if gap == "busy-peer" {
					s := cpuSpec(8, 1)
					s.AllowedMachines = []string{"peer"}
					j := submit(t, q, s)
					machineCheck(t, q, peer)
					_ = j
				}
				if gap == "stale-peer" {
					clock.Advance(LossAfter)
					machineCheck(t, q, a, run)
					machineCheck(t, q, b)
				}
				if gap == "maintained-peer" {
					if err := q.Leave(peer.ID, false); err != nil {
						t.Fatal(err)
					}
				}
			case "unavailable":
				if err := q.Leave(b.ID, false); err != nil {
					t.Fatal(err)
				}
			case "unqualified":
				// A declared supported target still needs its frozen environment
				// prepared on the destination. Source evidence alone is insufficient.
				if err := q.update(func() error {
					v := q.touchJob(run.ID)
					v.Spec.Environment = Environment{Name: "counter-runtime"}
					v.EnvironmentRecipes = map[string]EnvironmentRecipe{"cpu": {Key: "counter-runtime-v1"}}
					r := *v.Spec.ResumeCompatibility
					r.Targets = append([]ResumeTarget(nil), r.Targets...)
					r.Targets[0].Environment = "counter-runtime-v1"
					v.Spec.ResumeCompatibility = &r
					q.s.Machines[a.ID].Environments = []string{"counter-runtime-v1"}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "incompatible":
				if err := q.update(func() error {
					profile := *q.s.Machines[b.ID].CapabilityProfile
					profile.Arch = "arm64"
					q.s.Machines[b.ID].CapabilityProfile = &profile
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "no-checkpoint":
				if err := q.update(func() error { q.touchJob(run.ID).YieldGeneration = 0; return nil }); err != nil {
					t.Fatal(err)
				}
			case "production":
				if err := q.update(func() error {
					v := q.touchJob(run.ID)
					v.Spec.Priority = "production"
					v.Spec.Class = "production"
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "not-opted-in":
				if err := q.update(func() error {
					v := q.touchJob(run.ID)
					v.Spec.Resumable = false
					v.Spec.Preemptible = false
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "protected":
				if err := q.update(func() error { q.touchJob(run.ID).PreemptionCount = 2; return nil }); err != nil {
					t.Fatal(err)
				}
			}
			yieldDemand(t, q)
			if got := job(t, q, run.ID); got.State != "running" {
				t.Fatal("unsafe yield", gap, got.State, got.Reason)
			}
		})
	}
}

func yieldPublication(t *testing.T, q *Queue, run *Job, generation int64) checkpointPublication {
	t.Helper()
	data := []byte(fmt.Sprintf(`{"myce_checkpoint":{"generation":%d},"value":%d}`, generation, generation))
	path := filepath.Join(t.TempDir(), "save.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q.files.blob(ref), data, 0600); err != nil {
		t.Fatal(err)
	}
	return checkpointPublication{Result: result(run, false), Checkpoint: FileCheckpoint{Generation: generation, Files: FileManifest{"save.json": ref}}}
}
