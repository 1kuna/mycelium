package myce

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWorkerRootsMissingMountAndRecovery(t *testing.T) {
	nas := t.TempDir() // The mountpoint survives, so existence/statfs is not proof.
	mounted := false
	inventoryError := false
	w := Worker{ServiceConfig: &ServiceConfig{NAS: nas}, JobFileRoot: nas, ModelRoot: filepath.Join(nas, "models"), EnvironmentSourceRoot: filepath.Join(nas, "env"), EnvironmentRoot: t.TempDir(), MountRoots: func(context.Context) ([]string, error) {
		if inventoryError {
			return nil, errors.New("inventory failed")
		}
		if mounted {
			return []string{nas}, nil
		}
		return nil, nil
	}}
	checkpoint := filepath.Join(nas, "checkpoints")
	for _, step := range []struct{ mounted, bad bool }{{false, false}, {true, false}, {false, false}, {true, true}, {true, false}} {
		mounted, inventoryError = step.mounted, step.bad
		missing := w.sampleStorageRoots(context.Background(), checkpoint)
		if step.mounted && !step.bad {
			if len(missing) != 0 {
				t.Fatal("mount recovery not recognized", missing)
			}
			continue
		}
		for _, role := range []string{"job-files", "checkpoint", "model", "environment-source"} {
			if !slices.ContainsFunc(missing, func(r UnavailableRoot) bool { return r.Root == role && strings.Contains(r.Reason, "NAS mount") }) {
				t.Fatal("missing affected root", role, missing)
			}
		}
		if slices.ContainsFunc(missing, func(r UnavailableRoot) bool { return r.Root == "environment" }) {
			t.Fatal("local cache marked unavailable")
		}
	}
}

func TestWorkerRootsProductionAdmissionAndExactCustody(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "worker", "cpu", 4, 16)
	s := cpuSpec(1, 1)
	s.Priority = "production"
	s.SharedInputs = map[string]SharedInput{"asset": {NAS: "assets/input.bin", SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	j := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatal("missing initial assignment")
	}
	clock.Advance(time.Second)
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	check(t, q, m, assigned)
	if reason := q.startPlacementRejection(q.s.Jobs[j.ID]); reason == "" {
		t.Fatal("mount loss did not fence launch")
	}
	// This observation changes launch permission, never exact-attempt custody.
	if got := job(t, q, j.ID); got.Machine != assigned.Machine || got.Attempt != assigned.Attempt {
		t.Fatal("mount loss changed custody")
	}
	// A prelaunch refusal requeues with the same fence and does not spend a retry.
	r := result(assigned, false)
	r.Interrupted = true
	r.ExecutionCause = &RuntimeCause{Operation: "start_authorization", Class: "admission_changed"}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if check(t, q, m, nil).Job != nil {
		t.Fatal("NAS production job bypassed root admission")
	}
	if got := job(t, q, j.ID); !strings.Contains(got.Reason, "checkpoint unavailable") || got.InfrastructureRetries != 0 {
		t.Fatal("missing waiting reason or charged retry", got.Reason)
	}
	local := s
	local.SharedInputs = nil
	local.Name = "local-only"
	localJob := submit(t, q, local)
	run := check(t, q, m, nil).Job
	if run == nil || run.ID != localJob.ID {
		t.Fatal("local-only production blocked")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	if r := check(t, q, m, run); r.Stop || r.Kill {
		t.Fatal("mount loss stopped local execution")
	}
	finish(t, q, run, true)
	m.UnavailableRoots = nil
	recovered := check(t, q, m, nil).Job
	if recovered == nil || recovered.ID != j.ID {
		t.Fatal("mount recovery did not restore admission")
	}
	if err := q.Started(result(recovered, false)); err != nil {
		t.Fatal(err)
	}
	m.UnavailableRoots = []UnavailableRoot{{Root: "checkpoint", Reason: "configured NAS mount missing"}}
	if r := check(t, q, m, recovered); r.Stop || r.Kill {
		t.Fatal("mount loss revoked started custody")
	}
	finish(t, q, recovered, true)
}

func TestWorkerRootsDependencyChecks(t *testing.T) {
	input := SharedInput{NAS: "input", SHA256: strings.Repeat("a", 64), SizeBytes: 1}
	base := Job{Spec: cpuSpec(1, 1)}
	base.Spec.Priority = "production"
	for _, tc := range []struct {
		root string
		edit func(*Job, *Machine)
	}{
		{"checkpoint", func(j *Job, m *Machine) { j.Spec.SharedInputs = map[string]SharedInput{"data": input} }},
		{"model", func(j *Job, m *Machine) {
			j.Spec.ModelSnapshots = map[string]ModelSnapshot{"base": {Repo: "org/model"}}
		}},
		{"environment", func(j *Job, m *Machine) { j.Spec.Environment.Name = "runtime" }},
	} {
		t.Run(tc.root, func(t *testing.T) {
			m := cpuMachine("worker", "worker", "cpu", 4, 16)
			m.UnavailableRoots = []UnavailableRoot{{Root: tc.root, Reason: "configured NAS mount missing"}}
			j := cloneJob(&base)
			tc.edit(&j, &m)
			if storageRootReason(&j, &m) == "" {
				t.Fatal("NAS dependency bypassed admission")
			}
			if err := localAttemptAdmission(Worker{Machine: m}, nil, j, nil); err == nil {
				t.Fatal("local launch admitted missing root")
			}
			if tc.root == "checkpoint" {
				m.CachedSharedInputs = []string{input.key()}
				if reason := storageRootReason(&j, &m); reason != "" {
					t.Fatal("verified local input blocked", reason)
				}
			}
		})
	}
}

func TestWorkerRootsAutofsIsNotMountedNAS(t *testing.T) {
	for _, osname := range []string{"darwin", "linux"} {
		t.Run(osname, func(t *testing.T) {
			roots, err := workerMountedNASRoots(context.Background(), osname, func(context.Context, string, ...string) ([]byte, error) {
				if osname == "linux" {
					return []byte("map /nas autofs rw 0 0\nfixture /nas/online nfs rw 0 0\n"), nil
				}
				return []byte("map on /nas (autofs)\nfixture on /nas/online (smbfs)\n"), nil
			})
			if err != nil || len(roots) != 1 || roots[0] != "/nas/online" {
				t.Fatal("automount placeholder counted as NAS", roots, err)
			}
		})
	}
}

func TestWorkerRootsCachedModelAndDirectNAS(t *testing.T) {
	m := Machine{UnavailableRoots: []UnavailableRoot{{Root: "model", Reason: "configured NAS mount missing"}}, CachedModels: []string{"model"}}
	j := Job{Spec: cpuSpec(1, 1)}
	j.Spec.Priority = "production"
	j.Spec.Needs.Model = "model"
	model := Model{Name: "model"}
	if storageRootReasonForModel(&j, &m, model) != "" {
		t.Fatal("local cached model blocked")
	}
	model.DirectNAS = true
	if storageRootReasonForModel(&j, &m, model) == "" {
		t.Fatal("direct NAS model bypassed missing mount")
	}
}
