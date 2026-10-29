package myce

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestSeedProjectsRestartPreservesLivePolicy(t *testing.T) {
	for _, removeAll := range []bool{false, true} {
		name := "operator edits"
		if removeAll {
			name = "operator removes all projects"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			config := filepath.Join(dir, "projects.yaml")
			if err := os.WriteFile(config, []byte("sample:\n  weight: 1\n  allowed_machines: [worker-a]\nremoved:\n  weight: 1\n"), 0600); err != nil {
				t.Fatal(err)
			}
			clock := &fakeClock{now: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)}
			open := func() *Queue {
				t.Helper()
				q, err := Open(filepath.Join(dir, "state", "queue.db"), filepath.Join(dir, "checkpoints"), clock)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { q.Close() })
				policies, err := ReadProjectPolicies(config)
				if err != nil {
					t.Fatal(err)
				}
				if err := q.SeedProjects(policies); err != nil {
					t.Fatal(err)
				}
				return q
			}
			q := open()
			initial := q.PriorityPolicy()
			if initial.Revision != 1 || initial.Policy.Projects["sample"].Weight != 1 {
				t.Fatalf("bootstrap not applied: %+v", initial)
			}
			policy := initial.Policy
			policy.Tiers = []PriorityTier{{Name: "operator", Selectors: []string{"*/*"}}}
			policy.Projects = map[string]ProjectPolicy{}
			if !removeAll {
				policy.Projects["sample"] = ProjectPolicy{Weight: 3, AllowedMachines: []string{"worker-a", "worker-b"}, DefaultCallPriority: "production", MaxConcurrent: 4, MaxConcurrentExperiments: 2, MaxMemoryShare: 0.5}
			}
			live, err := q.SetPriorityPolicy(initial.Revision, policy)
			if err != nil {
				t.Fatal(err)
			}
			if live.Revision != initial.Revision+1 {
				t.Fatalf("operator edit did not advance revision: %+v", live)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := open()
			if got := reopened.PriorityPolicy(); !reflect.DeepEqual(got, live) {
				t.Fatalf("stale startup config replaced live policy:\ngot  %+v\nwant %+v", got, live)
			}
		})
	}
}
