package myce

import (
	"errors"
	"testing"
)

func isolationCapacityFixture(t *testing.T, e Extension) (*Queue, Machine) {
	t.Helper()
	q, _ := fixture(t)
	m := machine("allowed", "fake")
	m.TotalMemoryGB = 8
	m.Capabilities = nil // Known capacity, but no kernel isolation or qualification.
	m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
	check(t, q, m, nil)
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{m.ID}}}); err != nil {
		t.Fatal(err)
	}
	return q, m
}

func requireIsolationCapacityRefusal(t *testing.T, q *Queue, s Spec) {
	t.Helper()
	_, err := q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "job_capacity_invalid" {
		t.Fatalf("want project capacity refusal, got %v", err)
	}
}

// R1-P1: structural admission must retain the exact frozen registration's
// exception while allowing submission before extension qualification.
func TestDeclaredIsolationProjectCapacity(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		legacy     bool
	}{
		{name: "declared", mode: "unsandboxed"},
		{name: "legacy", legacy: true},
		{name: "new-default"},
		{name: "explicit-sandboxed", mode: "sandboxed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := compositionExtension(t, false)
			e.Isolation = tc.mode
			if tc.mode == "unsandboxed" {
				e.Reason = "native helper needs ordinary host IPC"
			}
			e = freezeComposition(t, e)
			q, m := isolationCapacityFixture(t, e)
			if tc.legacy {
				// Seed the pre-policy persisted registration shape, then exercise
				// public registration replay without inventing a policy override.
				if err := q.update(func() error {
					q.s.Extensions = map[string]Extension{extensionKey(e): e}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			s := Spec{Name: "capacity", Project: "test", Extension: extensionKey(e), RequireTarget: false}
			if tc.mode != "unsandboxed" && !tc.legacy {
				requireIsolationCapacityRefusal(t, q, s)
				// A kernel-capable host admits the same unqualified registration.
				m.Capabilities = []string{"native_isolation_landlock"}
				check(t, q, m, nil)
			}
			j := submit(t, q, s)
			if len(j.Extensions) != 1 || j.Extensions[0].Digest != e.Digest || j.Extensions[0].Isolation != tc.mode || j.Extensions[0].SandboxDefault == tc.legacy {
				t.Fatal("capacity admission changed frozen registration policy", j.Extensions)
			}
			if j.Spec.Isolation != "" || j.Spec.Reason != "" {
				t.Fatal("temporary capacity policy escaped into the submitted spec", j.Spec)
			}
			if reply := check(t, q, m, nil); reply.Job != nil {
				t.Fatal("capacity admission bypassed dispatch qualification", reply.Job)
			}
			// The exception waives kernel confinement only, never capacity.
			s.SizeHint.MemoryGB = m.TotalMemoryGB + 1
			requireIsolationCapacityRefusal(t, q, s)
		})
	}
}

func TestDeclaredIsolationProjectCapacityKeepsCandidateAuthority(t *testing.T) {
	sandboxed := compositionExtension(t, false)
	sandboxed.Isolation = "sandboxed"
	sandboxed = freezeComposition(t, sandboxed)
	q, m := isolationCapacityFixture(t, sandboxed)
	if err := q.AddExtension(sandboxed); err != nil {
		t.Fatal(err)
	}
	// This alternative has an exception, but cannot fit the allowed host.
	declared := compositionExtension(t, false)
	declared.Version, declared.Isolation, declared.Reason = "2", "unsandboxed", "native helper needs host IPC"
	declared.Variants[0].Needs.MemoryGB = m.TotalMemoryGB + 1
	declared = freezeComposition(t, declared)
	if err := q.AddExtension(declared); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "alternatives", Project: "test", Kind: "sample", RequireTarget: false}
	requireIsolationCapacityRefusal(t, q, s)
	// Once the fitting sandboxed candidate has confinement, admit it without
	// demanding qualification or borrowing the oversized candidate's exception.
	m.Capabilities = []string{"native_isolation_landlock"}
	check(t, q, m, nil)
	submit(t, q, s)
}
