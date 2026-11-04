package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestToolsResolveRefusalAndRouting(t *testing.T) {
	q, clock := fixture(t)
	missing := cpuMachine("bare", "", "cpu", 8, 16)
	missing.ToolInventory = map[string][]string{"": {"git"}}
	check(t, q, missing, nil)
	s := cpuSpec(1, 1)
	s.RequireTarget = true
	s.Needs.Tools = []string{"python3.12", "node", "npm", "npx", "git"}
	report, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Targets) != 1 || report.Targets[0].Eligible || !strings.Contains(report.Targets[0].Reason, "tools_missing: missing tools [python3.12, node, npm, npx] on bare") {
		t.Fatalf("resolve: %+v", report.Targets)
	}
	_, err = q.Submit(s)
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "no_qualified_target" || !strings.Contains(api.Message, "tools_missing") || !strings.Contains(api.Message, "on bare") {
		t.Fatalf("refusal: %v", err)
	}
	// An older worker with no inventory also refuses explicit tool requirements.
	missing.ToolInventory = nil
	check(t, q, missing, nil)
	if _, err := q.Submit(s); err == nil {
		t.Fatal("unknown tool inventory admitted")
	}
	ready := cpuMachine("prepared", "", "cpu", 8, 16)
	ready.ToolInventory = map[string][]string{"": slices.Clone(s.Needs.Tools)}
	check(t, q, ready, nil)
	clock.Advance(time.Second)
	j := submit(t, q, s)
	if reply := check(t, q, missing, nil); reply.Job != nil {
		t.Fatal("routed to missing tools", reply.Job)
	}
	if reply := check(t, q, ready, nil); reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatalf("present tools did not proceed: %+v", reply)
	}
}

func TestToolsPreparedEnvironmentInventory(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "cpu")
	m.ToolInventory = map[string][]string{"": {"git"}, "runtime-key": {"node", "python3.12"}}
	m.Environments = []string{"runtime-key"}
	m.EnvironmentBuilder = true
	s := cpuSpec(1, 1)
	s.Environment = Environment{Name: "runtime"}
	s.Needs.Tools = []string{"node", "python3.12"}
	j := &Job{Spec: s, EnvironmentRecipes: map[string]EnvironmentRecipe{"cpu": {Key: "runtime-key"}}}
	if !toolsFit(j, &m) {
		t.Fatal("prepared environment tools ignored")
	}
	m.ToolInventory["runtime-key"] = []string{"python3.12"}
	if toolsFit(j, &m) || !strings.Contains(targetFitReason("tools", j, &m), "node") {
		t.Fatal("missing prepared tool admitted")
	}
	// Cold recipes can prepare, then the very same placement predicate rechecks.
	m.Environments = nil
	if !toolsFit(j, &m) {
		t.Fatal("cold environment cannot prepare")
	}
	check(t, q, m, nil)
}

func TestToolsLaunchRechecksEffectivePath(t *testing.T) {
	w := recoveryWorker(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.Machine.HostMemoryMeasured = true
	w.Machine.HostAvailableMemoryGB, w.Machine.FreeHostMemoryGB = 64, 64
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	marker := filepath.Join(t.TempDir(), "started")
	j := recoveryJob(t, []string{"sh", "-c", "touch \"$MARKER\""})
	j.Spec.Run.Env = map[string]string{"MARKER": marker}
	j.Spec.Needs.Tools = []string{"myce-missing-python3.12", "myce-missing-node"}
	w = workerWithPreparedCheckpoint(t, w, store, j)
	p, err := startAfterFixtureInputs(t, w, store, j)
	if p != nil || err == nil {
		t.Fatalf("missing tools launched: %v %v", p, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("payload started")
	}
	r := startFailureOutcome(result(&j, false), err)
	cause := safeRuntimeCause(r.ExecutionCause)
	if cause == nil || cause.Class != "tools_missing" || !strings.Contains(cause.String(), "myce-missing-node") || cause.Host != w.Machine.ID {
		t.Fatalf("lost tool diagnostic: %+v %v", r, err)
	}
}

func TestToolsInventoryAndValidation(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"node", "python3.12"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "not-executable"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	names := executableTools(root)
	if !slices.Equal(names, []string{"node", "python3.12"}) {
		t.Fatal(names)
	}
	if missing := missingExecutableTools([]string{"node", "git"}, root); !slices.Equal(missing, []string{"git"}) {
		t.Fatal(missing)
	}
	s := cpuSpec(1, 1)
	s.Needs.Tools = []string{"python3.12", "node", "npm", "npx", "git"}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.Needs.Tools = []string{"../private/tool"}
	if err := s.Validate(); err == nil {
		t.Fatal("tool path accepted")
	}
}

// Undeclared prerequisites inside arbitrary scripts remain the author's
// responsibility, but conventional shell missing-command exits are actionable.
func TestToolsPresentLaunchAndMissingCommandDiagnostic(t *testing.T) {
	w := recoveryWorker(t)
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.Machine.HostMemoryMeasured = true
	w.Machine.HostAvailableMemoryGB, w.Machine.FreeHostMemoryGB = 64, 64
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = w.Machine.Session
	j := recoveryJob(t, []string{"sh", "-c", "myce-intentionally-absent-tool"})
	j.Spec.Needs.Tools = []string{"sh"}
	w = workerWithPreparedCheckpoint(t, w, store, j)
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stop(w.Clock.Now(), 0, true) })
	var r Result
	await(t, func() bool {
		select {
		case f := <-p.done:
			if f.err != nil {
				t.Fatal(f.err)
			}
			r = f.result
			return true
		default:
			return false
		}
	})
	cause := safeRuntimeCause(r.ExecutionCause)
	if r.Success || cause == nil || cause.ExitCode == nil || *cause.ExitCode != 127 || !strings.Contains(cause.String(), "declare prerequisites in needs.tools") {
		t.Fatalf("silent missing command: %+v", r)
	}
}

func TestToolsVariantValidationAndCallerRequirements(t *testing.T) {
	e := compositionExtension(t, false)
	e.Variants[0].Needs.Tools = []string{"../private/tool"}
	if err := e.validateVariants(); err == nil {
		t.Fatal("variant accepted tool path")
	}
	e.Variants[0].Needs.Tools = []string{"git"}
	if err := e.validateVariants(); err != nil {
		t.Fatal(err)
	}
	m := machine("worker", "fake")
	m.CapabilityProfile.OS = e.Variants[0].Target.OS
	m.CapabilityProfile.Arch = e.Variants[0].Target.Arch
	m.ToolInventory = map[string][]string{"": {"git"}}
	s := spec("experiment")
	s.Extension = extensionKey(e)
	s.Needs.Tools = []string{"node", "git"}
	candidates := extensionJobs(&Job{Spec: s, Extensions: []Extension{e}}, &m, false)
	if len(candidates) != 1 || !slices.Equal(candidates[0].Spec.Needs.Tools, []string{"git", "node"}) {
		t.Fatalf("caller tools lost: %+v", candidates)
	}
	if toolsFit(candidates[0], &m) || !strings.Contains(targetFitReason("tools", candidates[0], &m), "node") {
		t.Fatal("missing caller prerequisite admitted")
	}
	m.ToolInventory[""] = []string{"git", "node"}
	if !toolsFit(candidates[0], &m) {
		t.Fatal("union of present tools refused")
	}
	if !slices.Equal(e.Variants[0].Needs.Tools, []string{"git"}) {
		t.Fatal("mutated shared variant")
	}
}
