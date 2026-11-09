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

func TestToolsLaunchRechecksEffectivePath(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
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

	})

	t.Run("ToolsPresentLaunchAndMissingCommandDiagnostic", func(t *testing.T) {
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

	})
}

// Undeclared prerequisites inside arbitrary scripts remain the author's
// responsibility, but conventional shell missing-command exits are actionable.
