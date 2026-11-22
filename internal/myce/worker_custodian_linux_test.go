package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #1709: the CLI guard must recognize an exact custodian scope while keeping
// the ordinary worker-service allowlist closed to scopes and unrelated units.
func TestCustodianEntryScopeIdentity(t *testing.T) {
	id := "0123456789abcdef0123456789abcdef"
	base := "/user.slice/user-1000.slice/user@1000.service/myce-jobs.slice/" + custodianUnit(id)
	for _, suffix := range []string{"", "/worker"} {
		b := []byte("0::" + base + suffix + "\n")
		if got, err := custodianCgroup(b, id); err != nil || got != base {
			t.Fatalf("valid independent scope refused: %q %v", got, err)
		}
		if got := workerUnitFromCgroup(b); got != "" {
			t.Fatalf("scope widened worker allowlist: %s", got)
		}
	}
	for _, path := range []string{"/myce-worker.service", base + "/payload", "/" + custodianUnit("ffffffffffffffffffffffffffffffff")} {
		if _, err := custodianCgroup([]byte("0::"+path), id); err == nil {
			t.Fatal("unrelated cgroup accepted", path)
		}
	}
	if _, err := custodianCgroup([]byte("0::"+base), "../invalid"); err == nil {
		t.Fatal("invalid attempt identity accepted")
	}
}

// systemd-run can exec the helper and then exit on an entry-point refusal.
// Exercise the real launcher/wait path with a test-owned executable, without
// creating or changing any service manager objects.
func TestCustodianLauncherExitProducesRefusal(t *testing.T) {
	q, clock, host := fleetAtStartProbe(t)
	j := job(t, q, host.StartProbe)
	received := clock.Now()
	j.LaunchTimings = &LaunchTimings{AssignmentReceivedAt: received, ObservedAt: received, CurrentPhase: launchDispatch}
	var dbPath string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&dbPath); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "systemd-run"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$MYCE_TEST_LAUNCH_ARGS\"\nexit 23\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	argsPath := filepath.Join(dir, "launcher-args")
	t.Setenv("MYCE_TEST_LAUNCH_ARGS", argsPath)
	s := &workerStore{dir: dir, stateName: "attempt.json", state: workerState{Job: j, Session: j.Session}}
	w := Worker{Clock: clock, Machine: Machine{ID: j.Machine}, ServiceConfig: &ServiceConfig{State: dir, Binary: "/unused", Config: "/unused", Slice: "mycetest0123456789abcdef0123456789abcdef.slice"}}
	p, err := w.startAssignedAttempt(t.Context(), s, *j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	select {
	case f := <-p.done:
		r := f.result
		if r.Error != errCustodianStartup.Error() || r.ReleasePending || r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.ExecutionEvidence.Sidecars != executionNotStarted || r.ExecutionCause == nil || !r.ExecutionCause.PrelaunchRefused || r.ExecutionCause.LaunchRefusal != custodianStartupFailed {
			t.Fatalf("launcher exit lost startup refusal: %+v", f.result)
		}
		if r.LaunchTimings == nil || !r.LaunchTimings.AssignmentReceivedAt.Equal(received) ||
			r.LaunchTimings.CurrentPhase != "" || r.LaunchTimings.CommandStartAt != nil ||
			r.LaunchTimings.Phases[len(r.LaunchTimings.Phases)-1].Phase != launchCustodianSetup {
			t.Fatalf("launcher refusal lost the launch boundaries: %+v", r.LaunchTimings)
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		r.ReleaseOnly = true
		if err := q.Complete(r); err != nil {
			t.Fatal("release replay", err)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("launcher exit was retained as uncertain startup")
	}
	args, err := os.ReadFile(argsPath)
	if err != nil || !strings.Contains(string(args), "--slice="+w.ServiceConfig.custodianSlice()+"\n") || strings.Contains(string(args), "--slice=myce-jobs.slice") {
		t.Fatalf("custodian escaped configured slice: %s %v", args, err)
	}
	fleetTick(t, q)
	if q.s.FleetUpgrade.State != "stopped" || host.Failure != "worker_start_probe_failed" || host.WorkerDone || q.s.FleetUpgrade.Hosts[1].Phase != "pending" {
		t.Fatalf("refused probe advanced rollout: %+v", q.fleetUpgradeStatus())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dbPath, q.root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got := job(t, reopened, j.ID)
	if got.State != "cancelled" || got.Losses != 0 || got.InfrastructureRetries != 0 || !got.ReleaseConfirmed || got.Ledger == nil || !got.Ledger.Complete || len(got.AttemptHistory) != 1 {
		t.Fatalf("probe lost release, history, or accounting: %+v", got)
	}
	a := got.AttemptHistory[0]
	if a.Attempt != j.Attempt || a.ExecutionCause == nil || a.ExecutionCause.LaunchRefusal != custodianStartupFailed || !a.ExecutionCause.PrelaunchRefused || a.ExecutionEvidence == nil || a.ExecutionEvidence.Main != executionNotStarted {
		t.Fatalf("reopened queue lost startup refusal: %+v", a)
	}
}
