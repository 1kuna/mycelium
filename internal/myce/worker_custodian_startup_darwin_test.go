package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDarwinCustodianRetainsStartupDiagnosticsAfterCleanup(t *testing.T) {
	spool, id := t.TempDir(), ID()
	dir := custodianDir(spool)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	j := check(t, q, machine("worker", "fake"), nil).Job
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	execution := Execution{ID: j.ID, Attempt: j.Attempt}
	req := custodianRequest{Protocol: custodianProtocol, ID: id, Machine: Machine{ID: j.Machine}, State: workerState{Job: j, Session: j.Session}}
	if err := saveWorkerJSON(dir, custodianName(id, "request"), req); err != nil {
		t.Fatal(err)
	}
	if err := RecordCustodianStartupFailure(spool, id); err != nil {
		t.Fatal(err)
	}
	// The refusal can be visible while launchd still reports the process. The
	// child writes its fatal error and exits before the next manager read.
	t.Setenv("CUSTODIAN_TEST_DIR", dir)
	t.Setenv("CUSTODIAN_TEST_ID", id)
	installCustodianTestLaunchctl(t, `#!/bin/sh
test -z "${MYCE_TOKEN+x}" || exit 97
case "$1" in
bootstrap)
  /usr/libexec/PlistBuddy -c 'Print :StandardErrorPath' "$3" > "$CUSTODIAN_TEST_DIR/stderr-path"
  ;;
print)
  test "$2" = "gui/$(/usr/bin/id -u)/local.mycelium.attempt.$CUSTODIAN_TEST_ID" || exit 98
  if test -f "$CUSTODIAN_TEST_DIR/printed"; then
    printf '\tstate = not running\n\truns = 1\n\tlast exit code = 42\n'
  else
    : > "$CUSTODIAN_TEST_DIR/printed"
    stderr=$(/bin/cat "$CUSTODIAN_TEST_DIR/stderr-path")
    /usr/bin/yes long-error | /usr/bin/head -c 12000 > "$stderr"
    printf '\nfinal startup error\n' >> "$stderr"
    printf '\tstate = running\n\tpid = 42\n\truns = 1\n'
  fi
  printf '\tenvironment = {\n\t\tfixture = private-environment-value\n\t}\n'
  ;;
bootout)
  /usr/bin/grep -q '"last_exit_code":42' "$CUSTODIAN_TEST_DIR/custodian-$CUSTODIAN_TEST_ID.startup.json" || exit 99
  printf 'retired' > "$CUSTODIAN_TEST_DIR/retired"
  ;;
esac
`)
	if err := launchAttemptCustodian(t.Context(), ServiceConfig{Binary: "/fixture/myce"}, dir, id, execution); err != nil {
		t.Fatal(err)
	}
	parent := req.State
	parent.Custodian = id
	p, err := (Worker{Clock: clock}).recoverAttempt(&workerStore{dir: spool, state: parent})
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	select {
	case f := <-p.done:
		r := f.result
		if f.err != nil || p.started() || r.Execution != execution || r.Machine != j.Machine || r.Session != j.Session || r.ReleasePending ||
			r.ExecutionEvidence == nil || r.ExecutionEvidence.Main != executionNotStarted || r.ExecutionCause == nil ||
			r.ExecutionCause.LaunchRefusal != custodianStartupFailed {
			t.Fatalf("wrong pre-Started exit: %+v %v", r, f.err)
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		cleanupCustodian(dir, id, r)
	case <-time.After(testWaitTimeout):
		t.Fatal("startup refusal did not replay")
	}
	if got := job(t, q, j.ID); got.State != "cancelled" || !got.ReleaseConfirmed || got.CommandStarted != nil && *got.CommandStarted {
		t.Fatalf("pre-Started attempt did not release: %+v", got)
	}
	name := custodianName(id, "startup")
	var got custodianStartupDiagnostic
	if err := readCustodianJSON(dir, name, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != id || got.Execution == nil || *got.Execution != execution || got.Bootstrap.Outcome != "success" ||
		got.Snapshot != "finished_without_owner_pid" {
		t.Fatalf("attempt or bootstrap evidence missing: %+v", got)
	}
	if got.Registration == nil || got.Registration.Outcome != "registered" || got.Registration.PID != 0 ||
		got.Registration.ExitCode == nil || *got.Registration.ExitCode != 42 || got.Registration.Runs != 1 {
		t.Fatalf("terminal manager evidence missing: %+v", got.Registration)
	}
	if got.StderrSource != "stderr" || len(got.Stderr) != custodianStartupOutputBytes || !strings.HasSuffix(got.Stderr, "final startup error\n") {
		t.Fatal("fatal stderr was not retained as a bounded tail")
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || strings.Contains(string(data), "private-environment-value") {
		t.Fatal("startup diagnostic retained a launchd environment value", err)
	}
	info, err := os.Stat(filepath.Join(dir, name))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("startup diagnostic must be private", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "retired")); err != nil {
		t.Fatal("manager was not retired after diagnostics were saved", err)
	}
	for _, retired := range []string{custodianName(id, "request"), custodianName(id, "snapshot"), "custodian-" + id + ".stderr"} {
		if _, err := os.Stat(filepath.Join(dir, retired)); !os.IsNotExist(err) {
			t.Fatalf("retired artifact remains: %s: %v", retired, err)
		}
	}
}

func TestDarwinCustodianRetainsBootstrapFailureWithoutRelease(t *testing.T) {
	dir, id := t.TempDir(), ID()
	execution := Execution{ID: ID(), Attempt: 2}
	t.Setenv("CUSTODIAN_TEST_ID", id)
	installCustodianTestLaunchctl(t, `#!/bin/sh
test -z "${MYCE_TOKEN+x}" || exit 97
case "$1" in
bootstrap)
  printf 'fixture registration refused' >&2
  exit 5
  ;;
print)
  printf 'Could not find service "%s"\n' "local.mycelium.attempt.$CUSTODIAN_TEST_ID" >&2
  exit 113
  ;;
esac
`)
	if err := launchAttemptCustodian(t.Context(), ServiceConfig{}, dir, id, execution); err == nil {
		t.Fatal("failed bootstrap was accepted")
	}
	var got custodianStartupDiagnostic
	if err := readCustodianJSON(dir, custodianName(id, "startup"), &got); err != nil {
		t.Fatal(err)
	}
	if got.Execution == nil || *got.Execution != execution || got.Bootstrap.CommandCode == nil ||
		*got.Bootstrap.CommandCode != 5 || got.BootstrapOutput != "fixture registration refused" ||
		got.Registration == nil || got.Registration.Outcome != "not_registered" || got.Snapshot != "missing" {
		t.Fatalf("bootstrap refusal evidence missing: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, custodianName(id, "snapshot"))); !os.IsNotExist(err) {
		t.Fatal("a manager observation created a custody snapshot", err)
	}
}

func installCustodianTestLaunchctl(t *testing.T, script string) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "launchctl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("MYCE_TOKEN", "fixture-only")
}
