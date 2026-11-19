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
	j := Job{ID: ID(), Attempt: 1, Machine: "worker", Session: "session"}
	s := &workerStore{dir: dir, stateName: "attempt.json", state: workerState{Job: &j, Session: j.Session}}
	w := Worker{Clock: RealClock{}, Machine: Machine{ID: j.Machine}, ServiceConfig: &ServiceConfig{State: dir, Binary: "/unused", Config: "/unused", Slice: "mycetest0123456789abcdef0123456789abcdef.slice"}}
	p, err := w.startCustodian(t.Context(), s, j)
	if err != nil {
		t.Fatal(err)
	}
	defer p.custodian.detach()
	select {
	case f := <-p.done:
		if f.result.Error != errCustodianStartup.Error() || f.result.ReleasePending || f.result.ExecutionEvidence == nil || f.result.ExecutionEvidence.Main != executionNotStarted {
			t.Fatalf("launcher exit lost startup refusal: %+v", f.result)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("launcher exit was retained as uncertain startup")
	}
	args, err := os.ReadFile(argsPath)
	if err != nil || !strings.Contains(string(args), "--slice="+w.ServiceConfig.custodianSlice()+"\n") || strings.Contains(string(args), "--slice=myce-jobs.slice") {
		t.Fatalf("custodian escaped configured slice: %s %v", args, err)
	}
}
