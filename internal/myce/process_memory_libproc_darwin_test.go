//go:build darwin && cgo

package myce

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/probeguard"
)

// The libproc probe must agree with the ps sampler's identities and RSS
// semantics on a real process tree, so the guard limit keeps its meaning.
func TestLibprocOwnedResidentMatchesPS(t *testing.T) {
	before := probeguard.Before
	probeguard.Before = nil
	defer func() { probeguard.Before = before }()
	self := os.Getpid()
	birth, err := processBirth(self)
	if err != nil {
		t.Fatal(err)
	}
	if _, libBirth, err := libprocResident(self); err != nil || libBirth != birth {
		t.Fatalf("libproc birth %q differs from kinfo birth %q: %v", libBirth, birth, err)
	}
	cmd := exec.Command("sh", "-c", "sleep 30 & sleep 30 & wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
	time.Sleep(200 * time.Millisecond)
	root := cmd.Process.Pid
	rootBirth, err := processBirth(root)
	if err != nil {
		t.Fatal(err)
	}
	roots := map[int]string{root: rootBirth}
	fast, fastMembers, err, ok := darwinFastOwnedResident(roots)
	if err != nil || !ok {
		t.Fatalf("fast probe: %v ok=%t", err, ok)
	}
	slow, slowMembers, err := darwinOwnedResident(roots)
	if err != nil {
		t.Fatal(err)
	}
	// The trees are idle; allow page-level drift between the two reads.
	if fastMembers != 2 || slowMembers != 2 || fast == 0 || max(fast, slow)-min(fast, slow) > 1<<20 {
		t.Fatalf("libproc %d bytes/%d members, ps %d bytes/%d members", fast, fastMembers, slow, slowMembers)
	}
	state, err, ok := darwinFastPressure()
	if err != nil || !ok || state.available <= 0 || state.level == 0 {
		t.Fatalf("fast pressure: %+v %v %t", state, err, ok)
	}
}
