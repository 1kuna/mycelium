package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Exercise the real Linux child identity and signal path even when host policy
// refuses namespaces. Like nsenter, this single-threaded parent forks exactly
// one joined child and waits without forwarding TERM. The real namespace test
// separately proves the confinement/exec path.
func TestNativeNamespaceCPUChildTERMRetainsWaiter(t *testing.T) {
	dir := t.TempDir()
	g, main, gate := nativeNamespaceWaiter(t, dir,
		`read gate || exit 125; exec 5<&0; "$@" <&5 & wait "$!"`,
		nativeNamespaceMemberArgs([]string{"/bin/sh", "-c", `
mkfifo save-fifo || exit 1
exec 5<> save-fifo
trap 'printf term > term-started; read save <&5; printf saved > saved; exit 0' TERM
printf ready > main-ready
while :; do read line <&5; done
`}))
	g.p.mu.Lock()
	err := g.releaseMember(main, gate)
	if err == nil {
		g.main = main
	}
	g.p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	gate.Close()
	awaitFile(t, filepath.Join(dir, "main-ready"))
	saveGate, err := os.OpenFile(filepath.Join(dir, "save-fifo"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer saveGate.Close()

	// A sibling helper must remain alive while the CPU main checkpoints.
	helper := exec.Command("/bin/sleep", "60")
	helper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: main.PID}
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	helperMember := &nativeMember{Name: "checkpoint", PID: helper.Process.Pid, cmd: helper, done: make(chan struct{})}
	go func() {
		err := helper.Wait()
		g.p.mu.Lock()
		helperMember.err = err
		helperMember.Exit = observedNativeExit(helper, time.Now())
		g.p.mu.Unlock()
		close(helperMember.done)
	}()
	t.Cleanup(func() { _ = helper.Process.Kill(); <-helperMember.done })
	helperBirth, err := processBirth(helper.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	// The waiter and joined child are reaped after saving. Like a real sidecar,
	// the surviving helper must retain its own birth in custody for group cleanup.
	g.p.mu.Lock()
	helperMember.Birth = helperBirth
	g.members = append(g.members, helperMember)
	g.custody.Groups[main.PID][helperMember.PID] = helperBirth
	err = g.saveLocked()
	g.p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	g.p.stop(time.Now(), 2*time.Second, false)
	awaitFile(t, filepath.Join(dir, "term-started"))
	select {
	case <-main.done:
		t.Fatalf("waiter exited before checkpoint release: %v", main.err)
	case <-time.After(150 * time.Millisecond):
	}
	if birth, err := processBirth(main.PID); err != nil || birth != main.Birth {
		t.Fatalf("TERM killed the waiting parent: %v", err)
	}
	if birth, err := processBirth(helper.Process.Pid); err != nil || birth != helperBirth {
		t.Fatalf("CPU checkpoint lost its sibling helper: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "saved")); !os.IsNotExist(err) {
		t.Fatalf("checkpoint crossed its held barrier: %v", err)
	}
	if _, err := saveGate.WriteString("save\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-main.done:
		if main.err != nil {
			t.Fatal(main.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not report checkpoint completion")
	}
	if data, err := os.ReadFile(filepath.Join(dir, "saved")); err != nil || string(data) != "saved" {
		t.Fatalf("checkpoint missing: %q %v", data, err)
	}
	if birth, err := processBirth(helper.Process.Pid); err != nil || birth != helperBirth {
		t.Fatalf("helper died before final group cleanup: %v", err)
	}
	if err := g.cleanupProcesses(nil, false); err != nil {
		t.Fatal(err)
	}
	if main.signalTarget != nil {
		t.Error("final cleanup retained the child pidfd")
	}
	if members, err := workerGroupMembers(main.PID); err != nil || len(members) != 0 {
		t.Fatalf("final group cleanup left processes: %v %v", members, err)
	}
}

func TestNativeNamespaceCanceledChildHandshakeCannotExecLate(t *testing.T) {
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, "fork-fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	g, main, gate := nativeNamespaceWaiter(t, dir,
		`read gate || exit 125; printf waiting > waiter-ready; read proceed < fork-fifo; exec 5<&0; "$@" <&5 & wait "$!"`,
		nativeNamespaceMemberArgs([]string{"/bin/sh", "-c", `printf late > main-ready`}))
	released := make(chan error, 1)
	go func() {
		g.p.mu.Lock()
		defer g.p.mu.Unlock()
		released <- g.releaseMember(main, gate)
	}()
	awaitFile(t, filepath.Join(dir, "waiter-ready"))
	g.cancel()
	select {
	case err := <-released:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handshake ignored cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled handshake did not return")
	}
	gate.Close() // The ordinary launch abort closes its deferred gate writer.
	forkGate, err := os.OpenFile(filepath.Join(dir, "fork-fifo"), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer forkGate.Close()
	if _, err := forkGate.WriteString("fork\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-main.done:
	case <-time.After(5 * time.Second):
		t.Fatal("late child did not reject the closed exec gate")
	}
	if _, err := os.Stat(filepath.Join(dir, "main-ready")); !os.IsNotExist(err) {
		t.Fatalf("payload ran after handshake cancellation: %v", err)
	}
}

func nativeNamespaceWaiter(t *testing.T, dir, script string, child []string) (*nativeGroup, *nativeMember, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/sh", append([]string{"-c", script, "namespace-waiter"}, child...)...)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = dir, reader, os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		reader.Close()
		writer.Close()
		t.Fatal(err)
	}
	reader.Close()
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		writer.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	identity := ID()
	p := &process{job: Job{ID: ID(), Spec: Spec{Needs: ResourceNeeds{Platform: Platform{Required: []string{"cpu"}}}}}}
	main := &nativeMember{Name: "main", PID: cmd.Process.Pid, Birth: birth, cmd: cmd, done: make(chan struct{})}
	g := &nativeGroup{p: p, namespace: &nativeNamespace{pid: 1}, worker: Worker{Clock: RealClock{}}, ctx: ctx, cancel: cancel, dir: dir, identity: identity, supervisor: cmd, members: []*nativeMember{main}, custody: workerState{Identity: identity, Groups: map[int]map[int]string{main.PID: {main.PID: birth}}}}
	p.native = g
	go func() {
		err := cmd.Wait()
		g.p.mu.Lock()
		main.err = err
		main.Exit = observedNativeExit(cmd, time.Now())
		g.p.mu.Unlock()
		close(main.done)
	}()
	t.Cleanup(func() {
		cancel()
		if err := g.cleanupProcesses(writer, false); err != nil {
			t.Error(err)
		}
	})
	return g, main, writer
}
