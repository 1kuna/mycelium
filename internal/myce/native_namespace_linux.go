package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Ignore TERM only in bwrap's host monitor. Killing that monitor triggers
// --die-with-parent's SIGKILL chain, bypassing the payload's checkpoint grace.
// Exec preserves its PID, PGID, cgroup membership and inherited gate FDs.
func nativeNamespaceMonitor(args []string) error {
	if len(args) == 0 {
		return errors.New("native namespace monitor requires an executable")
	}
	signal.Ignore(unix.SIGTERM)
	return unix.Exec(args[0], args, os.Environ())
}

func nativeNamespaceExec(args, env []string) error {
	if len(args) == 0 {
		return errors.New("native namespace exec requires an executable")
	}
	// Install a caught disposition even if TERM was inherited as SIG_IGN.
	// The kernel resets caught handlers to SIG_DFL at exec, unlike SIG_IGN.
	// This also restores the readiness shell before the anchor installs its
	// own intentional TERM trap. No signal masking leaks into the payload.
	signal.Notify(make(chan os.Signal, 1), unix.SIGTERM)
	return unix.Exec(args[0], args, env)
}

func nativeNamespaceMonitorArgs(bwrap string, args []string) ([]string, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return append([]string{executable, "_native-exec", "namespace-monitor", bwrap}, args...), nil
}

// The namespace init is a descendant of the existing PGID anchor. Custody,
// cgroup attachment and recovery continue to use host PIDs and the old PGID.
type nativeNamespace struct {
	support                  nativeLinuxSupport
	pid                      int
	infoReader, infoWriter   *os.File
	readyReader, readyWriter *os.File
}

func (w Worker) prepareNativeGroupIsolation(cmd *exec.Cmd, spool, work string, env map[string]string, network bool) (*nativeNamespace, error) {
	s, err := nativeIsolationSupport(network)
	if err != nil {
		return nil, err
	}
	n := &nativeNamespace{support: s}
	cmd.Env = environmentList(jobBaseEnvironment())
	if s.mode != "namespace" {
		return n, nil
	}
	cmd.Dir = work
	p, err := w.nativeLinuxPolicy(cmd, spool, work, env, nil)
	if err != nil {
		return nil, err
	}
	n.infoReader, n.infoWriter, err = os.Pipe()
	if err != nil {
		return nil, err
	}
	n.readyReader, n.readyWriter, err = os.Pipe()
	if err != nil {
		n.close()
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		n.close()
		return nil, err
	}
	args := append(nativeBwrapPolicyArgs(p), "--info-fd", "3", "--")
	args = append(args, executable, "_native-exec", "namespace-anchor", "/bin/sh", "-c", `printf R >&4; exec 4>&-; exec "$@"`, "myce-namespace-ready")
	args = append(args, cmd.Args...)
	args, err = nativeNamespaceMonitorArgs(s.bwrap, args)
	if err != nil {
		n.close()
		return nil, err
	}
	// The bwrap fork itself follows the gate: all descendants inherit the
	// anchor's control cgroup, and a failed attach never creates namespaces.
	cmd.Path = "/bin/sh"
	cmd.Args = append([]string{"/bin/sh", "-c", `read gate || exit 125; exec "$@"`, "myce-namespace-anchor"}, args...)
	cmd.ExtraFiles = append(cmd.ExtraFiles, n.infoWriter, n.readyWriter)
	return n, nil
}

func (n *nativeNamespace) close() {
	if n == nil {
		return
	}
	if n.infoReader != nil {
		n.infoReader.Close()
		n.infoReader = nil
	}
	if n.infoWriter != nil {
		n.infoWriter.Close()
		n.infoWriter = nil
	}
	if n.readyReader != nil {
		n.readyReader.Close()
		n.readyReader = nil
	}
	if n.readyWriter != nil {
		n.readyWriter.Close()
		n.readyWriter = nil
	}
}

func (n *nativeNamespace) activate(ctx context.Context, gate *os.File) error {
	if n == nil || n.support.mode != "namespace" {
		return nil
	}
	defer n.close()
	n.infoWriter.Close()
	n.infoWriter = nil
	n.readyWriter.Close()
	n.readyWriter = nil
	if _, err := gate.WriteString("start\n"); err != nil {
		return err
	}
	// A namespace launcher that never reports its child is broken, not a
	// job to wait on. Bound this tiny handshake, then let the caller kill and
	// verify its existing PGID/cgroup using the ordinary abort path.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	type result struct {
		pid int
		err error
	}
	done := make(chan result, 1)
	reader := n.infoReader
	ready := n.readyReader
	go func() {
		var info struct {
			PID int `json:"child-pid"`
		}
		err := json.NewDecoder(reader).Decode(&info)
		if err == nil {
			var marker [1]byte
			_, err = io.ReadFull(ready, marker[:])
			if err == nil && marker[0] != 'R' {
				err = errors.New("invalid namespace readiness marker")
			}
		}
		done <- result{info.PID, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return fmt.Errorf("native namespace anchor handshake: %w", r.err)
		}
		if r.pid <= 0 {
			return errors.New("native namespace anchor returned no child PID")
		}
		n.pid = r.pid
		return nil
	case <-ctx.Done():
		return fmt.Errorf("native namespace anchor handshake: %w", ctx.Err())
	}
}

func (n *nativeNamespace) isolate(w Worker, cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network, main bool) error {
	if n == nil || n.pid == 0 {
		return w.isolateNativeCommand(cmd, spool, work, env, ports, network)
	}
	if network {
		return errors.New("native_network_unavailable: Linux outbound transport is not qualified")
	}
	if err := nativeWrapPayloadCheck(cmd); err != nil {
		return err
	}
	p, err := w.nativeLinuxPolicy(cmd, spool, work, env, ports)
	if err != nil {
		return err
	}
	p.Namespace = true
	args, err := nativePolicyArgs(p, cmd.Args[4:])
	if err != nil {
		return err
	}
	if main {
		// Keep the joined child on the existing launch pipe until its host
		// identity and pidfd are in worker custody. FD 3/4 stay untouched.
		args = nativeNamespaceMemberArgs(args)
		cmd.Args[2] = `read gate || exit 125; exec "$@"`
	}
	// nsenter must fork after setns(CLONE_NEWPID), its default behavior. Do not
	// use --no-fork: that would execute the payload outside the PID namespace.
	enter := []string{n.support.nsenter, "--target", strconv.Itoa(n.pid), "--user", "--mount", "--pid", "--ipc", "--net", "--root", "--wdns", cmd.Dir, "--preserve-credentials", "--"}
	if err := nativeWrapPayload(cmd, append(enter, args...)); err != nil {
		return err
	}
	cmd.Env = environmentList(jobBaseEnvironment())
	return nil
}

func nativeNamespaceMemberArgs(args []string) []string {
	return append([]string{"/bin/sh", "-c", `read gate || exit 125; exec "$@" < /dev/null`, "myce-namespace-member"}, args...)
}

// Called with p.mu held, just like the original single launch gate. Neither
// cancellation nor cleanup can signal/reap a half-published main member.
func (g *nativeGroup) releaseMember(m *nativeMember, gate *os.File) error {
	if err := g.ctx.Err(); err != nil {
		return err
	}
	if _, err := gate.WriteString("start\n"); err != nil {
		return err
	}
	if g.namespace == nil || g.namespace.pid == 0 || m.Name != "main" {
		return nil
	}
	pid, birth, target, err := nativeNamespaceChildTarget(g.ctx, m.PID, m.Birth)
	if err != nil {
		return fmt.Errorf("native namespace member handshake: %w", err)
	}
	m.signalTarget = target
	g.custody.Groups[g.supervisor.Process.Pid][pid] = birth
	if err := g.saveLocked(); err != nil {
		return err
	}
	if err := g.ctx.Err(); err != nil {
		return err
	}
	_, err = gate.WriteString("exec\n")
	return err
}

func (m *nativeMember) signal(signal syscall.Signal) error {
	if m.signalTarget != nil {
		return unix.PidfdSendSignal(int(m.signalTarget.Fd()), unix.Signal(signal), nil, 0)
	}
	return syscall.Kill(m.PID, signal)
}

func nativeNamespaceChildTarget(ctx context.Context, parent int, parentBirth string) (int, string, *os.File, error) {
	// Match the anchor handshake: a broken launcher must not hold the launch
	// lock indefinitely. Normal cost is one procfs snapshot; failure costs at
	// most two seconds before ordinary PGID/cgroup abort cleanup takes over.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	childrenPath := fmt.Sprintf("/proc/%d/task/%d/children", parent, parent)
	for {
		if err := ctx.Err(); err != nil {
			return 0, "", nil, err
		}
		birth, err := processBirth(parent)
		if err != nil {
			return 0, "", nil, err
		}
		if birth != parentBirth || parentBirth == "" {
			return 0, "", nil, errors.New("namespace member parent identity changed")
		}
		data, err := os.ReadFile(childrenPath)
		if err != nil {
			return 0, "", nil, err
		}
		children := strings.Fields(string(data))
		if len(children) > 1 {
			return 0, "", nil, errors.New("namespace member has no unique joined child")
		}
		if len(children) == 1 {
			pid, err := strconv.Atoi(children[0])
			if err != nil || pid <= 0 {
				return 0, "", nil, errors.New("invalid namespace member child PID")
			}
			childBirth, err := processBirth(pid)
			if err != nil {
				return 0, "", nil, err
			}
			fd, err := unix.PidfdOpen(pid, 0)
			if err != nil {
				return 0, "", nil, err
			}
			target := os.NewFile(uintptr(fd), "native-namespace-member")
			// Recheck both births and the direct-child relation after acquiring
			// the pidfd. Never signal a recycled numeric PID or a replacement
			// child; the trusted joined shell is still held at its exec gate.
			currentChild, childErr := processBirth(pid)
			currentParent, parentErr := processBirth(parent)
			currentChildren, childrenErr := os.ReadFile(childrenPath)
			if childErr != nil || parentErr != nil || childrenErr != nil || currentChild != childBirth || currentParent != parentBirth || strings.TrimSpace(string(currentChildren)) != children[0] {
				target.Close()
				return 0, "", nil, errors.New("namespace member child identity changed")
			}
			return pid, childBirth, target, nil
		}
		select {
		case <-ctx.Done():
			return 0, "", nil, ctx.Err()
		case <-tick.C:
		}
	}
}

func (n *nativeNamespace) probe(ctx context.Context, port string, group int, path string, listener bool) (bool, bool, error) {
	if n == nil || n.pid == 0 {
		return false, false, nil
	}
	executable, err := os.Executable()
	if err != nil {
		return true, false, err
	}
	mode := "probe"
	if listener {
		mode = "listener"
	}
	args := []string{"--target", strconv.Itoa(n.pid), "--user", "--net", "--preserve-credentials", "--", executable, "_native-exec", mode, port, strconv.Itoa(group)}
	if !listener {
		args = append(args, path)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, n.support.nsenter, args...)
	cmd.Env = environmentList(jobBaseEnvironment())
	out, err := cmd.Output()
	if strings.TrimSpace(string(out)) == "foreign" {
		return true, false, errNativeForeignListener
	}
	if err != nil {
		return true, false, err
	}
	return true, strings.TrimSpace(string(out)) == "ready", nil
}

func (g *nativeGroup) verifyIsolationListener(ctx context.Context, port string, group int) error {
	if handled, ready, err := g.namespace.probe(ctx, port, group, "", true); handled {
		if err != nil {
			return err
		}
		if !ready {
			return errors.New("native listener ownership unconfirmed")
		}
		return nil
	}
	return g.worker.verifyNativeListener(ctx, port, group)
}
