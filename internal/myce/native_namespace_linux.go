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

func nativeNamespaceMonitorFileArgs(bwrap string, options, command []string, work string) ([]string, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	path, digest, err := writeNativeLaunchFile(work, []byte(strings.Join(options, "\x00")+"\x00"))
	if err != nil {
		return nil, err
	}
	return append([]string{executable, "_native-exec", "namespace-monitor-file", bwrap, path, digest}, command...), nil
}

// Bubblewrap expands NUL-separated options from an FD and takes the command
// from its outer argv. The mount grants can be too large for exec argv.
func nativeNamespaceMonitorFile(args []string) error {
	if len(args) < 4 {
		return errors.New("native namespace monitor requires option metadata and a command")
	}
	data, err := readNativeLaunchFile(args[1], args[2])
	if err != nil {
		return err
	}
	fd, err := unix.MemfdCreate("myce-bwrap-args", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "myce-bwrap-args")
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		return err
	}
	if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, 0); err != nil {
		return err
	}
	return nativeNamespaceMonitor(append([]string{args[0], "--args", strconv.Itoa(fd), "--"}, args[3:]...))
}

// The namespace init is a descendant of the existing PGID anchor. Custody,
// cgroup attachment and recovery continue to use host PIDs and the old PGID.
type nativeNamespace struct {
	support                  nativeLinuxSupport
	pid                      int
	anchor                   *os.File
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
	if err := os.MkdirAll(nativeLaunchRoot(work), 0700); err != nil {
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
	options := append(nativeBwrapPolicyArgs(p), "--info-fd", "3")
	command := append([]string{executable, "_native-exec", "namespace-anchor", "/bin/sh", "-c", `printf R >&4; exec 4>&-; exec "$@"`, "myce-namespace-ready"}, cmd.Args...)
	args, err := nativeNamespaceMonitorFileArgs(s.bwrap, options, command, work)
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
	n.closeHandshake()
	if n.anchor != nil {
		n.anchor.Close()
		n.anchor = nil
	}
}

func (n *nativeNamespace) closeHandshake() {
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

func (n *nativeNamespace) activate(ctx context.Context, gate *os.File, group int) error {
	if n == nil || n.support.mode != "namespace" {
		return nil
	}
	defer n.closeHandshake()
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
		anchor, err := pinNativeNamespaceAnchor(r.pid, group)
		if err != nil {
			return err
		}
		n.pid = r.pid
		n.anchor = anchor
		return nil
	case <-ctx.Done():
		return fmt.Errorf("native namespace anchor handshake: %w", ctx.Err())
	}
}

func pinNativeNamespaceAnchor(pid, group int) (*os.File, error) {
	if pid <= 0 || group <= 0 {
		return nil, fmt.Errorf("%w: missing anchor PID or process group", errNativeNamespaceAnchor)
	}
	fd, err := unix.Open(fmt.Sprintf("/proc/%d", pid), unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open anchor: %v", errNativeNamespaceAnchor, err)
	}
	anchor := os.NewFile(uintptr(fd), "native-namespace-anchor")
	if pgid, err := unix.Getpgid(pid); err != nil || pgid != group {
		anchor.Close()
		return nil, fmt.Errorf("%w: anchor process group could not be verified", errNativeNamespaceAnchor)
	}
	// If the numeric lookup raced PID reuse, this proc directory still refers
	// to the original process. Its network namespace is unavailable on exit.
	if err := checkNativeNamespaceAnchor(fd); err != nil {
		anchor.Close()
		return nil, err
	}
	return anchor, nil
}

func checkNativeNamespaceAnchor(fd int) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(fd, "ns/net", &stat, 0); err != nil {
		return fmt.Errorf("%w: inspect anchor: %v", errNativeNamespaceAnchor, err)
	}
	return nil
}

func (n *nativeNamespace) checkAnchor() error {
	if n == nil || n.support.mode != "namespace" {
		return nil
	}
	if n.anchor == nil {
		return errNativeNamespaceAnchor
	}
	return checkNativeNamespaceAnchor(int(n.anchor.Fd()))
}

func (n *nativeNamespace) isolate(w Worker, cmd *exec.Cmd, spool, work string, env map[string]string, ports []string, network, main bool) error {
	if n == nil || n.support.mode != "namespace" {
		return w.isolateNativeCommand(cmd, spool, work, env, ports, network)
	}
	if err := n.checkAnchor(); err != nil {
		return err
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
	args, err := nativePolicyFileArgs(p, cmd.Args[4:], work)
	if err != nil {
		return err
	}
	enter := n.joinArgs(cmd, args[0], cmd.Dir)
	if main {
		// Keep the joined child on the existing launch pipe until its host
		// identity and pidfd are in worker custody. FD 3/4 stay untouched.
		args = nativeNamespaceMemberArgs(args)
		cmd.Args[2] = `read gate || exit 125; exec "$@"`
	}
	if err := nativeWrapPayload(cmd, append(enter, args...)); err != nil {
		return err
	}
	cmd.Env = environmentList(jobBaseEnvironment())
	return nil
}

func (n *nativeNamespace) joinArgs(cmd *exec.Cmd, executable, dir string) []string {
	// Keep launch receipt FDs 3/4 reserved. The exec gate preserves the
	// anchor descriptor, and the payload helper closes it before authored code.
	for len(cmd.ExtraFiles) < 2 {
		cmd.ExtraFiles = append(cmd.ExtraFiles, nil)
	}
	fd := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, n.anchor)
	return []string{executable, "_native-exec", "namespace-join", n.support.nsenter, strconv.Itoa(fd), dir}
}

func nativeNamespaceJoin(args []string) error {
	if len(args) < 4 {
		return errors.New("native namespace join requires a launcher, anchor, directory and command")
	}
	enter, dir := args[0], args[2]
	target, err := strconv.Atoi(args[1])
	if err != nil || target < 5 {
		return fmt.Errorf("%w: missing anchor descriptor", errNativeNamespaceAnchor)
	}
	// Use only the inherited activation-time proc directory, even across
	// the exec that enters the owning user namespace.
	if err := checkNativeNamespaceAnchor(target); err != nil {
		return err
	}
	shared := "ns/net"
	if dir != "" {
		shared = "ns/ipc"
	}
	fd, err := unix.Openat(target, shared, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("%w: open %s: %v", errNativeNamespaceAnchor, shared, err)
	}
	defer unix.Close(fd)
	owner, err := unix.IoctlRetInt(fd, unix.NS_GET_USERNS)
	if err != nil {
		return fmt.Errorf("get native namespace owner: %w", err)
	}
	defer unix.Close(owner)
	var ownerStat, userStat unix.Stat_t
	if err := unix.Fstat(owner, &ownerStat); err != nil {
		return err
	}
	if err := unix.Fstatat(target, "ns/user", &userStat, 0); err != nil {
		return fmt.Errorf("%w: inspect anchor user namespace: %v", errNativeNamespaceAnchor, err)
	}
	if err := checkNativeNamespaceAnchor(target); err != nil {
		return err
	}
	proc := fmt.Sprintf("/proc/self/fd/%d", target)
	join := []string{enter, "--preserve-credentials", "--user=" + proc + "/ns/user", "--net=" + proc + "/ns/net"}
	if dir != "" {
		// nsenter's PID entry must fork so that the payload becomes a member.
		// util-linux versions with an optional --wdns argument require '='.
		join = append(join,
			"--mount="+proc+"/ns/mnt", "--pid="+proc+"/ns/pid",
			"--ipc="+proc+"/ns/ipc", "--root="+proc+"/root", "--wdns="+dir)
	}
	join = append(join, "--")
	join = append(join, args[3:]...)
	if ownerStat.Dev != userStat.Dev || ownerStat.Ino != userStat.Ino {
		// Bubblewrap's private devpts setup can put the anchor in a nested
		// user namespace. Enter the owner first so nsenter can join the shared
		// namespaces before switching to the anchor's final user namespace.
		ownerFD, err := unix.FcntlInt(uintptr(owner), unix.F_DUPFD, 5)
		if err != nil {
			return err
		}
		defer unix.Close(ownerFD)
		join = append([]string{enter, fmt.Sprintf("--user=/proc/self/fd/%d", ownerFD), "--preserve-credentials", "--"}, join...)
	}
	return unix.Exec(enter, join, os.Environ())
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
	if err := g.namespace.checkAnchor(); err != nil {
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
		return errors.Join(fmt.Errorf("native namespace member handshake: %w", err), g.namespace.checkAnchor())
	}
	m.signalTarget = target
	m.PayloadPID, m.PayloadBirth = pid, birth
	g.custody.Groups[g.supervisor.Process.Pid][pid] = birth
	if err := g.saveLocked(); err != nil {
		return err
	}
	if err := g.ctx.Err(); err != nil {
		return err
	}
	if err := g.namespace.checkAnchor(); err != nil {
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
	if n == nil || n.support.mode != "namespace" {
		return false, false, nil
	}
	if err := n.checkAnchor(); err != nil {
		return true, false, err
	}
	executable, err := os.Executable()
	if err != nil {
		return true, false, err
	}
	mode := "probe"
	if listener {
		mode = "listener"
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable)
	args := append(n.joinArgs(cmd, executable, ""), executable, "_native-exec", mode, port, strconv.Itoa(group))
	if !listener {
		args = append(args, path)
	}
	cmd.Args = args
	cmd.Env = environmentList(jobBaseEnvironment())
	out, err := cmd.Output()
	if anchorErr := n.checkAnchor(); anchorErr != nil {
		return true, false, anchorErr
	}
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

func (g *nativeGroup) memberSignalCapture(m *nativeMember) SignalCapture {
	r := SignalCapture{StartedAt: m.PayloadStartedAt, Directory: m.cmd.Dir}
	if m.memory != nil {
		r.Cgroup = signalCgroup(m.memory.path)
	}
	if g.namespace == nil || g.namespace.pid == 0 {
		r.PID, r.Birth = m.PID, m.Birth
	} else {
		r.PID, r.Birth = m.PayloadPID, m.PayloadBirth
	}
	return r
}
