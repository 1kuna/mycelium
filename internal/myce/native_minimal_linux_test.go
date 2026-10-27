package myce

import (
	"context"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func init() {
	if len(os.Args) < 2 {
		return
	}
	var err error
	switch os.Args[1] {
	case "_native-minimal-baseline":
		os.Exit(0)
	case "_native-minimal-connect":
		fd, e := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		err = e
		if err == nil {
			err = unix.Connect(fd, &unix.SockaddrUnix{Name: os.Args[2]})
			unix.Close(fd)
		}
	case "_native-minimal-attack":
		err = nativeMinimalAttack()
	case "_native-minimal-sidecar":
		err = nativeMinimalSidecar()
	default:
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// These tests plan/open disposable files only; no namespace or payload launch.
func TestNativeMinimalRootPinnedConstruction(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "regular")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	b := &nativeMinimalBuilder{seen: map[string]bool{}, read: []string{root}}
	defer b.close()
	if err := b.tree(unix.AT_FDCWD, root, root); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	// Replacing a previously validated regular file with a live socket does
	// not change the FD-pinned source of the already constructed mount.
	fd := nativeMinimalListener(t, path)
	defer unix.Close(fd)
	if len(b.receipt.Files) != 1 {
		t.Fatalf("wrong pinned file set: %+v", b.receipt.Files)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(b.fds[0].Fd()), &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
		t.Fatal("pinned source changed type", err)
	}
	aliasFD, err := unix.Open(path+".old", unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(aliasFD)
	if err := nativeMinimalWritableFD(aliasFD, b.receipt.Files); err == nil {
		t.Fatal("writable hardlink/inode alias accepted")
	}
	for _, arg := range b.args {
		if arg == "--bind" || arg == "--ro-bind" {
			t.Fatal("read tree was writable")
		}
	}
	b2 := &nativeMinimalBuilder{seen: map[string]bool{}, read: []string{root}}
	defer b2.close()
	if err := b2.tree(unix.AT_FDCWD, root, root); err == nil {
		t.Fatal("live SEQPACKET socket accepted as read mount")
	}
	if nativeMinimalKernelRoot("/sys") || nativeMinimalKernelRoot(root) {
		t.Fatal("broad/operator root treated as immutable sysfs")
	}
	if err := b.kernelDirectory(root, "/sys/devices"); err == nil {
		t.Fatal("ordinary directory accepted as kernel sysfs")
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(path, alias); err != nil {
		t.Fatal(err)
	}
	b3 := &nativeMinimalBuilder{seen: map[string]bool{}, read: []string{alias}}
	defer b3.close()
	if err := b3.tree(unix.AT_FDCWD, alias, alias); err == nil {
		t.Fatal("escaping socket symlink accepted")
	}
}

func TestNativeMinimalRootCapabilityRefusal(t *testing.T) {
	work := t.TempDir()
	planner := exec.Command("/bin/true")
	planner.Dir = work
	defaultPolicy, err := (Worker{}).nativeLinuxPolicy(planner, t.TempDir(), work, map[string]string{"MYCE_NATIVE_MINIMAL_ROOT": "1"}, nil)
	if err != nil || defaultPolicy.MinimalRoot {
		t.Fatal("authored environment enabled minimal-root mode", err)
	}
	w := Worker{NativeMinimalRoot: true, StateDir: t.TempDir()}
	key := nativeMinimalSupportKey(w)
	nativeMinimalDetection.Store(key, func() nativeLinuxSupport {
		return nativeLinuxSupport{mode: "unavailable", reason: "minimal_root_policy_denied"}
	})
	defer nativeMinimalDetection.Delete(key)
	if err := w.requireNativeMinimalRoot(); err == nil {
		t.Fatal("startup accepted missing minimal-root capability")
	}
	cmd := exec.Command("/bin/sh", "-c", `read gate; exec "$@"`, "myce-member-test", "/bin/true")
	before := slices.Clone(cmd.Args)
	if err := w.isolateNativeCommand(cmd, w.StateDir, t.TempDir(), map[string]string{"MYCE_NATIVE_MINIMAL_ROOT": "1"}, nil, false); err == nil || !strings.Contains(err.Error(), "minimal_root_policy_denied") {
		t.Fatal("opt-in fell back after refusal", err)
	}
	if !slices.Equal(before, cmd.Args) {
		t.Fatal("refused command was wrapped for execution")
	}
	// A request alone must not establish the new boundary, even if Namespace
	// is true. Existing host-root seccomp tests retain their SEQPACKET denial.
	if _, err := nativeVerifyMinimalRoot(nativeLinuxPolicy{Namespace: true, MinimalRoot: true}); err == nil {
		t.Fatal("policy boolean authorized minimal-root socket permission")
	}
}

func nativeMinimalListener(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	if err := unix.Listen(fd, 16); err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	client, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		unix.Close(fd)
		t.Fatal(err)
	}
	err = unix.Connect(client, &unix.SockaddrUnix{Name: path})
	peer, peerErr := unix.GetsockoptUcred(client, unix.SOL_SOCKET, unix.SO_PEERCRED)
	unix.Close(client)
	if err != nil || peerErr != nil || peer.Uid != uint32(os.Getuid()) {
		unix.Close(fd)
		t.Fatalf("same-type same-UID fixture not live: %v %v", err, peerErr)
	}
	return fd
}

// Runtime enforcement is explicitly opt-in and requires the operator's userns
// policy approval. No unavailable stack gets an unconfined substitute.
func TestNativeMinimalRootLinuxAttacks(t *testing.T) {
	if os.Getenv("MYCE_TEST_NATIVE_MINIMAL_ROOT") != "1" {
		t.Skip("requires explicit approved disposable Linux namespace qualification")
	}
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		t.Skip("minimal-root capability unavailable: bwrap missing")
	}
	help, helpErr := exec.Command(bwrap, "--help").CombinedOutput()
	if helpErr != nil || !strings.Contains(string(help), "--ro-bind-fd") || !strings.Contains(string(help), "--bind-fd") {
		t.Skip("minimal-root capability unavailable: bubblewrap FD binds required (0.10+)")
	}
	if _, err := exec.LookPath("nsenter"); err != nil {
		t.Skip("minimal-root capability unavailable: nsenter missing")
	}
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 6 {
		t.Skip("minimal-root capability unavailable: Landlock ABI 6 required")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	image, err := elf.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	for _, program := range image.Progs {
		if program.Type == elf.PT_INTERP {
			t.Skip("disposable probe requires a static test binary (CGO_ENABLED=0); no ambient host runtime import")
		}
	}
	work, cache, runtimeRoot := t.TempDir(), t.TempDir(), t.TempDir()
	for _, path := range []string{filepath.Join(work, ".tmp"), filepath.Join(work, ".scratch")} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	model := filepath.Join(cache, "model-tool")
	source, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	dest, err := os.OpenFile(model, os.O_CREATE|os.O_WRONLY, 0700)
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(dest, source)
	if err := errors.Join(copyErr, dest.Close(), source.Close()); err != nil {
		t.Fatal(err)
	}
	tool := filepath.Join(runtimeRoot, "tool")
	if err := os.Link(executable, tool); err != nil {
		t.Fatal(err)
	}
	foreign, control := nativeIsolationLinuxSeqpacketFixtures(t)
	sibling := filepath.Join(t.TempDir(), "sibling.sock")
	siblingFD := nativeMinimalListener(t, sibling)
	defer unix.Close(siblingFD)
	candidateRoot := t.TempDir()
	candidate := filepath.Join(candidateRoot, "control.sock")
	candidateFD := nativeMinimalListener(t, candidate)
	defer unix.Close(candidateFD)
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	queue, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		l.Close()
		t.Fatal(err)
	}
	defer queue.Close()
	baselineTCP, err := net.Dial("tcp4", queue.Addr().String())
	if err != nil {
		l.Close()
		t.Fatal("queue TCP fixture is not live", err)
	}
	baselineTCP.Close()
	l.Close()
	env := jobBaseEnvironment()
	env["MYCE_MINIMAL_FOREIGN"], env["MYCE_MINIMAL_CONTROL"], env["MYCE_MINIMAL_SIBLING"], env["MYCE_MINIMAL_CANDIDATE"] = foreign, control, sibling, candidate
	env["MYCE_MINIMAL_MODEL"], env["MYCE_MINIMAL_TOOL"], env["MYCE_MINIMAL_PORT"] = model, tool, strconv.Itoa(port)
	env["MYCE_MINIMAL_QUEUE_PORT"] = strconv.Itoa(queue.Addr().(*net.TCPAddr).Port)
	env["MYCE_SCRATCH_DIR"], env["TMPDIR"] = filepath.Join(work, ".scratch"), filepath.Join(work, ".tmp")
	membership, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	own, err := nativeCurrentCgroup(membership, mounts)
	if err != nil {
		t.Skipf("minimal-root metrics capability unavailable: %v", err)
	}
	env["MYCE_MINIMAL_METRICS"] = own
	metricRoot := t.TempDir()
	for _, leaf := range []string{"main", "sidecar", "sibling", "aggregate", "control"} {
		dir := filepath.Join(metricRoot, leaf)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range append(slices.Clone(nativeMemoryMetricNames), "cgroup.procs", "memory.high") {
			if err := os.WriteFile(filepath.Join(dir, name), []byte("synthetic"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	env["MYCE_MINIMAL_MEMBER_METRICS"] = metricRoot
	p := nativeLinuxPolicy{Namespace: true, MinimalRoot: true, Read: []string{executable, runtimeRoot, "/proc/meminfo", "/sys/devices"}, ReadOnly: []string{cache}, Write: []string{work}, Ports: []uint16{uint16(port)}, MetricCgroups: []string{filepath.Join(metricRoot, "main"), filepath.Join(metricRoot, "sidecar")}, Env: environmentList(env)}
	smoke, err := nativePolicyArgs(p, []string{executable, "_native-minimal-baseline"})
	if err != nil {
		t.Fatal(err)
	}
	smoke, err = nativeMinimalLaunchArgs(bwrap, p, smoke, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probe := exec.CommandContext(ctx, smoke[0], smoke[1:]...)
	probe.Dir, probe.Env = work, environmentList(jobBaseEnvironment())
	out, err := probe.CombinedOutput()
	if err != nil {
		message := strings.ToLower(string(out))
		if strings.Contains(message, "operation not permitted") || strings.Contains(message, "permission denied") {
			t.Skipf("minimal-root service-context policy refusal: %v: %s", err, out)
		}
		t.Fatalf("valid minimal-root capability probe: %v\n%s", err, out)
	}
	// Demonstrate the actual defect with the same live endpoint type: even a
	// read-only mount admits pathname connect. This is a disposable baseline.
	baseline := exec.CommandContext(ctx, bwrap, append(append(nativeBwrapBase(), "--ro-bind", executable, executable, "--ro-bind", candidateRoot, candidateRoot, "--"), executable, "_native-minimal-connect", candidate)...)
	baseline.Env = environmentList(jobBaseEnvironment())
	if out, err := baseline.CombinedOutput(); err != nil {
		t.Fatalf("read-only SEQPACKET baseline: %v\n%s", err, out)
	}
	for _, unsafe := range []string{candidateRoot, sibling} {
		bad := p
		bad.ReadOnly = append(slices.Clone(p.ReadOnly), unsafe)
		if b, err := nativeBuildMinimalRoot(bad, false); err == nil {
			b.close()
			t.Errorf("live socket candidate accepted: %s", unsafe)
		}
	}
	aliasRoot := t.TempDir()
	if err := os.Symlink(candidate, filepath.Join(aliasRoot, "escape")); err != nil {
		t.Fatal(err)
	}
	bad := p
	bad.ReadOnly = append(slices.Clone(p.ReadOnly), aliasRoot)
	if b, err := nativeBuildMinimalRoot(bad, false); err == nil {
		b.close()
		t.Fatal("socket path alias accepted")
	}
	args, err := nativePolicyArgs(p, []string{executable, "_native-minimal-attack"})
	if err != nil {
		t.Fatal(err)
	}
	args, err = nativeMinimalLaunchArgs(bwrap, p, args, false)
	if err != nil {
		t.Fatal(err)
	}
	attack := exec.CommandContext(ctx, args[0], args[1:]...)
	attack.Dir, attack.Env = work, environmentList(jobBaseEnvironment())
	if out, err := attack.CombinedOutput(); err != nil {
		t.Fatalf("minimal-root attack: %v\n%s", err, out)
	}
	if err := os.Remove(filepath.Join(work, ".scratch", "ready")); err != nil {
		t.Fatal(err)
	}
	if out, err := nativeMinimalServiceProbe(ctx, p, nativeLinuxSupport{bwrap: bwrap, nsenter: mustNativeMinimalNSenter(t)}, work, []string{executable, "_native-minimal-attack"}); err != nil {
		t.Fatalf("joined anchor attack: %v\n%s", err, out)
	}
	// Exercise the validation-to-mount replacement race against the actual
	// constructed mounts as well as the planner test above. The host endpoint
	// is live and same type; the payload still sees the pinned regular inode.
	replacementRoot := t.TempDir()
	replacement := filepath.Join(replacementRoot, "replaced")
	if err := os.WriteFile(replacement, []byte("pinned regular"), 0600); err != nil {
		t.Fatal(err)
	}
	raced := p
	raced.ReadOnly = append(slices.Clone(p.ReadOnly), replacementRoot)
	env["MYCE_MINIMAL_CANDIDATE"] = replacement
	raced.Env = environmentList(env)
	b, err := nativeBuildMinimalRoot(raced, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	if err := os.Rename(replacement, replacement+".old"); err != nil {
		t.Fatal(err)
	}
	replacementFD := nativeMinimalListener(t, replacement)
	defer unix.Close(replacementFD)
	args, err = nativePolicyArgs(raced, []string{executable, "_native-minimal-attack"})
	if err != nil {
		t.Fatal(err)
	}
	mountArgs := slices.Clone(b.args)
	for i, arg := range mountArgs {
		for j, file := range b.fds {
			if arg == strconv.Itoa(int(file.Fd())) {
				mountArgs[i] = strconv.Itoa(j + 3)
				break
			}
		}
	}
	command := exec.CommandContext(ctx, bwrap, append(append(mountArgs, "--"), args...)...)
	command.ExtraFiles, command.Dir, command.Env = b.fds, work, environmentList(jobBaseEnvironment())
	// Remove the previous own sidecar's disposable readiness artifact.
	if err := os.Remove(filepath.Join(work, ".scratch", "ready")); err != nil {
		t.Fatal(err)
	}
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("pinned replacement attack: %v\n%s", err, out)
	}
}

func nativeMinimalAttack() error {
	if err := nativeIsolationLinuxUnixSocketDenials(false); err != nil {
		return err
	}
	for _, flags := range []int{0, unix.SOCK_CLOEXEC, unix.SOCK_NONBLOCK, unix.SOCK_CLOEXEC | unix.SOCK_NONBLOCK} {
		if err := nativeIsolationLinuxOwnSeqpacket(flags); err != nil {
			return err
		}
	}
	for _, key := range []string{"MYCE_MINIMAL_CONTROL", "MYCE_MINIMAL_SIBLING", "MYCE_MINIMAL_CANDIDATE", "MYCE_MINIMAL_FOREIGN"} {
		fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			return err
		}
		err = unix.Connect(fd, &unix.SockaddrUnix{Name: os.Getenv(key)})
		unix.Close(fd)
		if err == nil {
			return fmt.Errorf("foreign same-type endpoint reached: %s", key)
		}
		if !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.EPERM) && !errors.Is(err, unix.ECONNREFUSED) && !errors.Is(err, unix.ENOTSOCK) {
			return fmt.Errorf("unexpected endpoint denial %s: %w", key, err)
		}
	}
	for _, path := range []string{"/run/own", "/tmp/own", "/dev/shm/own", filepath.Join(os.Getenv("MYCE_SCRATCH_DIR"), "main")} {
		if err := os.WriteFile(path, []byte("own"), 0600); err != nil {
			return err
		}
	}
	model := os.Getenv("MYCE_MINIMAL_MODEL")
	if _, err := os.ReadFile(model); err != nil {
		return err
	}
	if f, err := os.OpenFile(model, os.O_WRONLY, 0); err == nil {
		f.Close()
		return errors.New("model writable")
	}
	if err := exec.Command(model, "_native-minimal-baseline").Run(); !errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("read-only model execution not denied: %v", err)
	}
	tool := os.Getenv("MYCE_MINIMAL_TOOL")
	if data, err := os.ReadFile(tool); err != nil || len(data) == 0 {
		return fmt.Errorf("runtime/cache read: %v", err)
	}
	if f, err := os.OpenFile(tool, os.O_WRONLY, 0); err == nil {
		f.Close()
		return errors.New("runtime/cache writable")
	}
	if err := exec.Command(tool, "_native-minimal-baseline").Run(); err != nil {
		return fmt.Errorf("runtime/cache execution: %w", err)
	}
	queuePort, err := strconv.Atoi(os.Getenv("MYCE_MINIMAL_QUEUE_PORT"))
	if err != nil {
		return err
	}
	queueFD, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	err = unix.Connect(queueFD, &unix.SockaddrInet4{Port: queuePort, Addr: [4]byte{127, 0, 0, 1}})
	unix.Close(queueFD)
	if !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.EPERM) {
		return fmt.Errorf("queue TCP port not Landlock denied: %v", err)
	}
	if _, err := os.ReadFile("/proc/meminfo"); err != nil {
		return err
	}
	if _, err := os.ReadFile("/proc/self/cgroup"); err != nil {
		return err
	}
	for _, name := range nativeMemoryMetricNames {
		path := filepath.Join(os.Getenv("MYCE_MINIMAL_METRICS"), name)
		if _, err := os.ReadFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("own metric: %w", err)
		}
		if f, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
			f.Close()
			return errors.New("metric writable")
		}
		for _, leaf := range []string{"main", "sidecar"} {
			path := filepath.Join(os.Getenv("MYCE_MINIMAL_MEMBER_METRICS"), leaf, name)
			if data, err := os.ReadFile(path); err != nil || string(data) != "synthetic" {
				return fmt.Errorf("own member metric missing: %s: %v", path, err)
			}
			if f, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
				f.Close()
				return fmt.Errorf("own member metric writable: %s", path)
			}
		}
		for _, leaf := range []string{"sibling", "aggregate", "control"} {
			if _, err := os.ReadFile(filepath.Join(os.Getenv("MYCE_MINIMAL_MEMBER_METRICS"), leaf, name)); err == nil {
				return fmt.Errorf("non-member metric readable: %s", leaf)
			}
		}
	}
	for _, path := range []string{filepath.Join(os.Getenv("MYCE_MINIMAL_METRICS"), "cgroup.procs"), filepath.Join(os.Getenv("MYCE_MINIMAL_METRICS"), "memory.high"), filepath.Join(filepath.Dir(os.Getenv("MYCE_MINIMAL_METRICS")), "memory.current")} {
		if _, err := os.ReadFile(path); err == nil {
			return fmt.Errorf("control/ancestor metric readable: %s", path)
		}
		if f, err := os.OpenFile(path, os.O_WRONLY, 0); err == nil {
			f.Close()
			return fmt.Errorf("control writable: %s", path)
		}
	}
	if err := unix.Unshare(unix.CLONE_NEWNS); !errors.Is(err, unix.EPERM) {
		return fmt.Errorf("unshare not seccomp denied: %v", err)
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(executable, "_native-minimal-sidecar")
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		return err
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(os.Getenv("MYCE_SCRATCH_DIR"), "ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("own sidecar did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://127.0.0.1:" + os.Getenv("MYCE_MINIMAL_PORT"))
	if err != nil {
		return err
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "own" {
		return errors.New("sidecar loopback/scratch exchange failed")
	}
	return nil
}

func nativeMinimalSidecar() error {
	l, err := net.Listen("tcp4", "127.0.0.1:"+os.Getenv("MYCE_MINIMAL_PORT"))
	if err != nil {
		return err
	}
	defer l.Close()
	if err := os.WriteFile(filepath.Join(os.Getenv("MYCE_SCRATCH_DIR"), "ready"), []byte("ready"), 0600); err != nil {
		return err
	}
	conn, err := l.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	body, err := os.ReadFile(filepath.Join(os.Getenv("MYCE_SCRATCH_DIR"), "main"))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(conn, "HTTP/1.0 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	return err
}

func mustNativeMinimalNSenter(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("nsenter")
	if err != nil {
		t.Fatal(err)
	}
	return path
}
