package myce

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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

// Dispatch before TestMain's fixture provisioning: the hostile payload must
// not create or acquire worker-owned directories on its way to the assertions.
func init() {
	if len(os.Args) > 1 && os.Args[1] == "_native-isolation-test" {
		if err := nativeIsolationLinuxAttack(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Fprint(os.Stdout, "isolated")
		os.Exit(0)
	}
}

func nativeIsolationLinuxAttack() error {
	work := os.Getenv("MYCE_NATIVE_TEST_WORK")
	if work == "" {
		var err error
		work, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	if os.Getenv("TMPDIR") != filepath.Join(work, ".tmp") {
		return errors.New("worker temp directory is not private")
	}
	if err := os.WriteFile(filepath.Join(work, "own-output"), []byte("own"), 0600); err != nil {
		return fmt.Errorf("own work: %w", err)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("TMPDIR"), "own-temp"), []byte("own"), 0600); err != nil {
		return fmt.Errorf("own temp: %w", err)
	}
	// Match CUDA's pthread_setname_np path: rename another live thread in
	// this process, while the protected list below still denies worker proc.
	threads, err := filepath.Glob("/proc/self/task/*/comm")
	if err != nil {
		return err
	}
	renamed := false
	for _, path := range threads {
		if filepath.Base(filepath.Dir(path)) == strconv.Itoa(os.Getpid()) {
			continue
		}
		before, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte("myce-gpu-test"), 0600); err != nil {
			return fmt.Errorf("own runtime thread name: %w", err)
		}
		after, err := os.ReadFile(path)
		if restoreErr := os.WriteFile(path, before, 0600); restoreErr != nil {
			return restoreErr
		}
		if err != nil || strings.TrimSpace(string(after)) != "myce-gpu-test" {
			return errors.New("own runtime thread name was not written")
		}
		renamed = true
		break
	}
	if !renamed {
		return errors.New("missing runtime thread for GPU naming regression")
	}
	for _, path := range strings.Split(os.Getenv("MYCE_NATIVE_TEST_PROTECTED"), string(os.PathListSeparator)) {
		if _, err := os.ReadFile(path); err == nil {
			return errors.New("protected contents readable")
		}
		if err := os.WriteFile(path, []byte("poisoned"), 0600); err == nil {
			return errors.New("protected contents writable")
		}
	}
	for _, domain := range []int{unix.AF_UNIX, unix.AF_PACKET, unix.AF_NETLINK} {
		if fd, err := unix.Socket(domain, unix.SOCK_STREAM, 0); err == nil {
			unix.Close(fd)
			return errors.New("forbidden socket created")
		}
	}
	if pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0); !errors.Is(err, unix.EPERM) {
		if err == nil {
			unix.Close(pair[0])
			unix.Close(pair[1])
		}
		return errors.New("reconnectable Unix datagram socketpair not denied")
	}
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("private stream socketpair: %w", err)
	}
	defer unix.Close(pair[0])
	if _, err := unix.Write(pair[0], []byte("x")); err != nil {
		return err
	}
	var value [1]byte
	if _, err := unix.Read(pair[1], value[:]); err != nil || value[0] != 'x' {
		return errors.New("private stream IPC failed")
	}
	control := &unix.SockaddrUnix{Name: os.Getenv("MYCE_NATIVE_TEST_SOCKET")}
	if err := unix.Connect(pair[0], control); err == nil {
		return errors.New("connected stream pair reached control socket")
	}
	if err := unix.Shutdown(pair[0], unix.SHUT_RDWR); err != nil {
		return err
	}
	if err := unix.Connect(pair[0], control); err == nil {
		return errors.New("shutdown stream pair reached control socket")
	}
	unix.Close(pair[1])
	if err := unix.Connect(pair[0], control); err == nil {
		return errors.New("closed-peer stream pair reached control socket")
	}
	if fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0); err == nil {
		unix.Close(fd)
		return errors.New("offline UDP created")
	}
	for _, protocol := range []int{unix.IPPROTO_SCTP, unix.IPPROTO_MPTCP} {
		if fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, protocol); !errors.Is(err, unix.EPERM) {
			if err == nil {
				unix.Close(fd)
			}
			return errors.New("non-TCP protocol not seccomp denied")
		}
	}
	for _, address := range []string{os.Getenv("MYCE_NATIVE_TEST_CONTROL"), "unix:" + os.Getenv("MYCE_NATIVE_TEST_SOCKET")} {
		network := "tcp4"
		if strings.HasPrefix(address, "unix:") {
			network, address = "unix", strings.TrimPrefix(address, "unix:")
		}
		if conn, err := net.DialTimeout(network, address, time.Second); err == nil {
			conn.Close()
			return errors.New("control listener reachable")
		}
	}
	if address := os.Getenv("MYCE_NATIVE_TEST_SIDECAR"); address != "" {
		conn, err := net.DialTimeout("tcp4", address, time.Second)
		if err != nil {
			return fmt.Errorf("declared sidecar unreachable: %w", err)
		}
		conn.Close()
	}
	if _, _, errno := unix.Syscall6(unix.SYS_PROCESS_VM_READV, uintptr(os.Getppid()), 0, 0, 0, 0, 0); errno != unix.EPERM {
		return errors.New("process memory access not seccomp denied")
	}
	// The harmless SIGCONT still exercises Landlock's external signal scope.
	if err := unix.Kill(os.Getppid(), unix.SIGCONT); !errors.Is(err, unix.EPERM) {
		return errors.New("external process signal not denied")
	}
	if os.Getenv("MYCE_NATIVE_TEST_SEQPACKET_PATH") != "" {
		return nativeIsolationLinuxSeqpacketAttack()
	}
	return nil
}

func nativeIsolationLinuxControlSocket(t *testing.T) string {
	t.Helper()
	// Gate/job TMPDIR paths can exceed Linux's 108-byte sockaddr_un limit.
	// Keep a real pathname socket so the control-access attack stays covered.
	dir, err := os.MkdirTemp("/tmp", "myce-isolation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return filepath.Join(dir, "control.sock")
}

func TestNativeIsolationLinuxWorkerSiblingAndControl(t *testing.T) {
	if support := nativeLinuxDetection(); support.mode == "unavailable" {
		t.Skipf("host confinement unavailable: %s", support.reason)
	}
	setWorkerSession(t)
	w := recoveryWorker(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"ok":true}`) }))
	defer api.Close()
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	credential := filepath.Join(t.TempDir(), "synthetic-worker-credential")
	if err := os.WriteFile(credential, []byte("synthetic-private"), 0600); err != nil {
		t.Fatal(err)
	}
	socket := nativeIsolationLinuxControlSocket(t)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	start := func(command []string, env map[string]string) (*process, *workerStore) {
		t.Helper()
		job := recoveryJob(t, command)
		job.Spec.Resumable, job.Spec.Logs, job.Spec.Run.Env = false, "keep", env
		store, err := newAttemptStore(root, job)
		if err != nil {
			t.Fatal(err)
		}
		p, err := w.start(store, job)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.stop(w.Clock.Now(), 0, true) })
		return p, store
	}
	sibling, siblingStore := start([]string{"/bin/sh", "-c", `printf sibling >secret; printf sibling >"$TMPDIR/secret"; touch ready; while :; do sleep 0.05; done`}, nil)
	siblingWork := sibling.job.Spec.Run.Workdir
	await(t, func() bool {
		select {
		case finish := <-sibling.done:
			out, _ := os.ReadFile(filepath.Join(siblingStore.dir, siblingStore.state.Log))
			t.Fatalf("sibling exited: %+v\n%s", finish, out)
		default:
		}
		_, err := os.Stat(filepath.Join(siblingWork, "ready"))
		return err == nil
	})
	defer func() {
		sibling.stop(w.Clock.Now(), 0, true)
		select {
		case finish := <-sibling.done:
			if finish.err != nil || finish.result.ReleasePending {
				t.Errorf("sibling cleanup: %+v", finish)
			}
		case <-time.After(10 * time.Second):
			t.Error("sibling cleanup timed out")
		}
	}()
	protected := []string{filepath.Join(siblingWork, "secret"), filepath.Join(siblingWork, ".tmp", "secret"), credential, fmt.Sprintf("/proc/%d/environ", os.Getpid()), fmt.Sprintf("/proc/%d/task/%d/comm", os.Getpid(), os.Getpid())}
	for _, path := range protected {
		if _, err := os.ReadFile(path); err != nil {
			t.Fatal("protected fixture unavailable", err)
		}
	}
	for _, target := range []struct{ network, address string }{{"unix", socket}, {"tcp4", strings.TrimPrefix(api.URL, "http://")}} {
		conn, err := net.DialTimeout(target.network, target.address, time.Second)
		if err != nil {
			t.Fatal("control fixture unavailable", err)
		}
		conn.Close()
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	attack, store := start([]string{executable, "_native-isolation-test"}, map[string]string{"MYCE_NATIVE_TEST_PROTECTED": strings.Join(protected, string(os.PathListSeparator)), "MYCE_NATIVE_TEST_CONTROL": strings.TrimPrefix(api.URL, "http://"), "MYCE_NATIVE_TEST_SOCKET": socket, "TMPDIR": "/tmp/ignored-authored-temp"})
	select {
	case finish := <-attack.done:
		out, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
		if finish.err != nil || !finish.result.Success || finish.result.ReleasePending || !strings.Contains(string(out), "isolated") {
			t.Fatalf("worker attack: %+v\n%s", finish, out)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker attack timed out")
	}
	for _, path := range []string{"own-output", ".tmp/own-temp"} {
		if _, err := os.Stat(filepath.Join(attack.job.Spec.Run.Workdir, path)); err != nil {
			t.Error(err)
		}
	}
	if data, err := os.ReadFile(credential); err != nil || string(data) != "synthetic-private" {
		t.Errorf("credential changed: %v", err)
	}
}

func TestNativeIsolationLinuxLoaderEnvironment(t *testing.T) {
	if support := nativeLinuxDetection(); support.mode == "unavailable" {
		t.Skipf("host confinement unavailable: %s", support.reason)
	}
	compiler, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("C compiler required for real dynamic-loader attack fixture")
	}
	root := t.TempDir()
	marker := filepath.Join(root, "escaped")
	source := filepath.Join(root, "preload.c")
	library := filepath.Join(root, "preload.so")
	if err := os.WriteFile(source, []byte(`#include <stdio.h>
#include <stdlib.h>
__attribute__((constructor)) static void attack(void) {
 const char *p = getenv("NATIVE_PRELOAD_MARKER");
 if (p) { FILE *f = fopen(p, "w"); if (f) { fputs("escaped", f); fclose(f); } }
}
`), 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(compiler, "-shared", "-fPIC", "-o", library, source).CombinedOutput(); err != nil {
		t.Fatalf("preload fixture compilation: %v\n%s", err, out)
	}
	baseline := exec.Command("/bin/true")
	baseline.Env = append(os.Environ(), "LD_PRELOAD="+library, "NATIVE_PRELOAD_MARKER="+marker)
	if err := baseline.Run(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal("constructor baseline did not execute", err)
	}
	for _, group := range []bool{false, true} {
		t.Run(strconv.FormatBool(group), func(t *testing.T) {
			w := recoveryWorker(t)
			w.JobFileRoot = root
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			j := recoveryJob(t, []string{"/bin/true"})
			j.Spec.Resumable = false
			j.Spec.Inputs = map[string]string{"preload.so": "preload.so"}
			j.Spec.Run.Env = map[string]string{"LD_PRELOAD": "{work}/preload.so", "NATIVE_PRELOAD_MARKER": marker}
			if group {
				j.Spec.Run.MemoryGB = .125
				j.Spec.Needs.HostMemoryGB = j.Spec.Run.MemoryGB
				w.HostMemorySource = func(context.Context) (float64, error) { return 1, nil }
			}
			p, err := w.start(store, j)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case finish := <-p.done:
				if finish.err != nil || !finish.result.Success || finish.result.ReleasePending {
					out, _ := os.ReadFile(filepath.Join(store.dir, store.state.Log))
					t.Fatalf("preload payload: %+v\n%s", finish, out)
				}
			case <-time.After(10 * time.Second):
				p.stop(w.Clock.Now(), 0, true)
				t.Fatal("preload payload timed out")
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("authored loader environment escaped before confinement: %v", err)
			}
		})
	}
}

func TestNativeIsolationLinuxFallback(t *testing.T) {
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 6 {
		t.Skip("Landlock ABI 6 unavailable; no hygiene-only fallback")
	}
	for _, sidecars := range []bool{false, true} {
		t.Run(strconv.FormatBool(sidecars), func(t *testing.T) {
			root := t.TempDir()
			work := filepath.Join(root, "own")
			if err := os.MkdirAll(filepath.Join(work, ".tmp"), 0700); err != nil {
				t.Fatal(err)
			}
			secret := filepath.Join(root, "worker-credential")
			sibling := filepath.Join(root, "sibling-output")
			for _, path := range []string{secret, sibling} {
				if err := os.WriteFile(path, []byte("synthetic-private"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			control, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer control.Close()
			socket := nativeIsolationLinuxControlSocket(t)
			unixListener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer unixListener.Close()
			env := map[string]string{"TMPDIR": filepath.Join(work, ".tmp"), "MYCE_NATIVE_TEST_WORK": work, "MYCE_NATIVE_TEST_PROTECTED": strings.Join([]string{secret, sibling, fmt.Sprintf("/proc/%d/environ", os.Getpid()), fmt.Sprintf("/proc/%d/task/%d/comm", os.Getpid(), os.Getpid())}, string(os.PathListSeparator)), "MYCE_NATIVE_TEST_CONTROL": control.Addr().String(), "MYCE_NATIVE_TEST_SOCKET": socket}
			abstract, pathname := nativeIsolationLinuxSeqpacketFixtures(t)
			env["MYCE_NATIVE_TEST_SEQPACKET_ABSTRACT"] = abstract
			env["MYCE_NATIVE_TEST_SEQPACKET_PATH"] = pathname
			ports := []string{}
			if sidecars {
				listener, err := net.Listen("tcp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				_, port, _ := net.SplitHostPort(listener.Addr().String())
				ports = append(ports, port)
				env["MYCE_NATIVE_TEST_SIDECAR"] = listener.Addr().String()
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("/bin/sh", "-c", `read gate || exit 125; exec "$@"`, "myce-member-attack", executable, "_native-isolation-test")
			cmd.Dir = work
			policy, err := (Worker{}).nativeLinuxPolicy(cmd, root, work, env, ports)
			if err != nil {
				t.Fatal(err)
			}
			// The accepted pathname-connect residual must not depend on a file
			// grant to the fixture, even when sidecar TCP ports are present.
			for _, grant := range append(append(slices.Clone(policy.Read), policy.ReadOnly...), policy.Write...) {
				canonical, err := nativeCanonical(grant)
				if err != nil {
					continue // Optional runtime roots need not exist.
				}
				if rel, err := filepath.Rel(canonical, pathname); err == nil && (rel == "." || filepath.IsLocal(rel)) {
					t.Fatal("pathname SEQPACKET fixture falls inside a file grant")
				}
			}
			args, err := nativePolicyArgs(policy, cmd.Args[4:])
			if err != nil {
				t.Fatal(err)
			}
			if err := nativeWrapPayload(cmd, args); err != nil {
				t.Fatal(err)
			}
			cmd.Env = environmentList(env)
			cmd.Stdin = strings.NewReader("start\n")
			out, err := cmd.CombinedOutput()
			if err != nil || string(out) != "isolated" {
				t.Fatalf("confined attack: %v\n%s", err, out)
			}
			for _, path := range []string{secret, sibling} {
				if data, err := os.ReadFile(path); err != nil || string(data) != "synthetic-private" {
					t.Errorf("protected fixture changed: %v", err)
				}
			}
		})
	}
}

func TestNativeIsolationLinuxGateAndQueuePort(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command("/bin/sh", "-c", `read gate || exit 125; exec "$@"`, "myce-supervisor-test", "/bin/true")
	cmd.Dir = root
	gate := slices.Clone(cmd.Args[:4])
	ready, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Close()
	defer readyWriter.Close()
	cmd.ExtraFiles = []*os.File{readyWriter}
	if err := nativeWrapPayload(cmd, []string{"/native-helper", "/bin/true"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cmd.Args[:4], gate) || cmd.Path != "/bin/sh" || cmd.ExtraFiles[0] != readyWriter {
		t.Fatal("isolation replaced the worker gate or receipt descriptors")
	}
	w := Worker{Client: Client{URL: "http://127.0.0.1:8921"}}
	if _, err := w.nativeLinuxPolicy(cmd, root, root, nil, []string{"8921"}); err == nil || !strings.Contains(err.Error(), "control port") {
		t.Fatalf("queue port not rejected: %v", err)
	}
	if _, err := nativeIsolationSupport(true); err == nil || !strings.Contains(err.Error(), "native_network_unavailable") {
		t.Fatalf("unqualified egress accepted: %v", err)
	}
}
