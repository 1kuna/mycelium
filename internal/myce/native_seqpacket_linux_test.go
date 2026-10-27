package myce

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

func init() {
	if len(os.Args) == 3 && os.Args[1] == "_native-namespace-socket-test" {
		// Exercise the real namespace-mode seccomp policy without requiring
		// permission to create namespaces, and never confine the test runner.
		runtime.LockOSThread()
		err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)
		if err == nil {
			err = nativeApplySeccomp(true, os.Args[2] == "true")
		}
		if err == nil {
			err = nativeIsolationLinuxUnixSocketDenials(true)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		// The existing connected stream-pair exception also remains usable.
		pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		unix.Close(pair[0])
		unix.Close(pair[1])
		os.Exit(0)
	}
}

func TestNativeIsolationLinuxNamespaceSocketPolicy(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tcp := range []bool{false, true} {
		t.Run(strconv.FormatBool(tcp), func(t *testing.T) {
			if out, err := exec.Command(executable, "_native-namespace-socket-test", strconv.FormatBool(tcp)).CombinedOutput(); err != nil {
				t.Fatalf("namespace socket policy: %v\n%s", err, out)
			}
		})
	}
}

func nativeIsolationLinuxUnixSocketDenials(namespace bool) error {
	for _, kind := range []int{unix.SOCK_STREAM, unix.SOCK_DGRAM, unix.SOCK_SEQPACKET} {
		if kind == unix.SOCK_SEQPACKET && !namespace {
			continue
		}
		for _, flags := range []int{0, unix.SOCK_CLOEXEC, unix.SOCK_NONBLOCK, unix.SOCK_CLOEXEC | unix.SOCK_NONBLOCK} {
			fd, err := unix.Socket(unix.AF_UNIX, kind|flags, 0)
			if err == nil {
				unix.Close(fd)
			}
			if !errors.Is(err, unix.EPERM) {
				return fmt.Errorf("Unix socket type %#x flags %#x not seccomp denied: %v", kind, flags, err)
			}
		}
	}
	for _, request := range []struct{ kind, protocol int }{
		{unix.SOCK_SEQPACKET, 1},
		{unix.SOCK_SEQPACKET | 0x40000000, 0}, // Not CLOEXEC or NONBLOCK.
	} {
		fd, err := unix.Socket(unix.AF_UNIX, request.kind, request.protocol)
		if err == nil {
			unix.Close(fd)
		}
		if !errors.Is(err, unix.EPERM) {
			return fmt.Errorf("invalid SEQPACKET request not seccomp denied: %v", err)
		}
	}
	for _, kind := range []int{unix.SOCK_DGRAM, unix.SOCK_SEQPACKET} {
		pair, err := unix.Socketpair(unix.AF_UNIX, kind|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
		if err == nil {
			unix.Close(pair[0])
			unix.Close(pair[1])
		}
		if !errors.Is(err, unix.EPERM) {
			return fmt.Errorf("Unix socketpair type %#x not seccomp denied: %v", kind, err)
		}
	}
	return nil
}

// Called inside the existing fallback attack, with and without reserved TCP
// ports, after its sibling/temp/credential/process/stream-control assertions.
func nativeIsolationLinuxSeqpacketAttack() error {
	if err := nativeIsolationLinuxUnixSocketDenials(false); err != nil {
		return err
	}
	for _, flags := range []int{0, unix.SOCK_CLOEXEC, unix.SOCK_NONBLOCK, unix.SOCK_CLOEXEC | unix.SOCK_NONBLOCK} {
		if err := nativeIsolationLinuxOwnSeqpacket(flags); err != nil {
			return err
		}
	}
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Connect(fd, &unix.SockaddrUnix{Name: os.Getenv("MYCE_NATIVE_TEST_SEQPACKET_ABSTRACT")}); !errors.Is(err, unix.EACCES) && !errors.Is(err, unix.EPERM) {
		return fmt.Errorf("foreign abstract SEQPACKET not Landlock denied: %v", err)
	}
	// This is an explicitly accepted residual, not a file-tree grant or an
	// abstract-only claim: Landlock does not mediate pathname socket connects.
	if err := unix.Connect(fd, &unix.SockaddrUnix{Name: os.Getenv("MYCE_NATIVE_TEST_SEQPACKET_PATH")}); err != nil {
		return fmt.Errorf("accepted pathname SEQPACKET residual unavailable: %w", err)
	}
	peer, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil || peer.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("pathname SEQPACKET fixture is not same-UID: %v", err)
	}
	return nil
}

func nativeIsolationLinuxOwnSeqpacket(flags int) error {
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|flags, 0)
	if err != nil {
		return fmt.Errorf("own SEQPACKET flags %#x: %w", flags, err)
	}
	defer unix.Close(fd)
	descriptorFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil || (descriptorFlags&unix.FD_CLOEXEC != 0) != (flags&unix.SOCK_CLOEXEC != 0) {
		return fmt.Errorf("SEQPACKET CLOEXEC flag mismatch: %v", err)
	}
	statusFlags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil || (statusFlags&unix.O_NONBLOCK != 0) != (flags&unix.SOCK_NONBLOCK != 0) {
		return fmt.Errorf("SEQPACKET NONBLOCK flag mismatch: %v", err)
	}
	address := &unix.SockaddrUnix{Name: fmt.Sprintf("@myce-seqpacket-%d-%x", os.Getpid(), flags)}
	if err := unix.Bind(fd, address); err != nil {
		return fmt.Errorf("own abstract SEQPACKET bind: %w", err)
	}
	if err := unix.Listen(fd, 1); err != nil {
		return fmt.Errorf("own abstract SEQPACKET listen: %w", err)
	}
	client, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(client)
	if err := unix.Connect(client, address); err != nil {
		return fmt.Errorf("own abstract SEQPACKET connect: %w", err)
	}
	accepted, _, err := unix.Accept4(fd, unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK)
	if err != nil {
		return err
	}
	defer unix.Close(accepted)
	if _, err := unix.Write(client, []byte("x")); err != nil {
		return err
	}
	var data [1]byte
	if n, err := unix.Read(accepted, data[:]); err != nil || n != 1 || data[0] != 'x' {
		return fmt.Errorf("own abstract SEQPACKET IPC failed: %v", err)
	}
	return nil
}

func nativeIsolationLinuxSeqpacketFixtures(t *testing.T) (abstract, pathname string) {
	t.Helper()
	pathname = nativeIsolationLinuxControlSocket(t)
	abstract = "@myce-foreign-seqpacket-" + ID()
	for _, name := range []string{abstract, pathname} {
		fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { unix.Close(fd) })
		address := &unix.SockaddrUnix{Name: name}
		if err := unix.Bind(fd, address); err != nil {
			t.Fatal(err)
		}
		if err := unix.Listen(fd, 8); err != nil {
			t.Fatal(err)
		}
		// Prove both same-UID listeners accept connections before confinement,
		// so a missing or inaccessible fixture cannot masquerade as scoping.
		client, err := unix.Socket(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		err = unix.Connect(client, address)
		unix.Close(client)
		if err != nil {
			t.Fatalf("SEQPACKET fixture unavailable: %v", err)
		}
	}
	return abstract, pathname
}
