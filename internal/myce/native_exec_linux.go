package myce

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// NativeExec runs only in a disposable helper process, after the worker's
// cgroup/authorization gate. Restrictions apply to this OS thread and all of
// the payload's descendants; never call it in the worker process itself.
func NativeExec(args []string) error {
	if len(args) > 0 && args[0] == "namespace-monitor" {
		return nativeNamespaceMonitor(args[1:])
	}
	if len(args) > 0 && args[0] == "namespace-anchor" {
		return nativeNamespaceExec(args[1:], os.Environ())
	}
	if len(args) > 0 && (args[0] == "probe" || args[0] == "listener") {
		return nativeNamespaceProbe(args)
	}
	if len(args) < 3 || args[0] != "run" {
		return errors.New("native exec requires a policy and executable")
	}
	data, err := base64.RawStdEncoding.DecodeString(args[1])
	if err != nil {
		return errors.New("invalid native isolation policy")
	}
	var p nativeLinuxPolicy
	if err := json.Unmarshal(data, &p); err != nil {
		return errors.New("invalid native isolation policy")
	}
	runtime.LockOSThread()
	// Intentionally never unlock: even the error path must not lend a confined
	// thread back to Go's scheduler. The CLI exits on the returned error.
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("native no_new_privs: %w", err)
	}
	if err := nativeApplyLandlock(p); err != nil {
		return fmt.Errorf("native Landlock: %w", err)
	}
	if err := nativeApplySeccomp(p.Namespace, len(p.Ports) > 0); err != nil {
		return fmt.Errorf("native seccomp: %w", err)
	}
	// Keep start-by receipts 3/4 alive until _command-exec consumes them. All
	// other inherited helper descriptors close at exec, including Go runtime FDs.
	if err := unix.CloseRange(5, ^uint(0), unix.CLOSE_RANGE_CLOEXEC); err != nil {
		return fmt.Errorf("native descriptor hygiene: %w", err)
	}
	payloadEnv := p.Env
	if payloadEnv == nil {
		payloadEnv = os.Environ() // Trusted capability probe, without authored env.
	}
	env := make([]string, 0, len(payloadEnv))
	for _, value := range payloadEnv {
		if !strings.HasPrefix(value, "MYCE_TOKEN=") {
			env = append(env, value)
		}
	}
	if p.Namespace {
		return nativeNamespaceExec(args[2:], env)
	}
	return unix.Exec(args[2], args[2:], env)
}

func nativeApplyLandlock(p nativeLinuxPolicy) error {
	const read = unix.LANDLOCK_ACCESS_FS_EXECUTE | unix.LANDLOCK_ACCESS_FS_READ_FILE | unix.LANDLOCK_ACCESS_FS_READ_DIR
	const all = uint64(1<<16 - 1)
	const write = all &^ (unix.LANDLOCK_ACCESS_FS_MAKE_CHAR | unix.LANDLOCK_ACCESS_FS_MAKE_BLOCK | unix.LANDLOCK_ACCESS_FS_IOCTL_DEV)
	attr := unix.LandlockRulesetAttr{Access_fs: all, Access_net: unix.LANDLOCK_ACCESS_NET_BIND_TCP | unix.LANDLOCK_ACCESS_NET_CONNECT_TCP, Scoped: unix.LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET | unix.LANDLOCK_SCOPE_SIGNAL}
	fd, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&attr)), unsafe.Sizeof(attr), 0)
	if errno != 0 {
		return errno
	}
	defer unix.Close(int(fd))
	addPath := func(path string, access uint64, optional bool) error {
		pathFD, err := unix.Open(path, unix.O_PATH|unix.O_CLOEXEC, 0)
		if optional && errors.Is(err, unix.ENOENT) {
			return nil
		}
		if err != nil {
			return err
		}
		defer unix.Close(pathFD)
		var st unix.Stat_t
		if err := unix.Fstat(pathFD, &st); err != nil {
			return err
		}
		if st.Mode&unix.S_IFMT != unix.S_IFDIR {
			access &= unix.LANDLOCK_ACCESS_FS_EXECUTE | unix.LANDLOCK_ACCESS_FS_READ_FILE | unix.LANDLOCK_ACCESS_FS_WRITE_FILE | unix.LANDLOCK_ACCESS_FS_TRUNCATE | unix.LANDLOCK_ACCESS_FS_IOCTL_DEV
		}
		rule := unix.LandlockPathBeneathAttr{Allowed_access: access, Parent_fd: int32(pathFD)}
		_, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, fd, unix.LANDLOCK_RULE_PATH_BENEATH, uintptr(unsafe.Pointer(&rule)), 0, 0, 0)
		if errno != 0 {
			return errno
		}
		return nil
	}
	for _, path := range p.Read {
		if err := addPath(path, read, true); err != nil {
			return err
		}
	}
	for _, path := range p.Write {
		if err := addPath(path, write, false); err != nil {
			return err
		}
	}
	// Resolve self inside the payload's PID namespace, not in the worker. Do
	// not expose other processes' environ, mem, root or descriptors via /proc.
	for _, path := range []string{"/proc/self", "/proc/thread-self"} {
		if err := addPath(path, read, false); err != nil {
			return err
		}
	}
	if p.Namespace {
		if err := addPath("/dev/shm", write, false); err != nil {
			return err
		}
	}
	for _, path := range append([]string{"/dev/null", "/dev/zero", "/dev/full", "/dev/random", "/dev/urandom"}, p.Devices...) {
		if err := addPath(path, unix.LANDLOCK_ACCESS_FS_READ_FILE|unix.LANDLOCK_ACCESS_FS_WRITE_FILE|unix.LANDLOCK_ACCESS_FS_IOCTL_DEV, true); err != nil {
			return err
		}
	}
	for _, port := range p.Ports {
		rule := struct{ Access, Port uint64 }{unix.LANDLOCK_ACCESS_NET_BIND_TCP | unix.LANDLOCK_ACCESS_NET_CONNECT_TCP, uint64(port)}
		_, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, fd, 2 /* LANDLOCK_RULE_NET_PORT */, uintptr(unsafe.Pointer(&rule)), 0, 0, 0)
		if errno != 0 {
			return errno
		}
	}
	_, _, errno = unix.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, fd, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func nativeSeccompFilter(namespace, tcp bool) ([]unix.SockFilter, error) {
	var arch uint32
	switch runtime.GOARCH {
	case "amd64":
		arch = unix.AUDIT_ARCH_X86_64
	case "arm64":
		arch = unix.AUDIT_ARCH_AARCH64
	default:
		return nil, errors.New("native seccomp architecture unsupported")
	}
	stmt := func(code uint16, k uint32) unix.SockFilter { return unix.SockFilter{Code: code, K: k} }
	jump := func(k uint32, jt, jf uint8) unix.SockFilter {
		return unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: k, Jt: jt, Jf: jf}
	}
	deny := stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_ERRNO|uint32(unix.EPERM))
	f := []unix.SockFilter{stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 4), jump(arch, 1, 0), stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_KILL_PROCESS), stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 0)}
	// The x32 ABI shares the x86 audit architecture but different syscall
	// numbers. Compat architectures and x32 must not bypass the native filter.
	if runtime.GOARCH == "amd64" {
		f = append(f, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K, K: 0x40000000, Jf: 1}, deny)
	}
	blocked := []uint32{unix.SYS_PTRACE, unix.SYS_PROCESS_VM_READV, unix.SYS_PROCESS_VM_WRITEV, unix.SYS_PIDFD_GETFD, unix.SYS_KCMP, unix.SYS_IO_URING_SETUP, unix.SYS_OPEN_BY_HANDLE_AT, unix.SYS_NAME_TO_HANDLE_AT, unix.SYS_MOUNT, unix.SYS_UMOUNT2, unix.SYS_PIVOT_ROOT, unix.SYS_CHROOT, unix.SYS_SETNS, unix.SYS_UNSHARE, unix.SYS_BPF, unix.SYS_PERF_EVENT_OPEN, unix.SYS_USERFAULTFD, unix.SYS_KEYCTL, unix.SYS_ADD_KEY, unix.SYS_REQUEST_KEY, unix.SYS_REBOOT, unix.SYS_KEXEC_LOAD}
	blocked = append(blocked, unix.SYS_FSOPEN, unix.SYS_FSCONFIG, unix.SYS_FSMOUNT, unix.SYS_FSPICK, unix.SYS_MOVE_MOUNT, unix.SYS_OPEN_TREE, unix.SYS_MOUNT_SETATTR)
	if !namespace {
		blocked = append(blocked, unix.SYS_SHMGET, unix.SYS_SHMAT, unix.SYS_SHMCTL, unix.SYS_SEMGET, unix.SYS_SEMCTL, unix.SYS_SEMOP, unix.SYS_SEMTIMEDOP, unix.SYS_MSGGET, unix.SYS_MSGCTL, unix.SYS_MSGSND, unix.SYS_MSGRCV)
	}
	for _, number := range blocked {
		f = append(f, jump(number, 0, 1), deny)
	}
	if !tcp {
		// No sockaddr dereferencing in classic BPF: deny every new socket in
		// fallback instead of pretending a port-only allowlist is loopback-only.
		f = append(f, jump(unix.SYS_SOCKET, 0, 1), deny)
	} else {
		// Only TCP (not SCTP/MPTCP, which Landlock does not promise to mediate).
		// Fallback's reserved ports include the accepted offhost exception.
		// Unix, packet, raw and UDP sockets have no exception.
		f = append(f, jump(unix.SYS_SOCKET, 0, 12), stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 16), jump(unix.AF_INET, 2, 0), jump(unix.AF_INET6, 1, 0), deny, stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 24), stmt(unix.BPF_ALU|unix.BPF_AND|unix.BPF_K, 0xf), jump(unix.SOCK_STREAM, 1, 0), deny, stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 32), jump(0, 2, 0), jump(unix.IPPROTO_TCP, 1, 0), deny)
	}
	// asyncio and ordinary process wakeups need stream socketpairs. A Unix
	// stream pair remains ESTABLISHED after shutdown/peer close and cannot be
	// reconnected. Datagram pairs can reconnect and must remain denied.
	f = append(f, jump(unix.SYS_SOCKETPAIR, 0, 10),
		stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 16), jump(unix.AF_UNIX, 1, 0), deny,
		stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 24), stmt(unix.BPF_ALU|unix.BPF_AND|unix.BPF_K, ^uint32(unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK)), jump(unix.SOCK_STREAM, 1, 0), deny,
		stmt(unix.BPF_LD|unix.BPF_W|unix.BPF_ABS, 32), jump(0, 1, 0), deny)
	f = append(f, stmt(unix.BPF_RET|unix.BPF_K, unix.SECCOMP_RET_ALLOW))
	return f, nil
}

func nativeApplySeccomp(namespace, tcp bool) error {
	filter, err := nativeSeccompFilter(namespace, tcp)
	if err != nil {
		return err
	}
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	_, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, 0, uintptr(unsafe.Pointer(&program)))
	runtime.KeepAlive(filter)
	if errno != 0 {
		return errno
	}
	return nil
}

// Trusted worker probe: join only user/net namespaces, keeping host procfs so
// the existing PGID/socket ownership check still uses host process identities.
func nativeNamespaceProbe(args []string) error {
	if len(args) < 3 {
		return errors.New("native readiness probe arguments missing")
	}
	group, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}
	if err := verifyNativeListener(args[1], group); err != nil {
		if errors.Is(err, errNativeForeignListener) {
			fmt.Fprint(os.Stdout, "foreign")
		}
		return err
	}
	if args[0] == "listener" {
		fmt.Fprint(os.Stdout, "ready")
		return nil
	}
	if len(args) != 4 || !strings.HasPrefix(args[3], "/") {
		return errors.New("native readiness probe path missing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:"+args[1]+args[3], nil)
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		fmt.Fprint(os.Stdout, "ready")
	}
	return nil
}
