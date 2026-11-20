package myce

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
	"strings"
	"syscall"
	"testing"
)

func TestNativeLaunchBindingE2BIG(t *testing.T) {
	j, env := syntheticInputBindings(771)
	for i := range 4 {
		env[fmt.Sprintf("AUTHORED_%d", i)] = "1"
	}
	policy := nativeLinuxPolicy{Plain: true, Env: environmentList(env)}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	// This is the old production nativePolicyArgs wire format. The supervisor
	// does not need to release its gate: exec rejects the string before it runs.
	encoded := base64.RawStdEncoding.EncodeToString(data)
	limit := 32 * os.Getpagesize() // Linux MAX_ARG_STRLEN, including NUL.
	if len(encoded)+1 <= limit {
		t.Skipf("771 bindings encode to %d bytes; this kernel's per-string limit is %d", len(encoded)+1, limit)
	}
	t.Logf("bindings=771 env_bytes=%d policy_argument_bytes=%d per_string_limit=%d", len(strings.Join(policy.Env, "\x00"))+1, len(encoded)+1, limit)
	plain := exec.Command("/bin/sh", "-c", "exit 0", "synthetic", "a", "b", "c", "d")
	plain.Env = policy.Env
	if err := plain.Run(); err != nil {
		t.Fatalf("same binding environment fits without inline policy: %v", err)
	}
	legacy := exec.Command("/bin/sh", "-c", "exit 0", "myce-supervisor-synthetic", "synthetic-helper", "_native-exec", "run", encoded)
	legacy.Env = []string{"PATH=/usr/bin:/bin"}
	if err := legacy.Run(); !errors.Is(err, syscall.E2BIG) {
		t.Fatalf("inline policy should reproduce kernel E2BIG, got %v", err)
	}
	work := filepath.Join(t.TempDir(), "work")
	if err := os.MkdirAll(work, 0700); err != nil {
		t.Fatal(err)
	}
	if err := bindManagedInputs(work, j, maps.Clone(env), env); err != nil {
		t.Fatal(err)
	}
	// Exercise the production policy loader and credential boundary, withholding
	// only a synthetic path. This requires ABI 2, like ordinary native workers.
	policy.Env = environmentList(env)
	policy.Deny = []string{filepath.Join(work, "denied")}
	argv, err := nativePolicyFileArgs(policy, []string{"/bin/sh", "-ec", `test -r "$MYCE_BINDINGS_FILE"; test -z "${MYCE_INPUT_INPUT_0000_` + strings.Repeat("A", 48) + `+x}"`}, work)
	if err != nil {
		t.Fatal(err)
	}
	for _, arg := range argv {
		if len(arg)+1 >= limit {
			t.Fatal("file-backed policy still exceeds the per-string limit")
		}
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("file-backed native launch: %v: %s", err, out)
	}
}

func TestNativeNamespaceOptionsFile(t *testing.T) {
	bwrap, err := exec.LookPath("bwrap")
	if err != nil {
		t.Skip("bubblewrap is not installed; namespace option transport unverified")
	}
	work := filepath.Join(t.TempDir(), "work")
	if err := os.Mkdir(work, 0700); err != nil {
		t.Fatal(err)
	}
	// Parse a large mount list without creating namespaces or changing the
	// filesystem. --help exits after option expansion, even on a host that
	// refuses unprivileged namespaces. Paths contain spaces to guard NUL framing.
	cache := filepath.Join(t.TempDir(), "synthetic input cache")
	if err := os.Mkdir(cache, 0700); err != nil {
		t.Fatal(err)
	}
	policy := nativeLinuxPolicy{InputMountRoots: []string{cache}}
	for i := range 10000 {
		path := filepath.Join(cache, fmt.Sprintf("%064x/data", i))
		policy.Read = append(policy.Read, path)
	}
	options := append(nativeBwrapPolicyArgs(policy), "--help")
	if len(options) > 100 || len(policy.Read) != 10000 {
		t.Fatal("binding mounts grew or exact Landlock grants changed")
	}
	argv, err := nativeNamespaceMonitorFileArgs(bwrap, options, work)
	if err != nil {
		t.Fatal(err)
	}
	if len(argv) != 6 {
		t.Fatal("namespace options still grow exec argv")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if out, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(out), "--ro-bind") {
		t.Fatalf("file-backed bubblewrap option parsing: %v: %s", err, out)
	}
}

// Fault injection runs in a disposable helper. It simulates unavailable or
// unsupported Landlock and failures while installing its exact grants.
func init() {
	if len(os.Args) > 4 && os.Args[1] == "_native-landlock-failure-test" {
		runtime.LockOSThread()
		call, e1 := strconv.Atoi(os.Args[2])
		code, e2 := strconv.Atoi(os.Args[3])
		if e1 != nil || e2 != nil {
			os.Exit(124)
		}
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			os.Exit(124)
		}
		filter := []unix.SockFilter{
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: uint32(call), Jf: 1},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(code)},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
		}
		program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
		_, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, 0, uintptr(unsafe.Pointer(&program)))
		runtime.KeepAlive(filter)
		if errno != 0 {
			os.Exit(124)
		}
		err := NativeExec(os.Args[4:])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(125)
		}
		os.Exit(0)
	}
}

func TestNativeInputMountLandlockBoundary(t *testing.T) {
	work := t.TempDir()
	cache := filepath.Join(work, "input-cache")
	if err := os.Mkdir(cache, 0700); err != nil {
		t.Fatal(err)
	}
	own, sibling := filepath.Join(cache, "own"), filepath.Join(cache, "sibling")
	for _, path := range []string{own, sibling} {
		if err := os.WriteFile(path, []byte("synthetic"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	policy := nativeLinuxPolicy{Namespace: true, InputMountRoots: []string{cache}, Read: []string{"/usr", "/bin", "/lib", "/lib64", own}}
	// The whole cache is already visible to this helper, as under a root mount.
	// No namespace capability skip may bypass proof of the Landlock boundary.
	argv, err := nativePolicyFileArgs(policy, []string{"/bin/sh", "-ec", `test "$(cat "$1")" = synthetic; if cat "$2" 2>/dev/null; then exit 22; fi; printf boundary-ok`, "synthetic", own, sibling}, work)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	out, err := cmd.CombinedOutput()
	if err != nil || string(out) != "boundary-ok" {
		t.Fatalf("exact grants with visible cache root: %v: %s", err, out)
	}
	for _, call := range []int{unix.SYS_LANDLOCK_CREATE_RULESET, unix.SYS_LANDLOCK_ADD_RULE, unix.SYS_LANDLOCK_RESTRICT_SELF} {
		for _, code := range []syscall.Errno{syscall.ENOSYS, syscall.EINVAL, syscall.EOPNOTSUPP} {
			t.Run(fmt.Sprintf("%d/%d", call, code), func(t *testing.T) {
				args := append([]string{"_native-landlock-failure-test", strconv.Itoa(call), strconv.Itoa(int(code))}, argv[2:]...)
				cmd := exec.Command(argv[0], args...)
				cmd.Env = []string{"PATH=/usr/bin:/bin"}
				out, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 125 || !strings.Contains(string(out), "native Landlock:") || strings.Contains(string(out), "boundary-ok") {
					t.Fatalf("Landlock failure reached payload or lost reason: %v: %s", err, out)
				}
			})
		}
	}
}
