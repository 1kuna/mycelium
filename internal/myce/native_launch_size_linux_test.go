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
