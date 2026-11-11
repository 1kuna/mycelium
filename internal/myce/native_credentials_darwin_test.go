package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// #1309: the plain-job profile denies only its paths (except allowed
// descendants), still lets the job apply its own sandbox, as SwiftPM does, and
// keeps setuid /bin/ps working (a fleet probe hit its exec refusal).
func TestNativeCredentialProfileAllowsNestedSandbox(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	secret, allowed := filepath.Join(dir, "root", "secret"), filepath.Join(dir, "root", "state", "file")
	for _, path := range []string{secret, allowed} {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("/bin/sh", "-c", `cat "$1" >/dev/null 2>&1 && exit 3
cat "$2" >/dev/null || exit 4
/usr/bin/sandbox-exec -p '(version 1)(allow default)' /usr/bin/true || exit 5
/bin/ps -o pid= -p $$ >/dev/null || exit 6`, "probe", secret, allowed)
	cmd.Args, cmd.Path = nativeCredentialProfileArgs([]string{filepath.Join(dir, "root")}, []string{filepath.Join(dir, "root", "state")}, cmd), "/usr/bin/sandbox-exec"
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("profile: %v\n%s", err, out)
	}
}
