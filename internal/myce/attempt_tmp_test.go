package myce

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A server that binds a UUID-named socket in TMPDIR (vLLM's ZMQ IPC) must fit
// sun_path (104 bytes on Darwin) with a typical worker state path, through
// both the plain batch launch and the native group launch. The attempt tree
// alone left no room, so vLLM failed to start natively.
func TestTmpSocketRoom(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	bind := `import os, socket
out = os.path.join(os.environ["NATIVE_TEST_DIR"], "socket.out")
try:
    socket.socket(socket.AF_UNIX).bind(os.path.join(os.environ["TMPDIR"], "u" * 36))
    open(out, "w").write("bound")
except OSError as e:
    open(out, "w").write(os.environ["TMPDIR"] + ": " + repr(e))`
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "group"}[group], func(t *testing.T) {
			// Keep the fixture's spool as short as a default install's.
			t.Setenv("TMPDIR", "/tmp")
			s := nativeSpec(t)
			// Sandboxes deny Unix stream sockets (MY-23); servers like this
			// run as plain native jobs, unsandboxed by default (MY-25).
			s.Isolation = ""
			s.Run.Command = []string{python, "-c", bind}
			if !group {
				s.Run.Sidecars = nil
			}
			r := nativeStart(t, s, "fake")
			result := nativeFinish(t, r)
			if got, _ := os.ReadFile(filepath.Join(r.dir, "socket.out")); !result.Success || string(got) != "bound" {
				t.Fatalf("socket in TMPDIR: %s %+v", got, result)
			}
		})
	}
}
