package main

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

// The quickstart and EOF regression exercise the same terminal collector.
const devCommandPTYProbe = `
import os, pty, select, sys, time
pid, fd = pty.fork()
if pid == 0:
    os.execvp(sys.argv[1], sys.argv[1:])
done, status = 0, 0
try:
    os.write(fd, b'terminal-input\n')
    data = b''
    until = time.monotonic() + 15
    while time.monotonic() < until:
        ready, _, _ = select.select([fd], [], [], 0.1)
        if ready:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            data += chunk
    done, status = os.waitpid(pid, os.WNOHANG)
    # EOF only closes the terminal. The process may still be exiting.
    while done == 0 and time.monotonic() < until:
        select.select([], [], [], min(0.01, max(0, until - time.monotonic())))
        done, status = os.waitpid(pid, os.WNOHANG)
    assert done == pid and os.waitstatus_to_exitcode(status) == 0 and b'terminal-ok' in data, {'done': done, 'status': status, 'output': data}
finally:
    try:
        if done != pid:
            os.kill(pid, 15)
            os.waitpid(pid, 0)
    except ProcessLookupError:
        pass
    os.close(fd)
`

func TestDevCommandPTYEOFBeforeExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	child := `import os, sys, time
assert sys.stdin.readline().strip() == "terminal-input"
print("terminal-ok", flush=True)
os.close(0); os.close(1); os.close(2)
time.sleep(0.25) # Keep the child alive after its terminal descriptors close.
`
	out, err := exec.CommandContext(ctx, "python3", "-c", devCommandPTYProbe, "python3", "-c", child).CombinedOutput()
	if err != nil {
		t.Fatalf("terminal EOF preceded child exit: %v\n%s", err, out)
	}
}
