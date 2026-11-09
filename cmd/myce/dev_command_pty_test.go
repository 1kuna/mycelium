package main

import (
	"context"
	"os/exec"
	"strings"
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
            # An interrupt may land after waitpid reaps but before assignment.
            # Only a successful non-reaping check still owns this PID.
            try:
                owned, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                owned = pid
            if owned == 0:
                try:
                    os.kill(pid, 15)
                    os.waitpid(pid, 0)
                except ProcessLookupError:
                    pass
    finally:
        os.close(fd)
`

func TestDevCommandPTYInterruptedReap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Raise after the OS reaps the child but before the collector can assign
	// its return value. Intercept the forbidden signal without sending it.
	prefix := `import os
real_waitpid, real_kill = os.waitpid, os.kill
reaped = set()
def interrupted_waitpid(pid, options):
    done, status = real_waitpid(pid, options)
    if done == pid:
        reaped.add(pid)
        raise KeyboardInterrupt
    return done, status
def checked_kill(pid, signal):
    if pid in reaped:
        raise RuntimeError('signal-of-reaped-child')
    return real_kill(pid, signal)
os.waitpid, os.kill = interrupted_waitpid, checked_kill
`
	child := `import sys; sys.stdin.readline(); print("terminal-ok", flush=True)`
	out, err := exec.CommandContext(ctx, "python3", "-c", prefix+devCommandPTYProbe, "python3", "-c", child).CombinedOutput()
	if ctx.Err() != nil || err == nil || !strings.Contains(string(out), "KeyboardInterrupt") || strings.Contains(string(out), "signal-of-reaped-child") {
		t.Fatalf("interrupted reap signalled a released PID or lost the exception: %v\n%s", err, out)
	}
}
