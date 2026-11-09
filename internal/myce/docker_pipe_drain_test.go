package myce

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// A delayed parent-side diagnostic consumer is not a failed CLI. The old
// mean-only WaitDelay rejected the complete response with exec.ErrWaitDelay.
func TestDockerRPCDrainsBufferedOutputWithinLatencyEnvelope(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "docker")
	pidFile := binary + ".pid"
	script := "#!/bin/sh\nprintf '[]\\n'\nprintf '%s' \"$$\" > \"$0.pid\"\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	d := &dockerAttempt{dir: root, binary: binary, env: withoutToken(os.Environ()), state: dockerExecution{Endpoint: "fake"}}
	// Give the fixture a controlled completed response sample. Learning from
	// this tiny shell's incidental speed could leave less than await's poll
	// interval for reap observation and resumed consumption on a fast host.
	const responseLatency = time.Second
	d.noteRPCLatency("inspect", responseLatency)
	entered, resume := make(chan struct{}, 1), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	old := queueHostMemorySource
	queueHostMemorySource = func(float64) (MemorySource, error) {
		return func(context.Context) (float64, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-resume
			return 32, nil
		}, nil
	}
	joined := make(chan struct{})
	defer func() {
		unblock()
		<-joined
		queueHostMemorySource = old
	}()
	var out []byte
	var rpcErr error
	go func() {
		defer close(joined)
		out, rpcErr = d.callWithTimeout(testWaitTimeout, "inspect")
	}()
	select {
	case <-entered:
	case <-time.After(testWaitTimeout):
		t.Fatal("output consumer not entered")
	}
	// Wait reaps the child before starting its pipe-drain timer. ESRCH proves
	// the CLI has exited while the parent still holds its buffered response.
	await(t, func() bool {
		b, err := os.ReadFile(pidFile)
		if err != nil {
			return false
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		return err == nil && pid > 0 && errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	})
	// The first completed sample has a three-mean allowance (mean + four
	// half-mean deviations). Two means exceed the old cutoff but fit that
	// allowance. The controlled one-second sample leaves a full second for
	// reap observation, timer overshoot and resumed consumption.
	<-time.After(2 * responseLatency)
	unblock()
	<-joined
	if rpcErr != nil {
		t.Fatalf("successful CLI output rejected: %v (pipe drain timeout: %v)", rpcErr, errors.Is(rpcErr, exec.ErrWaitDelay))
	}
	if string(out) != "[]\n" {
		t.Fatalf("lost buffered response: %q", out)
	}
}
