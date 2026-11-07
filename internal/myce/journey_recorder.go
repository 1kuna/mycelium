package myce

import (
	"net/http"
	"os/exec"
	"time"
)

// journeyObserver is an unexported test seam. Ordinary callers leave it nil.
// Implementations may only copy already available facts; no queue operation,
// storage/network I/O, clock advance, signal, or recovery probe is permitted.
// The bounded recorder and failure logging are compiled only in _test.go.
// Locked callbacks borrow their arguments only for the duration of the call.
type journeyObserver interface {
	handler(http.Handler) http.Handler
	attachRuntime(*modelRuntime)
	startResult(*process, Job, error)
	dockerLocked(*dockerAttempt, string)
	runtimeLocked(*modelRuntime, string)
	nativeLocked(*nativeGroup, string)
	nativeFacts(string, string, string, memberOutcome)
	result(string, Job, Result, error)
	rpc(string, string, *readinessProgress, error)
	rpcStart(string, *readinessProgress, readinessHistory, time.Duration)
	rpcReturn(*dockerAttempt, string, *readinessProgress, error, error, error)
	commandReturn(string, *exec.Cmd, error, error, int, int)
}
