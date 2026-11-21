package myce

import (
	"errors"
	"net/http"
)

const (
	custodianStartupFailed = "custodian_startup_failed"
	assignmentWithdrawn    = "assignment_withdrawn"
	launchRPCStale         = "launch_rpc_stale"
	queueLaunchRefused     = "queue_launch_refused"
	launchRPCConflict      = "launch_rpc_conflict"
	launchRPCFailed        = "launch_rpc_failed"
	localLaunchRefused     = "local_launch_refused"
)

var launchRefusalMessages = map[string]string{
	custodianStartupFailed: "detached custodian exited before startup",
	assignmentWithdrawn:    "worker received a stop directive before starting the assignment",
	launchRPCStale:         "queue launch RPC rejected stale attempt custody (HTTP 409)",
	queueLaunchRefused:     "queue launch RPC refused the current assignment (HTTP 409)",
	launchRPCConflict:      "queue launch RPC returned HTTP 409 without a refusal boundary",
	launchRPCFailed:        "queue launch RPC did not return usable authorization",
	localLaunchRefused:     "unclassified local refusal before execution",
}

var errAssignmentWithdrawn = errors.New("assignment withdrawn before command started")

// Previous queues strictly decode completion before processing it. On that
// refusal, omit the new boundary from the wire copy and keep exact release
// custody, positive prelaunch proof, and the worker's durable diagnostic.
func withoutLaunchRefusal(in any) (any, bool) {
	result, ok := in.(Result)
	if pointer, isPointer := in.(*Result); isPointer && pointer != nil {
		result, ok = *pointer, true
	}
	if !ok {
		return in, false
	}
	strip := func(cause *RuntimeCause) *RuntimeCause {
		if cause == nil || cause.LaunchRefusal == "" {
			return cause
		}
		copy := *cause
		copy.LaunchRefusal = ""
		return &copy
	}
	execution, cleanup := strip(result.ExecutionCause), strip(result.CleanupCause)
	changed := execution != result.ExecutionCause || cleanup != result.CleanupCause
	result.ExecutionCause, result.CleanupCause = execution, cleanup
	return result, changed
}

// Preserve request_stale and HTTP 409 for existing callers. Only batch
// launch reports whether the request passed the exact custody check.
type batchLaunchConflict struct{ boundary string }

func (e *batchLaunchConflict) Error() string { return ErrStale.Error() }
func (e *batchLaunchConflict) Unwrap() error { return ErrStale }

// Only /api/workers/launch supplies this wrapper. Started and custody reports
// share errWorkerStartAuthorization but cannot supply launch RPC evidence.
type launchRPCError struct{ error }

func (e *launchRPCError) Unwrap() error { return e.error }

func launchRefusalBoundary(err error) string {
	var rpc *launchRPCError
	var reply *HTTPError
	switch {
	case errors.Is(err, errCustodianStartup):
		return custodianStartupFailed
	case errors.Is(err, errAssignmentWithdrawn):
		return assignmentWithdrawn
	case !errors.As(err, &rpc):
		return localLaunchRefused
	case errors.As(rpc, &reply) && reply.Status == http.StatusConflict:
		if c := reply.Cause; c != nil && c.Operation == "start_authorization" && c.Class == "admission_changed" {
			switch c.LaunchRefusal {
			case launchRPCStale, queueLaunchRefused:
				return c.LaunchRefusal
			}
		}
		return launchRPCConflict
	default:
		return launchRPCFailed
	}
}
