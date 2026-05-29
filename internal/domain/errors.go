package domain

import "errors"

var (
	ErrNoFit           = errors.New("no unit can fit this preset under its constraints")
	ErrContextOverflow = errors.New("request exceeded the preset's context cap")
	ErrPreempted       = errors.New("instance was preempted")
	ErrNotReady        = errors.New("backend did not become ready before timeout")
	ErrUnreachable     = errors.New("node is unreachable")
	ErrStaleFence      = errors.New("plan was built on a stale resource version; re-decide")
	ErrUnsupported     = errors.New("unsupported operation")
)
