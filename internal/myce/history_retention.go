package myce

import "context"

// File/log retention authorizes only the named payloads. Job intent, wait/error
// attribution and exact-attempt custody are unique evidence. History maintenance
// may compact exact duplicates, but elapsed time never authorizes job deletion.
func (q *Queue) pruneJobHistory(_ ...context.Context) error { return nil }
