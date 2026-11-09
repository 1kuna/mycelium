package myce

import (
	"bytes"
	"strings"
	"testing"
)

// The writer checks the production logging boundary, not just the log text.
type launchLogWriter struct {
	q *Queue
	t *testing.T
	bytes.Buffer
}

func (w *launchLogWriter) Write(b []byte) (int, error) {
	if strings.Contains(string(b), "event=slow_launch_authorization") || strings.Contains(string(b), "event=slow_queue_mutex_hold") {
		if !w.q.mu.TryLock() {
			w.t.Error("slow launch logging held the queue lock")
		} else {
			w.q.mu.Unlock()
		}
	}
	return w.Buffer.Write(b)
}
