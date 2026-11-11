package myce

import "testing"

// Test queues publish admission-wait evidence inside the committing call, so
// fixtures may read planning state after any transaction without racing a
// background build. Tests of the production asynchronous builder opt in with
// asynchronousAdmissionWait and synchronize through drainAdmissionWait.
func init() { synchronousAdmissionWaitDefault = true }

func asynchronousAdmissionWait(t testing.TB, q *Queue) {
	t.Helper()
	b := &q.admissionWaitBuild
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running || b.pending != nil {
		t.Fatal("switch the admission-wait mode before the queue commits test work")
	}
	b.synchronous = false
}

func TestAdmissionWaitSynchronousDefaultPublishesBeforeReturn(t *testing.T) {
	q, _ := fixture(t)
	if !q.admissionWaitBuild.synchronous {
		t.Fatal("test queues must default to synchronous admission-wait evidence")
	}
	s := cpuSpec(4, 1)
	s.Priority = "production"
	demand := submit(t, q, s)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	b := &q.admissionWaitBuild
	b.mu.Lock()
	running, pending := b.running, b.pending
	b.mu.Unlock()
	// Unlocked read on purpose: -race proves no build is still writing.
	if running || pending != nil || q.s.Jobs[demand.ID].AdmissionWait == nil || q.s.Jobs[demand.ID].AdmissionWait.Latest == nil {
		t.Fatal("commit returned before its admission-wait evidence was published")
	}
}
