//go:build !race

package myce

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// #459/#470: qualification used to repeatedly hash plain-command input recipes
// across every catalogue entry while worker check-ins waited for the queue.
func TestExtensionPlainBacklogCheckin(t *testing.T) {
	q, clock := fixture(t)
	m := machine("worker", "fake")
	m.ExtensionFingerprint = "synthetic-native-identity"
	m.DiskVolumes = []DiskVolume{{ID: "local", Roots: []string{"data", "file-cache", "checkpoint"}, TotalBytes: 1 << 40, FreeBytes: 1 << 39, SampledAt: clock.Now()}}
	for i := range 48 {
		m.CachedSharedInputs = append(m.CachedSharedInputs, fmt.Sprintf("%064x", i))
	}
	check(t, q, m, nil)
	sentinel := submit(t, q, spec("production"))
	assignment := check(t, q, m, nil).Job
	if assignment == nil || assignment.ID != sentinel.ID {
		t.Fatal("production sentinel was not assigned")
	}
	if err := q.Started(result(assignment, true)); err != nil {
		t.Fatal(err)
	}
	m.FreeMemoryGB = 0
	if err := q.update(func() error {
		q.s.Extensions = map[string]Extension{}
		for i := range 149 {
			id := fmt.Sprint(i)
			q.s.Extensions[id] = Extension{Name: "extension-" + id, Version: "1", Digest: fmt.Sprintf("%064x", i), Resumable: true}
		}
		for i := range 194 {
			id := fmt.Sprint(i)
			s := spec("experiment")
			s.SharedInputs = map[string]SharedInput{}
			for k, size := range []int64{608, 637, 4063} {
				s.SharedInputs[fmt.Sprint(k)] = SharedInput{SHA256: strings.Repeat(fmt.Sprint(k+1), 64), SizeBytes: size}
			}
			q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "queued", Queued: clock.Now()}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	contact := Checkin{Machine: m, Running: &Execution{ID: sentinel.ID, Attempt: assignment.Attempt}}
	start := time.Now()
	reply, err := q.Checkin(contact)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.ExtensionQualificationPlan) != 0 {
		t.Fatal("plain commands manufactured qualification demand", reply.ExtensionQualificationPlan)
	}
	// Same control-latency contract as the retained check-in incident regression:
	// at most half the one-second heartbeat interval, without race instrumentation.
	if elapsed >= 500*time.Millisecond {
		t.Errorf("catalogue/plain-backlog check-in %s exceeds half the 1s heartbeat interval", elapsed)
	}
	if got := job(t, q, sentinel.ID); got.State != "running" || got.Attempt != assignment.Attempt || got.Losses != 0 {
		t.Error("qualification planning interrupted the production sentinel")
	}
	t.Logf("149 extensions, 194 plain jobs, three shared inputs, 48 cached input IDs: check-in=%s", elapsed)
}
