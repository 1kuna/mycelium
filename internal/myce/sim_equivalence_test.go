package myce

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func (q *Queue) originalBatchBeforeCall(c *ModelCall, m *Machine) bool {
	for _, j := range q.s.Jobs {
		memory := m.FreeMemoryGB
		for _, s := range q.s.ModelSlots {
			if s.Machine == m.ID && !q.slotPinned(s) && s.State != "unload_failed" && legacyRank(s.Priority) > legacyRank(j.Spec.Priority) {
				model, _ := q.model(s.Model)
				memory += model.MemoryGB
			}
		}
		for _, r := range q.s.Jobs {
			if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") && (r.Spec.Resumable || j.Spec.Priority == "production") && legacyRank(r.Spec.Priority) > legacyRank(j.Spec.Priority) {
				memory += r.Spec.Needs.MemoryGB
			}
		}
		if j.State == "queued" && q.capReason(j) == "" && fits(j, m, memory) && q.fairBefore(j, callJob(c), m) {
			return true
		}
	}
	return false
}

func TestSimSchedulingScanEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	clock := &simClock{time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	for trial := 0; trial < 300; trial++ {
		q := &Queue{simulation: true, clock: clock, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}, Policies: map[string]ProjectPolicy{}, FairUsage: map[string]FairUsage{}, ModelSlots: map[string]*ModelSlot{}}}
		m := &Machine{ID: "worker", GPU: "fake", FreeMemoryGB: 8, TotalMemoryGB: 16, CapabilityProfile: machine("worker", "fake").CapabilityProfile}
		q.s.Machines[m.ID] = m
		for p := 0; p < 3; p++ {
			name := string(rune('a' + p))
			q.s.Policies[name] = ProjectPolicy{Weight: float64(p + 1)}
			q.s.FairUsage[name] = FairUsage{At: clock.Now(), Seconds: rng.Float64() * 100}
		}
		for i := 0; i < 30; i++ {
			project := string(rune('a' + rng.Intn(3)))
			s := simTestStream(project, []string{"production", "deadline", "experiment"}[rng.Intn(3)], 0, 10, 1)
			s.MemoryGB = float64(1 + rng.Intn(10))
			s.Checkpoint = float64(rng.Intn(2))
			s.Deadline = 10
			id := fmt.Sprintf("job-%03d", i)
			state := "queued"
			machine := ""
			if i < 3 {
				state = []string{"running", "stopping"}[rng.Intn(2)]
				machine = m.ID
			}
			q.s.Jobs[id] = &Job{ID: id, Spec: simSpec(&simTask{stream: s}, clock.Now()), State: state, Machine: machine, Queued: clock.Now().Add(time.Duration(i) * time.Second)}
		}
		var original *Job
		for _, j := range q.s.Jobs {
			if j.State == "queued" && fits(j, m, m.FreeMemoryGB) && q.capReason(j) == "" && (original == nil || q.fairBefore(j, original, m)) {
				original = j
			}
		}
		got := q.candidate(m, nil)
		if got == nil || original == nil || got.ID != original.ID {
			t.Fatalf("candidate mismatch at trial %d", trial)
		}
		for _, priority := range []string{"production", "deadline", "experiment"} {
			c := &ModelCall{ID: "call", Project: "b", Priority: priority, Queued: clock.Now()}
			if q.batchBeforeCall(c, m) != q.originalBatchBeforeCall(c, m) {
				t.Fatalf("call precedence mismatch at trial %d %s", trial, priority)
			}
		}
	}
}
