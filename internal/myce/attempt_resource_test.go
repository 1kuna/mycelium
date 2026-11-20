package myce

import "testing"

func TestAttemptResourceFailureRequiresOwnCeilingEvidence(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		success, interrupted bool
		source               string
		events               MemoryEventCounts
		oom                  bool
	}{
		{name: "successful pressure", success: true, events: MemoryEventCounts{OOM: 1}},
		{name: "max pressure only", events: MemoryEventCounts{Max: 5}},
		{name: "host OOM kill only", events: MemoryEventCounts{OOMKill: 1}},
		{name: "unenforced observer", source: processGroupMemorySource, events: MemoryEventCounts{OOM: 1}},
		{name: "infrastructure stop", interrupted: true, events: MemoryEventCounts{OOM: 1}},
		{name: "attempt OOM", events: MemoryEventCounts{OOM: 1}, oom: true},
		{name: "attempt group kill", events: MemoryEventCounts{OOMGroupKill: 1}, oom: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fakeClock{}
			now := clock.Now()
			j := &Job{StartedAt: &now, MemoryEnvelope: &JobMemoryEnvelope{HostGB: 2.125}}
			r := Result{Success: tc.success, Interrupted: tc.interrupted, Error: "exit 1", MemoryPeak: &AttemptMemoryPeak{Source: tc.source, Events: &tc.events}}
			got := classifyAttemptResourceFailure(j, r)
			if got.OutOfMemory != tc.oom || got.Success != tc.success || got.Interrupted != tc.interrupted {
				t.Fatalf("%+v", got)
			}
			if tc.oom && got.Error != "exceeded its 2.125 GiB memory ceiling" {
				t.Fatal(got.Error)
			}
		})
	}
}
