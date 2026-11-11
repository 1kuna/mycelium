package myce

import (
	"strings"
	"testing"
)

// Each fixture is a stored job as the queue leaves it; the public projection
// must attach one typed finding with an owner and an action, and must never
// copy worker text (the "/private" marker) into it.
func TestJobFindingsFixtures(t *testing.T) {
	zero, three := 0, 3
	private := "/private/worker/path secret-ish text"
	for _, tc := range []struct {
		name, code, owner string
		job               Job
	}{
		{"exit zero, directory output", "output_not_regular", findingOwnerJob, Job{State: "failed", Reason: "command failed: " + private,
			ExecutionCause: &RuntimeCause{Operation: "outputs", Class: "not_regular", Finalization: &FinalizationDetail{Step: "output_validation", Output: "result", Error: "not_regular", CommandExitCode: &zero}}}},
		{"missing output", "output_not_found", findingOwnerJob, Job{State: "failed", Reason: "command failed: " + private,
			ExecutionCause: &RuntimeCause{Operation: "outputs", Class: "not_found", Finalization: &FinalizationDetail{Step: "output_path", Output: "out.json", Error: "not_found"}}}},
		{"publication storage full", "output_publication", findingOwnerMycelium, Job{State: "failed", Reason: "command failed: " + private,
			ExecutionCause: &RuntimeCause{Operation: "outputs", Class: "storage_full", Finalization: &FinalizationDetail{Step: "output_upload", Output: "out.json", Error: "storage_full"}}}},
		{"rss guard stop", "memory_limit", findingOwnerJob, Job{State: "failed", Reason: "command failed: " + private,
			ExecutionCause: &RuntimeCause{Operation: "command", Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: 64 << 30, LimitBytes: 2 << 30, Quality: "owned_session_rss"}}}},
		{"wall time", "wall_time_limit", findingOwnerJob, Job{State: "failed", Reason: "execution failed: execution: wall_time_limit", Spec: Spec{WallTime: "1h"}, ExecutionCause: wallTimeCause()}},
		{"oom twice", "out_of_memory", findingOwnerJob, Job{State: "failed", Reason: "out of memory after one retry on a larger hypha"}},
		{"oom beyond every host", "memory_exceeds_hosts", findingOwnerJob, Job{State: "failed", Reason: "needs more memory than any eligible host allows (96.0 GiB)"}},
		{"infrastructure exhausted", "infrastructure_retries_exhausted", findingOwnerMycelium, Job{State: "failed", Losses: 3, Reason: "failed after 3 infrastructure losses: " + private}},
		{"infrastructure requeue", "infrastructure_retry", findingOwnerMycelium, Job{State: "queued", Losses: 1, Reason: "execution interrupted; cause unconfirmed"}},
		{"missing library", "library_missing", findingOwnerJob, Job{State: "failed", Reason: "command failed: " + private,
			ExecutionCause: &RuntimeCause{Operation: "launch", Class: machoLibraryMissing, Linkage: &MachOLinkage{Binary: "bin/tool", Library: "/opt/local/lib/libfoo.dylib"}}}},
		{"command exit", "command_exit", findingOwnerJob, Job{State: "failed", Reason: "command failed: " + private, ExecutionCause: &RuntimeCause{Operation: "main", Class: "command_exit", ExitCode: &three}}},
		{"segfault", "signal_crash", findingOwnerJob, Job{State: "failed", Attempt: 1, Reason: "command failed: " + private,
			AttemptHistory: []AttemptOutcome{{Attempt: 1, SignalEvidence: &SignalEvidence{Signal: "SIGSEGV", Attribution: "payload_pid"}}}}},
		{"untyped failure", "cause_unknown", findingOwnerUnknown, Job{State: "failed", Reason: "command failed: " + private}},
		{"upstream failed", "upstream_failed", findingOwnerJob, Job{State: "failed", DiagnosticCode: "upstream_failed", Reason: "producer piece has no accepted outputs"}},
		{"healthy", "", "", Job{State: "succeeded", Losses: 1}},
		{"cancelled", "", "", Job{State: "cancelled", Reason: "cancel requested"}},
		{"plain lifecycle failure", "", "", Job{State: "failed", Reason: "give-up time reached"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := statusJobValue(&tc.job).Finding
			if tc.code == "" {
				if f != nil {
					t.Fatalf("unexpected finding %+v", f)
				}
				return
			}
			if f == nil || f.Code != tc.code || f.Owner != tc.owner || f.What == "" || f.Action == "" {
				t.Fatalf("finding %+v, want %s/%s", f, tc.code, tc.owner)
			}
			if text := f.Text(); strings.Contains(text, "/private") || strings.Count(text, "Next:") != 1 {
				t.Fatalf("unsafe or multi-action text: %s", text)
			}
		})
	}
}

// The 2026-11-06 case: 64 GiB measured against a 2 GiB declaration must name
// both measurements and a declaration that would have fit.
func TestJobFindingCarriesMeasuredEvidence(t *testing.T) {
	j := Job{State: "failed", ExecutionCause: &RuntimeCause{Operation: "command", Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: 64 << 30, LimitBytes: 2 << 30, Quality: "owned_session_rss"}}}
	text := statusJobValue(&j).Finding.Text()
	for _, want := range []string{"64.0 GiB", "2.0 GiB", "declare at least 64.0 GiB", "your job"} {
		if !strings.Contains(text, want) {
			t.Fatalf("%q missing from %s", want, text)
		}
	}
}
