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
	started := true
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
		{"unstarted missing cause", "prelaunch_cause_missing", findingOwnerMycelium, Job{State: "failed", Attempt: 7, CommandStarted: &started, Reason: "command failed: " + private, ExecutionEvidence: unstartedExecution(7)}},
		{"stale unstarted evidence", "cause_unknown", findingOwnerUnknown, Job{State: "failed", Attempt: 7, Reason: "command failed: " + private, ExecutionEvidence: unstartedExecution(6)}},
		{"typed startup boundary", "recorded_failure", findingOwnerUnknown, Job{State: "failed", Attempt: 7, Reason: "command failed: " + private, ExecutionEvidence: unstartedExecution(7), ExecutionCause: &RuntimeCause{Operation: "start_handoff", Class: "timeout"}}},
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

func TestJobFindingsDeadEndPrivacyAndRepeatProjection(t *testing.T) {
	for _, reason := range []string{
		"dead end: no registered implementation matches a known host (private-name); action: /private/secret",
		"dead end: private-name declares no native check for private-work; action: /private/secret",
		"dead end: private-name declares no native baseline for private-work; action: /private/secret",
		"dead end: the native check for private-name failed on private-host (/private/secret); action: /private/secret",
		"dead end: environment preparation failed on every compatible builder; action: /private/secret",
		"dead end: no known host can prepare the required environment; action: /private/secret",
		"dead end: no known host can fit the native check's resource requirements; action: /private/secret",
		"dead end: no host can size this auto-memory job (/private/secret); action: /private/secret",
	} {
		j := statusJobValue(&Job{State: "failed", DiagnosticCode: deadEndWait, Reason: reason})
		text := JobReason(&j)
		if j.Finding == nil || strings.Count(text, "Next:") != 1 || strings.Contains(text, "private") || strings.Contains(text, "execution failed") {
			t.Fatal(text)
		}
		again := statusJobValue(&j)
		if JobReason(&again) != text {
			t.Fatalf("second projection changed finding: %s -> %s", text, JobReason(&again))
		}
	}
}
