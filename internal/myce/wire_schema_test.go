package myce

import (
	"encoding/json"
	generate "github.com/invopop/jsonschema"
	validate "github.com/santhosh-tekuri/jsonschema/v6"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const schemaRoot = "../../docs/schemas"

// Regenerate after intentional wire changes:
// MYCE_UPDATE_SCHEMAS=1 go test ./internal/myce -run '^TestWireSchemas$'
// Reflection preserves the real JSON tags/types; annotations describe the
// stable consumer surface without changing the production structs.
func generatedJobSchema(t *testing.T) []byte {
	t.Helper()
	r := generate.Reflector{Anonymous: true, AllowAdditionalProperties: true}
	data, err := json.Marshal(r.Reflect(&Job{}))
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err = json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	// encoding/json emits nil Go slices/maps as null. Reflection describes their
	// nonnil type; allow the standard nil representation as well.
	var nullableCollections func(any)
	nullableCollections = func(value any) {
		switch v := value.(type) {
		case map[string]any:
			if v["type"] == "array" {
				v["type"] = []string{"array", "null"}
			}
			if v["type"] == "object" {
				if _, ok := v["additionalProperties"].(map[string]any); ok {
					v["type"] = []string{"object", "null"}
				}
			}
			for _, child := range v {
				nullableCollections(child)
			}
		case []any:
			for _, child := range v {
				nullableCollections(child)
			}
		}
	}
	nullableCollections(schema)
	schema["title"] = "Mycelium Job reply v1"
	schema["description"] = "Generated from Job and Spec JSON types by TestWireSchemas. This is a reply schema, not submission validation: a cancelled reservation has a minimal zero-valued spec. x-contract marks stable identity/lifecycle fields versus diagnostic projections; reason text is never a machine contract. Optional fields follow Go omitempty; observation replies include spec_projection describing omitted spec fields, including files and run. Additional diagnostics may grow."
	defs := schema["$defs"].(map[string]any)
	// GET Job replies redact execution payloads, including run.command.
	// Spec encoding itself stays unchanged for durable submission fingerprints.
	delete(defs["RunSpec"].(map[string]any), "required")
	for _, definition := range []string{"Job", "Spec"} {
		properties := defs[definition].(map[string]any)["properties"].(map[string]any)
		for _, property := range properties {
			property.(map[string]any)["x-contract"] = "diagnostic"
		}
		stable := []string{"id", "spec", "spec_projection", "state", "attempt", "machine", "session", "release_confirmed", "finished_at", "queued", "pause", "pause_revision", "cancelled_at", "infrastructure_retries", "dependency", "cancellation", "cancellation_scope", "ledger", "delivery", "recovered_results"}
		if definition == "Spec" {
			stable = []string{"project", "submission_key", "after", "output_delivery"}
		}
		for _, name := range stable {
			properties[name].(map[string]any)["x-contract"] = "stable"
		}
	}
	binding := defs["PieceBinding"].(map[string]any)["properties"].(map[string]any)
	binding["state"].(map[string]any)["enum"] = []string{"blocked", "ready"}
	binding["execution"].(map[string]any)["description"] = "Immutable producing child job ID and accepted attempt for a selected-piece edge; zero value for a whole-parent edge."
	binding["executions"].(map[string]any)["description"] = "Whole-parent edge: accepted producing executions keyed by mapped producer piece. Immutable across retry and restart."
	job := defs["Job"].(map[string]any)["properties"].(map[string]any)
	job["recovered_results"].(map[string]any)["description"] = "Append-only recovery-time evidence linked to exact failed executions. Never changes state, native outcome, ordinary output_refs or dependency eligibility. Metadata and original provenance survive history pruning; bytes expire at each record's expires_at."
	recovered := defs["RecoveredResult"].(map[string]any)["properties"].(map[string]any)
	recovered["kind"].(map[string]any)["const"] = "recovered"
	recovered["recovered_by"].(map[string]any)["const"] = "operator"
	recovered["requested_by"].(map[string]any)["description"] = "Operator-asserted requester label, not a separately authenticated user identity."
	recovered["manifest_sha256"].(map[string]any)["description"] = "SHA-256 of canonical Go JSON for files (sorted keys, compact encoding), verified at recovery time."
	job["spec_projection"].(map[string]any)["description"] = "Observation privacy policy: projected is true; omitted lists stripped paths relative to spec, regardless of submitted presence. Empty placeholders do not retain submitted values. Absence of spec.files is not evidence that no files were submitted."
	job["start_window"].(map[string]any)["description"] = "Effective original production start-window length in nanoseconds, retained across recovery. Available before startup; absent for experiments."
	job["start_by_explicit"].(map[string]any)["description"] = "Whether submitted start_by was caller-owned and absolute. New jobs persist true or false; absent on older records means preserve their existing cutoff as absolute."
	specProperties := defs["Spec"].(map[string]any)["properties"].(map[string]any)
	specProperties["files"].(map[string]any)["description"] = "SHA-256/size-addressed inputs. Native jobs stage into work; managed Docker file batches require docker_files_v1 and mount private copies read-only at /myce/inputs, preserving relative names. Cannot mix NAS inputs, caller workdir or shared_inputs with Docker."
	specProperties["outputs"].(map[string]any)["description"] = "Declared relative output names to relative publication prefixes. Managed Docker file batches write beneath /myce/work; only verified current-attempt outputs become output_refs after all writers settle."
	dockerProperties := defs["RunSpec"].(map[string]any)["properties"].(map[string]any)
	dockerProperties["docker"].(map[string]any)["description"] = "Opt-in managed direct docker run argv with a local digest-pinned image. With files or outputs: worker-owned mounts, UID/GID, /myce/work working directory and environment only; caller mounts/env/workdir/user and shared_inputs are rejected. Requires Linux docker_files_v1 worker capability."
	specProperties["start_window"].(map[string]any)["description"] = "Optional production relative start duration (Go duration string), greater than zero and at most 1h. Defaults to 150s; explicit start_by takes precedence."
	specProperties["wall_time"].(map[string]any)["description"] = "Optional positive per-attempt Go duration from accepted Started; omission is unlimited. Expiry follows ordinary command-failure policy, never infrastructure loss."
	job["wall_time"].(map[string]any)["description"] = "Execution allowance, distinct from runtime estimates. Expiry/remaining appear after Started; elapsed grace or terminal state does not prove release."
	delete(job, "ledger_entries") // Persistence only; the supported read surface is paginated.
	delete(defs, "JobLedgerEntry")
	job["ledger"].(map[string]any)["description"] = "Retained ledger summary: complete is historical coverage, high_water is the last durable sequence. GET /api/jobs/ID/ledger enumerates immutable entries to a fixed high_water. Older incomplete history is explicit; no diagnostic cap applies."
	job["isolation"].(map[string]any)["description"] = "Native stage exception diagnostic: unsandboxed: followed by the author reason, or the fixed legacy registration reason. Retained across lifecycle and payload redaction."
	job["cancellation"].(map[string]any)["description"] = "Read-only cancellation acknowledgement after durable cancellation. Same aggregate release as cancel, including descendants; not_applicable explicitly settles never-attempted work without physical-release evidence."
	job["cancellation_scope"].(map[string]any)["description"] = "Read-only retained fan-out declaration: every piece key, materialization/acceptance, optional wave ID, registered dependent IDs, and flat descendant job/release statuses. Future submissions appear on subsequent reads."
	for _, definition := range []string{"CancellationMember", "CancellationPiece"} {
		props := defs[definition].(map[string]any)["properties"].(map[string]any)
		props["release"].(map[string]any)["enum"] = []string{"not_applicable", "pending", "confirmed"}
	}
	ackProperties := defs["Cancellation"].(map[string]any)["properties"].(map[string]any)
	for _, name := range []string{"job_id", "submission_key", "release", "acknowledged_at"} {
		ackProperties[name].(map[string]any)["type"] = []string{"string", "null"}
	}
	job["state"].(map[string]any)["enum"] = []string{"queued", "paused", "waiting", "running", "stopping", "succeeded", "failed", "cancelled"}
	for _, name := range []string{"id", "machine", "session"} {
		job[name].(map[string]any)["minLength"] = 1
	}
	job["attempt"].(map[string]any)["minimum"] = 0
	job["attempt_history"].(map[string]any)["description"] = "Ended-attempt diagnostic evidence. Open work is pinned; capacity-aware maintenance may compact redundant closed-attempt diagnoses. Immutable custody/learning ledgers remain separate. Historical causes are not reconstructed."
	job["memory_attempts"].(map[string]any)["description"] = "Persisted memory facts, independent of learning and diagnostic history. Retention follows local capacity and evidence dependencies; open attempts and distinct measurements are preserved. memory_attempts_dropped counts historical evictions. Prelaunch attempts without a peak are omitted. Exact attempt/machine/session, observed memory_peak when available, and the resolved memory_envelope. Missing counters are unknown, not zero. Historical readings cannot be reconstructed."
	job["resource_attempts"].(map[string]any)["description"] = "Per-attempt cumulative CPU and block I/O counters with latest I/O PSI, retaining every distinct attempt. resource_attempts_dropped reports historical evictions. Exact attempt/machine/session fenced; missing counters unknown; process-tree CPU a sampled lower bound. Observational only."
	job["resource_attempts_dropped"].(map[string]any)["minimum"] = 0
	job["memory_attempts_dropped"].(map[string]any)["minimum"] = 0
	job["memory_attempts_dropped"].(map[string]any)["description"] = "Number of historical memory attempt rows evicted by earlier releases. Omitted zero. Prelaunch attempts without a peak do not create rows or increment this count."
	peak := defs["AttemptMemoryPeak"].(map[string]any)["properties"].(map[string]any)
	peak["device_gb"].(map[string]any)["description"] = "Sampled per-attempt process GPU memory in GiB, not GPU-wide memory.used or an allocator high-water mark. CUDA uses nvidia-smi compute-apps even on unified-memory devices. Sampling is a lower bound; missing means unknown, not zero."
	peak["device_unavailable"].(map[string]any)["description"] = "Diagnostic reason no usable process GPU sample was retained. New CUDA/XPU cgroup observers report this when device_gb is absent; older workers may omit both. Does not authorize reduced unified-memory accounting."
	peak["events"].(map[string]any)["description"] = "Attempt-parent hierarchical memory.events counts. Absent means not read; explicit zero means a read zero. Max per counter across samples within the attempt."
	peak["swap_gb"].(map[string]any)["description"] = "Attempt-parent memory.swap.peak in GiB, max across samples. Absent when unavailable; zero is measured. memory.swap.current is pressure evidence only, never a substitute peak."
	peak["constrained"].(map[string]any)["description"] = "Observed constraint evidence; omitted false. Constrained facts are retained even when learning rejects them."
	peak["pressure_unavailable"].(map[string]any)["description"] = "Pressure coverage was missing, malformed or had no valid cumulative-counter time window. Rejects this attempt's memory learning while preserving prior qualified evidence; raw diagnostic peaks remain."
	job["infrastructure_retries"].(map[string]any)["minimum"] = 0
	job["infrastructure_retries"].(map[string]any)["description"] = "Production infrastructure requeues already used. Omitted zero; default maximum is 2, configurable by the queue. Stage failures do not consume this budget."
	job["reason"].(map[string]any)["description"] = "Sanitized diagnostic only. Do not parse or match its text."
	job["release_confirmed"].(map[string]any)["description"] = "Exact current execution release proof when true. Omitted false is not confirmation; older copies may still have pending custody. Use the cancellation acknowledgement for aggregate release."
	job["finished_at"].(map[string]any)["description"] = "Recorded terminal time, when present. Historical terminal records may omit it."
	job["machine"].(map[string]any)["description"] = "Assigned execution identity; omitted before assignment or after requeue."
	job["session"].(map[string]any)["description"] = "Worker session of the assigned execution; combine with job ID and attempt."
	spec := defs["Spec"].(map[string]any)["properties"].(map[string]any)
	spec["disk"].(map[string]any)["description"] = "Optional fallback input/output/checkpoint size hints in bytes; known measured manifests take precedence. Missing size evidence uses only the volume free floor."
	spec["priority"].(map[string]any)["enum"] = []string{"", "production", "deadline", "experiment"}
	for _, name := range []string{"project", "submission_key"} {
		spec[name].(map[string]any)["minLength"] = 1
	}
	spec["submission_key"].(map[string]any)["description"] = "Exact project-scoped submission identity echo. Omitted for an unkeyed job."
	spec["submission_key"].(map[string]any)["pattern"] = `^[A-Za-z0-9._:-]{1,128}$`
	spec["submission_key"].(map[string]any)["maxLength"] = 128
	result, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(result, '\n')
}

func wireJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func wireError(t *testing.T, err *HTTPError) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	respond(recorder, nil, err) // Exercise the actual HTTPError-to-JSON wire path.
	if recorder.Code != err.Status {
		t.Fatalf("HTTP status %d != %d", recorder.Code, err.Status)
	}
	var body map[string]any
	if e := json.Unmarshal(recorder.Body.Bytes(), &body); e != nil {
		t.Fatal(e)
	}
	return body
}

func wireExamples(t *testing.T) map[string]any {
	t.Helper()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	s := spec("experiment")
	s.Name = "example-job"
	s.Project = "example"
	s.SubmissionKey = "example-key"
	selected := s
	selected.AllowedMachines = []string{"example-machine"}
	selected.Needs.GPU = &GPUSelection{Devices: []string{"0", "1"}}
	deviceJob := Job{ID: "example-device-job", Spec: selected, State: "running", Machine: "example-machine", AllocatedDevices: []string{"1"}, Attempt: 1}
	queued := Job{ID: "example-job-id", Spec: s, State: "queued", Queued: now, Checkpoint: "example-checkpoints/example-job-id"}
	running := queued
	running.State, running.Machine, running.Session, running.Attempt = "running", "example-worker-id", "example-session-id", 1
	running.AssignedAt, running.StartedAt = &now, &now
	running.Starts = 1
	succeeded := running
	succeeded.State = "succeeded"
	succeeded.FinishedAt = &now
	succeeded.ReleaseConfirmed = true
	host, cgroup, swap := 1.0, 2.0, 0.0
	succeeded.MemoryAttempts = []AttemptMemoryFacts{{Attempt: 1, Machine: running.Machine, Session: running.Session,
		MemoryPeak:     &AttemptMemoryPeak{HostGB: &host, CgroupGB: &cgroup, SwapGB: &swap, Events: &MemoryEventCounts{}},
		MemoryEnvelope: &JobMemoryEnvelope{Source: "exploratory first run", Machine: running.Machine, HostGB: 4, Enforcement: "cgroup-v2"}}}
	reserved := Job{ID: "example-reserved-id", Spec: Spec{Project: "example", SubmissionKey: "example-reserved-key"}, State: "cancelled", Queued: now, CancelledAt: &now, FinishedAt: &now, Reason: "cancelled before submission"}
	ack := Cancellation{JobID: &queued.ID, Project: "example", SubmissionKey: &s.SubmissionKey, State: "cancelled", WillNotRun: true, Release: releaseState("not_applicable"), AcknowledgedAt: &now}
	pending := ack
	pending.State = "stopping"
	pending.Release = releaseState("pending")
	terminal := ack
	terminal.State = "succeeded"
	terminal.Release = releaseState("confirmed")
	removed := terminal
	removed.State = "removed"
	absent := ack
	absent.JobID = nil
	absent.State = "absent"
	expired := absent
	expired.State = "unknown"
	expired.Release = nil
	expired.AcknowledgedAt = nil
	aborted := queued
	aborted.ID, aborted.State, aborted.CancelledAt = "example-parent", "cancelled", &now
	parentAck := ack
	parentAck.JobID, parentAck.Release = &aborted.ID, releaseState("confirmed")
	aborted.Cancellation = &parentAck
	aborted.CancellationScope = &CancellationScope{
		Pieces: []CancellationPiece{
			{Key: "first", Materialized: true, Accepted: true, WaveID: "example-wave", WillNotRun: true, Release: "confirmed", Dependents: []string{}},
			{Key: "second", WillNotRun: true, Release: "not_applicable", Dependents: []string{"example-consumer"}},
		},
		Members: []CancellationMember{
			{JobID: "example-consumer", State: "cancelled", WillNotRun: true, Release: "not_applicable"},
			{JobID: "example-wave", State: "succeeded", WillNotRun: true, Release: "confirmed"},
		},
	}
	revision := uint64(7)
	return map[string]any{
		"job-cancelled-fan-out":       aborted,
		"job-device-selected":         deviceJob,
		"error-no-qualified-target":   wireError(t, &HTTPError{Status: 409, Code: "no_qualified_target", Message: "host memory need 8.125 GiB (8 + 0.125 slack) exceeds allowance 8 GiB on example-machine; intent retained locally"}),
		"error-machine-renamed":       wireError(t, &HTTPError{Status: 400, Code: "machine_renamed", Message: `machine old-gpu0 was renamed; use allowed_machines: [example-machine] plus needs.gpu.devices: ["0"]`}),
		"error-environment-unknown":   wireError(t, &HTTPError{Status: 404, Code: "environment_unknown", Message: rejectionMessages["environment_unknown"]}),
		"error-gpu-count-unsupported": wireError(t, &HTTPError{Status: 400, Code: "gpu_count_unsupported", Message: rejectionMessages["gpu_count_unsupported"]}),
		"job-queued-keyed":            queued, "job-running": running, "job-succeeded": succeeded, "job-reserved-cancelled": reserved,
		"cancel-queued": ack, "cancel-running-pending": pending, "cancel-terminal": terminal, "cancel-removed": removed, "cancel-absent": absent, "cancel-expired-unknown": expired,
		"error-job-unknown":         wireError(t, &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown", JobID: "example-unknown-id"}),
		"error-submission-conflict": wireError(t, submissionFailure(false)), "error-submission-expired": wireError(t, submissionFailure(true)),
		"error-submission-invalid": wireError(t, validateSubmissionKey("").(*HTTPError)),
		"error-policy-revision":    wireError(t, &HTTPError{Status: 409, Code: "policy_revision_conflict", CurrentRevision: &revision, Message: "policy revision changed; current revision 7"}),
		"error-extension-version":  wireError(t, frozenExtensionConflict("example@1", strings.Repeat("a", 64), strings.Repeat("b", 64)).(*HTTPError)),
		"error-job-ambiguous":      wireError(t, &HTTPError{Status: 409, Code: "job_id_ambiguous", Message: "job ID prefix is ambiguous; use a longer ID"}),
	}
}

func compileWireSchema(t *testing.T, name string) *validate.Schema {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(schemaRoot, name))
	if err != nil {
		t.Fatal(err)
	}
	var schema any
	if err = json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	compiler := validate.NewCompiler()
	compiler.AssertFormat()
	if err = compiler.AddResource("https://schemas.example/"+name, schema); err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.Compile("https://schemas.example/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func validateWireValue(t *testing.T, schema *validate.Schema, value any) {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(wireJSON(t, value), &decoded); err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(decoded); err != nil {
		t.Fatal(err)
	}
}
