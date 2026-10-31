package myce

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	generate "github.com/invopop/jsonschema"
	validate "github.com/santhosh-tekuri/jsonschema/v6"
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
	schema["description"] = "Generated from Job and Spec JSON types by TestWireSchemas. This is a reply schema, not submission validation: a cancelled reservation has a minimal zero-valued spec. x-contract marks stable identity/lifecycle fields versus diagnostic projections; reason text is never a machine contract. Optional fields follow Go omitempty; GET replies also omit execution payloads, including run.command. Additional diagnostics may grow."
	defs := schema["$defs"].(map[string]any)
	// GET Job replies redact execution payloads, including run.command.
	// Spec encoding itself stays unchanged for durable submission fingerprints.
	delete(defs["RunSpec"].(map[string]any), "required")
	for _, definition := range []string{"Job", "Spec"} {
		properties := defs[definition].(map[string]any)["properties"].(map[string]any)
		for _, property := range properties {
			property.(map[string]any)["x-contract"] = "diagnostic"
		}
		stable := []string{"id", "spec", "state", "attempt", "machine", "session", "release_confirmed", "finished_at", "queued", "cancelled_at", "infrastructure_retries", "dependency", "cancellation", "cancellation_scope", "ledger", "delivery", "recovered_results"}
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
	job["start_window"].(map[string]any)["description"] = "Effective original production start-window length in nanoseconds, retained across recovery. Available before startup; absent for experiments."
	job["start_by_explicit"].(map[string]any)["description"] = "Whether submitted start_by was caller-owned and absolute. New jobs persist true or false; absent on older records means preserve their existing cutoff as absolute."
	specProperties := defs["Spec"].(map[string]any)["properties"].(map[string]any)
	specProperties["start_window"].(map[string]any)["description"] = "Optional production relative start duration (Go duration string), greater than zero and at most 1h. Defaults to 150s; explicit start_by takes precedence."
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
	job["state"].(map[string]any)["enum"] = []string{"queued", "running", "stopping", "succeeded", "failed", "cancelled"}
	for _, name := range []string{"id", "machine", "session"} {
		job[name].(map[string]any)["minLength"] = 1
	}
	job["attempt"].(map[string]any)["minimum"] = 0
	job["attempt_history"].(map[string]any)["maxItems"] = attemptHistoryLimit
	job["attempt_history"].(map[string]any)["description"] = "Latest four ended attempts, persisted with the job. Existing classifier facts and queue decisions, not release proof or retry charges. Older planned requeues roll off; historical causes are not reconstructed."
	job["memory_attempts"].(map[string]any)["maxItems"] = memoryAttemptsLimit
	job["memory_attempts"].(map[string]any)["description"] = "Persisted memory facts, independent of learning and the four-entry diagnostic history. Retains the first and latest 31 rows; memory_attempts_dropped counts evicted rows. Prelaunch attempts without a peak are omitted. Exact attempt/machine/session, observed memory_peak when available, and the resolved memory_envelope. Missing counters are unknown, not zero. Historical readings cannot be reconstructed."
	job["memory_attempts_dropped"].(map[string]any)["minimum"] = 0
	job["memory_attempts_dropped"].(map[string]any)["description"] = "Number of memory attempt rows evicted by the 32-row limit. Omitted zero. Prelaunch attempts without a peak do not create rows or increment this count."
	peak := defs["AttemptMemoryPeak"].(map[string]any)["properties"].(map[string]any)
	peak["device_gb"].(map[string]any)["description"] = "Sampled per-attempt process GPU memory in GiB, not GPU-wide memory.used or an allocator high-water mark. CUDA uses nvidia-smi compute-apps even on unified-memory devices. Sampling is a lower bound; missing means unknown, not zero."
	peak["device_unavailable"].(map[string]any)["description"] = "Diagnostic reason no usable process GPU sample was retained. New CUDA/XPU cgroup observers report this when device_gb is absent; older workers may omit both. Does not authorize reduced unified-memory accounting."
	peak["events"].(map[string]any)["description"] = "Attempt-parent hierarchical memory.events counts. Absent means not read; explicit zero means a read zero. Max per counter across samples within the attempt."
	peak["swap_gb"].(map[string]any)["description"] = "Attempt-parent memory.swap.peak in GiB, max across samples. Absent when unavailable; zero is measured. memory.swap.current is pressure evidence only, never a substitute peak."
	peak["constrained"].(map[string]any)["description"] = "Observed constraint evidence; omitted false. Constrained facts are retained even when learning rejects them."
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

func TestWireSchemas(t *testing.T) {
	generated := generatedJobSchema(t)
	ledgerReflector := generate.Reflector{Anonymous: true, AllowAdditionalProperties: true}
	ledgerSchema, ledgerErr := json.MarshalIndent(ledgerReflector.Reflect(&JobLedgerPage{}), "", "  ")
	if ledgerErr != nil {
		t.Fatal(ledgerErr)
	}
	ledgerSchema = append(ledgerSchema, '\n')
	ledgerPath := filepath.Join(schemaRoot, "job-ledger-v1.schema.json")
	if os.Getenv("MYCE_UPDATE_SCHEMAS") == "1" {
		if err := os.WriteFile(ledgerPath, ledgerSchema, 0644); err != nil {
			t.Fatal(err)
		}
	}
	actualLedger, ledgerErr := os.ReadFile(ledgerPath)
	if ledgerErr != nil || !bytes.Equal(actualLedger, ledgerSchema) {
		t.Fatal("ledger schema drift: regenerate with MYCE_UPDATE_SCHEMAS=1", ledgerErr)
	}
	ledgerValidation := compileWireSchema(t, "job-ledger-v1.schema.json")
	validateWireValue(t, ledgerValidation, JobLedgerPage{JobID: "job", Complete: true, Entries: []JobLedgerEntry{}})
	r := generate.Reflector{Anonymous: true, AllowAdditionalProperties: true}
	diskSchema, err := json.MarshalIndent(r.Reflect(&DiskVolume{}), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	diskSchema = append(diskSchema, '\n')
	if os.Getenv("MYCE_UPDATE_SCHEMAS") == "1" {
		if err := os.WriteFile(filepath.Join(schemaRoot, "disk-volume-v1.schema.json"), diskSchema, 0644); err != nil {
			t.Fatal(err)
		}
	}
	actualDisk, err := os.ReadFile(filepath.Join(schemaRoot, "disk-volume-v1.schema.json"))
	if err != nil || !bytes.Equal(actualDisk, diskSchema) {
		t.Fatal("disk schema drift: regenerate with MYCE_UPDATE_SCHEMAS=1", err)
	}
	disk := compileWireSchema(t, "disk-volume-v1.schema.json")
	validateWireValue(t, disk, DiskVolume{Roots: []string{"data", "environment"}, TotalBytes: 100 << 30, FreeBytes: 50 << 30, SampledAt: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)})
	if os.Getenv("MYCE_UPDATE_SCHEMAS") == "1" {
		if err := os.WriteFile(filepath.Join(schemaRoot, "job-v1.schema.json"), generated, 0644); err != nil {
			t.Fatal(err)
		}
		for name, value := range wireExamples(t) {
			if err := os.WriteFile(filepath.Join(schemaRoot, "examples", name+".json"), wireJSON(t, value), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	actual, err := os.ReadFile(filepath.Join(schemaRoot, "job-v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, generated) {
		t.Fatal("Job/Spec schema drift: regenerate with MYCE_UPDATE_SCHEMAS=1 go test ./internal/myce -run '^TestWireSchemas$'")
	}
	schemas := map[string]*validate.Schema{}
	for _, name := range []string{"job-v1.schema.json", "cancel-ack-v1.schema.json", "error-v1.schema.json"} {
		schemas[name] = compileWireSchema(t, name)
	}
	// Unkeyed cancellation and status redaction legitimately encode a null key
	// inside the embedded acknowledgement, just like the cancel endpoint.
	qUnkeyed, _ := fixture(t)
	unkeyed := submit(t, qUnkeyed, spec("experiment"))
	cancelAck(t, qUnkeyed, unkeyed.ID)
	unkeyedGET, err := qUnkeyed.jobSnapshot(unkeyed.ID)
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schemas["job-v1.schema.json"], unkeyedGET)
	for name, value := range wireExamples(t) {
		t.Run(name, func(t *testing.T) {
			kind := strings.SplitN(name, "-", 2)[0]
			schemaName := map[string]string{"job": "job-v1.schema.json", "cancel": "cancel-ack-v1.schema.json", "error": "error-v1.schema.json"}[kind]
			validateWireValue(t, schemas[schemaName], value)
		})
	}
	// Real queue values include assignments, actual S02 reservation binding, and
	// every nullable cancellation row; tests fail if wire omissions/types drift.
	q, c := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "schema-key"
	j := submit(t, q, s)
	validateWireValue(t, schemas["job-v1.schema.json"], j)
	// The retry counter is additive: old zero/omitted replies and new nonzero
	// replies validate against the same v1 reply contract.
	retried := cloneJob(j)
	retried.InfrastructureRetries = 1
	validateWireValue(t, schemas["job-v1.schema.json"], &retried)
	j = check(t, q, machine("one", "fake"), nil).Job
	validateWireValue(t, schemas["job-v1.schema.json"], j)
	ack, err := q.CancelJob(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schemas["cancel-ack-v1.schema.json"], ack)
	finish(t, q, j, true)
	validateWireValue(t, schemas["job-v1.schema.json"], q.s.Jobs[j.ID])
	ack, err = q.CancelSubmission(s.Project, s.SubmissionKey)
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schemas["cancel-ack-v1.schema.json"], ack)
	ack, err = q.CancelSubmission(s.Project, "reservation")
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schemas["cancel-ack-v1.schema.json"], ack)
	reserved := s
	reserved.SubmissionKey = "reservation"
	reserved.Run.Command = nil
	validateWireValue(t, schemas["job-v1.schema.json"], submit(t, q, reserved))
	ack, err = q.CancelSubmission(s.Project, "expired")
	if err != nil {
		t.Fatal(err)
	}
	c.Advance(submissionRetention)
	ack, err = q.CancelSubmission(s.Project, "expired")
	if err != nil {
		t.Fatal(err)
	}
	validateWireValue(t, schemas["cancel-ack-v1.schema.json"], ack)
	paths, err := filepath.Glob(filepath.Join(schemaRoot, "examples", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err = json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		prefix := strings.SplitN(filepath.Base(path), "-", 2)[0]
		schemaName := map[string]string{"job": "job-v1.schema.json", "cancel": "cancel-ack-v1.schema.json", "error": "error-v1.schema.json"}[prefix]
		if schemaName == "" {
			t.Fatalf("example has no schema: %s", path)
		}
		if err := schemas[schemaName].Validate(value); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestWireSchemaRejectsMalformedReplies(t *testing.T) {
	cases := []struct {
		schema, example, field string
		value                  any
	}{
		{"job-v1.schema.json", "job-queued-keyed", "state", "absent"},
		{"job-v1.schema.json", "job-running", "attempt", "one"},
		{"cancel-ack-v1.schema.json", "cancel-running-pending", "will_not_run", false},
		{"cancel-ack-v1.schema.json", "cancel-expired-unknown", "release", "unknown"},
		{"error-v1.schema.json", "error-job-unknown", "will_not_run", true},
		{"error-v1.schema.json", "error-submission-expired", "code", "unspecified"},
	}
	for _, tc := range cases {
		t.Run(tc.example+"/"+tc.field, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal(wireJSON(t, wireExamples(t)[tc.example]), &value); err != nil {
				t.Fatal(err)
			}
			value[tc.field] = tc.value
			if err := compileWireSchema(t, tc.schema).Validate(value); err == nil {
				t.Fatal("malformed reply accepted")
			}
		})
	}
}
