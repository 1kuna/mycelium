package myce

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// Hold is the one record for every reason Mycelium keeps runnable work waiting:
// a job's current wait, or a fence on a host or device. Owner clears it, Reason
// says why in one sentence, Clears names the condition, Expires is set only
// when a known time ends it, and Next is the one action to take. Detail keeps
// the producing mechanism's evidence. A Hold is an observation; admission
// decisions stay with the placement predicates that produce it.
type Hold struct {
	Kind    string     `json:"kind"`
	Scope   string     `json:"scope,omitempty"`
	Owner   string     `json:"owner"`
	Reason  string     `json:"reason"`
	Clears  string     `json:"clears"`
	Expires *time.Time `json:"expires,omitempty"`
	Next    string     `json:"next"`
	Detail  string     `json:"detail,omitempty"`
	// Platforms (os/arch/accelerator, never hosts) whose fresh hosts have room
	// for this waiting job now and are refused only by its authored targets,
	// comma-separated. A string keeps Hold comparable.
	RunsNowOn string `json:"runs_now_on,omitempty"`
}

// One reason and one next action, as default status shows them.
func (h *Hold) Text() string {
	if h == nil {
		return ""
	}
	text := h.Reason
	if h.Next != "" {
		text += "; next: " + h.Next
	}
	return text
}

// Full status adds the producing mechanism's evidence.
func (h *Hold) FullText() string {
	if h == nil {
		return ""
	}
	text := h.Text()
	if h.Expires != nil {
		text += "; expires " + h.Expires.UTC().Format(time.RFC3339)
	}
	if h.Detail != "" && h.Detail != h.Reason {
		text += "; " + strings.Join(strings.Fields(h.Detail), " ")
	}
	return text
}

func (h Hold) operatorAction() bool { return h.Owner == ownerOperator }

const (
	ownerQueue    = "queue"
	ownerOperator = "operator"
	ownerConsumer = "consumer"
	ownerProducer = "producer"
	ownerBuilder  = "builder"
	ownerStorage  = "storage owner"
	ownerPolicy   = "queue owner"
)

type holdKind struct{ owner, reason, clears, next string }

// Every mechanism that holds work is one row here. A new hold adds a row; it
// does not add its own status text, owner rules or next-action wording.
var holdKinds = map[string]holdKind{
	// Job waits: the scheduler's current reason for a queued job.
	"paused":                   {ownerConsumer, "paused", "resumed, or the pause's until time passes", "myce resume JOB_ID when it should run"},
	"fan_out_cap":              {ownerConsumer, "at its declared max_concurrency", "a running piece of the same parent finishes", "wait, or resubmit with a higher max_concurrency"},
	"upstream":                 {ownerProducer, "waiting for an upstream result", "the required upstream job succeeds and publishes its result", "complete and publish the required upstream result"},
	deviceFaultRetryHold:       {ownerQueue, "waiting for healthy accelerator capacity after a driver fault", "another healthy device or host fits, or the quarantined device proves healthy", "wait; Mycelium retries on healthy capacity automatically"},
	"launch_capacity":          {ownerQueue, "waiting for launch capacity or an exact release", "capacity evidence changes or the pending release is acknowledged", "wait; nothing to do"},
	"helper_charge":            {ownerQueue, "waiting for launch capacity that covers the job's measured helper memory", "capacity evidence changes or the pending release is acknowledged", "wait; nothing to do"},
	"checkpoint_root":          {ownerStorage, "saved progress storage is unavailable", "the queue's checkpoint root is reachable again", "restore the queue's checkpoint storage mount"},
	checkpointTargetHold:       {ownerQueue, "resuming on the target that wrote its checkpoint", "a host of that target can take it, or none remains in the fleet", "wait; if its checkpoints resume correctly on other targets, declare resume_compatibility for them"},
	"scarce_host_yield":        {ownerQueue, "waiting for portable work to yield a scarce host", "the yielding attempt saves its checkpoint and releases the host", "wait; nothing to do"},
	"shared_host_backoff":      {ownerQueue, "backing off a shared host whose owner needed it", "the host's owner pressure stays quiet for the measured recovery span", "wait; another eligible host can still take it"},
	"storage_root":             {ownerStorage, "a worker's storage root is unreachable", "the storage root is reachable again", "restore the worker's storage mount"},
	productionParityHostHold:   {ownerConsumer, "production work is held to the machine kinds its project approved", "the project approves a machine kind that can run it", "compare outputs with the reference on a machine of a kind that can run it, then myce parity add --evidence JOB_ID"},
	"no_matching_device":       {ownerConsumer, "no selected device is online and compatible", "a selected device is online and compatible", "check the device selector, or bring a selected device online"},
	"log_policy":               {ownerConsumer, "no eligible host supports this log policy", "a host that supports logs: none is eligible", "use the default log policy, or add a host that supports logs: none"},
	"project_cap":              {ownerQueue, "waiting under the project's concurrency limit", "the project's running work drops below its limit", "wait; nothing to do"},
	"production_admission":     {ownerQueue, "waiting for production to finish starting", "the production arrival or setup window passes", "wait; nothing to do"},
	"unified_cuda_headroom":    {ownerQueue, "a unified-memory host's headroom is kept for expected production", "the expected production starts, or another host can take it", "wait; nothing to do"},
	"pinned_model":             {ownerOperator, "a pinned model prevents production placement", "the pin is removed or the model is unloaded", "unpin the model if production should use that host"},
	"disk_headroom":            {ownerQueue, "waiting for disk space on an eligible host", "enough local disk is free", "wait; free disk space on the host if this persists"},
	"file_store_capacity":      {ownerQueue, "waiting for file-store capacity to retry outputs", "the file store reacquires the output reservation", "wait; nothing to do"},
	"storage_parked":           {ownerConsumer, "this job's storage protection is parked", "the job uses file-store outputs or async_nas", "resubmit with file-store outputs or async_nas"},
	"reservation":              {ownerQueue, "waiting for a reservation on an eligible host to end", "the reservation is released or expires", "wait; nothing to do"},
	"capacity":                 {ownerQueue, "waiting for capacity on an eligible host", "the named capacity is released", "wait; keep other work moving"},
	"memory":                   {ownerQueue, "waiting for memory on an eligible host", "enough measured memory is free", "wait; keep other work moving"},
	"host_memory_headroom":     {ownerQueue, "waiting for host memory on an eligible host", "enough measured host memory is free", "wait; keep other work moving"},
	"cold_probe_margin":        {ownerQueue, "waiting for host memory above the first-run launch margin", "enough measured host memory is free above the launch margin", "wait; keep other work moving"},
	memoryEvidenceRecovering:   {ownerQueue, "waiting for safe capacity or fresh comparable memory evidence", "a host fits with safe capacity, or comparable memory evidence arrives", "wait; keep other work moving"},
	"host_backfill":            {ownerQueue, "waiting for host CPU capacity", "enough host cores are free", "wait; keep other work moving"},
	"cpu_isolation":            {ownerOperator, "an eligible host cannot keep CPU work off its accelerators", "the host qualifies accelerator isolation, or another host fits", "check the host's installed CUDA/XPU runtimes"},
	"shared_input":             {ownerQueue, "waiting for a shared input to stage", "the shared input is staged on an eligible host", "wait; nothing to do"},
	"shared_input_unreported":  {ownerQueue, "waiting for workers to report shared-input cache capacity", "an eligible worker reports its cache capacity", "wait; nothing to do"},
	"extension":                {ownerConsumer, "no eligible host has a qualified extension for this work", "an extension is installed and qualified on an eligible host", "install or qualify the extension"},
	"capability":               {ownerConsumer, "no eligible host has a required capability", "a host with the capability is eligible", "drop the capability requirement, or add a capable host"},
	"no_eligible_host":         {ownerConsumer, "no host can run this job right now", "an eligible host with enough capacity is online", "check the job's needs and allowed machines"},
	"waiting_for_worker":       {ownerQueue, "waiting for a worker to take this job", "an eligible worker checks in", "wait; nothing to do"},
	"model_engine":             {ownerQueue, "waiting for a model engine to recover", "the model engine on an eligible worker is ready", "wait; check the named worker if this persists"},
	"unknown_observation":      {ownerPolicy, "a required observation is missing or stale", "fresh worker observations arrive", "restore the missing observation named in the detail"},
	"unprepared":               {ownerQueue, "software is not prepared yet", "the queue prepares the environment", "submit normally; the queue prepares it before launch"},
	"waiting_for_builder":      {ownerQueue, "waiting for software preparation", "the named preparation finishes", "wait; ask the queue owner if preparation makes no progress"},
	"building":                 {ownerBuilder, "software is being prepared", "preparation finishes", "wait; inspect environment logs if progress stalls"},
	"deterministically_failed": {ownerConsumer, "software preparation failed", "the recipe is fixed and resubmitted", "fix the recipe and resubmit with changed inputs"},

	// Host and device fences.
	"unclassified":         {ownerOperator, "a host fence has no known classification", "an operator resolves it", "inspect the host fence"},
	"startup_release":      {ownerQueue, "a failed production start still holds its resources", "the exact execution is released", "wait; nothing to do"},
	"host_disabled":        {ownerOperator, "the host is disabled for maintenance", "an operator resumes the host", "myce host resume HOST when maintenance is done"},
	"reservation_active":   {ownerConsumer, "components are reserved for one job", "the reservation is released or expires", "myce reserve release ID when the reserved work is done"},
	"component_exclusion":  {ownerOperator, "components are excluded from placement", "an operator removes the exclusion", "myce exclude remove SELECTOR when the components should run work"},
	"device_maintenance":   {ownerOperator, "a device is disabled by an operator", "an operator enables the device", "myce host device enable HOST DEVICE when it should run work"},
	"host_unavailable":     {ownerQueue, "eligible hosts are unavailable", "an eligible host is available again", "wait; check host status if this persists"},
	"missing_extension":    {ownerConsumer, "no implementation is registered for this work", "an implementation is registered", "register an implementation with its targets, environment and native check"},
	"execution_coverage":   {ownerConsumer, "no registered implementation covers an eligible target", "an implementation covers a supported target", "run myce portability and add an implementation for a supported target"},
	"native_qualification": {ownerQueue, "the queue is qualifying the implementation on compatible hardware", "the native check passes, or fails the job with its action", "wait; nothing to do"},
	"production_parity":    {ownerConsumer, "production needs reference-output parity for this variant", "the project accepts parity for a machine kind that can run it", "compare outputs with the reference on an eligible host, then myce parity add --evidence JOB_ID"},
}

// reasonHold is a check's hold when it gives a reason, and nil when it passes.
func reasonHold(kind, reason string) *Hold {
	if reason == "" {
		return nil
	}
	return newHold(kind, reason)
}

// heldBy is a host fence's hold when one is set.
func heldBy(h Hold) *Hold {
	if h.Kind == "" {
		return nil
	}
	return &h
}

// newHold fills the shared owner, sentence, clear condition and next action
// for kind. Detail carries the producing mechanism's evidence.
func newHold(kind, detail string) *Hold {
	k, ok := holdKinds[kind]
	if !ok {
		k = holdKinds["unclassified"]
	}
	return &Hold{Kind: kind, Owner: k.owner, Reason: k.reason, Clears: k.clears, Next: k.next, Detail: detail}
}

// The job diagnostic code a scheduler wait kind reports, where it has one.
var holdDiagnosticCodes = map[string]string{
	memoryEvidenceRecovering:  memoryEvidenceRecovering,
	"host_memory_headroom":    "host_memory_headroom",
	"cold_probe_margin":       "host_memory_headroom",
	"shared_input":            "shared_input_cache",
	"shared_input_unreported": "shared_input_cache_unreported",
	"disk_headroom":           "disk_headroom",
	"host_backfill":           "host_capacity_accumulation",
	"cpu_isolation":           cpuIsolationUnavailable,
	"no_matching_device":      "no_matching_device",
}

// The efficiency report's wait category for a scheduler wait kind. Kinds not
// listed are capacity waits.
var holdEfficiencyCategories = map[string]string{
	"paused": "holds", "production_admission": "holds",
	"pinned_model": "holds", checkpointTargetHold: "holds", "reservation": "holds", "unified_cuda_headroom": "holds",
	"waiting_for_builder": "preparation", "building": "preparation", "unprepared": "preparation",
	"shared_input": "preparation", "shared_input_unreported": "preparation",
	"deterministically_failed": "environment", "extension": "environment",
	"missing_extension": "environment", "execution_coverage": "environment", "native_qualification": "environment",
	"production_parity": "environment", productionParityHostHold: "environment", "capability": "environment", "log_policy": "environment",
	"memory": "memory_reservation", "host_memory_headroom": "memory_reservation", memoryEvidenceRecovering: "memory_reservation",
	"host_backfill": "memory_reservation", "cold_probe_margin": "memory_reservation",
	"no_matching_device": "authored_platform_targets", "no_eligible_host": "authored_platform_targets",
	"project_cap": "priority_fairness",
	"upstream":    "dependency",
}

func (h *Hold) efficiencyCategory() string {
	if c := holdEfficiencyCategories[h.Kind]; c != "" {
		return c
	}
	return "capacity"
}

// A host or device fence: the kind's defaults, with the operator owning it
// whenever the producing mechanism says no automatic path will clear it.
func scopedHold(kind, scope, detail string, operator bool) Hold {
	h := *newHold(kind, detail)
	h.Scope = scope
	if operator {
		h.Owner = ownerOperator
	}
	return h
}

// Operator and consumer fences, from their own records, as hold rows.
func operatorHolds(reservations []Reservation, exclusions []ComponentExclusion, devices []DeviceMaintenance) []Hold {
	var out []Hold
	for _, r := range reservations {
		if r.ReleasedAt != nil || r.ExpiredAt != nil {
			continue
		}
		h := scopedHold("reservation_active", r.Selector, r.Text, false)
		if r.Owner != "" {
			h.Owner = r.Owner
		}
		expires := r.ExpiresAt
		h.Expires = &expires
		h.Next = strings.ReplaceAll(h.Next, "ID", r.ID)
		out = append(out, h)
	}
	for _, p := range exclusions {
		if p.Active {
			h := scopedHold("component_exclusion", p.Selector, fmt.Sprintf("set by %s since %s; reason=%q", p.SetBy, p.UpdatedAt.UTC().Format(time.RFC3339), p.Reason), true)
			h.Next = strings.ReplaceAll(h.Next, "SELECTOR", p.Selector)
			out = append(out, h)
		}
	}
	for _, d := range devices {
		if d.Disabled {
			h := scopedHold("device_maintenance", d.Host+"/"+d.Identity, fmt.Sprintf("host %s device %s (%s): disabled by operator since %s; reason=%q", d.Host, d.Identity, d.Platform, d.UpdatedAt.UTC().Format(time.RFC3339), d.Reason), true)
			h.Next = strings.NewReplacer("HOST", d.Host, "DEVICE", d.Identity).Replace(h.Next)
			out = append(out, h)
		}
	}
	return out
}

func sortHolds(holds []Hold) {
	slices.SortFunc(holds, func(a, b Hold) int {
		if c := strings.Compare(a.Scope, b.Scope); c != 0 {
			return c
		}
		return strings.Compare(a.Kind, b.Kind)
	})
}

// One line per fence, as default status shows it.
func (h Hold) Line() string {
	text := "hold " + h.Scope + ": " + h.Reason
	if h.Detail != "" && h.Detail != h.Reason {
		text += " (" + strings.Join(strings.Fields(h.Detail), " ") + ")"
	}
	if h.Expires != nil {
		text += "; expires " + h.Expires.UTC().Format(time.RFC3339)
	}
	return text + "; next: " + h.Next
}

func pauseHold(j *Job) *Hold {
	if j.Pause == nil {
		return nil
	}
	h := newHold("paused", j.Pause.Reason)
	if j.Pause.Reason != "" {
		h.Reason = "paused: " + j.Pause.Reason
	}
	h.Next = strings.ReplaceAll(h.Next, "JOB_ID", j.ID)
	h.Expires = j.Pause.Until
	return h
}
