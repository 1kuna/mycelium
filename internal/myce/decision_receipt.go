package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// DecisionReceipt is historical observation, never placement authority. Protocol
// labels are fixed; deployment identities are anonymized before publication.
type DecisionReceipt struct {
	Incoming         DecisionIncoming `json:"incoming"`
	Memory           DecisionMemory   `json:"memory"`
	ProjectedMemory  DecisionMemory   `json:"projected_memory"`
	ImmediateMemory  DecisionMemory   `json:"immediate_memory"`
	Request          uint64           `json:"request"`
	ObservedThrough  time.Time        `json:"observed_through"`
	ThroughRevision  uint64           `json:"through_revision"`
	Checkins         uint64           `json:"checkins"`
	PolicyRevision   uint64           `json:"policy_revision"`
	Preparation      string           `json:"preparation,omitempty"`
	PreparationCount int              `json:"preparation_count"`
	ReleaseFence     string           `json:"release_fence,omitempty"`
	ReleaseCount     int              `json:"release_count"`
	releaseIDs       []string
	CheckinWorker    string            `json:"checkin_worker"`
	CheckinSession   string            `json:"checkin_session"`
	JobID            string            `json:"job_id"`
	Epoch            string            `json:"epoch"`
	Revision         uint64            `json:"revision"`
	At               time.Time         `json:"at"`
	Checkin          uint64            `json:"checkin"`
	Worker           string            `json:"worker"`
	Session          string            `json:"session"`
	Target           string            `json:"target"`
	Admission        string            `json:"admission"`
	Admitted         bool              `json:"admitted"`
	Winner           string            `json:"winner,omitempty"`
	WinnerClass      string            `json:"winner_class,omitempty"`
	Fairness         *DecisionFairness `json:"fairness,omitempty"`
	GlobalWinner     string            `json:"global_winner,omitempty"`
	Immediate        string            `json:"immediate"`
	EarlyReturn      string            `json:"early_return"`
	Reply            DecisionReply     `json:"reply"`
}
type DecisionFairness struct {
	Before bool   `json:"before"`
	Reason string `json:"reason"`
}

// Only fixed protocol labels and numeric/boolean facts cross the boundary.
type DecisionIncoming struct {
	Protocol      string `json:"protocol"`
	Recovering    bool   `json:"recovering"`
	Fresh         bool   `json:"fresh"`
	Started       bool   `json:"started"`
	Running       bool   `json:"running"`
	Reports       int    `json:"reports"`
	Devices       bool   `json:"devices"`
	Leaving       bool   `json:"leaving"`
	BenchmarkDone bool   `json:"benchmark_done"`
}
type DecisionMemory struct {
	Evaluated        bool    `json:"evaluated"`
	Auto             bool    `json:"auto"`
	Envelope         bool    `json:"envelope"`
	Refused          bool    `json:"refused"`
	Measured         bool    `json:"measured"`
	Unified          bool    `json:"unified"`
	HostGB           float64 `json:"host_gb"`
	DeviceGB         float64 `json:"device_gb"`
	EnvelopeHostGB   float64 `json:"envelope_host_gb"`
	EnvelopeDeviceGB float64 `json:"envelope_device_gb"`
	ProbeFloorGB     float64 `json:"probe_floor_gb"`
	Evidence         int     `json:"evidence"`
	HostNeedGB       float64 `json:"host_need_gb"`
	SlackGB          float64 `json:"slack_gb"`
	FreeHostGB       float64 `json:"free_host_gb"`
	AvailableHostGB  float64 `json:"available_host_gb"`
	ReservedHostGB   float64 `json:"reserved_host_gb"`
	AllowanceGB      float64 `json:"allowance_gb"`
	FreeDeviceGB     float64 `json:"free_device_gb"`
}

func decisionMemory(j *Job, m *Machine, deviceRoom float64) DecisionMemory {
	n := j.Spec.Needs
	d := DecisionMemory{Evaluated: true, Auto: n.Memory == "auto", Measured: m.HostMemoryMeasured, Unified: m.UnifiedMemory, HostGB: n.HostMemoryGB, DeviceGB: n.MemoryGB, HostNeedGB: hostNeed(m, n.MemoryGB, n.HostMemoryGB), SlackGB: m.MemoryCeilingSlackGB, FreeHostGB: m.FreeHostMemoryGB, AvailableHostGB: m.HostAvailableMemoryGB, ReservedHostGB: m.HostMemoryInUseGB, AllowanceGB: m.EffectiveHostMemoryBudgetGB, FreeDeviceGB: deviceRoom}
	if e := j.MemoryEnvelope; e != nil {
		d.Envelope, d.Refused, d.ProbeFloorGB, d.Evidence = true, e.Refusal != "", e.ProbeFloorGB, e.Evidence
		d.EnvelopeHostGB, d.EnvelopeDeviceGB = e.HostGB, e.DeviceGB
	}
	return d
}

type DecisionDirective struct {
	Execution
	Addressed bool   `json:"addressed"`
	Session   string `json:"session"`
	Parent    string `json:"parent,omitempty"`
	Offer     bool   `json:"offer"`
	Stop      bool   `json:"stop"`
	Kill      bool   `json:"kill"`
	Hold      bool   `json:"hold"`
}
type DecisionReply struct {
	Directives        []DecisionDirective `json:"directives"`
	DirectivesOmitted int                 `json:"directives_omitted"`
	AdmissionBlocked  bool                `json:"admission_blocked"`
	Identity          string              `json:"identity"`
	Stop              bool                `json:"stop"`
	Kill              bool                `json:"kill"`
	Hold              bool                `json:"hold"`
	Outcome           string              `json:"outcome"`
	Leave             bool                `json:"leave"`
	Assignments       int                 `json:"assignments"`
	Stops             int                 `json:"stops"`
	Kills             int                 `json:"kills"`
	Holds             int                 `json:"holds"`
	JobOffered        bool                `json:"job_offered"`
	JobExecution      string              `json:"job_execution,omitempty"`
	JobAttempt        int                 `json:"job_attempt"`
	WinnerOffered     bool                `json:"winner_offered"`
	WinnerExecution   string              `json:"winner_execution,omitempty"`
	WinnerAttempt     int                 `json:"winner_attempt"`
	GlobalOffered     bool                `json:"global_offered"`
	GlobalExecution   string              `json:"global_execution,omitempty"`
	GlobalAttempt     int                 `json:"global_attempt"`
	Preparation       bool                `json:"preparation"`
	Copy              bool                `json:"copy"`
	Model             bool                `json:"model"`
}
type DecisionPage struct {
	RecorderRetainedBytes int64             `json:"recorder_retained_bytes"`
	RecorderCapacityBytes int64             `json:"recorder_capacity_bytes"`
	Truncated             bool              `json:"truncated"`
	ControlDropped        uint64            `json:"control_dropped"`
	ControlOverwritten    uint64            `json:"control_overwritten"`
	StorageDeferred       bool              `json:"storage_deferred"`
	DurableAt             time.Time         `json:"durable_at"`
	At                    time.Time         `json:"at"`
	Receipts              []DecisionReceipt `json:"receipts"`
	Omitted               uint64            `json:"omitted"`
	CapacityBytes         int64             `json:"capacity_bytes"`
	RetainedBytes         int64             `json:"retained_bytes"`
	Changes               uint64            `json:"changes"`
}
type decisionKey struct{ job, target string }
type decisionCapture struct {
	trialMemory, trialProjected DecisionMemory
	incoming                    DecisionIncoming
	legacy                      Execution
	fences                      bool
	request                     uint64
	worker, session             string
	current                     decisionKey
	rows                        map[decisionKey]*DecisionReceipt
	remaining                   int64
	omitted                     uint64
	early                       string
	revision                    uint64
	at                          time.Time
}

func decisionCost(r *DecisionReceipt) int64 {
	// Two map entries and two receipt copies (capture and retained view), plus
	// pointer/map bookkeeping. Variable identities are charged at their real size.
	n := 2*int64(unsafe.Sizeof(*r)+unsafe.Sizeof(decisionKey{})+unsafe.Sizeof(DecisionFairness{})) + int64(3*(len(r.JobID)+len(r.Target)+len(r.Worker)+len(r.Session)+len(r.CheckinWorker)+len(r.CheckinSession)+len(r.Winner)+len(r.GlobalWinner)+len(r.Preparation)+len(r.ReleaseFence)+len(r.Reply.JobExecution)+len(r.Reply.WinnerExecution)+len(r.Reply.GlobalExecution)))
	for _, d := range r.Reply.Directives {
		n += 2*int64(unsafe.Sizeof(d)) + 3*int64(len(d.ID)+len(d.Session)+len(d.Parent))
	}
	return n
}
func (c *decisionCapture) row(j *Job, m *Machine) *DecisionReceipt {
	if c == nil || j.State != "queued" || j.Attempt != 0 {
		return nil
	}
	key := decisionKey{j.ID, targetKey(m)}
	if r := c.rows[key]; r != nil {
		return r
	}
	r := &DecisionReceipt{JobID: j.ID, Revision: c.revision, At: c.at, ObservedThrough: c.at, ThroughRevision: c.revision, Checkins: 1, Worker: m.ID, Session: m.Session, Target: targetKey(m), Admission: "not_evaluated", Immediate: "not_evaluated", EarlyReturn: "none"}
	cost := decisionCost(r)
	if cost > c.remaining {
		c.omitted++
		return nil
	}
	c.remaining -= cost
	c.rows[key] = r
	return r
}
func (q *Queue) decisionCheck(j *Job, m *Machine, ok bool, code string) bool {
	if !ok && q.decisionCapture != nil && q.decisionCapture.current == (decisionKey{j.ID, targetKey(m)}) {
		if r := q.decisionCapture.row(j, m); r != nil && (r.Admission == "not_evaluated" || r.Admission == "admitted") {
			r.Admission = code
			r.Admitted = false
			r.Memory, r.ProjectedMemory = q.decisionCapture.trialMemory, q.decisionCapture.trialProjected
		}
	}
	return ok
}
func (q *Queue) decisionAdmitted(j *Job, m *Machine, ok bool) {
	if r := q.decisionCapture.row(j, m); r != nil && ok {
		r.Admitted = true
		r.Admission = "admitted"
		r.Memory, r.ProjectedMemory = q.decisionCapture.trialMemory, q.decisionCapture.trialProjected
	}
}

// Trial facts follow the currently evaluated alternative. A failed receipt
// freezes its own facts; only an eventually admitted alternative replaces them.
func (q *Queue) decisionCandidateMemory(j *Job, m *Machine, memory float64, projected bool) {
	c := q.decisionCapture
	if c == nil || c.current != (decisionKey{j.ID, targetKey(m)}) {
		return
	}
	facts := decisionMemory(j, m, memory)
	if projected {
		c.trialProjected = facts
	} else {
		c.trialMemory, c.trialProjected = facts, DecisionMemory{}
	}
	if r := c.row(j, m); r != nil && (r.Admission == "not_evaluated" || r.Admission == "admitted") {
		r.Memory, r.ProjectedMemory = c.trialMemory, c.trialProjected
	}
}

func (q *Queue) decisionResourcesFit(j *Job, m *Machine, memory float64) bool {
	q.decisionCandidateMemory(j, m, memory, true)
	if q.decisionCapture == nil {
		return fitsResources(j, m, memory)
	}
	report := targetFit{stopFirst: true}
	ok := fitsResourcesChecked(j, m, memory, &report)
	return q.decisionCheck(j, m, ok, decisionFitCode(report.first))
}
func (q *Queue) decisionWinner(s *candidateSet, winner *Job) {
	if q.decisionCapture == nil || winner == nil {
		return
	}
	for _, j := range s.jobs {
		if r := q.decisionCapture.row(j, s.m); r != nil {
			r.Winner = winner.ID
			r.WinnerClass = decisionClass(winner.Spec)
			if j.ID != winner.ID {
				reason := ""
				before := q.beforeWithFairDecision(winner, j, s.m, s.score, s.active, &reason)
				r.Fairness = &DecisionFairness{before, reason}
			}
		}
	}
}
func decisionClass(s Spec) string {
	// Authored policy tier/project names are not safe protocol labels.
	switch jobClass(s) {
	case "production", "deadline", "experiment", "test", "benchmark":
		return jobClass(s)
	}
	return "other"
}

// Latest distinct evidence per job/target, not a heartbeat history. Capacity is
// one existing measured observation share, not a deployment-sized row/time cap.
type decisionStore struct {
	mu               sync.Mutex
	rows             map[decisionKey]DecisionReceipt
	bytes, capacity  int64
	omitted, changes uint64
}

func (q *Queue) decisionBudget(ctx context.Context) int64 {
	if q.control == nil {
		return 0
	}
	n, err := q.requestBudgets.capacity(ctx)
	if err != nil {
		return 0
	}
	return n
}
func (q *Queue) publishDecisions(c *decisionCapture, reply Reply, outcome string, budget int64) {
	if c == nil || q.control == nil {
		return
	}
	// One measured share covers the latest cache and retained recorder versions.
	// Capture/reply scratch uses its own request share, charged before allocation.
	scratch := budget
	for _, r := range c.rows {
		scratch -= decisionCost(r)
	}
	scratch = min(scratch, c.remaining)
	budget /= 2
	q.control.appendDecision(ControlEvent{}, budget)
	if len(c.rows) == 0 || budget < decisionCost(&DecisionReceipt{}) {
		store := &q.decisions
		store.mu.Lock()
		store.capacity = budget
		for store.bytes > budget && len(store.rows) > 0 {
			store.evict(3)
		}
		store.omitted += c.omitted
		store.mu.Unlock()
		return
	}
	d, directives := q.summarizeDecisionReply(c, reply, outcome, max(0, scratch))
	store := &q.decisions
	store.mu.Lock()
	defer store.mu.Unlock()
	store.capacity = budget
	store.omitted += c.omitted
	if store.rows == nil {
		store.rows = map[decisionKey]DecisionReceipt{}
	}
	for _, r := range c.rows {
		r.Epoch = q.control.epoch
		r.Checkin = c.revision
		r.Request = c.request
		r.CheckinWorker = q.control.anonymous(c.worker)
		r.CheckinSession = q.control.anonymous(c.session)
		r.Worker = q.control.anonymous(r.Worker)
		r.Session = q.control.anonymous(r.Session)
		r.Target = q.control.anonymous(r.Target)
		if r.Preparation != "" {
			r.Preparation = q.control.anonymous(r.Preparation)
		}
		if len(r.releaseIDs) > 0 {
			slices.Sort(r.releaseIDs)
			digest := sha256.New()
			encoder := json.NewEncoder(digest)
			for _, id := range r.releaseIDs {
				_ = encoder.Encode(id)
			}
			r.ReleaseFence = q.control.anonymous(hex.EncodeToString(digest.Sum(nil)))
			r.releaseIDs = nil
		}
		r.EarlyReturn = c.early
		r.Incoming = c.incoming
		r.Reply = d
		offer := func(id string) Execution {
			var selected Execution
			visitDecisionDirectives(c, reply, func(item DecisionDirective) {
				if item.Offer && (item.ID == id || item.Parent != "" && item.Parent == id) && (selected.ID == "" || item.ID < selected.ID) {
					selected = item.Execution
				}
			})
			return selected
		}
		job, winner, global := offer(r.JobID), offer(r.Winner), offer(r.GlobalWinner)
		r.Reply.JobExecution, r.Reply.JobAttempt, r.Reply.JobOffered = job.ID, job.Attempt, job.Attempt > 0
		r.Reply.WinnerExecution, r.Reply.WinnerAttempt, r.Reply.WinnerOffered = winner.ID, winner.Attempt, winner.Attempt > 0
		r.Reply.GlobalExecution, r.Reply.GlobalAttempt, r.Reply.GlobalOffered = global.ID, global.Attempt, global.Attempt > 0
		// Retain addressed directives within this capture's per-row share.
		// The digest and counts cover the entire reply if detail is shed.
		allowance := budget / int64(max(1, len(c.rows)))
		retained, directiveCost := 0, decisionCost(r)
		for _, item := range directives {
			n := 2*int64(unsafe.Sizeof(item)) + 3*int64(len(item.ID)+len(item.Session)+len(item.Parent))
			if directiveCost+n > allowance {
				break
			}
			directiveCost += n
			retained++
		}
		r.Reply.Directives = directives[:retained]
		r.Reply.DirectivesOmitted = d.DirectivesOmitted + len(directives) - retained
		// Own only the retained backing array, not the omitted reply tail.
		r.Reply.Directives = slices.Clone(r.Reply.Directives)
		key := decisionKey{r.JobID, r.Target + "/" + r.CheckinWorker}
		old, ok := store.rows[key]
		if ok && old.ThroughRevision > r.Revision && old.Epoch == r.Epoch {
			continue
		}
		// Keep the exact original check-in/revision/reply for repeated decisions.
		// A changing heartbeat timestamp alone buys no additional storage writes.
		if ok && sameDecision(old, *r) {
			if r.Revision > old.ThroughRevision {
				old.ObservedThrough = r.At
				old.ThroughRevision = r.Revision
				old.Checkins++
				store.rows[key] = old
			}
			continue
		}
		cost := decisionCost(r)
		if ok {
			store.bytes -= decisionCost(&old)
			delete(store.rows, key)
		}
		for store.bytes+cost > budget && len(store.rows) > 0 {
			if !store.evict(decisionValue(*r)) {
				break
			}
		}
		if store.bytes+cost > budget {
			store.omitted++
			continue
		}
		store.rows[key] = *r
		store.bytes += cost
		store.changes++
		// Existing guarded recorder handles serialization, SQL and physical-space
		// admission outside q.mu. No new persistence path or per-check-in commit.
		copy := *r
		q.control.appendDecision(ControlEvent{Kind: "decision", Operation: "checkin", Request: r.Request, Decision: &copy}, budget)
	}
	for store.bytes > budget && len(store.rows) > 0 {
		store.evict(3)
	}
}
func sameDecision(a, b DecisionReceipt) bool {
	// Continuous headroom jitter alone does not buy another durable record.
	// Preserve the original exact inputs/reply until sizing, a predicate or an
	// offer changes; repeat counters attest only that semantic decision.
	stable := func(x, y DecisionMemory) DecisionMemory {
		x.FreeHostGB, x.AvailableHostGB, x.ReservedHostGB = y.FreeHostGB, y.AvailableHostGB, y.ReservedHostGB
		x.AllowanceGB, x.FreeDeviceGB = y.AllowanceGB, y.FreeDeviceGB
		return x
	}
	a.Memory = stable(a.Memory, b.Memory)
	a.ProjectedMemory = stable(a.ProjectedMemory, b.ProjectedMemory)
	a.ImmediateMemory = stable(a.ImmediateMemory, b.ImmediateMemory)
	a.ObservedThrough = b.ObservedThrough
	a.ThroughRevision = b.ThroughRevision
	a.Checkins = b.Checkins
	a.At = b.At
	a.Revision = b.Revision
	a.Checkin = b.Checkin
	a.Request = b.Request
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// Keep unexplained dispatch gaps and rejected/competing decisions ahead of
// ordinary offers. Age breaks equal-value ties, so a flood of healthy pulls
// cannot evict the very evidence this capture was added to retain.
func decisionValue(r DecisionReceipt) int {
	if r.Reply.Outcome == "reply_failed" || r.Admitted && r.GlobalWinner == r.JobID && r.Immediate == "ready" && !r.Reply.JobOffered {
		return 3
	}
	if r.Admission != "admitted" && r.Admission != "not_evaluated" || r.EarlyReturn != "none" || r.Winner != "" && r.Winner != r.JobID {
		return 2
	}
	return 1
}
func (s *decisionStore) evict(value int) bool {
	var key decisionKey
	var oldest DecisionReceipt
	for k, r := range s.rows {
		if decisionValue(r) > value {
			continue
		}
		if key.job == "" || decisionValue(r) < decisionValue(oldest) || decisionValue(r) == decisionValue(oldest) && r.At.Before(oldest.At) {
			key, oldest = k, r
		}
	}
	if key.job == "" {
		return false
	}
	delete(s.rows, key)
	s.bytes -= decisionCost(&oldest)
	s.omitted++
	return true
}
func (q *Queue) decisionPage(id string, budgets ...int64) DecisionPage {
	s := &q.decisions
	s.mu.Lock()
	defer s.mu.Unlock()
	p := DecisionPage{At: q.clock.Now(), Receipts: []DecisionReceipt{}, Omitted: s.omitted, CapacityBytes: s.capacity, RetainedBytes: s.bytes, Changes: s.changes}
	budget := s.capacity
	if len(budgets) > 0 {
		budget = budgets[0]
	}
	used := int64(0)
	for _, r := range s.rows {
		if r.JobID != id {
			continue
		}
		n := decisionCost(&r)
		if n > budget-used {
			p.Omitted++
			p.Truncated = true
			break
		}
		used += n
		p.Receipts = append(p.Receipts, r)
	}
	slices.SortFunc(p.Receipts, func(a, b DecisionReceipt) int { return strings.Compare(a.Target, b.Target) })
	return p
}

// DecisionReceipts reads retained per-job evidence, including the shared
// recorder's durable ring after a restart. It never captures full status or
// takes q.mu; the existing WAL reader supplies the durable half.
func (q *Queue) DecisionReceipts(ctx context.Context, id string) (DecisionPage, error) {
	budget := q.decisionBudget(ctx)
	p := q.decisionPage(id, budget/2)
	if q.control == nil {
		return p, nil
	}
	q.control.mu.Lock()
	p.ControlDropped = q.control.dropped
	p.ControlOverwritten = q.control.overwritten
	p.StorageDeferred = q.control.storageDeferred
	p.DurableAt = q.control.durableAt
	p.RecorderRetainedBytes, p.RecorderCapacityBytes = q.control.decisionBytes, q.control.decisionCapacity
	q.control.mu.Unlock()
	if budget < decisionCost(&DecisionReceipt{}) {
		p.Truncated = true
		return p, nil
	}
	seen := map[string]bool{}
	used := int64(0)
	for _, r := range p.Receipts {
		seen[r.Target+"/"+r.CheckinWorker] = true
		used += decisionCost(&r)
	}
	if decisionCost(&DecisionReceipt{}) > budget/2-used {
		p.Truncated = true
		return p, nil
	}
	// Bound wire/decoder scratch as well as retained rows and deduplication keys.
	// A zero/exhausted share never starts a durable scan.
	// One output/cache copy plus deduplication uses half the share. The
	// other half covers wire, decoded event and decoder scratch (three copies).
	scratch := budget / 6
	if scratch == 0 {
		p.Truncated = true
		return p, nil
	}
	rows, err := q.usageReader.QueryContext(ctx, `SELECT CASE WHEN length(data)<=? THEN data END FROM control_events WHERE json_extract(data,'$.decision.job_id')=? ORDER BY seq DESC`, scratch, id)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return p, err
		}
		if len(raw) == 0 {
			p.Omitted++
			p.Truncated = true
			break
		}
		var e ControlEvent
		if err = json.Unmarshal(raw, &e); err != nil {
			return p, err
		}
		if e.Decision == nil {
			continue
		}
		r := *e.Decision
		key := r.Target + "/" + r.CheckinWorker
		if seen[key] {
			continue
		}
		n := decisionCost(&r)
		if n > budget-used-3*scratch {
			p.Omitted++
			p.Truncated = true
			break
		}
		// No key is retained for a row that did not fit. Its bookkeeping is
		// included in decisionCost; exhausting capacity stops the read.
		seen[key] = true
		used += n
		p.Receipts = append(p.Receipts, r)
	}
	slices.SortFunc(p.Receipts, func(a, b DecisionReceipt) int {
		if n := strings.Compare(a.Target, b.Target); n != 0 {
			return n
		}
		return strings.Compare(a.CheckinWorker, b.CheckinWorker)
	})
	return p, rows.Err()
}
func (a API) registerDecisions(mux *requestMux) {
	mux.HandleFunc("GET /api/jobs/{id}/decisions", func(w http.ResponseWriter, r *http.Request) {
		page, err := a.Queue.DecisionReceipts(r.Context(), r.PathValue("id"))
		respondOperation(w, page, err, "decision_unavailable")
	})
}

func decisionFitCode(dimension string) string {
	switch dimension {
	case "shared host profile":
		return "shared_host_profile"
	case "shared input cache":
		return "shared_input_cache"
	case "disk headroom":
		return "disk_headroom"
	case "tools":
		return "tools"
	case "card group":
		return "card_group"
	case "target":
		return "target"
	case "native network isolation":
		return "native_network"
	case "memory envelope":
		return "memory_envelope"
	case "recovering":
		return "recovering"
	case "device":
		return "device"
	case "platform":
		return "platform"
	case "cores":
		return "cpu_cores"
	case "cancelled":
		return "cancelled"
	case "Docker runtime":
		return "docker_runtime"
	case "log policy":
		return "log_policy"
	case "availability":
		return "availability"
	case "hardware capacity":
		return "hardware_capacity"
	case "extension":
		return "extension"
	case "GPU memory telemetry":
		return "gpu_memory_telemetry"
	case "gone":
		return "worker_gone"
	case "leaving":
		return "leaving"
	case "checkpoint":
		return "checkpoint_storage"
	case "GPU memory":
		return "gpu_memory"
	case "allow-list", "excluded_machines":
		return "machine_policy"
	case "environment":
		return "environment_coverage"
	}
	return "resource_fit"
}

// Copies only exact custody identities, never recipes, keys or paths. Called
// while the check-in transaction still owns q.mu, before maintain mutates plans.
func (q *Queue) snapshotDecisionFences(c *decisionCapture) {
	if c == nil || c.fences {
		return
	}
	c.fences = true
	for key, r := range c.rows {
		r.PolicyRevision = q.policyRevision()
		if j := q.s.Jobs[key.job]; j != nil && j.PreemptionPlacement != nil {
			r.ReleaseCount = len(j.PreemptionPlacement.Releases)
			bytes := int64(0)
			for _, id := range j.PreemptionPlacement.Releases {
				bytes += int64(len(id)) + int64(unsafe.Sizeof(id))
			}
			if bytes <= c.remaining {
				r.releaseIDs = slices.Clone(j.PreemptionPlacement.Releases)
				c.remaining -= bytes
			} else {
				c.omitted++
			}
		}
		for _, p := range q.s.PreparationPlans {
			if p.Machine == r.Worker && p.Session == r.Session && p.Attempt != "" {
				r.PreparationCount++
				if r.Preparation == "" || p.Attempt < r.Preparation {
					r.Preparation = p.Attempt
				}
			}
		}
	}
}

// Early returns describe an omitted admission pass, without running a second
// planner. Construct inventory targets once, even with a large queued backlog.
func (q *Queue) finishDecisionCapture(c *decisionCapture, m *Machine) {
	if c == nil || c.fences {
		return
	}
	if q.control != nil && c.early != "none" && c.remaining >= decisionCost(&DecisionReceipt{}) {
		targets := q.placementTargets(m)
		for _, j := range q.liveJobs() {
			for _, target := range targets {
				c.row(j, target)
			}
		}
	}
	q.snapshotDecisionFences(c)
	q.decisionCapture = nil
}
