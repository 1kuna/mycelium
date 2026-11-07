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
// labels are fixed; identities are anonymized before leaving the transaction.
type DecisionReceipt struct {
	Request          uint64    `json:"request"`
	ObservedThrough  time.Time `json:"observed_through"`
	ThroughRevision  uint64    `json:"through_revision"`
	Checkins         uint64    `json:"checkins"`
	PolicyRevision   uint64    `json:"policy_revision"`
	Preparation      string    `json:"preparation,omitempty"`
	PreparationCount int       `json:"preparation_count"`
	ReleaseFence     string    `json:"release_fence,omitempty"`
	ReleaseCount     int       `json:"release_count"`
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
type DecisionReply struct {
	Identity      string `json:"identity"`
	Stop          bool   `json:"stop"`
	Kill          bool   `json:"kill"`
	Hold          bool   `json:"hold"`
	Outcome       string `json:"outcome"`
	Leave         bool   `json:"leave"`
	Assignments   int    `json:"assignments"`
	Stops         int    `json:"stops"`
	Kills         int    `json:"kills"`
	Holds         int    `json:"holds"`
	JobOffered    bool   `json:"job_offered"`
	JobAttempt    int    `json:"job_attempt"`
	WinnerOffered bool   `json:"winner_offered"`
	WinnerAttempt int    `json:"winner_attempt"`
	GlobalOffered bool   `json:"global_offered"`
	GlobalAttempt int    `json:"global_attempt"`
	Preparation   bool   `json:"preparation"`
	Copy          bool   `json:"copy"`
	Model         bool   `json:"model"`
}
type DecisionPage struct {
	ControlDropped     uint64            `json:"control_dropped"`
	ControlOverwritten uint64            `json:"control_overwritten"`
	StorageDeferred    bool              `json:"storage_deferred"`
	DurableAt          time.Time         `json:"durable_at"`
	At                 time.Time         `json:"at"`
	Receipts           []DecisionReceipt `json:"receipts"`
	Omitted            uint64            `json:"omitted"`
	CapacityBytes      int64             `json:"capacity_bytes"`
	RetainedBytes      int64             `json:"retained_bytes"`
	Changes            uint64            `json:"changes"`
}
type decisionKey struct{ job, target string }
type decisionCapture struct {
	fences          bool
	request         uint64
	worker, session string
	current         decisionKey
	rows            map[decisionKey]*DecisionReceipt
	remaining       int64
	omitted         uint64
	early           string
	revision        uint64
	at              time.Time
}

func decisionCost(r *DecisionReceipt) int64 {
	// Two map entries and two receipt copies (capture and retained view), plus
	// pointer/map bookkeeping. Variable identities are charged at their real size.
	return 2*int64(unsafe.Sizeof(*r)+unsafe.Sizeof(decisionKey{})+unsafe.Sizeof(DecisionFairness{})) + int64(3*(len(r.JobID)+len(r.Target)+len(r.Worker)+len(r.Session)+len(r.CheckinWorker)+len(r.CheckinSession)+len(r.Winner)+len(r.GlobalWinner)+len(r.Preparation)+len(r.ReleaseFence)))
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
		}
	}
	return ok
}
func (q *Queue) decisionAdmitted(j *Job, m *Machine, ok bool) {
	if r := q.decisionCapture.row(j, m); r != nil && ok {
		r.Admitted = true
		r.Admission = "admitted"
	}
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
	n, err := q.requestBudgets.capacity(ctx)
	if err != nil {
		return 0
	}
	return n
}
func (q *Queue) publishDecisions(c *decisionCapture, reply Reply, outcome string, budget int64) {
	if c == nil {
		return
	}
	d := DecisionReply{Stop: reply.Stop, Kill: reply.Kill, Hold: reply.Hold, Outcome: outcome, Leave: reply.Leave, Preparation: len(reply.EnvironmentPlan) > 0, Copy: len(reply.CopyPlan) > 0, Model: len(reply.ModelPlan) > 0}
	// Hash only the safe control summary, incrementally, outside q.mu. The
	// retained receipt has fixed shape even for a large multi-attempt reply.
	digest := sha256.New()
	encoder := json.NewEncoder(digest)
	_ = encoder.Encode(d)
	offered := map[string]int{}
	for _, a := range reply.Attempts {
		execution := Execution{ID: a.ID, Attempt: a.Attempt}
		_ = encoder.Encode(struct {
			Execution
			Offer, Stop, Kill, Hold bool
		}{execution, a.Job != nil, a.Stop, a.Kill, a.Hold})
		if a.Job != nil {
			offered[a.ID] = a.Attempt
			d.Assignments++
		}
		if a.Stop {
			d.Stops++
		}
		if a.Kill {
			d.Kills++
		}
		if a.Hold {
			d.Holds++
		}
	}
	if j := reply.Job; j != nil {
		_ = encoder.Encode(Execution{ID: j.ID, Attempt: j.Attempt})
		offered[j.ID] = j.Attempt
		d.Assignments++
	}
	d.Identity = q.control.anonymous(hex.EncodeToString(digest.Sum(nil)))
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
		r.Reply = d
		r.Reply.JobAttempt = offered[r.JobID]
		r.Reply.JobOffered = r.Reply.JobAttempt > 0
		r.Reply.WinnerAttempt = offered[r.Winner]
		r.Reply.WinnerOffered = r.Reply.WinnerAttempt > 0
		r.Reply.GlobalAttempt = offered[r.GlobalWinner]
		r.Reply.GlobalOffered = r.Reply.GlobalAttempt > 0
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
		q.control.append(ControlEvent{Kind: "decision", Operation: "checkin", Request: r.Request, Decision: &copy})
	}
	for store.bytes > budget && len(store.rows) > 0 {
		store.evict(3)
	}
}
func sameDecision(a, b DecisionReceipt) bool {
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
func (q *Queue) decisionPage(id string) DecisionPage {
	s := &q.decisions
	s.mu.Lock()
	defer s.mu.Unlock()
	p := DecisionPage{At: q.clock.Now(), Receipts: []DecisionReceipt{}, Omitted: s.omitted, CapacityBytes: s.capacity, RetainedBytes: s.bytes, Changes: s.changes}
	for _, r := range s.rows {
		if r.JobID == id {
			p.Receipts = append(p.Receipts, r)
		}
	}
	slices.SortFunc(p.Receipts, func(a, b DecisionReceipt) int { return strings.Compare(a.Target, b.Target) })
	return p
}

// DecisionReceipts reads retained per-job evidence, including the shared
// recorder's durable ring after a restart. It never captures full status or
// takes q.mu; the existing WAL reader supplies the durable half.
func (q *Queue) DecisionReceipts(ctx context.Context, id string) (DecisionPage, error) {
	budget := q.decisionBudget(ctx)
	p := q.decisionPage(id)
	q.control.mu.Lock()
	p.ControlDropped = q.control.dropped
	p.ControlOverwritten = q.control.overwritten
	p.StorageDeferred = q.control.storageDeferred
	p.DurableAt = q.control.durableAt
	q.control.mu.Unlock()
	seen := map[string]bool{}
	receipts := p.Receipts
	p.Receipts = []DecisionReceipt{}
	used := int64(0)
	add := func(r DecisionReceipt) {
		key := r.Target + "/" + r.CheckinWorker
		if seen[key] {
			return
		}
		seen[key] = true
		n := decisionCost(&r)
		if n > budget-used {
			p.Omitted++
			return
		}
		used += n
		p.Receipts = append(p.Receipts, r)
	}
	for _, r := range receipts {
		add(r)
	}
	rows, err := q.usageReader.QueryContext(ctx, `SELECT data FROM control_events WHERE json_extract(data,'$.decision.job_id')=? ORDER BY seq DESC`, id)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return p, err
		}
		var e ControlEvent
		if err = json.Unmarshal(raw, &e); err != nil {
			return p, err
		}
		if e.Decision != nil {
			add(*e.Decision)
		}
	}
	slices.SortFunc(p.Receipts, func(a, b DecisionReceipt) int { return strings.Compare(a.Target, b.Target) })
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
