package myce

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"
)

const (
	warmPiecesFixed      = "fixed"
	warmPiecesCapability = "warm_pieces_fixed_v1"
	warmBoundaryYield    = "warm pieces: yielded at an item boundary"
	predicateWarmPieces  = AdmissionPredicate("warm_pieces_capability")
)

func (s Spec) warmPieces() bool  { return s.WarmPieces == warmPiecesFixed }
func (s Spec) childFanOut() bool { return s.FanOut != nil && !s.warmPieces() }

func validateWarmPieces(s Spec) error {
	if s.WarmPieces == "" {
		return nil
	}
	if !s.warmPieces() {
		return errors.New("advanced.warm_pieces currently accepts fixed")
	}
	if s.FanOut == nil || s.extensionJob() || s.Kind != "" || s.Program != nil || s.Workload != nil || s.ExtensionCheck != nil {
		return errors.New("warm_pieces requires plain-command pieces")
	}
	if s.After != nil {
		return errors.New("warm_pieces cannot use after inputs")
	}
	if s.Execution != nil && s.Execution.Main.Workdir != "" {
		return errors.New("warm_pieces uses a private working directory; omit execution.main.workdir")
	}
	if s.Needs.GPU != nil && (len(s.Needs.GPU.Devices) > 1 || s.Needs.GPU.Count != nil && *s.Needs.GPU.Count > 1) {
		return errors.New("warm_pieces supports CPU or one accelerator device")
	}
	if len(s.Inputs) != 0 || s.OutputDelivery != "" || s.OutputDestination != nil || s.OutputRetention != "" || s.StorageProtocol != "" || s.ephemeral() {
		return errors.New("warm_pieces requires synchronous file-store outputs and managed worker data")
	}
	if s.CheckpointPolicy != "" {
		return errors.New("warm_pieces uses ordinary file-store checkpoint generations; omit advanced.checkpoint")
	}
	for name := range s.Outputs {
		if workerOwnedOutput(name) {
			return errors.New("warm_pieces output names cannot use worker-owned prefixes")
		}
	}
	return nil
}

func warmWorkerRefusal(m *Machine) string {
	if !slices.Contains(m.Capabilities, warmPiecesCapability) {
		return "worker does not support fixed warm pieces"
	}
	if len(m.groupMembers) > 1 {
		return "fixed warm pieces require one device or CPU"
	}
	return ""
}

// Delivery stays on the parent through acknowledgement and queue restart. The
// declaration index serves as its sequence within each attempt.
type WarmPieceDelivery struct {
	Key       string     `json:"key"`
	Sequence  int        `json:"sequence"`
	Attempt   int        `json:"attempt"`
	OfferedAt time.Time  `json:"offered_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
}

type WarmPieceProgress struct {
	Delivery            *WarmPieceDelivery   `json:"delivery,omitempty"`
	Failures            map[string]Execution `json:"failures,omitempty"` // Zero execution means the job ended before the item settled.
	LastSettledAt       *time.Time           `json:"last_settled_at,omitempty"`
	StartupSeconds      float64              `json:"startup_seconds,omitempty"`
	Loads               int                  `json:"loads,omitempty"` // Attempts whose startup is included above.
	ItemSeconds         float64              `json:"item_seconds,omitempty"`
	BetweenItemsSeconds float64              `json:"between_items_seconds,omitempty"`
}

type warmPieceRequest struct {
	Execution
	Machine    string       `json:"machine"`
	Session    string       `json:"session"`
	Op         string       `json:"op"`
	Key        string       `json:"key,omitempty"`
	Sequence   int          `json:"sequence,omitempty"`
	OutputRefs FileManifest `json:"output_refs,omitempty"`
	BeforeStop bool         `json:"before_stop,omitempty"`
}

type warmPieceReply struct {
	Delivery *WarmPieceDelivery `json:"delivery,omitempty"`
	End      bool               `json:"end,omitempty"`
}

func (q *Queue) warmPieceJob(r warmPieceRequest) (*Job, error) {
	j := q.s.Jobs[r.ID]
	m := q.s.Machines[r.Machine]
	if j == nil || !j.Spec.warmPieces() || j.FanOut == nil || j.FanOut.Warm == nil ||
		j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || m == nil ||
		m.Session != r.Session || m.Gone || cancelled(j) || j.StartedAt == nil {
		return nil, ErrStale
	}
	return j, nil
}

func warmPieceMatches(j *Job, r warmPieceRequest) bool {
	d := j.FanOut.Warm.Delivery
	return d != nil && d.Attempt == r.Attempt && d.Key == r.Key && d.Sequence == r.Sequence
}

// The result fence is used both before and after artifact verification.
func (q *Queue) warmPieceResult(r warmPieceRequest) (*Job, bool, error) {
	j, err := q.warmPieceJob(r)
	if err != nil {
		return nil, false, err
	}
	if r.Sequence < 1 || r.Sequence > len(j.Spec.FanOut.Pieces) || j.Spec.FanOut.Pieces[r.Sequence-1].Key != r.Key {
		return nil, false, ErrStale
	}
	if old, ok := j.FanOut.Results[r.Key]; ok {
		if r.Op == "success" && old.Execution == r.Execution && maps.Equal(old.OutputRefs, r.OutputRefs) {
			return j, true, nil
		}
		return nil, false, ErrStale
	}
	if old, ok := j.FanOut.Warm.Failures[r.Key]; ok {
		if r.Op == "failure" && old == r.Execution {
			return j, true, nil
		}
		return nil, false, ErrStale
	}
	if j.ReleaseConfirmed || wallTimeExpired(j, q.clock.Now()) || !warmPieceMatches(j, r) || j.FanOut.Warm.Delivery.StartedAt == nil ||
		(j.State != "running" && !(j.State == "stopping" && r.BeforeStop && plannedPriorityStop(j))) {
		return nil, false, ErrStale
	}
	return j, false, nil
}

func (q *Queue) warmPiece(ctx context.Context, r warmPieceRequest) (reply warmPieceReply, err error) {
	switch r.Op {
	case "success", "failure":
		return reply, q.commitWarmPiece(ctx, r)
	case "next", "start":
		if len(r.OutputRefs) != 0 || r.BeforeStop {
			return reply, errors.New("unexpected warm piece report fields")
		}
	default:
		return reply, errors.New("unknown warm piece operation")
	}
	err = q.updateRequest(ctx, func() error {
		j, err := q.warmPieceJob(r)
		if err != nil || j.ReleaseConfirmed || wallTimeExpired(j, q.clock.Now()) {
			return ErrStale
		}
		if j.State != "running" {
			if j.State == "stopping" && (r.Op == "next" && r.Key == "" && r.Sequence == 0 ||
				r.Op == "start" && warmPieceMatches(j, r)) {
				reply.End = true
				return nil
			}
			return ErrStale
		}
		switch r.Op {
		case "next":
			if r.Key != "" || r.Sequence != 0 {
				return errors.New("next does not select a piece")
			}
			if d := j.FanOut.Warm.Delivery; d != nil {
				reply.Delivery = d
				return nil
			}
			if j.FanOut.Next == len(j.Spec.FanOut.Pieces) {
				reply.End = true
				return nil
			}
			if q.warmPieceYields(j) {
				q.stop(j, warmBoundaryYield)
				reply.End = true
				return nil
			}
			j = q.touchJob(j.ID)
			now := q.clock.Now()
			progress := j.FanOut.Warm
			if !warmSettledOnAttempt(j) {
				progress.StartupSeconds += max(0, now.Sub(*j.StartedAt).Seconds())
				progress.Loads++
			} else if progress.LastSettledAt != nil {
				progress.BetweenItemsSeconds += max(0, now.Sub(*progress.LastSettledAt).Seconds())
			}
			d := &WarmPieceDelivery{Key: j.Spec.FanOut.Pieces[j.FanOut.Next].Key, Sequence: j.FanOut.Next + 1, Attempt: j.Attempt, OfferedAt: now}
			progress.Delivery, reply.Delivery = d, d
		case "start":
			if !warmPieceMatches(j, r) {
				return ErrStale
			}
			j = q.touchJob(j.ID)
			d := *j.FanOut.Warm.Delivery
			if d.StartedAt == nil {
				now := q.clock.Now()
				d.StartedAt = &now
				j.FanOut.Warm.Delivery = &d
			}
			reply.Delivery = &d
		}
		return nil
	}, nil)
	return reply, err
}

func (q *Queue) commitWarmPiece(ctx context.Context, r warmPieceRequest) error {
	if r.Op == "failure" && len(r.OutputRefs) != 0 {
		return errors.New("failed warm piece cannot publish outputs")
	}
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	unlock := requestLock(ctx, &q.mu)
	j, replay, err := q.warmPieceResult(r)
	var outputs map[string]string
	if err == nil && !replay {
		outputs = maps.Clone(j.Spec.Outputs)
	}
	unlock()
	if err != nil || replay {
		return err
	}
	if r.Op == "success" {
		if err := validateOutputManifest(outputs, r.OutputRefs); err != nil {
			return err
		}
		verify, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := q.verifyManifest(verify, r.OutputRefs); err != nil {
			return err
		}
	}
	return q.updateRequest(ctx, func() error {
		j, replay, err := q.warmPieceResult(r)
		if err != nil || replay {
			return err
		}
		j = q.touchJob(j.ID)
		if r.Op == "success" {
			if j.FanOut.Results == nil {
				j.FanOut.Results = map[string]PieceResult{}
			}
			j.FanOut.Results[r.Key] = PieceResult{Execution: r.Execution, OutputRefs: maps.Clone(r.OutputRefs)}
			if j.OutputRefs == nil {
				j.OutputRefs = FileManifest{}
			}
			for name, ref := range r.OutputRefs {
				j.OutputRefs[r.Key+"/"+name] = ref
			}
		} else {
			recordWarmPieceFailure(j, r.Key, r.Execution)
		}
		now := q.clock.Now()
		progress := j.FanOut.Warm
		progress.ItemSeconds += max(0, now.Sub(*progress.Delivery.StartedAt).Seconds())
		progress.LastSettledAt = &now
		progress.Delivery = nil
		j.FanOut.Next++
		j.FileCheckpoints, j.ResumeFiles, j.CheckpointTarget = nil, nil, nil
		j.LocalCheckpointGeneration, j.Progress = 0, 0
		q.maintain()
		return nil
	}, nil)
}

func recordWarmPieceFailure(j *Job, key string, attempt Execution) {
	if j.FanOut.Warm.Failures == nil {
		j.FanOut.Warm.Failures = map[string]Execution{}
	}
	j.FanOut.Warm.Failures[key] = attempt
}

func warmFinalState(j *Job) (state, reason string) {
	if j.FanOut.Next != len(j.Spec.FanOut.Pieces) {
		return "failed", "warm pieces main exited before all items settled"
	}
	if len(j.FanOut.Warm.Failures) != 0 {
		return "failed", plainPieceFailure(slices.Sorted(maps.Keys(j.FanOut.Warm.Failures)), len(j.Spec.FanOut.Pieces))
	}
	return "succeeded", ""
}

func (q *Queue) maintainWarmPieces(j *Job) {
	if !j.Spec.warmPieces() || !terminal(j.State) || j.FanOut.Warm.Delivery == nil && (j.State != "failed" || j.FanOut.Next == len(j.Spec.FanOut.Pieces)) {
		return
	}
	j = q.touchJob(j.ID)
	j.FanOut.Warm.Delivery = nil
	if j.State == "failed" {
		for _, p := range j.Spec.FanOut.Pieces[j.FanOut.Next:] {
			recordWarmPieceFailure(j, p.Key, Execution{})
		}
		j.FanOut.Next = len(j.Spec.FanOut.Pieces)
	}
}

func warmSettledOnAttempt(j *Job) bool {
	if j.FanOut.Next == 0 {
		return false
	}
	key := j.Spec.FanOut.Pieces[j.FanOut.Next-1].Key
	return j.FanOut.Results[key].Execution.Attempt == j.Attempt || j.FanOut.Warm.Failures[key].Attempt == j.Attempt
}

// Mean measured seconds from Started to the first offer, or zero without a
// measurement.
func warmLoadSeconds(p *WarmPieceProgress) float64 {
	if p.Loads == 0 {
		return 0
	}
	return p.StartupSeconds / float64(p.Loads)
}

// A same-tier turn yields only after the attempt has done at least one
// measured load's worth of item work, so each reload is repaid by at least as
// much item time (MY-34). Without a measurement the turn yields as before.
// Higher tiers are not delayed.
func (q *Queue) warmLoadRepaid(j *Job) bool {
	return q.clock.Now().Sub(*j.StartedAt).Seconds() >= 2*warmLoadSeconds(j.FanOut.Warm)
}

// Compare ready work only at an item boundary after this attempt has done
// work. A contender must actually need this attempt's resources and be able
// to use them after exact release. The projection grants no real capacity.
func (q *Queue) warmPieceYields(j *Job) bool {
	if !warmSettledOnAttempt(j) {
		return false
	}
	repaid := q.warmLoadRepaid(j)
	scan := q.placementScan()
	owner := q.s.Machines[j.Machine]
	if owner == nil || owner.Session != j.Session {
		return false
	}
	// Exclude work with an immediate alternative. Compare against the
	// original charge before making the local release projection.
	waiting := map[string]bool{}
	runnable := q.assignmentPlan(scan)
	for _, ready := range runnable {
		waiting[ready.ID] = true
	}
	continuation := *j
	continuation.State = "queued"
	continuation.PreemptionPlacement = nil
	original := map[string]*Machine{}
	projectedByMachine := map[string][]*Machine{}
	for _, m := range scan.machines {
		if hostID(m) != hostID(owner) || len(m.groupMembers) > 0 {
			continue
		}
		original[targetKey(m)] = m
		// RAM and cores are host-wide. VRAM is released only on the card
		// occupied by this attempt.
		projected := q.afterReleases(m, []*Job{j}, nil)
		if !j.Spec.Needs.cpuOnly() && batchOnTarget(j, m) {
			projected.FreeMemoryGB += deviceShareGB(j)
			if m.TotalMemoryGB > 0 {
				projected.FreeMemoryGB = min(m.TotalMemoryGB, projected.FreeMemoryGB)
			}
		}
		projectedByMachine[m.ID] = append(projectedByMachine[m.ID], &projected)
	}
	for id, targets := range projectedByMachine {
		// Rebuild group shapes from the projected cards. Equal shares are
		// capped by the smallest member, and release can change card ordering.
		groups := q.cardGroupTargets(q.s.Machines[id], targets)
		for _, projected := range append(targets, groups...) {
			m := original[targetKey(projected)]
			if m == nil {
				members := make([]*Machine, 0, len(projected.groupMembers))
				for _, card := range projected.groupMembers {
					members = append(members, original[targetKey(card)])
				}
				m = groupView(members)
				m.groupFloorGB = projected.groupFloorGB
			}
			// Rank after restoring custody so active usage still counts.
			set, eligible := q.warmProjectedContenders(j, projected, waiting)
			set.jobs = append(set.jobs, &continuation)
			for _, candidate := range eligible {
				if q.immediateCandidate(candidate, m) != nil && q.batchSlotFree(m, candidate) {
					continue
				}
				if set.before(candidate, &continuation) && (repaid || q.comparePriority(candidate.Spec, j.Spec) != 0) {
					return true
				}
			}
		}
	}
	return false
}

func (q *Queue) warmProjectedContenders(j *Job, projected *Machine, waiting map[string]bool) (*candidateSet, []*Job) {
	// Both the execution and its matching cleanup receipt must be absent
	// from this counterfactual. Leave historical copies and other charges in
	// place. All readers here run synchronously under the queue mutex.
	exact := releaseKey(HostRelease{Job: j.ID, Machine: j.Machine, Session: j.Session, Attempt: j.Attempt})
	release, hadRelease := q.s.HostReleases[exact]
	delete(q.s.HostReleases, exact)
	copies := j.Copies
	j.Copies = slices.Clone(copies)
	for i := range j.Copies {
		copy := &j.Copies[i]
		if copy.Attempt == j.Attempt && copy.Machine == j.Machine && copy.Session == j.Session {
			copy.ReleaseConfirmed = true
			break
		}
	}
	j.ReleaseConfirmed = true
	defer func() {
		j.ReleaseConfirmed = false
		j.Copies = copies
		if hadRelease {
			q.s.HostReleases[exact] = release
		}
	}()
	set := q.buildCandidates(projected)

	var eligible []*Job
	for _, candidate := range set.jobs {
		if candidate.ID == j.ID || waiting[candidate.ID] || candidate.Spec.Project == j.Spec.Project && q.comparePriority(candidate.Spec, j.Spec) == 0 {
			continue
		}
		if q.immediateCandidate(candidate, projected) != nil && q.batchSlotFree(projected, candidate) {
			eligible = append(eligible, candidate)
		}
	}
	return set, eligible
}
