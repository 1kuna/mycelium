package myce

import (
	"cmp"
	"math"
	"slices"
)

// A candidate set belongs to one read-only planning pass. Exclusions change
// which work may win, never which projects compete for fair share. Discard the
// set after applying a placement or otherwise changing queue state.
type candidateSet struct {
	q         *Queue
	m         *Machine
	jobs      []*Job
	scores    map[string]float64
	actives   map[string]float64
	resources map[batchResourceKey]batchResourceRoom
	ordered   map[string][]*Job
	projects  []string
}

func (q *Queue) candidates(m *Machine, scans ...*placementScan) *candidateSet {
	scan := q.placementScan(scans...)
	key := targetKey(m)
	if s := scan.sets[key]; s != nil {
		return s
	}
	s := q.buildCandidates(m, scan)
	scan.sets[key] = s
	scan.candidateBuilds++
	return s
}

func (q *Queue) buildCandidates(m *Machine, scans ...*placementScan) *candidateSet {
	scan := q.placementScan(scans...)
	if scan.memory == nil {
		scan.memory = newMemoryPlanningCache()
	}
	previous := q.memoryPlanning
	q.memoryPlanning = scan.memory
	defer func() { q.memoryPlanning = previous }()
	s := &candidateSet{q: q, m: m, jobs: make([]*Job, 0, len(q.liveJobs())), scores: map[string]float64{}, actives: map[string]float64{}}
	s.resources = map[batchResourceKey]batchResourceRoom{}
	fence := q.groupDemandFence(m)
	hostFence := q.hostBackfillFences(scan)[hostID(m)]
	for _, j := range q.liveJobs() {
		if fence != nil && j.ID != fence.ID && q.comparePriority(j.Spec, fence.Spec) >= 0 {
			continue
		}
		if j.State == "queued" && plainMemoryWork(j.Spec) && j.Spec.FanOut == nil {
			if scan.memoryIdentities == nil {
				scan.memoryIdentities = map[*Job]string{}
			}
			identity, ok := scan.memoryIdentities[j]
			if !ok {
				identity = profileMemoryIdentity(j)
				scan.memoryIdentities[j] = identity
			}
			copy := *j
			copy.planningMemoryIdentity = identity
			j = &copy
		}
		if candidate := q.batchCandidateWithResources(j, m, s.resources); candidate != nil && q.hostBackfillAllows(candidate, m, hostFence) {
			s.jobs = append(s.jobs, candidate)
		}
	}
	for _, c := range q.calls {
		if !q.callCanUse(m, c) {
			continue
		}
		j := callJob(c)
		j.State = "queued"
		j.Spec.Name = "model call: " + c.Model
		s.jobs = append(s.jobs, j)
	}
	return s
}

// A card stopped for a waiting multi-card demand stays withheld from equal and
// lower tiers until that demand starts. Otherwise the first card freed is
// backfilled and the demand never sees all its cards free at once.
func (q *Queue) groupDemandFence(m *Machine) *Job {
	limit := m.TotalMemoryGB
	if len(m.groupMembers) > 0 {
		limit = m.groupFloorGB
	}
	var fence *Job
	for _, victim := range q.liveJobs() {
		// A requeued victim has left its machine; its latest copy keeps the card.
		if victim.State != "queued" && victim.State != "stopping" || m.GPU == "cpu" {
			continue
		}
		for _, c := range victim.Copies {
			if c.PriorityStop == nil || c.Attempt != victim.Attempt || c.Machine != m.ID || !slices.ContainsFunc(targetDevices(m), func(id string) bool { return slices.Contains(c.AllocatedDevices, id) }) {
				continue
			}
			if d := q.s.Jobs[c.PriorityStop.Demand]; d != nil && d.State == "queued" && d.Spec.Needs.MemoryGB > limit && (fence == nil || q.comparePriority(d.Spec, fence.Spec) < 0) && q.cardsCanHold(m, d) {
				fence = d
			}
		}
	}
	return fence
}

// The hold lasts only while this host's usable same-backend cards, including
// the held ones, can still hold the demand in equal shares. A disabled,
// quarantined or missing card ends it, so the held card returns to other work.
func (q *Queue) cardsCanHold(m *Machine, d *Job) bool {
	held, least, others := targetDevices(m), math.Inf(1), []float64{}
	found := 0
	for _, card := range q.machineTargets(q.s.Machines[m.ID]) {
		if card.GPU != m.GPU || card.UnifiedMemory || card.MemoryError != "" || card.TotalMemoryGB <= 0 || !deviceAllowed(d, card) || !q.placementAllowed(d, card) {
			continue
		}
		if slices.Contains(held, card.Device) {
			least, found = min(least, card.TotalMemoryGB), found+1
		} else {
			others = append(others, card.TotalMemoryGB)
		}
	}
	if found < len(held) {
		return false
	}
	slices.SortFunc(others, func(a, b float64) int { return cmp.Compare(b, a) })
	for k := 0; float64(found+k)*least < d.Spec.Needs.MemoryGB; k++ {
		if k == len(others) {
			return false
		}
		least = min(least, others[k])
	}
	return true
}

func (s *candidateSet) score(j *Job, m *Machine) float64 {
	key := s.q.fairKey(j.Spec)
	if value, ok := s.scores[key]; ok {
		return value
	}
	if m.ID == "" {
		s.scores[key] = s.q.fairScore(j, m)
		return s.scores[key]
	}
	projects := s.fairProjects(j)
	s.scores[key] = s.q.fairScoreProjects(j, projects)
	return s.scores[key]
}

func (s *candidateSet) fairProjects(j *Job) map[string]bool {
	projects := map[string]bool{j.Spec.Project: true}
	for _, r := range s.jobs {
		if r.State == "queued" && s.q.comparePriority(r.Spec, j.Spec) == 0 && fairResourceFamily(r.Spec) == fairResourceFamily(j.Spec) && s.q.capReason(r) == "" {
			projects[r.Spec.Project] = true
		}
	}
	return projects
}

func (s *candidateSet) active(j *Job) float64 {
	key := s.q.fairKey(j.Spec)
	if value, ok := s.actives[key]; ok {
		return value
	}
	s.actives[key] = s.q.fairActive(j)
	return s.actives[key]
}

func (s *candidateSet) before(a, b *Job) bool {
	return s.q.beforeWithFair(a, b, s.m, s.score, s.active)
}

// Within-project order is immutable during a read-only pass. Sort it once
// instead of comparing every remaining job again for each exclusion.
func (s *candidateSet) pick(excluded map[string]bool) *Job {
	// Most passes only need the head. Do not sort the backlog to find it.
	if len(excluded) == 0 && s.ordered == nil {
		var best *Job
		for _, j := range s.jobs {
			if best == nil || s.before(j, best) {
				best = j
			}
		}
		return best
	}
	if s.ordered == nil {
		s.ordered = map[string][]*Job{}
		for _, j := range s.jobs {
			s.ordered[j.Spec.Project] = append(s.ordered[j.Spec.Project], j)
		}
		for project, jobs := range s.ordered {
			s.projects = append(s.projects, project)
			slices.SortFunc(jobs, func(a, b *Job) int {
				if s.before(a, b) {
					return -1
				}
				if s.before(b, a) {
					return 1
				}
				return 0
			})
		}
		slices.Sort(s.projects)
	}
	var best *Job
	for _, project := range s.projects {
		for _, j := range s.ordered[project] {
			if excluded[j.ID] {
				continue
			}
			if best == nil || s.before(j, best) {
				best = j
			}
			break
		}
	}
	return best
}

func (s *candidateSet) batchPlan(j *Job) *placementPlan {
	if len(s.q.s.PreparationPlans) != 0 {
		return s.q.roomPlan(j, s.m, true)
	}
	key := resourceKey(j)
	if room, ok := s.resources[key]; ok {
		return room.plan
	}
	plan := s.q.roomPlan(j, s.m, true)
	s.resources[key] = batchResourceRoom{plan: plan}
	return plan
}
