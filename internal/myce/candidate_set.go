package myce

import "slices"

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

func (q *Queue) candidates(m *Machine) *candidateSet {
	s := &candidateSet{q: q, m: m, scores: map[string]float64{}, actives: map[string]float64{}}
	s.resources = map[batchResourceKey]batchResourceRoom{}
	for _, j := range q.s.Jobs {
		if candidate := q.batchCandidateWithResources(j, m, s.resources); candidate != nil {
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

func (s *candidateSet) score(j *Job, m *Machine) float64 {
	key := s.q.fairKey(j.Spec)
	if value, ok := s.scores[key]; ok {
		return value
	}
	if m.ID == "" {
		s.scores[key] = s.q.fairScore(j, m)
		return s.scores[key]
	}
	projects := map[string]bool{j.Spec.Project: true}
	for _, r := range s.jobs {
		if r.State == "queued" && s.q.comparePriority(r.Spec, j.Spec) == 0 && fairResourceFamily(r.Spec) == fairResourceFamily(j.Spec) && s.q.capReason(r) == "" {
			projects[r.Spec.Project] = true
		}
	}
	s.scores[key] = s.q.fairScoreProjects(j, projects)
	return s.scores[key]
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
