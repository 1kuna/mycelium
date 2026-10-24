package myce

import "slices"

// A candidate set belongs to one read-only planning pass. Exclusions change
// which work may win, never which projects compete for fair share. Discard the
// set after applying a placement or otherwise changing queue state.
type candidateSet struct {
	q       *Queue
	m       *Machine
	jobs    []*Job
	scores  map[string]float64
	actives map[string]float64
}

func (q *Queue) candidates(m *Machine) *candidateSet {
	s := &candidateSet{q: q, m: m, scores: map[string]float64{}, actives: map[string]float64{}}
	for _, j := range q.s.Jobs {
		if candidate := q.batchCandidate(j, m); candidate != nil {
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

func (s *candidateSet) pick(excluded map[string]bool) *Job {
	byProject := map[string]*Job{}
	for _, j := range s.jobs {
		if excluded[j.ID] {
			continue
		}
		old := byProject[j.Spec.Project]
		if old == nil || s.before(j, old) {
			byProject[j.Spec.Project] = j
		}
	}
	projects := make([]string, 0, len(byProject))
	for project := range byProject {
		projects = append(projects, project)
	}
	slices.Sort(projects)
	var best *Job
	for _, project := range projects {
		j := byProject[project]
		if best == nil || s.before(j, best) {
			best = j
		}
	}
	return best
}
