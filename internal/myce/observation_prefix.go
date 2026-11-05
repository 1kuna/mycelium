package myce

// Prefix admission reads follow only the requested ID's bytes. No retained-job
// scan or policy matching runs on a rejected request. Updates recompute ancestors
// over byte children (at most the byte alphabet), independent of history size.
// Nodes and scalar intents use space proportional to retained ID bytes.
type observationRank struct {
	tier       int
	production bool
}
type observationPrefix struct {
	children    map[byte]*observationPrefix
	exact, best *observationRank
}

func setObservationPrefix(n *observationPrefix, id string, rank *observationRank) *observationPrefix {
	if n == nil {
		if rank == nil {
			return nil
		}
		n = &observationPrefix{}
	}
	if id == "" {
		n.exact = rank
	} else {
		if n.children == nil {
			n.children = make(map[byte]*observationPrefix)
		}
		child := setObservationPrefix(n.children[id[0]], id[1:], rank)
		if child == nil {
			delete(n.children, id[0])
		} else {
			n.children[id[0]] = child
		}
	}
	var best observationRank
	present := false
	merge := func(r *observationRank) {
		if r == nil {
			return
		}
		if !present {
			best = *r
			present = true
			return
		}
		best.tier = min(best.tier, r.tier)
		best.production = best.production || r.production
	}
	merge(n.exact)
	for _, child := range n.children {
		merge(child.best)
	}
	if !present {
		return nil
	}
	n.best = &best
	return n
}
func (s *waitStateIndex) indexIntent(id string, intent observationIntent) {
	view := Queue{s: state{PriorityPolicy: &PriorityPolicyRecord{Policy: s.policy}}}
	match := view.priorityMatch(intent.spec())
	rank := &observationRank{match.Index, jobClass(intent.spec()) == "production" || match.Tier == "production"}
	s.priorities = setObservationPrefix(s.priorities, id, rank)
}
func (s *waitStateIndex) setIntent(id string, j *Job) {
	if j == nil {
		if _, exists := s.intents[id]; exists {
			delete(s.intents, id)
			s.priorities = setObservationPrefix(s.priorities, id, nil)
		}
		return
	}
	intent := observationIntent{j.Spec.Project, j.Spec.Class, j.Spec.Priority}
	if old, exists := s.intents[id]; exists && old == intent {
		return
	}
	if s.intents == nil {
		s.intents = make(map[string]observationIntent)
	}
	s.intents[id] = intent
	s.indexIntent(id, intent)
}

// Live jobs take precedence; a keyed ledger retains its own classification until
// that durable answer expires. Called only while q.mu protects committed state.
func (q *Queue) observationJob(id string) *Job {
	if j := q.s.Jobs[id]; j != nil {
		return j
	}
	if q.records != nil {
		if r := q.s.Submissions[q.records.bindings[id]]; r != nil && r.Kind == submissionBinding {
			return r.Job
		}
	}
	return nil
}
