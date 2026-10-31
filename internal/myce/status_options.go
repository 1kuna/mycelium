package myce

import (
	"net/url"
	"sort"
	"strconv"
	"time"
)

// Bound retained history, never hide active work or outstanding cleanup custody.
// A page costs at most 1000 detailed jobs; the default keeps 100 recent answers.
const defaultStatusJobs = 100

type StatusJobsPage struct {
	Mode       string `json:"mode"`
	Total      int    `json:"total"`
	Returned   int    `json:"returned"`
	NextBefore string `json:"next_before,omitempty"`
}

type statusOptions struct {
	jobs, project, before string
	since                 time.Time
	limit                 int
	full                  bool
}

type statusSelection struct {
	ids  map[string]bool
	page StatusJobsPage
}

func parseStatusOptions(v url.Values) (statusOptions, error) {
	o := statusOptions{jobs: "recent", limit: defaultStatusJobs}
	if s := v.Get("jobs"); s != "" {
		o.jobs = s
	}
	if o.jobs != "recent" && o.jobs != "active" && o.jobs != "all" {
		return o, reject("status_query_invalid", nil)
	}
	o.project, o.before = v.Get("project"), v.Get("before")
	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > 1000 {
			return o, reject("status_query_invalid", err)
		}
		o.limit = n
	}
	if s := v.Get("since"); s != "" {
		var err error
		o.since, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return o, reject("status_query_invalid", err)
		}
	}
	if s := v.Get("detail"); s != "" {
		if s != "full" {
			return o, reject("status_query_invalid", nil)
		}
		o.full = true
		// Explicit full detail restores the pre-summary snapshot by default.
		if v.Get("jobs") == "" {
			o.jobs = "all"
		}
	}
	return o, nil
}

// Called under q.mu. Select before serializing history; keep the full scheduling
// context separately so filters cannot alter eligibility or queue order.
func (q *Queue) selectStatusJobs(o statusOptions) (*statusSelection, error) {
	s := &statusSelection{ids: map[string]bool{}, page: StatusJobsPage{Mode: o.jobs}}
	var jobs []*Job
	add := func(j *Job) {
		if o.project != "" && j.Spec.Project != o.project || !o.since.IsZero() && j.Queued.Before(o.since) {
			return
		}
		if o.jobs == "active" && !mutableJob(j) {
			return
		}
		s.page.Total++
		if o.jobs == "recent" && mutableJob(j) {
			s.ids[j.ID] = true
			return
		}
		jobs = append(jobs, j)
	}
	for _, j := range q.s.Jobs {
		add(j)
	}
	for _, c := range q.calls {
		add(callJob(c))
	}
	sort.Slice(jobs, func(i, j int) bool { return historyBefore(jobs[i], jobs[j]) })
	start := 0
	if o.before != "" {
		found := false
		for i, j := range jobs {
			if j.ID == o.before {
				start, found = i+1, true
				break
			}
		}
		if !found {
			return nil, reject("status_query_invalid", nil)
		}
	}
	end := len(jobs)
	if !o.full && o.jobs != "active" {
		end = min(end, start+o.limit)
	}
	for _, j := range jobs[start:end] {
		s.ids[j.ID] = true
	}
	if end < len(jobs) && end > start {
		s.page.NextBefore = jobs[end-1].ID
	}
	s.page.Returned = len(s.ids)
	return s, nil
}

func historyBefore(a, b *Job) bool {
	if !a.Queued.Equal(b.Queued) {
		return a.Queued.After(b.Queued)
	}
	return a.ID > b.ID
}

// Cold jobs outside this page still supply dependency identities/state to the
// planner. Omit their execution/attempt payloads from the locked JSON capture.
func statusDependencyJob(j *Job) *Job {
	return &Job{ID: j.ID, State: j.State, Queued: j.Queued, ReleaseConfirmed: j.ReleaseConfirmed,
		FanOut: j.FanOut, Dependency: j.Dependency, Spec: Spec{Project: j.Spec.Project, Class: j.Spec.Class}}
}
