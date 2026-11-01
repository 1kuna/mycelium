package myce

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Suggestions are read-only evidence from retained, accepted production work.
// No estimate, status read or inferred recurrence can enable a policy rule.
type HoldSuggestion struct {
	Rule        ScheduledHold `json:"rule"`
	Evidence    []string      `json:"evidence"`
	Uncertainty string        `json:"uncertainty"`
}

func (q *Queue) HoldSuggestions(contexts ...context.Context) ([]HoldSuggestion, error) {
	view, _, err := q.captureStatusObservation("", nil, contexts...)
	if err != nil {
		return nil, err
	}
	return view.holdSuggestions(), nil
}

func (q *Queue) holdSuggestions() []HoldSuggestion {
	type envelope struct {
		project, class, host string
		weekday              int
		start, end           string
		jobs                 []string
	}
	groups := map[string]*envelope{}
	for _, j := range q.s.Jobs {
		if j.State != "succeeded" || j.Spec.Priority != "production" || j.StartedAt == nil || j.FinishedAt == nil || !j.ReleaseConfirmed || j.Spec.FanOut != nil {
			continue
		}
		start, end := j.StartedAt.UTC(), j.FinishedAt.UTC()
		if !end.After(start) || start.Format("2006-01-02") != end.Format("2006-01-02") {
			continue
		}
		// Ceil the observed finish, without shortening measured occupancy.
		end = end.Truncate(time.Minute)
		if end.Before(*j.FinishedAt) {
			end = end.Add(time.Minute)
		}
		if start.Day() != end.Day() {
			continue
		} // Overnight observations need an explicit split.
		host := q.executionHost(j.Machine)
		if host == "" {
			continue
		}
		key := fmt.Sprintf("%s/%s/%s/%d", j.Spec.Project, jobClass(j.Spec), host, start.Weekday())
		g := groups[key]
		if g == nil {
			g = &envelope{project: j.Spec.Project, class: jobClass(j.Spec), host: host, weekday: int(start.Weekday()), start: start.Format("15:04"), end: end.Format("15:04")}
			groups[key] = g
		}
		g.start = min(g.start, start.Format("15:04"))
		g.end = max(g.end, end.Format("15:04"))
		g.jobs = append(g.jobs, j.ID)
	}
	out := []HoldSuggestion{}
	for _, g := range groups {
		if len(g.jobs) < 2 {
			continue
		}
		slices.Sort(g.jobs)
		out = append(out, HoldSuggestion{Rule: ScheduledHold{ID: fmt.Sprintf("observed-%s-%s-%s-%d", g.project, g.class, g.host, g.weekday), Enabled: false, Beneficiary: g.project + "/" + g.class, Resource: ComponentExclusion{Selector: "host=" + g.host}, Reason: "observed accepted production occupancy", Timezone: "UTC", Windows: []HoldWindow{{Weekdays: []int{g.weekday}, Start: g.start, End: g.end}}}, Evidence: g.jobs, Uncertainty: "observed envelope of retained successful executions only; future timing, recurrence and setup changes are unknown; owner must explicitly enable through policy set"})
	}
	slices.SortFunc(out, func(a, b HoldSuggestion) int { return strings.Compare(a.Rule.ID, b.Rule.ID) })
	return out
}

func (a API) registerHoldSuggestions(mux *requestMux) {
	mux.HandleFunc("GET /api/policy/hold-suggestions", func(w http.ResponseWriter, r *http.Request) {
		suggestions, err := a.Queue.HoldSuggestions(r.Context())
		respondOperation(w, suggestions, err, "policy_unavailable")
	})
}
