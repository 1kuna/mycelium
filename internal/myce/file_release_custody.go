package myce

import "slices"

// Path starts at the selected job's immediate member and ends at the blocker.
// An empty path means the selected job's own execution still has custody.
type FileReleaseCustodyBlocker struct {
	JobID        string                   `json:"job_id,omitempty"`
	State        string                   `json:"state,omitempty"`
	OtherProject bool                     `json:"other_project,omitempty"`
	Path         []FileReleaseCustodyStep `json:"path"`
}

type FileReleaseCustodyStep struct {
	Relationship string `json:"relationship"`
	JobID        string `json:"job_id,omitempty"`
	OtherProject bool   `json:"other_project,omitempty"`
}

// q.mu held. Explain the existing predicate without introducing new custody.
// Use one stable path per blocking job, even when fan-out and after edges alias.
func (q *Queue) fileReleaseCustodyBlockers(root *Job, project string) []FileReleaseCustodyBlocker {
	view := q.cancellationObservation()
	seen := map[string]bool{}
	var out []FileReleaseCustodyBlocker
	var walk func(*Job, []FileReleaseCustodyStep)
	walk = func(j *Job, path []FileReleaseCustodyStep) {
		if seen[j.ID] {
			return
		}
		seen[j.ID] = true
		if view.jobOwnReleasePending(j) {
			out = append(out, publicFileReleaseCustodyBlocker(j, project, path))
		}
		members := slices.Clone(view.cancellationMembers(j))
		slices.SortFunc(members, func(a, b *Job) int { return compareReleaseJSON(a.ID, b.ID) })
		for _, child := range members {
			relationship := "fan_out"
			if child.Spec.After != nil && child.Spec.After.Job == j.ID {
				relationship = "after"
			}
			step := FileReleaseCustodyStep{Relationship: relationship, JobID: child.ID}
			if child.Spec.Project != project {
				step.JobID, step.OtherProject = "", true
			}
			walk(child, append(slices.Clone(path), step))
		}
	}
	walk(root, []FileReleaseCustodyStep{})
	return out
}
