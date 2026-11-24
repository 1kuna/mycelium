package myce

import (
	"context"
	"fmt"
)

type ResizeJobsRequest struct {
	Jobs         []string `json:"jobs"`
	Project      string   `json:"project"`
	HostMemoryGB float64  `json:"host_memory_gb"`
	RequestedBy  string   `json:"requested_by"`
	Reason       string   `json:"reason"`
}

type ResizeJobsReceipt struct {
	Jobs         []string `json:"jobs"`
	HostMemoryGB float64  `json:"host_memory_gb"`
}

type JobResizeAudit struct {
	RequestedBy string  `json:"requested_by"`
	OldMemoryGB float64 `json:"old_host_memory_gb"`
	NewMemoryGB float64 `json:"new_host_memory_gb"`
	OldLimitGB  float64 `json:"old_limit_gb,omitempty"`
}

// ResizeJobs validates the whole selection and commits it under the assignment
// lock. The submission record's original fingerprint is independent of Spec.
func (q *Queue) ResizeJobs(ctx context.Context, req ResizeJobsRequest) (ResizeJobsReceipt, error) {
	var receipt ResizeJobsReceipt
	if bytes, err := memoryBytes(req.HostMemoryGB); err != nil || bytes <= 0 ||
		len(req.Jobs) == 0 || !recoveryText(req.Project) || !recoveryText(req.RequestedBy) || !recoveryText(req.Reason) {
		return receipt, &HTTPError{Status: 400, Code: "job_resize_invalid", Message: "resize requires job IDs, project, requester, reason and a positive host memory limit representable in bytes"}
	}
	err := q.updateRequest(ctx, func() error {
		jobs := make([]*Job, 0, len(req.Jobs))
		seen := make(map[string]bool, len(req.Jobs))
		for _, id := range req.Jobs {
			if seen[id] {
				continue
			}
			j := q.s.Jobs[id]
			if j == nil {
				return &HTTPError{Status: 404, Code: "job_unknown", Message: fmt.Sprintf("no batch job with ID %q", id)}
			}
			if j.Spec.Project != req.Project {
				return &HTTPError{Status: 403, Code: "job_project_mismatch", Message: fmt.Sprintf("job %s belongs to another project", id)}
			}
			if j.State == "running" || j.State == "stopping" || attemptReleasePending(j) || q.jobReleasePending(j) {
				return &HTTPError{Status: 409, Code: "job_resize_attempt_live", Message: fmt.Sprintf("job %s has an assigned, active or unreleased attempt", id)}
			}
			if j.State != "queued" {
				return &HTTPError{Status: 409, Code: "job_resize_not_queued", Message: fmt.Sprintf("job %s is %s; resize requires a queued job", id, j.State)}
			}
			if j.FanOut != nil && j.Spec.FanOut != nil && j.FanOut.Next >= len(j.Spec.FanOut.Pieces) {
				return &HTTPError{Status: 409, Code: "job_resize_no_pending_pieces", Message: fmt.Sprintf("job %s has already materialized all its pieces; resize each queued child by ID", id)}
			}
			jobs = append(jobs, j)
			seen[id] = true
		}
		receipt.HostMemoryGB = req.HostMemoryGB
		receipt.Jobs = make([]string, 0, len(jobs))
		for _, j := range jobs {
			receipt.Jobs = append(receipt.Jobs, j.ID)
			if j.HostMemoryOverrideGB == req.HostMemoryGB {
				continue
			}
			audit := &JobResizeAudit{RequestedBy: req.RequestedBy, OldMemoryGB: j.Spec.Needs.HostMemoryGB,
				NewMemoryGB: req.HostMemoryGB, OldLimitGB: memoryValue(j.Spec.MemoryLimitGB)}
			entry := ledgerAttempt(j, "host_memory_resized", req.Reason)
			entry.Release, entry.Resize = "confirmed", audit
			q.appendJobLedger(j, entry)
			j.HostMemoryOverrideGB = req.HostMemoryGB
			j.Spec.Needs.HostMemoryGB = req.HostMemoryGB
			limit := req.HostMemoryGB
			j.Spec.MemoryLimitGB = &limit
			if j.MemoryFallback != nil {
				fallback := *j.MemoryFallback
				fallback.HostMemoryGB = req.HostMemoryGB
				j.MemoryFallback = &fallback
			}
		}
		return nil
	}, nil)
	if err != nil {
		return ResizeJobsReceipt{}, err
	}
	return receipt, nil
}
