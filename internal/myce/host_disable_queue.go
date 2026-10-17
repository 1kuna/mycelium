package myce

import "strings"

func (q *Queue) chargeLoss(j *Job) {
	for i := range j.Copies {
		if j.Copies[i].Attempt == j.Attempt {
			j.Copies[i].LossCharged = true
		}
	}
}

// Operator intent refunds only the saved execution, never a sibling/new attempt.
func (q *Queue) refundOperatorStop(r Result) bool {
	if !r.OperatorStop {
		return false
	}
	j := q.s.Jobs[r.ID]
	if j == nil {
		return false
	}
	matched := false
	for i := range j.Copies {
		c := &j.Copies[i]
		if c.Machine != r.Machine || c.Session != r.Session || c.Attempt != r.Attempt {
			continue
		}
		matched = true
		if !r.ReleasePending {
			c.Recoverable = false
		}
		if c.LossCharged {
			j.Losses = max(0, j.Losses-1)
			c.LossCharged = false
		}
	}
	if matched && j.State == "failed" && j.Losses < 3 && strings.HasPrefix(j.Reason, "failed after 3 infrastructure losses:") {
		q.retry(j, "requeued: stopped by operator")
		j.RequeuedByOperator = true
	}
	return matched
}
