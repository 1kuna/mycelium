package myce

import (
	"errors"
	"strings"
)

func (q *Queue) chargeLoss(j *Job) {
	for i := range j.Copies {
		if j.Copies[i].Attempt == j.Attempt {
			j.Copies[i].LossCharged = true
		}
	}
}

// DisableHost fences placement first; only local cleanup confirmation releases attempts.
func (q *Queue) DisableHost(host, reason string, cleared bool) error {
	return q.DisableHostExecutions(host, reason, cleared, nil)
}
func (q *Queue) DisableHostExecutions(host, reason string, cleared bool, executions []Result) error {
	return q.DisableHostGeneration(host, reason, cleared, executions, 0)
}
func (q *Queue) DisableHostGeneration(host, reason string, cleared bool, executions []Result, generation uint64) error {
	if host == "" {
		return errors.New("host identity required")
	}
	if reason == "" {
		reason = "disabled by operator"
	}
	return q.update(func() error {
		if q.s.HostControls == nil {
			q.s.HostControls = map[string]HostControlGeneration{}
		}
		old := q.s.HostControls[host]
		if generation < old.Generation || (generation > 0 && generation == old.Generation && old.Enabled) {
			return errors.New("stale host disable generation")
		}
		if generation > 0 && generation == old.Generation && (old.Cleared || !cleared) {
			return nil
		}
		q.s.HostControls[host] = HostControlGeneration{Generation: generation, Cleared: cleared}

		if q.s.DisabledHosts == nil {
			q.s.DisabledHosts = map[string]string{}
		}
		q.s.DisabledHosts[host] = reason
		for _, m := range q.s.Machines {
			if hostID(m) != host {
				continue
			}
			m.Disabled, m.Leaving = reason, true
			delete(q.unreported, m.ID)
			if cleared {
				m.Gone = true
				m.Models = nil
				delete(q.s.BatchReleases, m.ID)
			}
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") {
					if cleared {
						if j.Reason == "cancel requested" {
							j.State = "cancelled"
							j.StopAt = nil
						} else {
							q.retry(j, "requeued: disabled by operator")
							j.RequeuedByOperator = true
						}
					} else {
						q.stop(j, "disabled by operator")
					}
				}
				if cleared {
					for i := range j.Copies {
						c := &j.Copies[i]
						if c.Machine == m.ID {
							c.Recoverable = false
							matched := false
							for _, r := range executions {
								if r.ID == j.ID && r.Attempt == c.Attempt && r.Machine == c.Machine && r.Session == c.Session {
									matched = true
									break
								}
							}
							if matched && j.State == "queued" {
								j.RequeuedByOperator = true
							}
							if matched && c.LossCharged {
								j.Losses = max(0, j.Losses-1)
								c.LossCharged = false
							}
						}
					}
					if j.State == "failed" && j.Losses < 3 && strings.HasPrefix(j.Reason, "failed after 3 infrastructure losses:") {
						q.retry(j, "requeued: disabled by operator")
						j.RequeuedByOperator = true
					}
					if j.ProductionHost == host {
						j.ProductionHost = ""
					}
				}
			}
			if cleared {
				for key, r := range q.s.HostReleases {
					if r.Machine == m.ID {
						delete(q.s.HostReleases, key)
					}
				}
				for key, s := range q.s.ModelSlots {
					if s.Machine == m.ID {
						delete(q.s.ModelSlots, key)
					}
				}
			}
			for _, c := range q.calls {
				if c.Machine == m.ID {
					c.Error = "host disabled by operator"
				}
			}
		}
		q.maintain()
		return nil
	})
}

type HostControlGeneration struct {
	Generation uint64 `json:"generation"`
	Enabled    bool   `json:"enabled"`
	Cleared    bool   `json:"cleared"`
}

func (q *Queue) EnableHost(host string) error { return q.EnableHostGeneration(host, 0) }
func (q *Queue) EnableHostGeneration(host string, generation uint64) error {
	if host == "" {
		return errors.New("host identity required")
	}
	return q.update(func() error {
		if q.s.HostControls == nil {
			q.s.HostControls = map[string]HostControlGeneration{}
		}
		old := q.s.HostControls[host]
		if generation < old.Generation || (generation > 0 && generation == old.Generation && !old.Enabled) {
			return errors.New("stale host enable generation")
		}
		if generation > 0 && !old.Enabled && !old.Cleared {
			return errors.New("disable cleanup must be acknowledged before enable")
		}
		q.s.HostControls[host] = HostControlGeneration{Generation: generation, Enabled: true}

		delete(q.s.DisabledHosts, host)
		for _, m := range q.s.Machines {
			if hostID(m) == host {
				m.Disabled = ""
				m.Leaving = false
			}
		}
		return nil
	})
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
