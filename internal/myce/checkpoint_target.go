package myce

import (
	"fmt"
	"slices"
	"strings"
)

// CheckpointOrigin is the target that wrote a resumable job's newest accepted
// checkpoint generation (#1437). Generations are portable as files, but an
// application's resume can depend on replay that holds only on the target that
// wrote them, so resumes stay on it unless the job declares otherwise.
type CheckpointOrigin struct {
	Target     ExecutionTarget `json:"target"`
	Generation int64           `json:"generation"`
	Attempt    int             `json:"attempt"`
}

const checkpointTargetHold = "checkpoint_target"

// The exact os/arch/platform a job runs as on m; CPU work ignores accelerators.
func jobTargetOn(j *Job, m *Machine) ExecutionTarget {
	t := machineTarget(m)
	if j.Spec.Needs.cpuOnly() {
		t.Platform = "cpu"
	}
	return t
}

// noteCheckpointTarget records the writer of a strictly newer accepted
// generation. A resume that has only copied the previous generation keeps the
// original writer, so a copy never moves the origin.
func noteCheckpointTarget(j *Job, m *Machine, attempt int, generation int64) {
	if m == nil || generation <= 0 || !j.Spec.Resumable {
		return
	}
	if c := j.CheckpointTarget; c != nil && generation <= c.Generation {
		return
	}
	t := jobTargetOn(j, m)
	if t.OS == "" || t.Arch == "" || t.Platform == "" {
		return
	}
	j.CheckpointTarget = &CheckpointOrigin{Target: t, Generation: generation, Attempt: attempt}
}

// Crossing targets needs the author's tested resume_compatibility for both the
// writing target and the destination; payload portability is not evidence.
func (q *Queue) checkpointPortableTo(j *Job, origin ExecutionTarget, m *Machine) bool {
	r := j.Spec.ResumeCompatibility
	return r != nil && slices.ContainsFunc(r.Targets, func(p ResumeTarget) bool { return p.Target == origin }) && q.yieldContractMatches(j, m)
}

// Hosts of target t this job may use. A host stays enrolled through
// maintenance, upgrades, busy periods and loss; only leaving the fleet or being
// replaced removes it, so a rollout never releases the wait.
func (q *Queue) checkpointTargetHosts(j *Job, t ExecutionTarget) []string {
	var hosts []string
	for _, m := range q.s.Machines {
		if m.Replacement != nil || m.Leaving || !j.Spec.allowsMachine(m.ID) || !q.projectAllows(j.Spec.Project, m.ID) {
			continue
		}
		found := jobTargetOn(j, m) == t
		for _, d := range m.Devices {
			view := machineTarget(m)
			view.Platform = d.Platform
			found = found || !j.Spec.Needs.cpuOnly() && view == t
		}
		if found {
			hosts = append(hosts, m.ID)
		}
	}
	slices.Sort(hosts)
	return hosts
}

// checkpointTargetRefusal keeps a resume off a target other than its
// checkpoint's while a host of that target remains enrolled.
func (q *Queue) checkpointTargetRefusal(j *Job, m *Machine) string {
	c := j.CheckpointTarget
	if c == nil || !j.Spec.Resumable || j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
		return ""
	}
	here := jobTargetOn(j, m)
	if here == c.Target || q.checkpointPortableTo(j, c.Target, m) {
		return ""
	}
	hosts := q.checkpointTargetHosts(j, c.Target)
	if len(hosts) == 0 {
		return ""
	}
	return fmt.Sprintf("checkpoint generation %d was written on %s, not %s; resuming on a host of that target (%s)", c.Generation, targetLabel(c.Target), targetLabel(here), strings.Join(hosts, ", "))
}

func (q *Queue) checkpointTargetAdmission(j *Job, m *Machine) *Hold {
	return reasonHold(checkpointTargetHold, q.checkpointTargetRefusal(j, m))
}

// Status names the checkpoint's target only when the rule excludes a host the
// job could otherwise use; otherwise the ordinary wait explains it.
func (q *Queue) checkpointTargetWait(j *Job, scans ...*placementScan) *Hold {
	if j.State != "queued" || j.CheckpointTarget == nil {
		return nil
	}
	for _, m := range q.planningMachines(scans...) {
		if q.jobAllowsMachine(j, m.ID) && executionTargetMatches(j.Spec, m) {
			if why := q.checkpointTargetRefusal(j, m); why != "" {
				return newHold(checkpointTargetHold, why)
			}
		}
	}
	return nil
}
