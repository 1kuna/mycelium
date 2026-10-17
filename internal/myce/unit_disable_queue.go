package myce

import (
	"errors"
	"fmt"
	"time"
)

type machineControl struct {
	PreviousSession string `json:"previous_session,omitempty"`
	Host            string `json:"host"`
	Session         string `json:"session"`
	Reason          string `json:"reason,omitempty"`
	HostControlGeneration
}

// ControlMachine accepts unit intent independently of the physical host's
// generation. Only the exact saved executions can confirm cleanup.
func (q *Queue) ControlMachine(host, machine, session, reason string, enable, cleared bool, executions []Result, generation uint64) error {
	return q.update(func() error {
		if err := q.controlMachine(host, machine, session, reason, enable, cleared, executions, generation, false); err != nil {
			return err
		}
		q.maintain()
		return nil
	})
}
func (q *Queue) controlMachine(host, machine, session, reason string, enable, cleared bool, executions []Result, generation uint64, hostScoped bool) error {
	m := q.s.Machines[machine]
	if host == "" || machine == "" || generation == 0 {
		return errors.New("unit control identity does not match machine")
	}
	// An enable conveys no cleanup evidence. A never-started installation has
	// neither a queue machine nor a saved worker session to acknowledge.
	if m == nil && enable {
		return nil
	}
	if m == nil || hostID(m) != host {
		return errors.New("unit control identity does not match machine")
	}
	key := machine
	if hostScoped {
		// Host and unit generations are independent monotonic sequences.
		hostControl := q.s.HostControls[host]
		if generation < hostControl.Generation || generation == hostControl.Generation && hostControl.Enabled {
			return errors.New("stale host receipt generation")
		}
		key = "\x00host/" + machine
	}
	old := q.s.MachineControls[key]
	// A machine name can move hosts; the former host's counter is unrelated.
	// This resets intent only, never saved execution custody.
	if old.Host != "" && old.Host != host {
		old = machineControl{}
	}
	if !enable && !(cleared && len(executions) == 0 && session == "") && (session == "" || m.Session != session && old.PreviousSession != session) {
		return errors.New("unit control session does not match machine history")
	}
	if generation < old.Generation {
		return fmt.Errorf("stale unit control generation; restore the unit control counter to at least %d, then rerun disable and enable", old.Generation)
	}
	if generation == old.Generation && old.Enabled != enable {
		return fmt.Errorf("unit control generation %d already records enable=%t; rerun disable and enable with a newer generation", generation, old.Enabled)
	}
	if enable && generation > old.Generation && !old.Cleared {
		return errors.New("unit cleanup must be acknowledged before enable")
	}
	// Validate the entire payload before fencing, refunding or settling anything.
	for _, r := range executions {
		if r.Machine != machine || r.Session != session || r.Attempt <= 0 || r.ReleasePending {
			return errors.New("unit execution identity mismatch")
		}
		j := q.s.Jobs[r.ID]
		matched := false
		if j != nil {
			for _, c := range j.Copies {
				if c.Machine == machine && c.Session == session && c.Attempt == r.Attempt {
					matched = true
					break
				}
			}
		}
		if !matched {
			return errors.New("unit execution does not match saved attempt")
		}
	}
	// A sessionless stale entry processed first can shadow live cleanup at the
	// same generation. Custody stays held and may escalate; it cannot leak.
	if generation == old.Generation && (old.Enabled || old.Cleared || !cleared) {
		return nil
	}
	if reason == "" && !enable {
		reason = "disabled by operator"
	}
	if q.s.MachineControls == nil {
		q.s.MachineControls = map[string]machineControl{}
	}
	q.s.MachineControls[key] = machineControl{Host: host, Session: session, PreviousSession: old.PreviousSession, Reason: reason, HostControlGeneration: HostControlGeneration{Generation: generation, Enabled: enable, Cleared: cleared}}
	if enable {
		m.Disabled = q.s.DisabledHosts[host]
		m.Leaving = m.Disabled != ""
		return nil
	}
	m.Disabled, m.Leaving = reason, true
	delete(q.unreported, machine)
	for _, r := range executions {
		j := q.s.Jobs[r.ID]
		current := j.Machine == machine && j.Session == session && j.Attempt == r.Attempt && (j.State == "running" || j.State == "stopping")
		if !cleared {
			if current {
				q.stop(j, "disabled by operator")
			}
			continue
		}
		// Mark confirmation before maintenance can retain the running release again.
		q.settleRelease(r)
		if current {
			j.ReleaseConfirmed = true
		}
		r.OperatorStop = true
		q.refundOperatorStop(r)
		if current {
			if cancelled(j) {
				j.State, j.StopAt = "cancelled", nil
			} else {
				q.retry(j, "requeued: disabled by operator")
				j.RequeuedByOperator = true
			}
		} else if j.State == "queued" {
			j.RequeuedByOperator = true
		}
		if j.ProductionHost == host {
			j.ProductionHost = ""
		}
	}
	if cleared {
		// A receipt may omit other executions. Keep their custody and machine cleanup
		// status; neither an empty receipt nor one unit can authorize blanket release.
		outstanding := false
		for _, r := range q.s.HostReleases {
			if r.Machine == machine {
				outstanding = true
			}
		}
		for _, j := range q.s.Jobs {
			if j.Machine == machine && (j.State == "running" || j.State == "stopping") {
				outstanding = true
			}
		}
		if !outstanding && m.Session == session {
			m.Gone = true
			m.DockerCleanup = ""
			m.Models = nil
			delete(q.s.BatchReleases, machine)
			for key, slot := range q.s.ModelSlots {
				if slot.Machine == machine && slot.Session == session {
					delete(q.s.ModelSlots, key)
				}
			}
		}
	}
	return nil
}

type localDisableReceipts struct {
	host, dir string
	next      time.Time
	nextLog   map[string]time.Time
}

// ConfigureLocalDisableReceipts no longer activates legacy receipt replay.
// S05 replaces this option with mandatory exact host reconciliation.
func (q *Queue) ConfigureLocalDisableReceipts(host, dir string) error {
	return errors.New("legacy local receipts unsupported; run myce host convert offline")
}
func (q *Queue) reconcileLocalDisableReceipts() {}
