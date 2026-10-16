package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	if !enable && !(cleared && len(executions) == 0) && (session == "" || m.Session != session && old.PreviousSession != session) {
		return errors.New("unit control session does not match machine history")
	}
	if generation < old.Generation || (generation == old.Generation && old.Enabled != enable) {
		return fmt.Errorf("stale unit control generation; restore the unit control counter to at least %d, then rerun disable and enable", old.Generation)
	}
	if enable && generation > old.Generation && old.Generation > 0 && !old.Cleared {
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
	if generation == old.Generation && (enable || old.Session == session) && (old.Enabled || old.Cleared || !cleared) {
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
			if j.Reason == "cancel requested" {
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

// ConfigureLocalDisableReceipts opts into read-only local receipt reconciliation.
// Call immediately after Open, before serving requests or running maintenance.
func (q *Queue) ConfigureLocalDisableReceipts(host, dir string) error {
	if host == "" || dir == "" {
		return errors.New("local disable receipts require host ID and host-state directory")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if _, err := os.ReadDir(abs); err != nil {
		return fmt.Errorf("local disable receipts require an existing readable host-state directory: %w", err)
	}
	return q.update(func() error {
		q.localReceipts = localDisableReceipts{host: host, dir: abs, nextLog: map[string]time.Time{}}
		q.maintain()
		return nil
	})
}

// Maintenance runs this before loss, deadline and escalation decisions. The
// existing queue runner ticks regularly; the injected Clock bounds disk scans.
func (q *Queue) reconcileLocalDisableReceipts() {
	config := &q.localReceipts
	if config.dir == "" || q.clock.Now().Before(config.next) {
		return
	}
	config.next = q.clock.Now().Add(time.Second)
	dirs := []string{config.dir}
	entries, err := os.ReadDir(filepath.Join(config.dir, "units"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		q.logLocalReceiptSkip("units-unreadable")
	}
	for _, entry := range entries {
		if entry.IsDir() && ValidWorkerUnit(entry.Name()) {
			dirs = append(dirs, filepath.Join(config.dir, "units", entry.Name()))
		}
	}
	for _, dir := range dirs {
		q.readLocalDisableReceipt(dir)
	}
}

func (q *Queue) readLocalDisableReceipt(dir string) {
	// Never contend with operator control. Atomic renames publish whole files;
	// the flag/receipt/counter must agree, and an enable invalidates the counter
	// before removing the receipt or flag. A mixed cycle cannot authorize cleanup.
	var flag struct {
		Generation uint64 `json:"generation"`
		Reason     string `json:"reason"`
	}
	var receipt hostReceipt
	read := func(name string, value any) bool {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			if name != "DISABLED" || !errors.Is(err, os.ErrNotExist) {
				q.logLocalReceiptSkip(name + "-unreadable-or-missing")
			}
			return false
		}
		if json.Unmarshal(data, value) != nil {
			q.logLocalReceiptSkip(name + "-invalid-json")
			return false
		}
		return true
	}
	var generation uint64
	if !read("DISABLED", &flag) || !read("disable-receipt.json", &receipt) || !read("control-generation.json", &generation) {
		return
	}
	if flag.Generation == 0 || receipt.Generation != flag.Generation || generation != flag.Generation || receipt.EnableGeneration != 0 {
		q.logLocalReceiptSkip("generation-mismatch-or-enabled")
		return
	}
	forgottenDir := dir
	if parent := unitParent(dir); parent != "" {
		forgottenDir = parent
	}
	forgotten, err := readForgottenQueues(forgottenDir)
	if err != nil {
		q.logLocalReceiptSkip("forgotten-queues-unreadable")
		return
	}
	seen := map[string]bool{}
	for _, w := range receipt.Workers {
		if w.Queue == "" {
			continue
		}
		if generation, ok := forgotten.Queues[w.Queue]; ok && w.Generation <= generation {
			q.logLocalReceiptSkip("forgotten-queue")
			continue
		}
		if w.Host != q.localReceipts.host || w.Generation != receipt.Generation {
			q.logLocalReceiptSkip("worker-host-or-generation-mismatch")
			continue
		}
		if unitParent(dir) != "" && w.Unit != filepath.Base(dir) {
			q.logLocalReceiptSkip("worker-unit-mismatch")
			continue
		}
		// Receipts from before the explicit session field retain it in executions.
		session := w.Session
		if session == "" && len(w.Executions) > 0 {
			session = w.Executions[0].Session
		}
		key := w.Queue + "\x00" + w.Host + "\x00" + w.Name + "\x00" + session
		if seen[key] {
			continue
		}
		seen[key] = true
		executions := append([]Result(nil), w.Executions...)
		for _, other := range receipt.Workers {
			otherSession := other.Session
			if otherSession == "" && len(other.Executions) > 0 {
				otherSession = other.Executions[0].Session
			}
			if other.State != w.State && other.Queue == w.Queue && other.Host == w.Host && other.Name == w.Name && otherSession == session && other.Generation == w.Generation && other.Unit == w.Unit {
				executions = append(executions, other.Executions...)
			}
		}
		if err := q.controlMachine(w.Host, w.Name, session, flag.Reason, false, true, executions, w.Generation, unitParent(dir) == ""); err != nil {
			q.logLocalReceiptSkip(err.Error())
		}
	}
}

func (q *Queue) logLocalReceiptSkip(reason string) {
	if q.clock.Now().Before(q.localReceipts.nextLog[reason]) {
		return
	}
	q.localReceipts.nextLog[reason] = q.clock.Now().Add(time.Minute)
	log.Printf("event=local_disable_receipt_skipped reason=%q", reason)
}
