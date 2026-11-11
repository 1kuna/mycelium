package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

const DefaultHealthStorageDelay = 5 * time.Minute

// HealthAlert is one record per open or recently closed problem: a kind and its
// subject (host, job, project, store...). Repeated firings update the record's
// last_at and count instead of adding rows. It contains identities and fixed
// diagnostic codes, never worker text. Receivers deduplicate DeliveryID, including
// after an ambiguous hook exit; a closing notification uses DeliveryID+"/closed".
type HealthAlert struct {
	Recovery    *AlertRecovery   `json:"recovery,omitempty"`
	Machine     string           `json:"machine,omitempty"`
	ClientKind  string           `json:"client_kind,omitempty"`
	Project     string           `json:"project,omitempty"`
	Reason      string           `json:"reason,omitempty"`
	Recipients  []string         `json:"recipients,omitempty"`
	DeliveryID  string           `json:"delivery_id"`
	Kind        string           `json:"kind"`
	Store       string           `json:"store,omitempty"`
	Host        string           `json:"host,omitempty"`
	Device      string           `json:"device,omitempty"`
	Reservation string           `json:"reservation,omitempty"`
	JobID       string           `json:"job_id,omitempty"`
	Attempt     int              `json:"attempt,omitempty"`
	At          time.Time        `json:"at"` // current episode's opening
	Delivered   bool             `json:"delivered"`
	Mount       *HostMountHealth `json:"mount,omitempty"`
	// Record fields. An empty State is a previous-release per-firing row; the
	// next transaction folds those into records (migrateHealthEvents).
	State           string     `json:"state,omitempty"` // "open" or "closed"
	FirstAt         *time.Time `json:"first_at,omitempty"`
	LastAt          *time.Time `json:"last_at,omitempty"`
	ClosedAt        *time.Time `json:"closed_at,omitempty"`
	Count           int        `json:"count,omitempty"`
	ClosedDelivered bool       `json:"closed_delivered,omitempty"`
}

// HealthAlertRetention bounds closed records, matching finished-job file
// retention (MY-47). Open records and unacknowledged configured notifications
// stay; the record count is bounded by distinct kinds and subjects.
const HealthAlertRetention = 72 * time.Hour

// healthAlertKey is the record identity: kind and subject. Attempt is the latest
// firing's detail, not a separate problem.
func healthAlertKey(e HealthAlert) string {
	return strings.Join([]string{e.Kind, e.Reason, e.Host, e.Machine, e.Device, e.Project, e.ClientKind, e.Store, e.Reservation, e.JobID, strings.Join(e.Recipients, ",")}, "\x00")
}

type healthDelivery struct {
	alert   HealthAlert
	key     string
	episode string
	closing bool
}

// pendingHealthDeliveries sends openings and closings only. A record that closed
// before its opening was delivered sends one closing notification.
func pendingHealthDeliveries(events []HealthAlert) []healthDelivery {
	var out []healthDelivery
	for _, r := range events {
		d := healthDelivery{alert: r, episode: r.DeliveryID}
		switch {
		case r.State == "open" && !r.Delivered:
		case r.State == "closed" && !(r.Delivered && r.ClosedDelivered):
			d.closing = true
			d.alert.DeliveryID += "/closed"
		default:
			continue
		}
		d.key = healthAlertKey(r)
		out = append(out, d)
	}
	return out
}

type healthCondition struct {
	Endings     []healthEnding `json:"endings,omitempty"`
	Since       time.Time      `json:"since"`
	LastAlert   time.Time      `json:"last_alert,omitempty"`
	Active      bool           `json:"active"`
	Samples     []healthSample `json:"samples,omitempty"`
	WasBad      bool           `json:"was_bad,omitempty"`
	SeenHealthy bool           `json:"seen_healthy,omitempty"`
	// Only rejected, oversized auto-memory demand needs a retained probe.
	// Accepted jobs already live in the queue. Drop the probe on recovery.
	Envelope *Job `json:"envelope,omitempty"`
	// Record is the healthAlertKey this active condition keeps open.
	Record string `json:"record,omitempty"`
}
type healthAlerts struct {
	DeliveryConfigured bool                       `json:"delivery_configured,omitempty"`
	Conditions         map[string]healthCondition `json:"conditions,omitempty"`
	Events             []HealthAlert              `json:"events,omitempty"`
}

// HealthAlertDelivery explains unacknowledged notifications without
// exposing the executable or claiming that pending events were attempted.
type HealthAlertDelivery struct {
	Configured bool `json:"configured"`
	Pending    int  `json:"pending"`
}

func (d HealthAlertDelivery) Summary() string {
	configuration := "health notifier is not configured"
	if d.Configured {
		configuration = "health notifier is configured"
	}
	return fmt.Sprintf("Health notifications: %s; %d alert openings or closings have no delivery acknowledgement.", configuration, d.Pending)
}

func (q *Queue) healthAlertDelivery() *HealthAlertDelivery {
	d := &HealthAlertDelivery{Configured: q.s.Health.DeliveryConfigured, Pending: len(pendingHealthDeliveries(q.s.Health.Events))}
	for _, e := range q.s.Health.Events {
		if e.State == "" && !e.Delivered {
			d.Pending++
		}
	}
	return d
}

func WithHealthStorageDelay(delay time.Duration) QueueOption {
	return func(q *Queue) { q.healthStorageDelay = delay }
}

// observeHealth is called inside the queue transaction. Only a new episode can
// alert; positive recovery rearms it. A wall-time cooldown can hide a new
// failure on a fast installation and has no local/protocol basis.
func (q *Queue) observeHealth(key string, bad bool, delay time.Duration, e HealthAlert) {
	if q.s.Health.Conditions == nil {
		q.s.Health.Conditions = map[string]healthCondition{}
	}
	c := q.s.Health.Conditions[key]
	now := q.clock.Now()
	if !bad {
		c.Since = time.Time{}
		c.Active = false
	} else {
		if c.Since.IsZero() {
			c.Since = now
		}
		if !c.Active && now.Sub(c.Since) >= delay {
			c.Active, c.LastAlert = true, now
			c.Record = q.recordHealthAlert(e, false)
		}
	}
	q.s.Health.Conditions[key] = c
}

// appendHealthAlert records a point event (no condition clears it): the record
// closes at once and delivers one notification per episode.
func (q *Queue) appendHealthAlert(e HealthAlert) { q.recordHealthAlert(e, true) }

// Records are a handful of rows; copy before editing because rollback and read
// snapshots may still own the previous slice. Every edited index is persisted.
func (q *Queue) editHealthEvents() []HealthAlert { return slices.Clone(q.s.Health.Events) }
func (q *Queue) healthEventChanged(i int) {
	if q.records == nil {
		return
	}
	if q.records.healthChanged == nil {
		q.records.healthChanged = map[int]bool{}
	}
	q.records.healthChanged[i] = true
}

// recordHealthAlert opens, reopens or updates the one record for e's kind and
// subject and returns its key. A reopen whose closing was never delivered
// continues the receiver's open episode instead of notifying again.
func (q *Queue) recordHealthAlert(e HealthAlert, point bool) string {
	now := q.clock.Now()
	key := healthAlertKey(e)
	events := q.editHealthEvents()
	i := slices.IndexFunc(events, func(r HealthAlert) bool { return r.State != "" && healthAlertKey(r) == key })
	n := e
	n.LastAt, n.State, n.ClosedAt, n.Delivered, n.ClosedDelivered = &now, "open", nil, false, false
	if point {
		n.State, n.ClosedAt = "closed", &now
	}
	if i < 0 {
		n.DeliveryID, n.At, n.FirstAt, n.Count = ID(), now, &now, 1
		n.Recovery = q.recoverHealthAlert(n)
		events = append(events, n)
		i = len(events) - 1
	} else {
		r := events[i]
		n.FirstAt, n.Count = r.FirstAt, r.Count+1
		if r.Delivered && r.State == "closed" && r.ClosedDelivered {
			n.DeliveryID, n.At = ID(), now
			n.Recovery = q.recoverHealthAlert(n)
		} else {
			n.DeliveryID, n.At, n.Delivered = r.DeliveryID, r.At, r.Delivered
			if point {
				n.ClosedDelivered = r.ClosedDelivered
			}
			if r.Delivered {
				n.Recovery = r.Recovery // published receipts are immutable
			} else {
				n.Recovery = q.recoverHealthAlert(n)
			}
		}
		events[i] = n
	}
	q.s.Health.Events = events
	q.healthEventChanged(i)
	return key
}

// reconcileHealthRecords closes open records no active condition keeps open
// (conditions are also deleted directly by detectors), then expires closed
// records after HealthAlertRetention. Removal swaps the last record into the
// gap so only two rows change.
func (q *Queue) reconcileHealthRecords() {
	open := map[string]bool{}
	for _, c := range q.s.Health.Conditions {
		if c.Active && c.Record != "" {
			open[c.Record] = true
		}
	}
	now := q.clock.Now()
	var events []HealthAlert
	edit := func() {
		if events == nil {
			events = q.editHealthEvents()
		}
	}
	for i, r := range q.s.Health.Events {
		if r.State == "open" && !open[healthAlertKey(r)] {
			edit()
			events[i].State, events[i].ClosedAt = "closed", &now
			q.healthEventChanged(i)
		}
	}
	current := q.s.Health.Events
	if events != nil {
		current = events
	}
	for i := len(current) - 1; i >= 0; i-- {
		r := current[i]
		if r.State != "closed" || r.ClosedAt == nil || now.Sub(*r.ClosedAt) < HealthAlertRetention || q.s.Health.DeliveryConfigured && !(r.Delivered && r.ClosedDelivered) {
			continue
		}
		edit()
		last := len(events) - 1
		events[i] = events[last]
		events = events[:last]
		current = events
		if i < last {
			q.healthEventChanged(i)
		}
	}
	if events != nil {
		q.s.Health.Events = events
	}
}

// migrateHealthEvents folds previous-release per-firing rows (empty State) into
// records, and rearms active conditions without a record so they reopen one in
// this pass. Migrated history is closed at its last firing; undelivered history
// sends one closing notification per record if a notifier is configured.
func (q *Queue) migrateHealthEvents() {
	for key, c := range q.s.Health.Conditions {
		if c.Active && c.Record == "" {
			c.Active = false
			q.s.Health.Conditions[key] = c
		}
	}
	if !slices.ContainsFunc(q.s.Health.Events, func(e HealthAlert) bool { return e.State == "" }) {
		return
	}
	var out []HealthAlert
	index := map[string]int{}
	for _, e := range q.s.Health.Events {
		if e.State != "" {
			index[healthAlertKey(e)] = len(out)
			out = append(out, e)
		}
	}
	created := map[string]bool{}
	for _, e := range q.s.Health.Events {
		if e.State != "" {
			continue
		}
		key, at := healthAlertKey(e), e.At
		i, ok := index[key]
		if !ok {
			n := e
			n.State, n.Count, n.FirstAt, n.LastAt, n.ClosedAt, n.Delivered = "closed", 0, &at, &at, &at, true
			i, index[key], created[key] = len(out), len(out), true
			out = append(out, n)
		}
		r := &out[i]
		r.Count++
		if r.FirstAt == nil || at.Before(*r.FirstAt) {
			r.FirstAt = &at
		}
		later := r.LastAt == nil || !at.Before(*r.LastAt)
		if later {
			r.LastAt = &at
		}
		if !created[key] {
			continue // a previous-release firing on an existing record: count only
		}
		if later {
			r.DeliveryID, r.At, r.Attempt, r.Recovery, r.Mount, r.ClosedAt = e.DeliveryID, e.At, e.Attempt, e.Recovery, e.Mount, &at
		}
		r.Delivered = r.Delivered && e.Delivered
		r.ClosedDelivered = r.Delivered
	}
	q.s.Health.Events = out
	for i := range out {
		q.healthEventChanged(i)
	}
}

func (q *Queue) detectHealth() {
	defer q.bodyPhaseFinish("health", q.bodyPhaseStart())
	q.migrateHealthEvents()
	defer q.reconcileHealthRecords()
	q.detectHostHygiene()
	q.detectReadinessHealth()
	// Job-specific pressure conditions follow live demand, rather than growing
	// with retained job history. Project input conditions are bounded by policy.
	for key := range q.s.Health.Conditions {
		id, jobKey := strings.CutPrefix(key, "storage-admission/")
		if !jobKey {
			id, jobKey = strings.CutPrefix(key, "storage-write/job/")
		}
		if jobKey {
			if j := q.s.Jobs[id]; j == nil || terminal(j.State) || cancelled(j) {
				delete(q.s.Health.Conditions, key)
			}
		}
	}

	delay := q.healthStorageDelay
	if delay <= 0 {
		delay = DefaultHealthStorageDelay
	}
	q.observeHealth("queue-storage", !q.fileCheckpoints && !q.nasAvailable, delay, HealthAlert{Kind: "checkpoint_store_unavailable", Store: "nas"})
	now := q.clock.Now()
	for _, m := range q.s.Machines {
		if m.Replacement != nil {
			continue
		}
		host := hostID(m)
		maintenance := q.s.HostMaintenance[host].Mode
		intentional := m.Leaving || maintenance == "disabled" || maintenance == "drained" || maintenance == "draining"
		stale := !q.workerFresh(m, now)
		if q.unreported[m.ID] && q.inRestartGrace(now) {
			stale = false
		}
		unhealthy := (q.s.HostMaintenance[host].SharedHost != nil && !q.sharedPressureFresh(m)) || len(m.UnavailableRoots) > 0 || m.MemoryError != "" || m.HostMemoryError != "" || m.InventoryReason != "" || slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return d.Reason != "" })
		cause := "host_health_unavailable"
		if stale {
			cause = "host_silent"
			if q.workerSuspect(m, now) {
				cause = "worker_suspect"
			}
		}
		q.observeHealth("host/"+m.ID, !intentional && (stale || unhealthy), 0, HealthAlert{Kind: "host_unhealthy_or_stale", Host: host, Reason: cause})
		checkpointRootMissing := slices.ContainsFunc(m.UnavailableRoots, func(r UnavailableRoot) bool { return r.Root == "checkpoint" })
		q.observeHealth("storage/"+m.ID, !intentional && (m.CheckpointUnavailable || checkpointRootMissing), delay, HealthAlert{Kind: "checkpoint_store_unavailable", Host: host})
		for _, v := range m.DiskVolumes {
			v = q.currentDiskVolume(v, m.DiskReceivedAt)
			bad := v.Error == "" && v.TotalBytes > 0 && v.FreeBytes < diskFreeFloor(v.TotalBytes)
			q.observeHealth("disk/"+m.ID+"/"+v.ID, !intentional && bad, 0, HealthAlert{Kind: "disk_below_guard", Host: host})
		}
	}
	q.detectIncidentHealth()
	q.detectRemainingHealth()
}
func (q *Queue) alertAttemptLoss(j *Job, outcome AttemptOutcome, quiet bool) {
	if j.Attempt == 0 || cancelled(j) || plannedStop(j) || historyPlannedStop(j) || j.Spec.Priority != "production" || outcome.PoliteStop || outcome.OperatorStop || outcome.PriorityStop != nil {
		return
	}
	timeout := func(c *RuntimeCause) bool { return c != nil && c.Class == "timeout" }
	if !quiet && !strings.HasPrefix(j.Reason, "machine quiet for ") && !timeout(outcome.ExecutionCause) && !timeout(outcome.CleanupCause) {
		return
	}
	if outcome.Attempt <= j.LastHealthAlertAttempt {
		return
	}
	q.touchJob(j.ID)
	j.LastHealthAlertAttempt = outcome.Attempt
	host := outcome.Machine
	if m := q.s.Machines[outcome.Machine]; m != nil {
		host = hostID(m)
	}
	q.appendHealthAlert(HealthAlert{Kind: "production_attempt_lost", Host: host, JobID: j.ID, Attempt: outcome.Attempt})
}

// ConfigureHealthNotifier validates the operator's local executable. It reuses
// completion notification's --check/JSON contract, but is queue-scoped.
func (q *Queue) ConfigureHealthNotifier(ctx context.Context, hook string) error {
	if hook == "" {
		q.mu.Lock()
		q.healthNotifier = ""
		q.mu.Unlock()
		return q.update(func() error {
			q.s.Health.DeliveryConfigured = false
			return nil
		})
	}
	resolved, err := exec.LookPath(hook)
	if err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(checkCtx, resolved, "--check")
	cmd.Env = healthNotifierEnv()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("health notifier check failed: %w", err)
	}
	q.mu.Lock()
	q.healthNotifier = resolved
	q.mu.Unlock()
	return q.update(func() error { q.s.Health.DeliveryConfigured = true; return nil })
}
func healthNotifierEnv() []string {
	return slices.DeleteFunc(os.Environ(), func(s string) bool { return strings.HasPrefix(s, "MYCE_TOKEN=") })
}
func (q *Queue) runHealthNotifier(ctx context.Context) {
	for {
		q.mu.RLock()
		hook, wake := q.healthNotifier, q.wake
		var pending []healthDelivery
		if hook != "" {
			pending = pendingHealthDeliveries(q.s.Health.Events)
		}
		q.mu.RUnlock()
		if hook == "" {
			return
		}
		failed := false
		for _, delivery := range pending {
			e := delivery.alert
			data, _ := json.Marshal(e)
			// Bound a wedged local hook so health delivery cannot strand shutdown.
			sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			cmd := exec.CommandContext(sendCtx, hook)
			cmd.Env, cmd.Stdin = healthNotifierEnv(), bytes.NewReader(data)
			err := cmd.Run()
			cancel()
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				log.Printf("health notifier delivery failed: %s", e.DeliveryID)
				failed = true
				break
			}
			err = q.ackHealthDelivery(ctx, delivery)
			if err != nil {
				failed = true
				break
			}
			// A burst of distinct incidents cannot flood the operator's receiver.
			// This is transport pacing; detectors still run only on queue mutations.
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		if failed {
			// Delivery retry, not health polling. Preserve the same delivery identity.
			timer := time.NewTimer(time.Minute)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-wake:
			}
		}
	}
}

// ackHealthDelivery marks one opening or closing delivered on the current record.
// A closing acknowledged after the record silently reopened starts a new
// episode, because the receiver now believes the problem closed.
func (q *Queue) ackHealthDelivery(ctx context.Context, d healthDelivery) error {
	return q.updateRequest(ctx, func() error {
		i := slices.IndexFunc(q.s.Health.Events, func(r HealthAlert) bool {
			return r.State != "" && r.DeliveryID == d.episode && healthAlertKey(r) == d.key
		})
		if i < 0 {
			return nil
		}
		events := q.editHealthEvents()
		r := &events[i]
		switch {
		case !d.closing:
			r.Delivered = true
		case r.State == "closed":
			r.Delivered, r.ClosedDelivered = true, true
		default:
			r.DeliveryID, r.At, r.Delivered, r.ClosedDelivered = ID(), q.clock.Now(), false, false
		}
		q.s.Health.Events = events
		q.healthEventChanged(i)
		return nil
	}, nil)
}
