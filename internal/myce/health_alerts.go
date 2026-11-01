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
const healthAlertCooldown = 15 * time.Minute

// HealthAlert contains identities and fixed diagnostic codes, never worker text.
// Receivers deduplicate DeliveryID, including after an ambiguous hook exit.
type HealthAlert struct {
	Project    string    `json:"project,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Recipients []string  `json:"recipients,omitempty"`
	DeliveryID string    `json:"delivery_id"`
	Kind       string    `json:"kind"`
	Store      string    `json:"store,omitempty"`
	Host       string    `json:"host,omitempty"`
	JobID      string    `json:"job_id,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
	At         time.Time `json:"at"`
	Delivered  bool      `json:"delivered"`
}
type healthCondition struct {
	Since     time.Time `json:"since"`
	LastAlert time.Time `json:"last_alert,omitempty"`
	Active    bool      `json:"active"`
}
type healthAlerts struct {
	DeliveryConfigured bool                       `json:"delivery_configured,omitempty"`
	Conditions         map[string]healthCondition `json:"conditions,omitempty"`
	Events             []HealthAlert              `json:"events,omitempty"`
}

func WithHealthStorageDelay(delay time.Duration) QueueOption {
	return func(q *Queue) { q.healthStorageDelay = delay }
}

// observeHealth is called inside the queue transaction. Only a new episode can
// alert; flapping is limited to one event per subject/code per 15 minutes.
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
		if !c.Active && now.Sub(c.Since) >= delay && (c.LastAlert.IsZero() || now.Sub(c.LastAlert) >= healthAlertCooldown) {
			c.Active, c.LastAlert = true, now
			q.appendHealthAlert(e)
		}
	}
	q.s.Health.Conditions[key] = c
}
func (q *Queue) appendHealthAlert(e HealthAlert) {
	e.DeliveryID, e.At = ID(), q.clock.Now()
	q.s.Health.Events = append(q.s.Health.Events, e)
	// Keep pending deliveries. Delivered history is a diagnostic window, not an
	// unbounded second job history. No configured notifier keeps the latest 256.
	q.pruneHealthEvents()
}
func (q *Queue) pruneHealthEvents() {
	for len(q.s.Health.Events) > 256 {
		i := slices.IndexFunc(q.s.Health.Events, func(e HealthAlert) bool { return e.Delivered || !q.s.Health.DeliveryConfigured })
		if i < 0 {
			return
		}
		q.s.Health.Events = slices.Delete(q.s.Health.Events, i, i+1)
	}
}

func (q *Queue) detectHealth() {
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
		if m.Replacement != nil || q.migrationHistoricalMachine(m) {
			continue
		}
		host := hostID(m)
		maintenance := q.s.HostMaintenance[host].Mode
		intentional := m.Leaving || maintenance == "disabled" || maintenance == "drained" || maintenance == "draining"
		stale := m.Gone || now.Sub(q.lastContact(m, now)) >= q.machineLossTimeout(m.ID)
		if q.unreported[m.ID] && now.Before(q.restartUntil) {
			stale = false
		}
		unhealthy := m.HostMemoryPolicyMismatch || m.MemoryError != "" || m.HostMemoryError != "" || m.InventoryReason != "" || slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return d.Reason != "" })
		q.observeHealth("host/"+m.ID, !intentional && (stale || unhealthy), 0, HealthAlert{Kind: "host_unhealthy_or_stale", Host: host})
		q.observeHealth("storage/"+m.ID, !intentional && m.CheckpointUnavailable, delay, HealthAlert{Kind: "checkpoint_store_unavailable", Host: host})
		for _, v := range m.DiskVolumes {
			v = q.currentDiskVolume(v, m.DiskReceivedAt)
			bad := v.Error == "" && v.TotalBytes > 0 && v.FreeBytes < diskFreeFloor(v.TotalBytes)
			q.observeHealth("disk/"+m.ID+"/"+v.ID, !intentional && bad, 0, HealthAlert{Kind: "disk_below_guard", Host: host})
		}
	}
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
		return nil
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
		var pending []HealthAlert
		if hook != "" {
			for _, e := range q.s.Health.Events {
				if !e.Delivered {
					pending = append(pending, e)
				}
			}
		}
		q.mu.RUnlock()
		if hook == "" {
			return
		}
		failed := false
		for _, e := range pending {
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
			err = q.update(func() error {
				for i := range q.s.Health.Events {
					if q.s.Health.Events[i].DeliveryID == e.DeliveryID {
						q.s.Health.Events[i].Delivered = true
					}
				}
				q.pruneHealthEvents()
				return nil
			})
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
