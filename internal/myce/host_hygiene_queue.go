package myce

import (
	"maps"
	"slices"
	"strings"
)

func validHostDrainReason(reason string) bool {
	return slices.Contains([]string{"", "host_memory_pressure", "host_swap_growth", "host_load_latency", "host_heartbeat_latency", "host_disk_pressure", "host_probe_unknown"}, reason)
}

func acceptHostHygiene(m *Machine) {
	if m.HostHealth == nil {
		return
	}
	// The incoming report may still be referenced by its caller or a read
	// snapshot. Only the queue's copy receives the effective classification.
	h := *m.HostHealth
	m.HostHealth = &h
	h.PressureEvaluation = nil
	if !validHostDrainReason(m.HostHealth.DrainReason) {
		m.HostHealth.DrainReason = ""
	}
	if len(m.HostHealth.History) > hostHealthHistorySize {
		m.HostHealth.History = m.HostHealth.History[len(m.HostHealth.History)-hostHealthHistorySize:]
	}
	if len(m.HostHealth.Baseline) > hostHealthHistorySize {
		m.HostHealth.Baseline = m.HostHealth.Baseline[len(m.HostHealth.Baseline)-hostHealthHistorySize:]
	}
	evaluateHostPressure(m)
}

func (q *Queue) detectHostHygiene() {
	for _, m := range q.s.Machines {
		if m.Replacement != nil {
			continue
		}
		now := q.clock.Now()
		reason := q.hostHygieneReasonAt(m, now)
		// After a queue restart, persisted evidence falls back to wall age, so
		// every host's probe looked expired until its first report: each
		// restart alerted every host at once (live: eight 5-host bursts in two
		// days). The age is the queue's outage, not host harm. Admission still
		// refuses; the alert waits for the report or the end of restart grace.
		if reason != "host_probe_unknown" || !q.unreported[m.ID] || !q.inRestartGrace(now) {
			q.observeHealth("hygiene/"+m.ID, reason != "", 0, HealthAlert{Kind: "host_degraded", Host: hostID(m), Reason: reason})
		}
		q.detectDiskExhaustionForecast(m)
		if m.HostHealth == nil || len(m.HostHealth.History) == 0 {
			continue
		}
		s := m.HostHealth.History[len(m.HostHealth.History)-1]
		for _, role := range []string{"nas", "checkpoint", "job-files"} {
			cause := ""
			var evidence *HostMountHealth
			for _, mount := range s.Mounts {
				if mount.Root == role && slices.Contains([]string{"mount_probe_failed", "mount_probe_slow", "mount_probe_stalled", "mount_macos_approval_required", "mount_linux_operator_required", "mount_inventory_unavailable", "mount_automount_pending"}, mount.Reason) {
					cause = mount.Reason
					copy := mount
					evidence = &copy
				}
			}
			q.observeHealth("hygiene-mount/"+m.ID+"/"+role, cause != "", 0, HealthAlert{Kind: "host_mount_degraded", Host: hostID(m), Store: role, Reason: cause, Mount: evidence})
			// A real stall/failure must wake even if an advisory already opened
			// the degradation episode on this root.
			hold := cause != "" && mountHealthWithholds(cause)
			q.observeHealth("hygiene-mount-hold/"+m.ID+"/"+role, hold, 0, HealthAlert{Kind: "host_mount_unavailable", Host: hostID(m), Store: role, Reason: cause, Mount: evidence})
			action := cause == "mount_macos_approval_required" || cause == "mount_linux_operator_required"
			// Escalation must wake even if a generic degradation already alerted.
			q.observeHealth("hygiene-mount-action/"+m.ID+"/"+role, action, 0, HealthAlert{Kind: "host_mount_action_required", Host: hostID(m), Store: role, Reason: cause})
		}
	}
}

// A sustained decline mostly explained by writes outside Mycelium's own
// allocation wakes the host owner once it has run at least as long as the
// space it leaves would last at its average rate: at a constant rate, once it
// has consumed half the space above the floor that was free when it began.
// Mycelium's own growth stays bounded by its filesystem budget and floor, so
// it never raises this. Context only: no drain, no cleanup of foreign data.
func (q *Queue) detectDiskExhaustionForecast(m *Machine) {
	forecast := map[string]bool{}
	if m.HygieneCollection != nil {
		for _, v := range m.HygieneCollection.DiskBudget {
			forecast[v.ID] = v.ID != "" && v.DeclineCause == "foreign_disk_growth" && v.ExhaustionSeconds != nil && v.DecliningSeconds > 0 && v.DecliningSeconds >= *v.ExhaustionSeconds
		}
	}
	prefix := "hygiene-disk-forecast/" + m.ID + "/"
	for key, c := range q.s.Health.Conditions {
		if id, ok := strings.CutPrefix(key, prefix); ok && c.Active {
			if _, reported := forecast[id]; !reported {
				forecast[id] = false // A volume no longer reported clears.
			}
		}
	}
	for _, id := range slices.Sorted(maps.Keys(forecast)) {
		q.observeHealth(prefix+id, forecast[id], 0, HealthAlert{Kind: "host_disk_exhaustion_forecast", Host: hostID(m), Store: id, Reason: "foreign_disk_growth"})
	}
}
