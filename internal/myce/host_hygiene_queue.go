package myce

import "slices"

func validHostDrainReason(reason string) bool {
	return slices.Contains([]string{"", "host_memory_pressure", "host_swap_growth", "host_load_latency", "host_heartbeat_latency", "host_disk_pressure", "host_probe_unknown"}, reason)
}

func acceptHostHygiene(m *Machine) {
	if m.HostHealth == nil {
		return
	}
	if !validHostDrainReason(m.HostHealth.DrainReason) {
		m.HostHealth.DrainReason = ""
	}
	if len(m.HostHealth.History) > hostHealthHistorySize {
		m.HostHealth.History = m.HostHealth.History[len(m.HostHealth.History)-hostHealthHistorySize:]
	}
	if len(m.HostHealth.Baseline) > hostHealthHistorySize {
		m.HostHealth.Baseline = m.HostHealth.Baseline[len(m.HostHealth.Baseline)-hostHealthHistorySize:]
	}
}

func (q *Queue) detectHostHygiene() {
	for _, m := range q.s.Machines {
		if m.Replacement != nil || q.migrationHistoricalMachine(m) {
			continue
		}
		reason := hostHygieneReasonAt(m, q.clock.Now())
		q.observeHealth("hygiene/"+m.ID, reason != "", 0, HealthAlert{Kind: "host_degraded", Host: hostID(m), Reason: reason})
		if m.HostHealth == nil || len(m.HostHealth.History) == 0 {
			continue
		}
		s := m.HostHealth.History[len(m.HostHealth.History)-1]
		for _, role := range []string{"nas", "checkpoint"} {
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
