package myce

import (
	"slices"
	"strings"
	"time"
)

// DeviceUse is queue-owned assignment history. Stable hardware identity keeps
// history across sessions, driver updates and inventory ordinal changes.
type DeviceUse struct {
	Host           string    `json:"host"`
	Platform       string    `json:"platform"`
	Identity       string    `json:"identity"`
	LastAssignedAt time.Time `json:"last_assigned_at"`
	Assignments    uint64    `json:"assignments"`
}

func deviceUseKey(host string, d MachineDevice) string {
	return identityHash([]string{host, d.Platform, d.Identity})
}

func (q *Queue) recordDeviceUse(m *Machine) {
	for _, d := range m.Devices {
		if !slices.Contains(targetDevices(m), d.ID) {
			continue
		}
		if q.s.DeviceUse == nil {
			q.s.DeviceUse = map[string]DeviceUse{}
		}
		key := deviceUseKey(hostID(m), d)
		use := q.s.DeviceUse[key]
		use.Host, use.Platform, use.Identity = hostID(m), d.Platform, d.Identity
		use.LastAssignedAt = q.clock.Now()
		use.Assignments++
		q.s.DeviceUse[key] = use
	}
}

// Only break a true tie between single accelerators on the same host. Existing
// cross-host ordering and strict placement preferences remain authoritative.
func (q *Queue) deviceUseTieBetter(a, b *Machine, fallback bool) bool {
	if hostID(a) != hostID(b) || a.GPU == "cpu" || b.GPU == "cpu" || a.Devices == nil || b.Devices == nil || len(a.group) > 0 || len(b.group) > 0 {
		return fallback
	}
	use := func(m *Machine) DeviceUse {
		for _, d := range m.Devices {
			if d.ID == m.Device {
				return q.s.DeviceUse[deviceUseKey(hostID(m), d)]
			}
		}
		return DeviceUse{}
	}
	au, bu := use(a), use(b)
	if !au.LastAssignedAt.Equal(bu.LastAssignedAt) {
		return au.LastAssignedAt.Before(bu.LastAssignedAt)
	}
	// Assignments distinguish serialized offers made in the same clock instant.
	if au.Assignments != bu.Assignments {
		return au.Assignments < bu.Assignments
	}
	return fallback
}

func (q *Queue) deviceUseStatus(host string) []DeviceUse {
	out := []DeviceUse{}
	for _, use := range q.s.DeviceUse {
		if use.Host == host {
			out = append(out, use)
		}
	}
	slices.SortFunc(out, func(a, b DeviceUse) int {
		return strings.Compare(deviceMaintenanceKey(a.Platform, a.Identity), deviceMaintenanceKey(b.Platform, b.Identity))
	})
	return out
}
