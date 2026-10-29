package myce

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
)

// DeviceMaintenance is queue-owned admission policy, independent of worker
// ordinals, sessions and health. Enabled records retain the revision fence.
type DeviceMaintenance struct {
	Host      string    `json:"host"`
	Platform  string    `json:"platform"`
	Identity  string    `json:"identity"`
	Disabled  bool      `json:"disabled"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
	Revision  uint64    `json:"revision"`
}

type DeviceMaintenanceRequest struct {
	Action     string  `json:"action"`
	Platform   string  `json:"platform"`
	Identity   string  `json:"identity"`
	Reason     string  `json:"reason,omitempty"`
	IfRevision *uint64 `json:"if_revision"`
}

func deviceMaintenanceKey(platform, identity string) string { return platform + ":" + identity }

func (q *Queue) deviceMaintenanceStatus(host string) []DeviceMaintenance {
	out := []DeviceMaintenance{}
	for _, p := range q.s.DeviceMaintenance[host] {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b DeviceMaintenance) int {
		return strings.Compare(deviceMaintenanceKey(a.Platform, a.Identity), deviceMaintenanceKey(b.Platform, b.Identity))
	})
	return out
}

// ResolveDevice accepts a displayed ID or a stable identity. The caller must
// send the resolved identity, never the ordinal, in its conditional mutation.
// Retained policy allows an absent device to be enabled by identity.
func (s HostMaintenanceStatus) ResolveDevice(selector string) (MachineDevice, error) {
	var matches []MachineDevice
	add := func(d MachineDevice) {
		if !slices.ContainsFunc(matches, func(old MachineDevice) bool { return old.Platform == d.Platform && old.Identity == d.Identity }) {
			matches = append(matches, d)
		}
	}
	for _, w := range s.Workers {
		for _, d := range w.Machine.Devices {
			if selector == d.ID || selector == d.Identity {
				add(d)
			}
		}
	}
	for _, p := range s.DeviceMaintenance {
		if selector == p.Identity {
			add(MachineDevice{Platform: p.Platform, Identity: p.Identity})
		}
	}
	if len(matches) != 1 {
		return MachineDevice{}, errors.New("DEVICE must resolve to exactly one stable inventory identity; inspect myce host status HOST")
	}
	return matches[0], nil
}

func (q *Queue) ChangeDeviceMaintenance(host string, r DeviceMaintenanceRequest) (HostMaintenanceStatus, error) {
	var out HostMaintenanceStatus
	if strings.TrimSpace(host) == "" || r.Platform == "" || r.Identity == "" || r.IfRevision == nil {
		return out, errors.New("host, platform, stable identity and if_revision are required")
	}
	if r.Action != "disable" && r.Action != "enable" || r.Action == "enable" && r.Reason != "" {
		return out, errors.New("expected disable with optional reason, or enable")
	}
	err := q.update(func() error {
		if err := q.validateMachineNames([]string{host}); err != nil {
			return err
		}
		key := deviceMaintenanceKey(r.Platform, r.Identity)
		policy, known := q.s.DeviceMaintenance[host][key]
		if !known {
			for _, m := range q.s.Machines {
				if hostID(m) == host && m.Replacement == nil {
					known = known || slices.ContainsFunc(m.Devices, func(d MachineDevice) bool { return d.Platform == r.Platform && d.Identity == r.Identity })
				}
			}
		}
		if !known {
			return errors.New("unknown stable device identity on host")
		}
		if policy.Revision != *r.IfRevision {
			return ErrStale
		}
		policy = DeviceMaintenance{Host: host, Platform: r.Platform, Identity: r.Identity, Disabled: r.Action == "disable", Reason: strings.TrimSpace(r.Reason), UpdatedAt: q.clock.Now(), Revision: policy.Revision + 1}
		if q.s.DeviceMaintenance == nil {
			q.s.DeviceMaintenance = map[string]map[string]DeviceMaintenance{}
		}
		if q.s.DeviceMaintenance[host] == nil {
			q.s.DeviceMaintenance[host] = map[string]DeviceMaintenance{}
		}
		q.s.DeviceMaintenance[host][key] = policy
		q.maintain()
		out = q.hostMaintenanceStatus(host)
		return nil
	})
	return out, err
}

func (q *Queue) deviceAdmissionBlocked(m *Machine) bool {
	if m == nil || m.GPU == "cpu" {
		return false
	}
	for _, d := range m.Devices {
		if d.ID == m.Device && d.Platform == m.GPU {
			return q.s.DeviceMaintenance[hostID(m)][deviceMaintenanceKey(d.Platform, d.Identity)].Disabled || q.componentExcluded(m, d.Platform, d.Identity)
		}
	}
	return q.componentExcluded(m, m.GPU, "")
}

const deviceMaintenanceStopCause = "planned_device_maintenance"

// Withdraw unstarted batch offers. Started attempts and their custody survive;
// the normal launch/Started fences serialize with the policy transaction.
func (q *Queue) maintainDeviceMaintenance() {
	for _, j := range q.liveJobs() {
		if j.State != "running" || j.StartedAt != nil || j.Spec.Needs.cpuOnly() {
			continue
		}
		if !q.deviceAdmissionBlocked(allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)) {
			continue
		}
		q.touchJob(j.ID)
		for i := range j.Copies {
			c := &j.Copies[i]
			if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && c.StopCause == "" {
				c.StopCause = deviceMaintenanceStopCause
			}
		}
		q.stop(j, deviceMaintenanceStopCause)
		q.retainRelease(j)
	}
}

func (a API) registerDeviceMaintenance(mux *requestMux) {
	mux.HandleFunc("POST /api/hosts/{id}/devices/maintenance", func(w http.ResponseWriter, r *http.Request) {
		var request DeviceMaintenanceRequest
		if !decode(w, r, &request) {
			return
		}
		status, err := a.Queue.ChangeDeviceMaintenance(r.PathValue("id"), request)
		respondOperation(w, status, err, "host_control_invalid")
	})
}
