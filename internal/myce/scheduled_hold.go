package myce

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
	"time"
)

// These owner-authored values belong only to the local policy/SQLite record.
// Nil policy collections preserve prior values; explicit empty collections clear.
type ScheduledHold struct {
	ID          string       `json:"id" yaml:"id"`
	Enabled     bool         `json:"enabled" yaml:"enabled"`
	Beneficiary string       `json:"beneficiary" yaml:"beneficiary"`
	Resource    HoldResource `json:"resource" yaml:"resource"`
	Reason      string       `json:"reason" yaml:"reason"`
	Timezone    string       `json:"timezone" yaml:"timezone"`
	Windows     []HoldWindow `json:"windows" yaml:"windows"`
}

type HoldResource struct {
	Host     string `json:"host,omitempty" yaml:"host,omitempty"`
	Machine  string `json:"machine,omitempty" yaml:"machine,omitempty"`
	Device   string `json:"device,omitempty" yaml:"device,omitempty"`
	Platform string `json:"platform,omitempty" yaml:"platform,omitempty"`
	Role     string `json:"role,omitempty" yaml:"role,omitempty"`
}

type HoldWindow struct {
	StartAt  *time.Time `json:"start_at,omitempty" yaml:"start_at,omitempty"`
	EndAt    *time.Time `json:"end_at,omitempty" yaml:"end_at,omitempty"`
	Weekdays []int      `json:"weekdays,omitempty" yaml:"weekdays,omitempty"`
	Start    string     `json:"start,omitempty" yaml:"start,omitempty"`
	End      string     `json:"end,omitempty" yaml:"end,omitempty"`
}

type ScheduledHoldStatus struct {
	ID          string   `json:"id"`
	Machine     string   `json:"machine"`
	Device      string   `json:"device,omitempty"`
	Beneficiary string   `json:"beneficiary"`
	Reason      string   `json:"reason"`
	State       string   `json:"state"`
	Blockers    []string `json:"blockers,omitempty"`
}

func selectorMatches(selector string, s Spec) bool {
	parts := strings.Split(selector, "/")
	if len(parts) != 2 {
		return false
	}
	project, _ := path.Match(parts[0], s.Project)
	class, _ := path.Match(parts[1], jobClass(s))
	return project && class
}

func validateScheduledHolds(p PriorityPolicy) error {
	ids := map[string]bool{}
	for _, h := range p.ScheduledHolds {
		if strings.TrimSpace(h.ID) == "" || ids[h.ID] || strings.TrimSpace(h.Reason) == "" || len(h.Windows) == 0 {
			return errors.New("scheduled holds require unique ID, reason and windows")
		}
		ids[h.ID] = true
		parts := strings.Split(h.Beneficiary, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return errors.New("hold beneficiary requires project/class")
		}
		for _, part := range parts {
			if _, err := path.Match(part, ""); err != nil {
				return fmt.Errorf("invalid hold beneficiary: %w", err)
			}
		}
		if h.Resource == (HoldResource{}) {
			return errors.New("hold requires a resource selector")
		}
		if h.Resource.Device != "" && !canonicalDeviceID(h.Resource.Device) {
			return errors.New("hold device must be a canonical device ID")
		}
		if h.Timezone == "" {
			return errors.New("hold timezone required")
		}
		if _, err := time.LoadLocation(h.Timezone); err != nil {
			return err
		}
		for _, w := range h.Windows {
			if w.StartAt != nil || w.EndAt != nil {
				if w.StartAt == nil || w.EndAt == nil || !w.StartAt.Before(*w.EndAt) || w.Start != "" || w.End != "" || len(w.Weekdays) != 0 {
					return errors.New("one-shot window requires start_at < end_at and no weekly fields")
				}
			} else {
				if parsed, err := time.Parse("15:04", w.Start); err != nil || parsed.Format("15:04") != w.Start {
					return errors.New("weekly start requires HH:MM")
				}
				if parsed, err := time.Parse("15:04", w.End); w.End != "24:00" && (err != nil || parsed.Format("15:04") != w.End) {
					return errors.New("weekly end requires HH:MM or 24:00")
				}
				if w.Start >= w.End || len(w.Weekdays) == 0 {
					return errors.New("weekly window requires weekdays and start < end; split overnight windows")
				}
				for _, d := range w.Weekdays {
					if d < 0 || d > 6 {
						return errors.New("weekdays must be 0 (Sunday) through 6")
					}
				}
			}
		}
	}
	for host, roles := range p.HostRoles {
		if strings.TrimSpace(host) == "" {
			return errors.New("host role mapping requires a host ID")
		}
		for _, role := range roles {
			if strings.TrimSpace(role) == "" {
				return errors.New("host roles must be nonempty")
			}
		}
	}
	return nil
}

func (h ScheduledHold) active(now time.Time) bool {
	if !h.Enabled {
		return false
	}
	for _, w := range h.Windows {
		if w.StartAt != nil {
			if !now.Before(*w.StartAt) && now.Before(*w.EndAt) {
				return true
			}
		} else if (ModelPin{Start: w.Start, End: w.End, Weekdays: w.Weekdays, Timezone: h.Timezone}).active(now) {
			return true
		}
	}
	return false
}

func (q *Queue) holdSelects(h ScheduledHold, m *Machine) bool {
	if m == nil {
		return false
	}
	r := h.Resource
	return (r.Host == "" || r.Host == hostID(m)) && (r.Machine == "" || r.Machine == m.ID) &&
		(r.Device == "" || r.Device == targetDevice(m)) && (r.Platform == "" || r.Platform == m.GPU) &&
		(r.Role == "" || slices.Contains(q.effectivePriorityPolicy().HostRoles[hostID(m)], r.Role))
}

// Only measured setup can withhold an otherwise usable pre-window interval.
// Unknown estimates do not invent an admission deadline or a cooldown.
func (h ScheduledHold) nextStart(now time.Time) *time.Time {
	var next *time.Time
	loc, err := time.LoadLocation(h.Timezone)
	if err != nil {
		return nil
	}
	local := now.In(loc)
	take := func(t time.Time) {
		if t.After(now) && (next == nil || t.Before(*next)) {
			copy := t
			next = &copy
		}
	}
	// Ask the standard timezone database for offsets over one weekly cycle.
	// Trying each offset represents both repeated civil minutes. A spring jump
	// can itself open a window whose authored start minute never occurred.
	offsets := map[int]bool{}
	horizon := local.AddDate(0, 0, 8)
	for cursor := local; ; {
		_, offset := cursor.Zone()
		offsets[offset] = true
		_, edge := cursor.ZoneBounds()
		if edge.IsZero() || edge.After(horizon) {
			break
		}
		if h.active(edge) && !h.active(edge.Add(-time.Nanosecond)) {
			take(edge)
		}
		cursor = edge
	}
	for _, w := range h.Windows {
		if w.StartAt != nil {
			take(*w.StartAt)
			continue
		}
		hm, _ := time.Parse("15:04", w.Start)
		for day := 0; day <= 7; day++ {
			date := time.Date(local.Year(), local.Month(), local.Day()+day, 12, 0, 0, 0, loc)
			if !slices.Contains(w.Weekdays, int(date.Weekday())) {
				continue
			}
			civil := time.Date(date.Year(), date.Month(), date.Day(), hm.Hour(), hm.Minute(), 0, 0, time.UTC)
			for offset := range offsets {
				candidate := civil.Add(-time.Duration(offset) * time.Second)
				wall := candidate.In(loc)
				if wall.Format("2006-01-02 15:04") == date.Format("2006-01-02")+" "+w.Start {
					take(candidate)
				}
			}
		}
	}
	return next
}

func (q *Queue) scheduledHoldReason(j *Job, m *Machine) string {
	if j != nil && j.StartedAt != nil && j.State != "queued" {
		return ""
	}
	now := q.clock.Now()
	for _, h := range q.effectivePriorityPolicy().ScheduledHolds {
		if !h.Enabled || !q.holdSelects(h, m) || j != nil && selectorMatches(h.Beneficiary, j.Spec) {
			continue
		}
		if h.active(now) {
			return fmt.Sprintf("scheduled hold %s for %s: %s", h.ID, h.Beneficiary, h.Reason)
		}
		if j != nil {
			if start := h.nextStart(now); start != nil {
				if setup, known := measuredSetupSeconds(j, m, false); known && setup > 0 && time.Duration(setup*float64(time.Second)) >= start.Sub(now) {
					return fmt.Sprintf("scheduled hold %s: measured setup %.0fs exceeds free window before %s", h.ID, setup, start.Format(time.RFC3339))
				}
			}
		}
	}
	return ""
}

func (q *Queue) scheduledWaitReason(j *Job) string {
	var reasons []string
	eligible := false
	for _, m := range q.plannerMachines() {
		if m.Gone || !q.jobAllowsMachine(j, m.ID) || !fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			continue
		}
		eligible = true
		reason := q.scheduledHoldReason(j, m)
		if reason == "" {
			return ""
		} // Another compatible target remains eligible.
		reasons = append(reasons, reason)
	}
	if !eligible {
		return ""
	}
	sort.Strings(reasons)
	return strings.Join(slices.Compact(reasons), "; ")
}

// Report exact retained obligations, including cleanup after a terminal result.
// Opening a window is never evidence that its predecessor physically released.
func (q *Queue) scheduledHoldStatuses() []ScheduledHoldStatus {
	var out []ScheduledHoldStatus
	for _, h := range q.effectivePriorityPolicy().ScheduledHolds {
		if !h.Enabled {
			continue
		}
		for _, m := range q.plannerMachines() {
			if !q.holdSelects(h, m) {
				continue
			}
			status := ScheduledHoldStatus{ID: h.ID, Machine: m.ID, Device: targetDevice(m), Beneficiary: h.Beneficiary, Reason: h.Reason, State: "scheduled"}
			if h.active(q.clock.Now()) {
				status.State = "exclusive_after_release"
				for _, o := range q.hostObligations(hostID(m)) {
					machine, _ := obligationSession(o)
					if machine != m.ID {
						continue
					}
					var spec *Spec
					device := ""
					switch {
					case o.Launch != nil:
						spec, device = o.Launch.Spec, o.Launch.Device
					case o.Call != nil:
						if c := q.obligationCall(o.Call); c != nil {
							s := callJob(c).Spec
							spec = &s
						}
						device = o.Call.Device
					case o.Engine != nil:
						if s := q.s.ModelSlots[slotKey(machine, o.Engine.Model, o.Engine.Device)]; s != nil {
							s := q.slotSpec(s)
							spec = &s
						}
						device = o.Engine.Device
					default:
						if j := q.s.Jobs[o.Result.ID]; j != nil {
							spec = &j.Spec
							for _, copy := range j.Copies {
								if copy.Machine == o.Result.Machine && copy.Session == o.Result.Session && copy.Attempt == o.Result.Attempt && len(copy.AllocatedDevices) == 1 {
									device = copy.AllocatedDevices[0]
									break
								}
							}
						}
					}
					if device != "" && targetDevice(m) != device {
						continue
					}
					if spec == nil || !selectorMatches(h.Beneficiary, *spec) {
						status.Blockers = append(status.Blockers, obligationKey(o))
					}
				}
				if reason := q.startupReleaseBlocker(hostID(m)); reason != "" {
					status.Blockers = append(status.Blockers, "startup release pending")
				}
				if q.maintenanceBlocked(hostID(m)) {
					status.Blockers = append(status.Blockers, "host maintenance")
				}
				if reason := q.dockerBlocker(hostID(m)); reason != "" {
					status.Blockers = append(status.Blockers, "Docker cleanup pending")
				}
				if len(status.Blockers) > 0 {
					status.State = "draining"
				}
			}
			out = append(out, status)
		}
	}
	slices.SortFunc(out, func(a, b ScheduledHoldStatus) int {
		return strings.Compare(a.ID+"/"+a.Machine+"/"+a.Device, b.ID+"/"+b.Machine+"/"+b.Device)
	})
	return out
}

func cloneHoldPolicy(p PriorityPolicy) PriorityPolicy {
	if p.ScheduledHolds != nil {
		p.ScheduledHolds = slices.Clone(p.ScheduledHolds)
		for i := range p.ScheduledHolds {
			p.ScheduledHolds[i].Windows = slices.Clone(p.ScheduledHolds[i].Windows)
			for k := range p.ScheduledHolds[i].Windows {
				w := &p.ScheduledHolds[i].Windows[k]
				w.Weekdays = slices.Clone(w.Weekdays)
				if w.StartAt != nil {
					t := *w.StartAt
					w.StartAt = &t
				}
				if w.EndAt != nil {
					t := *w.EndAt
					w.EndAt = &t
				}
			}
		}
	}
	if p.HostRoles != nil {
		roles := map[string][]string{}
		for host, v := range p.HostRoles {
			roles[host] = slices.Clone(v)
		}
		p.HostRoles = roles
	}
	return p
}

// Whole-host production holds must not manufacture demand for a job that the
// owner calendar currently excludes. Existing Started obligations still drain.
func (q *Queue) scheduledDemandBlocked(j *Job) bool {
	if len(q.effectivePriorityPolicy().ScheduledHolds) == 0 {
		return false
	}
	found := false
	for _, m := range q.plannerMachines() {
		if j.ProductionHost != "" && hostID(m) != j.ProductionHost || !q.jobAllowsMachine(j, m.ID) || !fitsWithoutHostMemory(j, m, m.TotalMemoryGB) {
			continue
		}
		found = true
		if q.scheduledHoldReason(j, m) == "" {
			return false
		}
	}
	return found
}
