package myce

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type UsageSummary struct {
	Name        string  `json:"name"`
	GPUHours    float64 `json:"gpu_hours"`
	BusyPercent float64 `json:"busy_percent"`
	IdleHours   float64 `json:"idle_hours"`
	PinnedHours float64 `json:"pinned_hours"`
}
type DailyUsage struct {
	Date        string  `json:"date"`
	Project     string  `json:"project,omitempty"`
	Machine     string  `json:"machine,omitempty"`
	GPUHours    float64 `json:"gpu_hours"`
	BusyPercent float64 `json:"busy_percent"`
}
type Preemption struct {
	At      time.Time `json:"at"`
	Job     string    `json:"job"`
	Project string    `json:"project"`
	For     string    `json:"for"`
}
type ProductionStart struct {
	At      time.Time `json:"at"`
	Job     string    `json:"job"`
	Seconds float64   `json:"seconds"`
}
type UsageReport struct {
	Since            time.Time         `json:"since"`
	Until            time.Time         `json:"until"`
	Projects         []UsageSummary    `json:"projects"`
	Machines         []UsageSummary    `json:"machines"`
	Daily            []DailyUsage      `json:"daily"`
	Preemptions      []Preemption      `json:"preemptions"`
	ProductionStarts []ProductionStart `json:"production_starts"`
}
type usageSample struct {
	At                                   int64
	Span                                 int
	Machine, Device, Project, Job, Model string
	Busy, Idle, Pinned, Memory, Observed float64
}

// Attribute simultaneous work equally: one device never contributes more than
// one GPU-second per second. Pins reserve memory, but are idle unless serving.
func (q *Queue) deviceUsage(m *Machine) []usageSample {
	rows := []usageSample{}
	active := 0
	for _, j := range q.s.Jobs {
		if j.Machine == m.ID && (j.State == "running" || j.State == "stopping") && j.StartedAt != nil {
			rows = append(rows, usageSample{Project: j.Spec.Project, Job: j.ID, Memory: j.Spec.Needs.MemoryGB, Busy: 1})
			active++
		}
	}
	seen := map[string]bool{}
	for _, c := range q.calls {
		if c.Machine == m.ID && c.Error == "" {
			key := c.Project + "/" + c.Model
			if !seen[key] {
				model, _ := q.model(c.Model)
				rows = append(rows, usageSample{Project: c.Project, Model: c.Model, Memory: model.MemoryGB, Busy: 1})
				active++
				seen[key] = true
			}
		}
	}
	if active > 0 {
		for i := range rows {
			rows[i].Busy /= float64(active)
			rows[i].Observed = rows[i].Busy
		}
	}
	// A pin is a separate reservation, never added to busy/observed totals.
	reserved := map[string]bool{}
	for _, p := range q.s.Pins {
		if p.Assigned == m.ID {
			if s := q.s.ModelSlots[slotKey(m.ID, p.Model)]; s != nil && q.slotPinned(s) {
				model, _ := q.model(p.Model)
				reserved[p.Model] = true
				rows = append(rows, usageSample{Project: p.Project, Model: p.Model, Pinned: 1, Memory: model.MemoryGB})
			}
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.Machine == m.ID && slot.State != "unloaded" && !reserved[slot.Model] {
			found := false
			for _, r := range rows {
				if r.Model == slot.Model {
					found = true
				}
			}
			if !found {
				model, _ := q.model(slot.Model)
				rows = append(rows, usageSample{Model: slot.Model, Memory: model.MemoryGB})
			}
		}
	}
	if active == 0 {
		rows = append(rows, usageSample{Idle: 1, Observed: 1})
	}
	for i := range rows {
		rows[i].Machine = m.ID
		rows[i].Device = m.Device
	}
	return rows
}
func (q *Queue) accountUsage(now time.Time) {
	from := q.s.UsageAt
	if from.IsZero() || !now.After(from) {
		q.s.UsageAt = now
		return
	}
	if q.s.FairUsage == nil {
		q.s.FairUsage = map[string]FairUsage{}
	}
	for project, u := range q.s.FairUsage {
		q.s.FairUsage[project] = FairUsage{Seconds: decayed(u, now), At: now}
	}
	from = maxTime(from, now.Add(-90*24*time.Hour))
	for _, m := range q.s.Machines {
		if m.Gone || q.unreported[m.ID] || m.MemoryError != "" {
			continue
		}
		// Heartbeats define observation coverage. Unknown outage time is not idle.
		end := minTime(now, m.LastSeen.Add(q.machineLossTimeout(m.ID)))
		if !end.After(from) {
			continue
		}
		rows := q.deviceUsage(m)
		for start := from; start.Before(end); {
			stop := minTime(end, start.Truncate(time.Minute).Add(time.Minute))
			seconds := stop.Sub(start).Seconds()
			for _, r := range rows {
				r.At = start.Truncate(time.Minute).Unix()
				r.Span = 60
				r.Busy *= seconds
				r.Idle *= seconds
				r.Pinned *= seconds
				r.Memory *= seconds
				r.Observed *= seconds
				q.usageWrites = append(q.usageWrites, r)
				if r.Busy > 0 && r.Project != "" {
					u := q.s.FairUsage[r.Project]
					k := math.Ln2 / FairHalfLife.Seconds()
					credit := (1 - math.Exp(-k*seconds)) / k * math.Exp(-k*now.Sub(stop).Seconds()) * (r.Busy / seconds)
					u.Seconds += credit
					u.At = now
					q.s.FairUsage[r.Project] = u
				}
			}
			start = stop
		}
	}
	q.s.UsageAt = now
	q.usageMaintenance = now.Sub(q.s.UsageMaintenance) >= time.Hour
	if q.usageMaintenance {
		q.s.UsageMaintenance = now
	}
	starts := q.s.ProductionStarts[:0]
	for _, p := range q.s.ProductionStarts {
		if !p.At.Before(now.Add(-90 * 24 * time.Hour)) {
			starts = append(starts, p)
		}
	}
	q.s.ProductionStarts = starts
	keep := q.s.Preemptions[:0]
	for _, p := range q.s.Preemptions {
		if !p.At.Before(now.Add(-90 * 24 * time.Hour)) {
			keep = append(keep, p)
		}
	}
	q.s.Preemptions = keep
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func (q *Queue) persistUsage(data []byte) error {
	tx, e := q.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	for _, r := range q.usageWrites {
		_, e = tx.Exec(`INSERT INTO usage_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(at,span,machine,device,project,job,model) DO UPDATE SET busy=busy+excluded.busy,idle=idle+excluded.idle,pinned=pinned+excluded.pinned,memory_seconds=memory_seconds+excluded.memory_seconds,observed=observed+excluded.observed`, r.At, r.Span, r.Machine, r.Device, r.Project, r.Job, r.Model, r.Busy, r.Idle, r.Pinned, r.Memory, r.Observed)
		if e != nil {
			return e
		}
	}
	now := q.clock.Now()
	// After seven days retain hourly dimensions/totals, rather than minute rows.
	if q.usageMaintenance {
		cutoff := now.Add(-7 * 24 * time.Hour).Truncate(time.Hour).Unix()
		_, e = tx.Exec(`INSERT INTO usage_samples SELECT (at/3600)*3600,3600,machine,device,project,job,model,SUM(busy),SUM(idle),SUM(pinned),SUM(memory_seconds),SUM(observed) FROM usage_samples WHERE span=60 AND at<? GROUP BY (at/3600),machine,device,project,job,model ON CONFLICT(at,span,machine,device,project,job,model) DO UPDATE SET busy=busy+excluded.busy,idle=idle+excluded.idle,pinned=pinned+excluded.pinned,memory_seconds=memory_seconds+excluded.memory_seconds,observed=observed+excluded.observed`, cutoff)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(`DELETE FROM usage_samples WHERE (span=60 AND at<?) OR at+span<=?`, cutoff, now.Add(-90*24*time.Hour).Unix()); e != nil {
			return e
		}
	}

	if _, e = tx.Exec("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", data); e != nil {
		return e
	}
	return tx.Commit()
}
func (q *Queue) Usage(since time.Time) (UsageReport, error) {
	if e := q.Tick(); e != nil {
		return UsageReport{}, e
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.usageLocked(since)
}
func (q *Queue) usageLocked(since time.Time) (UsageReport, error) {
	now := q.clock.Now()
	out := UsageReport{Since: since, Until: now, Projects: []UsageSummary{}, Machines: []UsageSummary{}, Daily: []DailyUsage{}, Preemptions: []Preemption{}, ProductionStarts: []ProductionStart{}}
	if since.After(now) {
		return out, fmt.Errorf("since must precede current time")
	}
	rows, e := q.db.Query(`SELECT at,span,machine,device,project,job,model,busy,idle,pinned,memory_seconds,observed FROM usage_samples WHERE at+span>? AND at<?`, since.Unix(), now.Unix()+1)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	projects := map[string]*UsageSummary{}
	machines := map[string]*UsageSummary{}
	days := map[string]*DailyUsage{}
	coverage := map[string]float64{}
	dayCoverage := map[string]float64{}
	for rows.Next() {
		var r usageSample
		if e = rows.Scan(&r.At, &r.Span, &r.Machine, &r.Device, &r.Project, &r.Job, &r.Model, &r.Busy, &r.Idle, &r.Pinned, &r.Memory, &r.Observed); e != nil {
			return out, e
		}
		// Boundary buckets are proportional estimates (hourly after downsampling).
		overlap := minTime(now, time.Unix(r.At+int64(r.Span), 0)).Sub(maxTime(since, time.Unix(r.At, 0))).Seconds()
		factor := max(0, min(1, overlap/float64(r.Span)))
		// Current partial minute has actual seconds, so do not scale its end again.
		if r.At >= since.Unix() {
			factor = 1
		}
		r.Busy *= factor
		r.Idle *= factor
		r.Pinned *= factor
		r.Observed *= factor
		m := machines[r.Machine]
		if m == nil {
			m = &UsageSummary{Name: r.Machine}
			machines[r.Machine] = m
		}
		m.GPUHours += r.Busy / 3600
		m.IdleHours += r.Idle / 3600
		m.PinnedHours += r.Pinned / 3600
		coverage[r.Machine] += r.Observed
		date := time.Unix(r.At, 0).UTC().Format("2006-01-02")
		mk := date + "/machine/" + r.Machine
		d := days[mk]
		if d == nil {
			d = &DailyUsage{Date: date, Machine: r.Machine}
			days[mk] = d
		}
		d.GPUHours += r.Busy / 3600
		dayCoverage[mk] += r.Observed
		if r.Project != "" {
			p := projects[r.Project]
			if p == nil {
				p = &UsageSummary{Name: r.Project}
				projects[r.Project] = p
			}
			p.GPUHours += r.Busy / 3600
			p.PinnedHours += r.Pinned / 3600
			pk := date + "/project/" + r.Project
			d = days[pk]
			if d == nil {
				d = &DailyUsage{Date: date, Project: r.Project}
				days[pk] = d
			}
			d.GPUHours += r.Busy / 3600
		}
	}
	if e = rows.Err(); e != nil {
		return out, e
	}
	for _, m := range machines {
		if coverage[m.Name] > 0 {
			m.BusyPercent = m.GPUHours * 3600 / coverage[m.Name] * 100
		}
		out.Machines = append(out.Machines, *m)
	}
	for _, p := range projects {
		out.Projects = append(out.Projects, *p)
	}
	for key, d := range days {
		if dayCoverage[key] > 0 {
			d.BusyPercent = d.GPUHours * 3600 / dayCoverage[key] * 100
		}
		out.Daily = append(out.Daily, *d)
	}
	sort.Slice(out.Machines, func(i, j int) bool { return out.Machines[i].Name < out.Machines[j].Name })
	sort.Slice(out.Projects, func(i, j int) bool { return out.Projects[i].Name < out.Projects[j].Name })
	sort.Slice(out.Daily, func(i, j int) bool {
		a, b := out.Daily[i], out.Daily[j]
		return a.Date+a.Machine+a.Project < b.Date+b.Machine+b.Project
	})
	for _, p := range q.s.Preemptions {
		if !p.At.Before(since) {
			out.Preemptions = append(out.Preemptions, p)
		}
	}
	for _, p := range q.s.ProductionStarts {
		if !p.At.Before(since) {
			out.ProductionStarts = append(out.ProductionStarts, p)
		}
	}
	return out, nil
}
