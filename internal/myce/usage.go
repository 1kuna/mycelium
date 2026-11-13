package myce

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

type UsageSummary struct {
	Name        string  `json:"name"`
	GPUHours    float64 `json:"gpu_hours"`
	BusyPercent float64 `json:"busy_percent"`
	IdleHours   float64 `json:"idle_hours"`
	PinnedHours float64 `json:"pinned_hours"`
}
type CardUsage struct {
	Machine string `json:"machine"`
	Device  string `json:"device"`
	UsageSummary
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
	Cards            []CardUsage       `json:"cards"`
	Daily            []DailyUsage      `json:"daily"`
	Preemptions      []Preemption      `json:"preemptions"`
	ProductionStarts []ProductionStart `json:"production_starts"`
}
type usageSample struct {
	At                                   int64
	Span                                 int
	Machine, Device, Project, Job, Model string
	// Tier/family/Fair are internal fairness dimensions; reporting keeps its
	// existing started-work GPU-hour semantics.
	Tier, Family                         string
	Fair                                 float64
	FairAllocation                       string
	Busy, Idle, Pinned, Memory, Observed float64
}

// Attribute simultaneous work equally: one device never contributes more than
// one GPU-second per second. Pins reserve memory, but are idle unless serving.
func (q *Queue) deviceUsage(m *Machine) []usageSample {
	return q.targetUsage(m, false)
}

// Physical recording includes model reservations/calls on a containing group.
// Planner fairness continues to consume its original target views.
func (q *Queue) targetUsage(m *Machine, physical bool) []usageSample {
	onDevice := func(device string) bool {
		return device == m.Device || physical && slices.Contains(strings.Split(device, ","), m.Device)
	}
	rows := []usageSample{}
	active, allocated := 0, 0
	for _, j := range q.liveJobs() {
		if (batchOnTarget(j, m) || m.Devices != nil && m.GPU == "cpu" && j.Machine == m.ID && j.Spec.Needs.cpuOnly()) && batchReservesCapacity(j) {
			busy, fair := 0.0, 1.0
			if j.Spec.Needs.cpuOnly() {
				fair = float64(j.Spec.Needs.cores())
			}
			if j.StartedAt != nil && !j.Spec.Needs.cpuOnly() {
				busy = 1
				active++
			}
			rows = append(rows, usageSample{FairAllocation: fairAllocationKey(m, j.ID, fmt.Sprint(j.Attempt)), Project: j.Spec.Project, Job: j.ID, Memory: j.Spec.Needs.MemoryGB, Busy: busy, Fair: fair, Tier: q.priorityMatch(j.Spec).Tier, Family: fairResourceFamily(j.Spec)})
			if !j.Spec.Needs.cpuOnly() {
				allocated++
			}
		}
	}
	seen := map[string]bool{}
	for _, c := range q.calls {
		if c.Machine == m.ID && (m.Devices == nil || onDevice(c.Device) && m.GPU != "cpu") && c.Error == "" {
			key := fairUsageKey(q.priorityMatch(callJob(c).Spec).Tier, c.Project, c.Model)
			if seen[key] {
				continue
			}
			model, _ := q.model(c.Model)
			rows = append(rows, usageSample{FairAllocation: fairAllocationKey(m, "model:"+c.Model, c.Attempt), Project: c.Project, Model: c.Model, Memory: model.MemoryGB, Busy: 1, Fair: 1, Tier: q.priorityMatch(callJob(c).Spec).Tier, Family: "gpu"})
			active++
			allocated++
			seen[key] = true
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
		if p.Assigned == m.ID && (m.Devices == nil || onDevice(p.AssignedDevice) && m.GPU != "cpu") {
			if s := q.s.ModelSlots[slotKey(m.ID, p.Model, p.AssignedDevice)]; s != nil && q.slotPinned(s) {
				model, _ := q.model(p.Model)
				reserved[p.Model] = true
				rows = append(rows, usageSample{Project: p.Project, Model: p.Model, Pinned: 1, Memory: model.MemoryGB})
			}
		}
	}
	for _, slot := range q.s.ModelSlots {
		if (slotOnTarget(slot, m) || physical && slot.Machine == m.ID && m.GPU != "cpu" && onDevice(slot.Device)) && slot.State != "unloaded" && !reserved[slot.Model] {
			found := false
			for _, r := range rows {
				if r.Model == slot.Model {
					found = true
				}
			}
			if !found {
				model, _ := q.model(slot.Model)
				row := usageSample{Project: model.Project, Model: slot.Model, Memory: model.MemoryGB}
				if slot.State == "loading" {
					row.Project = slot.Project
					row.Tier = q.priorityMatch(q.slotSpec(slot)).Tier
					row.Family, row.Fair = "gpu", 1
					row.FairAllocation = fairAllocationKey(m, "model:"+slot.Model, slot.Attempt)
					allocated++
				}
				rows = append(rows, row)
			}
		}
	}
	if active == 0 && m.GPU != "cpu" {
		rows = append(rows, usageSample{Idle: 1, Observed: 1})
	}
	for i := range rows {
		if allocated > 0 && rows[i].Family != "cpu" {
			rows[i].Fair /= float64(allocated)
		}
		rows[i].Machine = m.ID
		rows[i].Device = m.Device
	}
	return rows
}

func (q *Queue) accountUsage(now time.Time) {
	from := q.s.UsageAt
	if from.IsZero() || now.Before(from) {
		q.s.UsageAt = now
		return
	}
	if q.s.FairUsage == nil {
		q.s.FairUsage = map[string]FairUsage{}
	}
	q.accountFairHistory(from, now)
	if !now.After(from) {
		return
	}
	// Record physical cards, never synthetic placement groups.
	var targets []*Machine
	for _, m := range q.s.Machines {
		targets = append(targets, q.machineTargets(m)...)
	}
	for _, m := range targets {
		if m.Gone || q.unreported[m.ID] || m.MemoryError != "" {
			continue
		}
		// Heartbeats define observation coverage. Unknown outage time is not idle.
		end := minTime(now, m.LastSeen.Add(q.machineLossTimeout(m.ID)))
		if !end.After(from) {
			continue
		}
		rows := q.targetUsage(m, true)
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
			}
			start = stop
		}
	}
	q.s.UsageAt = now

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
func (q *Queue) persistUsage(timing *queueUpdateTiming) error {
	measured := queuePersistenceTiming{}
	defer func() {
		if timing != nil {
			timing.persistence = measured
		}
		if q.observePersistence != nil {
			q.observePersistence(measured)
		}
	}()
	started := holdNow(q.holdClock)
	var tx *sql.Tx
	var e error
	if timing != nil && timing.measureWriter {
		// Keep the existing transaction and lock order; only separate pool
		// acquisition from SQL work for the instruction request's slow span.
		conn, err := timing.writer(q.db, q.clock)
		if err != nil {
			return err
		}
		defer conn.Close()
		tx, e = conn.BeginTx(context.Background(), nil)
	} else {
		tx, e = q.db.Begin()
	}
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

	if e = q.persistModelIntervals(tx); e != nil {
		return e
	}
	measured.SQL += holdNow(q.holdClock).Sub(started)
	next, e := q.persistState(tx, &measured)
	if e != nil {
		return e
	}
	started = holdNow(q.holdClock)
	e = timing.commit(tx)
	measured.Commit = holdNow(q.holdClock).Sub(started)
	if e != nil {
		return e
	}
	if q.persisted == nil {
		q.persisted = map[string][]byte{}
	}
	q.commitHistories()
	changedJobs := false
	for key, value := range next {
		q.noteEvidenceRecord(key, q.persisted[key], value)
		// persistState reuses the immutable byte slice for unchanged records.
		// Invalidate only after SQLite commit, including removal, and never on
		// ordinary check-ins that merely journal an unchanged live job.
		if id, ok := strings.CutPrefix(key, "jobs/"); ok && (len(value) == 0 || len(q.persisted[key]) == 0 || &value[0] != &q.persisted[key][0]) {
			changedJobs = true
			q.invalidateJobSnapshot(id)
		}
		if value == nil {
			delete(q.persisted, key)
		} else {
			q.persisted[key] = value
		}
	}
	if changedJobs {
		// Preserve immediate cancellation-custody and blocker transitions even
		// when the selected job's own durable envelope did not change.
		q.invalidateDependentSnapshots()
	}
	if q.readRoot == nil {
		q.publishReadRecords(q.persisted)
	} else {
		q.publishReadRecords(next)
	}
	return nil
}
func (q *Queue) Usage(since time.Time, requestContexts ...context.Context) (UsageReport, error) {
	requestCtx := requestContext(requestContexts)

	if e := q.TickContext(requestCtx); e != nil {
		return UsageReport{}, e
	}
	return q.usageSnapshot(since, requestCtx)
}

func (q *Queue) usageSnapshot(since time.Time, requestContexts ...context.Context) (UsageReport, error) {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	v := &Queue{clock: observationClock{q.clock.Now()}, s: state{
		Preemptions:      append([]Preemption(nil), q.s.Preemptions...),
		ProductionStarts: append([]ProductionStart(nil), q.s.ProductionStarts...),
	}}
	requestUnlock1()
	return v.readUsage(q.usageReader, since, requestCtx)
}

func (q *Queue) readUsage(db *sql.DB, since time.Time, contexts ...context.Context) (UsageReport, error) {
	now := q.clock.Now()
	out := UsageReport{Since: since, Until: now, Projects: []UsageSummary{}, Machines: []UsageSummary{}, Cards: []CardUsage{}, Daily: []DailyUsage{}, Preemptions: []Preemption{}, ProductionStarts: []ProductionStart{}}
	if since.After(now) {
		return out, fmt.Errorf("since must precede current time")
	}
	rows, e := db.QueryContext(requestContext(contexts), `SELECT at,span,machine,device,project,job,model,busy,idle,pinned,memory_seconds,observed FROM usage_samples WHERE at+span>? AND at<?`, since.Unix(), now.Unix()+1)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	projects := map[string]*UsageSummary{}
	machines := map[string]*UsageSummary{}
	days := map[string]*DailyUsage{}
	coverage := map[string]float64{}
	dayCoverage := map[string]float64{}
	// Old queues also recorded synthetic group rows. Single-card batch rows
	// already include attempts on containing groups; only group model rows
	// need expansion. Normalize observation coverage per bucket/physical card.
	type bucketKey struct {
		at              int64
		span            int
		machine, device string
	}
	targets := map[bucketKey]usageSample{}
	cards := map[string]*CardUsage{}
	cardCoverage := map[string]float64{}
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
		key := bucketKey{r.At, r.Span, r.Machine, r.Device}
		target := targets[key]
		target.Observed += r.Observed
		// A group batch row duplicates the physical-card batch rows.
		if !strings.Contains(r.Device, ",") || r.Job == "" {
			target.Busy += r.Busy
			target.Pinned += r.Pinned
		} else {
			r.Busy, r.Pinned = 0, 0
		}
		targets[key] = target
		// GPU-hours attribute each card used by a model group to its project.
		r.Busy *= float64(len(strings.Split(r.Device, ",")))
		r.Pinned *= float64(len(strings.Split(r.Device, ",")))
		date := time.Unix(r.At, 0).UTC().Format("2006-01-02")
		var d *DailyUsage
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
	physical := map[bucketKey]usageSample{}
	for key, target := range targets {
		for _, device := range strings.Split(key.device, ",") {
			cardKey := key
			cardKey.device = device
			card := physical[cardKey]
			card.Busy += target.Busy
			card.Pinned += target.Pinned
			card.Observed = max(card.Observed, target.Observed)
			physical[cardKey] = card
		}
	}
	for key, sample := range physical {
		if sample.Observed == 0 && sample.Busy == 0 && sample.Pinned == 0 {
			continue
		} // CPU rows.
		busy := min(sample.Busy, sample.Observed)
		idle := max(0, sample.Observed-busy)
		cardKey := key.machine + "/" + key.device
		card := cards[cardKey]
		if card == nil {
			card = &CardUsage{Machine: key.machine, Device: key.device, UsageSummary: UsageSummary{Name: key.device}}
			cards[cardKey] = card
		}
		card.GPUHours += busy / 3600
		card.IdleHours += idle / 3600
		card.PinnedHours += sample.Pinned / 3600
		cardCoverage[cardKey] += sample.Observed
		m := machines[key.machine]
		if m == nil {
			m = &UsageSummary{Name: key.machine}
			machines[key.machine] = m
		}
		m.GPUHours += busy / 3600
		m.IdleHours += idle / 3600
		m.PinnedHours += sample.Pinned / 3600
		coverage[key.machine] += sample.Observed
		date := time.Unix(key.at, 0).UTC().Format("2006-01-02")
		mk := date + "/machine/" + key.machine
		d := days[mk]
		if d == nil {
			d = &DailyUsage{Date: date, Machine: key.machine}
			days[mk] = d
		}
		d.GPUHours += busy / 3600
		dayCoverage[mk] += sample.Observed
	}
	for key, card := range cards {
		if cardCoverage[key] > 0 {
			card.BusyPercent = card.GPUHours * 3600 / cardCoverage[key] * 100
		}
		out.Cards = append(out.Cards, *card)
	}
	sort.Slice(out.Cards, func(i, j int) bool {
		a, b := out.Cards[i], out.Cards[j]
		return a.Machine < b.Machine || a.Machine == b.Machine && a.Device < b.Device
	})

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
