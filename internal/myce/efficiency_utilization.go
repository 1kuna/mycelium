package myce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// Per-card occupancy and "would it have fit at measured size" accounting
// (#2183). Each maintenance diagnostic observes every card or CPU pool and every
// waiting job once, on the detached capture it already builds. Each observation
// holds until the next one or until its inventory goes stale, whichever is
// first; uncovered time is unknown, never idle or busy. Totals live in UTC day
// buckets in the state root, so a report reads them without touching history.

const (
	cardBusy             = "busy"
	cardBusyWorkWaiting  = "busy_with_work_waiting"
	cardIdleWorkWaiting  = "idle_with_work_waiting"
	cardIdleNoDemand     = "idle_no_demand"
	cardHeld             = "held"
	cardUnknown          = "unknown"
	utilizationDays      = 14
	utilizationReportAge = 7
)

// Would-fit blockers: the job fit physically once one inflated term is
// charged at its measurement.
const (
	fitHolderGrowth  = "holder_pending_growth" // running holders charged only up to their measured peak
	fitOwnCharge     = "own_charge"            // the job charged at its own measured peak
	fitCardExclusive = "card_exclusive_unmeasured_device"
	fitFence         = "accumulation_fence"
	fitStale         = "unknown_stale_inventory"
)

type UtilizationLedger struct {
	Since time.Time                  `json:"since"`
	At    time.Time                  `json:"at"`
	Cards map[string]CardObservation `json:"cards,omitempty"`
	Jobs  map[string]JobFit          `json:"jobs,omitempty"`
	Days  []UtilizationDay           `json:"days,omitempty"`
}

type CardObservation struct {
	Host       string    `json:"host"`
	Machine    string    `json:"machine"`
	Device     string    `json:"device"`
	Platform   string    `json:"platform"`
	State      string    `json:"state"`
	Reason     string    `json:"reason,omitempty"`
	ValidUntil time.Time `json:"valid_until"`
}

type JobFit struct {
	Blocker    string    `json:"blocker"`
	ValidUntil time.Time `json:"valid_until"`
}

type UtilizationDay struct {
	Date string `json:"date"`
	// Card key -> "state" or "state/reason" -> seconds.
	Cards    map[string]map[string]float64 `json:"card_seconds,omitempty"`
	WouldFit map[string]WouldFitCount      `json:"would_fit,omitempty"`
}

type WouldFitCount struct {
	// Episodes counts a job entering this blocker; a job that leaves and
	// returns counts again.
	Episodes   int     `json:"episodes"`
	JobSeconds float64 `json:"job_seconds"`
}

type utilizationObservation struct {
	at    time.Time
	cards map[string]CardObservation
	jobs  map[string]JobFit
}

// observeUtilization runs on the detached diagnostic capture. categories holds
// each queued job's efficiency category; kinds its scheduler hold kind.
func (q *Queue) observeUtilization(categories, kinds map[string]string) utilizationObservation {
	now := q.clock.Now()
	o := utilizationObservation{at: now, cards: map[string]CardObservation{}, jobs: map[string]JobFit{}}
	var demand []*Job
	for _, j := range q.liveJobs() {
		if j.State == "queued" && !cancelled(j) && dependencyReady(j) && !expired(j.Spec.GiveUpAt, now) &&
			(!j.Spec.childFanOut() || j.FanOut == nil || j.FanOut.Next < len(j.Spec.FanOut.Pieces)) {
			demand = append(demand, j)
		}
	}
	slices.SortFunc(demand, func(a, b *Job) int { return strings.Compare(a.ID, b.ID) })
	for _, t := range q.plannerMachines() {
		m := q.s.Machines[t.ID]
		if m == nil || len(t.group) > 1 {
			continue // A card group is not a card; its members are observed.
		}
		until := m.LastSeen.Add(q.machineLossTimeout(m.ID))
		fresh := !m.Gone && !q.unreported[m.ID] && now.Before(until) && q.workerFresh(m, now)
		device := t.Device
		if device == "" {
			device = t.GPU
		}
		c := CardObservation{Host: hostID(m), Machine: m.ID, Device: device, Platform: t.GPU, ValidUntil: until}
		key := c.Host + "/" + c.Machine + "/" + c.Device
		mode := q.s.HostMaintenance[hostID(m)].Mode
		switch {
		case !fresh:
			c.State = cardUnknown
		case m.Leaving || m.Disabled != "" || mode == "drained" || mode == "disabled":
			c.State = cardHeld
		default:
			busy := q.targetOccupied(t)
			waiting, unknown, firstCategory, fit := false, false, "", ""
			for _, j := range demand {
				compatibility, qualification := q.idleCompatibility(j, t, true)
				unknown = unknown || compatibility == "unknown"
				if compatibility != "compatible" || qualification != "accepted" {
					continue
				}
				waiting = true
				if firstCategory == "" {
					firstCategory = categories[j.ID]
				}
				blocker := q.measuredFitBlocker(j, t, categories[j.ID], kinds[j.ID])
				if blocker != "" && fit == "" {
					fit = blocker
				}
				if blocker != "" {
					// Name the target that needs the fewest counterfactuals.
					if old, ok := o.jobs[j.ID]; !ok || old.Blocker == fitStale || strings.Count(blocker, "+") < strings.Count(old.Blocker, "+") {
						o.jobs[j.ID] = JobFit{Blocker: blocker, ValidUntil: until}
					}
				}
			}
			reason := firstCategory
			if fit != "" {
				reason = "would_fit:" + fit
			}
			switch {
			case waiting && busy:
				c.State, c.Reason = cardBusyWorkWaiting, reason
			case busy:
				c.State = cardBusy
			case waiting:
				c.State, c.Reason = cardIdleWorkWaiting, reason
			case unknown:
				c.State = cardUnknown
			default:
				c.State = cardIdleNoDemand
			}
		}
		if !fresh {
			// Work that could only run here waits on stale inventory: unknown.
			for _, j := range demand {
				if categories[j.ID] != "memory_reservation" && categories[j.ID] != "capacity" && categories[j.ID] != "unknown" {
					continue
				}
				if _, ok := o.jobs[j.ID]; ok || !q.jobAllowsMachine(j, m.ID) {
					continue
				}
				if compatibility, _ := q.idleCompatibility(j, t, false); compatibility == "unknown" {
					o.jobs[j.ID] = JobFit{Blocker: fitStale, ValidUntil: now}
				}
			}
		}
		o.cards[key] = c
	}
	return o
}

// targetOccupied: an attempt or a loaded model holds this card or CPU pool.
func (q *Queue) targetOccupied(t *Machine) bool {
	for _, j := range q.liveJobs() {
		if batchReservesCapacity(j) && (batchOnTarget(j, t) || t.Devices != nil && t.GPU == "cpu" && j.Machine == t.ID && j.Spec.Needs.cpuOnly()) {
			return true
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slotOnTarget(slot, t) && slotReservesMemory(slot) && slot.State != "unloaded" {
			return true
		}
	}
	return false
}

// measuredFitBlocker names what kept a refused job off target t when it fits
// physically there with one inflated term charged at its measurement, or ""
// when it does not fit even then (a real limit) or is not refused on room.
// It reads the ledger the scheduler already built; it never admits anything.
func (q *Queue) measuredFitBlocker(j *Job, t *Machine, category, kind string) string {
	if category != "memory_reservation" && category != "capacity" {
		return ""
	}
	candidates := q.memoryCandidates(j, t)
	if len(candidates) == 0 {
		return ""
	}
	c := candidates[0]
	if t.EffectiveCPUCores > 0 && c.Spec.Needs.cores() > t.FreeCPUCores {
		return "" // Cores are measured exactly; no counterfactual frees them.
	}
	l := q.hostMemoryLedgers[t.ID]
	if l == nil || !l.AvailableKnown {
		return ""
	}
	card := ""
	if !c.Spec.Needs.cpuOnly() && t.GPU != "cpu" && !q.batchSlotFree(t, c) {
		if !q.cardHolderUnmeasured(t) {
			return "" // The card's measured use leaves no room.
		}
		card = fitCardExclusive
	}
	charge := hostNeed(t, c.Spec.Needs.MemoryGB, c.Spec.Needs.HostMemoryGB)
	own := charge
	if peak, ok := ownMeasuredPeak(j); ok {
		own = min(own, peak)
	}
	room, releasable := l.SignedRoomGB, q.releasableGrowth(t, l)
	ram := ""
	switch {
	case charge <= room:
	case charge <= room+releasable:
		ram = fitHolderGrowth
	case own <= room+releasable:
		ram = fitOwnCharge
	default:
		return ""
	}
	switch {
	case ram != "" && card != "":
		return ram + "+" + card
	case ram != "":
		return ram
	case card != "":
		return card
	case kind == "host_backfill":
		// Room exists at current charges; a fence withholds it for other work.
		return fitFence
	}
	return ""
}

// releasableGrowth is the outstanding growth reserve the ledger holds for
// running attempts beyond what each has measurably used at its peak so far.
// Attempts with no fresh sample keep their whole reserve.
func (q *Queue) releasableGrowth(t *Machine, l *HostMemoryLedger) float64 {
	now, total := q.clock.Now(), 0.0
	for _, o := range l.Owners {
		if o.Kind != "attempt" || o.Execution == nil {
			continue
		}
		sample, ok := q.hostMemorySamples[o.Machine][attemptKey{o.Execution.ID, o.Execution.Attempt, o.Session}]
		if !ok || !sample.freshAt(now) {
			continue
		}
		current := pooledFootprint(sample, unifiedCUDA(t))
		total += max(0, o.GrowthGB-max(0, sample.peakGB-current))
	}
	return total
}

// A card holder with no fresh device sample: sharing is refused because the
// device peak is unknown, not because the card is measurably full.
func (q *Queue) cardHolderUnmeasured(t *Machine) bool {
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if !batchReservesCapacity(j) || !batchOnTarget(j, t) {
			continue
		}
		s := q.hostMemorySamples[j.Machine][jobKey(*j)]
		if s.deviceAt.IsZero() || !now.Before(s.deviceAt.Add(deviceMemorySampleFreshFor)) {
			return true
		}
	}
	return false
}

// The job's own measured host peak: its retained accepted attempts, else its
// learned pool peak. Nothing measured means its charge stands.
func ownMeasuredPeak(j *Job) (float64, bool) {
	peak, ok := 0.0, false
	for _, f := range j.MemoryAttempts {
		if f.MemoryPeak != nil && f.LearningReason == "eligible" && f.MemoryPeak.HostGB != nil && validMemoryValue(*f.MemoryPeak.HostGB) {
			peak, ok = max(peak, *f.MemoryPeak.HostGB), true
		}
	}
	if !ok && j.Memory != nil && j.Memory.RAM.PeakGB > 0 {
		peak, ok = j.Memory.RAM.PeakGB, true
	}
	return peak, ok
}

// observe credits the previous observation up to now and returns a new ledger;
// the receiver is a published generation and is never mutated.
func (l *UtilizationLedger) observe(o utilizationObservation) *UtilizationLedger {
	next := &UtilizationLedger{Since: o.at, At: o.at, Cards: o.cards, Jobs: o.jobs}
	previous := map[string]JobFit{}
	if l != nil {
		if !o.at.After(l.At) {
			return l
		}
		next.Since, next.Days, previous = l.Since, cloneUtilizationDays(l.Days), l.Jobs
	} else {
		l = &UtilizationLedger{At: o.at}
	}
	day := func(date string) *UtilizationDay {
		for i := range next.Days {
			if next.Days[i].Date == date {
				return &next.Days[i]
			}
		}
		next.Days = append(next.Days, UtilizationDay{Date: date})
		return &next.Days[len(next.Days)-1]
	}
	for key, c := range l.Cards {
		stop := minTime(o.at, c.ValidUntil)
		state := c.State
		if c.Reason != "" {
			state += "/" + c.Reason
		}
		credit(l.At, stop, func(date string, seconds float64) { day(date).addCard(key, state, seconds) })
		credit(maxTime(l.At, stop), o.at, func(date string, seconds float64) { day(date).addCard(key, cardUnknown, seconds) })
	}
	for _, f := range l.Jobs {
		credit(l.At, minTime(o.at, maxTime(f.ValidUntil, l.At)), func(date string, seconds float64) {
			d := day(date)
			if d.WouldFit == nil {
				d.WouldFit = map[string]WouldFitCount{}
			}
			n := d.WouldFit[f.Blocker]
			n.JobSeconds += seconds
			d.WouldFit[f.Blocker] = n
		})
	}
	for id, f := range o.jobs {
		if old, ok := previous[id]; !ok || old.Blocker != f.Blocker {
			d := day(o.at.UTC().Format(time.DateOnly))
			if d.WouldFit == nil {
				d.WouldFit = map[string]WouldFitCount{}
			}
			n := d.WouldFit[f.Blocker]
			n.Episodes++
			d.WouldFit[f.Blocker] = n
		}
	}
	slices.SortFunc(next.Days, func(a, b UtilizationDay) int { return strings.Compare(a.Date, b.Date) })
	if len(next.Days) > utilizationDays {
		next.Days = next.Days[len(next.Days)-utilizationDays:]
	}
	return next
}

func (d *UtilizationDay) addCard(card, state string, seconds float64) {
	if d.Cards == nil {
		d.Cards = map[string]map[string]float64{}
	}
	if d.Cards[card] == nil {
		d.Cards[card] = map[string]float64{}
	}
	d.Cards[card][state] += seconds
}

// credit splits [from, to) at UTC midnights.
func credit(from, to time.Time, add func(date string, seconds float64)) {
	for from.Before(to) {
		midnight := from.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
		stop := minTime(to, midnight)
		add(from.UTC().Format(time.DateOnly), stop.Sub(from).Seconds())
		from = stop
	}
}

func cloneUtilizationDays(days []UtilizationDay) []UtilizationDay {
	out := make([]UtilizationDay, len(days))
	for i, d := range days {
		out[i] = UtilizationDay{Date: d.Date, WouldFit: maps.Clone(d.WouldFit)}
		if d.Cards != nil {
			out[i].Cards = make(map[string]map[string]float64, len(d.Cards))
			for k, v := range d.Cards {
				out[i].Cards[k] = maps.Clone(v)
			}
		}
	}
	return out
}

// UtilizationReport covers the last seven UTC days, today included, from the
// first observation at the latest. Unknown is the window less observed time.
type UtilizationReport struct {
	Since                  time.Time         `json:"since"`
	Through                time.Time         `json:"through"`
	IdleCardHoursWhileWait float64           `json:"idle_card_hours_while_work_waited"`
	BusyCardHoursWhileWait float64           `json:"busy_card_hours_while_work_waited"`
	UnknownCardHours       float64           `json:"unknown_card_hours"`
	Cards                  []CardUtilization `json:"cards"`
	WouldFit               []WouldFitRow     `json:"would_have_fit_at_measured_size"`
	Coverage               string            `json:"coverage"`
}

type CardUtilization struct {
	Host     string `json:"host"`
	Machine  string `json:"machine"`
	Device   string `json:"device"`
	Platform string `json:"platform"`
	Current  string `json:"current_state"`
	// Hours by state; the waiting states also by reason. A reason
	// "would_fit:X" means a waiting job fit at measured size but for X.
	Hours        map[string]float64 `json:"state_hours"`
	IdleByReason map[string]float64 `json:"idle_while_waiting_hours_by_reason,omitempty"`
	BusyByReason map[string]float64 `json:"busy_while_waiting_hours_by_reason,omitempty"`
}

type WouldFitRow struct {
	Blocker  string  `json:"blocker"`
	Episodes int     `json:"job_episodes"`
	JobHours float64 `json:"job_hours"`
}

func (l *UtilizationLedger) report(now time.Time) *UtilizationReport {
	if l == nil {
		return nil
	}
	first := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, 1-utilizationReportAge)
	r := &UtilizationReport{Since: maxTime(first, l.Since), Through: now,
		Coverage: "per card or CPU pool, from maintenance observations; each holds until the next or until its inventory goes stale. Unknown is window time not observed. Would-fit charges running holders only up to their measured peak, or the job at its own measured peak; it is a counterfactual, not an admission decision."}
	live := l.observe(utilizationObservation{at: now, cards: l.Cards, jobs: l.Jobs})
	window := now.Sub(r.Since).Seconds()
	cards := map[string]*CardUtilization{}
	fits := map[string]*WouldFitRow{}
	for key, c := range l.Cards {
		cards[key] = &CardUtilization{Host: c.Host, Machine: c.Machine, Device: c.Device, Platform: c.Platform, Current: c.State, Hours: map[string]float64{}}
	}
	for _, d := range live.Days {
		if d.Date < first.Format(time.DateOnly) {
			continue
		}
		for key, states := range d.Cards {
			c := cards[key]
			if c == nil {
				continue // The card has left inventory.
			}
			for state, seconds := range states {
				base, reason, _ := strings.Cut(state, "/")
				c.Hours[base] += seconds / 3600
				switch base {
				case cardIdleWorkWaiting:
					if c.IdleByReason == nil {
						c.IdleByReason = map[string]float64{}
					}
					c.IdleByReason[reason] += seconds / 3600
				case cardBusyWorkWaiting:
					if c.BusyByReason == nil {
						c.BusyByReason = map[string]float64{}
					}
					c.BusyByReason[reason] += seconds / 3600
				}
			}
		}
		for blocker, n := range d.WouldFit {
			row := fits[blocker]
			if row == nil {
				row = &WouldFitRow{Blocker: blocker}
				fits[blocker] = row
			}
			row.Episodes += n.Episodes
			row.JobHours += n.JobSeconds / 3600
		}
	}
	for _, c := range cards {
		observed := 0.0
		for state, hours := range c.Hours {
			if state != cardUnknown {
				observed += hours
			}
		}
		c.Hours[cardUnknown] = max(0, window/3600-observed)
		r.IdleCardHoursWhileWait += c.Hours[cardIdleWorkWaiting]
		r.BusyCardHoursWhileWait += c.Hours[cardBusyWorkWaiting]
		r.UnknownCardHours += c.Hours[cardUnknown]
		r.Cards = append(r.Cards, *c)
	}
	for _, row := range fits {
		r.WouldFit = append(r.WouldFit, *row)
	}
	slices.SortFunc(r.Cards, func(a, b CardUtilization) int {
		return strings.Compare(a.Host+"/"+a.Machine+"/"+a.Device, b.Host+"/"+b.Machine+"/"+b.Device)
	})
	slices.SortFunc(r.WouldFit, func(a, b WouldFitRow) int { return strings.Compare(a.Blocker, b.Blocker) })
	return r
}

func (r *UtilizationReport) lines() []string {
	if r == nil {
		return nil
	}
	lines := []string{fmt.Sprintf("  utilization %s to %s: %.1f idle card-hours while compatible work waited; %.1f busy card-hours while more compatible work waited; %.1f unknown card-hours",
		r.Since.UTC().Format(time.RFC3339), r.Through.UTC().Format(time.RFC3339), r.IdleCardHoursWhileWait, r.BusyCardHoursWhileWait, r.UnknownCardHours)}
	for _, c := range r.Cards {
		lines = append(lines, fmt.Sprintf("    card %s/%s (%s, now %s): busy %.1f h; idle while work waited %.1f h%s; busy while more work waited %.1f h%s; idle with no demand %.1f h; held %.1f h; unknown %.1f h",
			c.Host, c.Device, c.Platform, c.Current, c.Hours[cardBusy], c.Hours[cardIdleWorkWaiting], reasonWords(c.IdleByReason), c.Hours[cardBusyWorkWaiting], reasonWords(c.BusyByReason), c.Hours[cardIdleNoDemand], c.Hours[cardHeld], c.Hours[cardUnknown]))
	}
	if len(r.WouldFit) == 0 {
		lines = append(lines, "    would have fit at measured size: none observed")
	}
	for _, f := range r.WouldFit {
		lines = append(lines, fmt.Sprintf("    would have fit at measured size, blocked by %s: %d job episodes, %.1f job-hours", f.Blocker, f.Episodes, f.JobHours))
	}
	return lines
}

func reasonWords(byReason map[string]float64) string {
	if len(byReason) == 0 {
		return ""
	}
	parts := []string{}
	for _, reason := range slices.Sorted(maps.Keys(byReason)) {
		name := reason
		if name == "" {
			name = "unknown"
		}
		parts = append(parts, fmt.Sprintf("%s %.1f", name, byReason[reason]))
	}
	return " (" + strings.Join(parts, ", ") + ")"
}
