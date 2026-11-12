package myce

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Accounting follows the owning job's retention, not a second time-series
// store. Rates are queue-clock observations; missing coverage is never zero use.
// Host rows live only as long as their inventory. No payload or command is kept.
type ResourceIntegral struct {
	ReservedCoveredMemoryGBSeconds float64  `json:"reserved_covered_memory_gb_seconds"`
	ReservedDeviceGBSeconds        float64  `json:"reserved_device_gb_seconds"`
	MeasuredDeviceGBSeconds        float64  `json:"measured_device_gb_seconds"`
	DeviceCoveredSeconds           float64  `json:"device_covered_seconds"`
	ReservedMemoryGBSeconds        float64  `json:"reserved_memory_gb_seconds"`
	MeasuredMemoryGBSeconds        float64  `json:"measured_memory_gb_seconds"`
	MemoryCoveredSeconds           float64  `json:"memory_covered_seconds"`
	ReservedCPUSeconds             float64  `json:"reserved_cpu_seconds"`
	MeasuredCPUSeconds             *float64 `json:"measured_cpu_seconds" jsonschema:"nullable"`
	CPUObservedElapsedSeconds      float64  `json:"cpu_observed_elapsed_seconds"`
	CPUObservedAttempts            int      `json:"cpu_observed_attempts"`
	CPULowerBound                  bool     `json:"cpu_lower_bound"`
	ReadBytes                      *uint64  `json:"read_bytes" jsonschema:"nullable"`
	WriteBytes                     *uint64  `json:"write_bytes" jsonschema:"nullable"`
	ReadOps                        *uint64  `json:"read_ops" jsonschema:"nullable"`
	WriteOps                       *uint64  `json:"write_ops" jsonschema:"nullable"`
	IOObservedAttempts             int      `json:"io_observed_attempts"`
	IOLowerBound                   bool     `json:"io_lower_bound"`
	Seconds                        float64  `json:"seconds"`
}

func (a *ResourceIntegral) add(b ResourceIntegral) {
	a.ReservedCoveredMemoryGBSeconds += b.ReservedCoveredMemoryGBSeconds
	a.ReservedDeviceGBSeconds += b.ReservedDeviceGBSeconds
	a.MeasuredDeviceGBSeconds += b.MeasuredDeviceGBSeconds
	a.DeviceCoveredSeconds += b.DeviceCoveredSeconds
	a.ReservedMemoryGBSeconds += b.ReservedMemoryGBSeconds
	a.MeasuredMemoryGBSeconds += b.MeasuredMemoryGBSeconds
	a.MemoryCoveredSeconds += b.MemoryCoveredSeconds
	a.ReservedCPUSeconds += b.ReservedCPUSeconds
	a.Seconds += b.Seconds
	a.CPUObservedElapsedSeconds += b.CPUObservedElapsedSeconds
	a.CPUObservedAttempts += b.CPUObservedAttempts
	a.CPULowerBound = a.CPULowerBound || b.CPULowerBound
	a.IOObservedAttempts += b.IOObservedAttempts
	a.IOLowerBound = a.IOLowerBound || b.IOLowerBound
	for _, pair := range [][2]**uint64{{&a.ReadBytes, &b.ReadBytes}, {&a.WriteBytes, &b.WriteBytes}, {&a.ReadOps, &b.ReadOps}, {&a.WriteOps, &b.WriteOps}} {
		if *pair[1] == nil {
			continue
		}
		if *pair[0] == nil {
			*pair[0] = new(uint64)
		}
		if **pair[1] > ^uint64(0)-**pair[0] {
			**pair[0] = ^uint64(0)
			a.IOLowerBound = true
		} else {
			**pair[0] += **pair[1]
		}
	}
	if b.MeasuredCPUSeconds != nil {
		if a.MeasuredCPUSeconds == nil {
			a.MeasuredCPUSeconds = new(float64)
		}
		*a.MeasuredCPUSeconds += *b.MeasuredCPUSeconds
	}
}

type EfficiencyAllocation struct {
	Host  string `json:"host"`
	Work  string `json:"work"`
	Basis string `json:"basis"`
	ResourceIntegral
}
type efficiencyRate struct {
	Release          string    `json:"release,omitempty"`
	ReservedDeviceGB float64   `json:"reserved_device_gb"`
	MeasuredDeviceGB *float64  `json:"measured_device_gb" jsonschema:"nullable"`
	DeviceValidUntil time.Time `json:"device_valid_until"`
	Host             string    `json:"host,omitempty"`
	Basis            string    `json:"basis,omitempty"`
	ReservedMemoryGB float64   `json:"reserved_memory_gb"`
	ReservedCPUCores float64   `json:"reserved_cpu_cores"`
	MeasuredMemoryGB *float64  `json:"measured_memory_gb" jsonschema:"nullable"`
	ValidUntil       time.Time `json:"valid_until"`
}
type JobEfficiency struct {
	RetainedRates []efficiencyRate       `json:"retained_rates,omitempty"`
	Since         time.Time              `json:"since"`
	At            time.Time              `json:"at"`
	Work          string                 `json:"work"`
	Blocker       string                 `json:"blocker,omitempty"`
	WaitSeconds   map[string]float64     `json:"wait_seconds"`
	Allocations   []EfficiencyAllocation `json:"allocations,omitempty"`
	Rate          efficiencyRate         `json:"rate"`
}
type EfficiencyHost struct {
	Identity string    `json:"identity"`
	Host     string    `json:"host"`
	Since    time.Time `json:"since"`
	At       time.Time `json:"at"`
	ResourceIntegral
	Rate                 efficiencyRate `json:"current"`
	TotalMemoryGB        *float64       `json:"total_memory_gb" jsonschema:"nullable"`
	PhysicalUsedMemoryGB *float64       `json:"physical_used_memory_gb" jsonschema:"nullable"`
	IOStatus             string         `json:"io_status"`
	IOPressure           *IOPressure    `json:"io_pressure" jsonschema:"nullable"`
}
type EfficiencyWait struct {
	Active  bool               `json:"active"`
	Blocker string             `json:"current_blocker,omitempty"`
	JobID   string             `json:"job_id,omitempty"`
	Project string             `json:"project"`
	Seconds map[string]float64 `json:"seconds"`
}
type StrandedDemand struct {
	JobID    string   `json:"job_id"`
	Project  string   `json:"project"`
	Host     string   `json:"host"`
	Fields   []string `json:"excluding_fields"`
	MemoryGB float64  `json:"memory_gb"`
	CPUCores int      `json:"cpu_cores"`
}
type PredictionError struct {
	JobID                string   `json:"job_id"`
	Attempt              int      `json:"attempt"`
	Work                 string   `json:"work"`
	Basis                string   `json:"basis"`
	ReservedHostGB       *float64 `json:"reserved_host_gb" jsonschema:"nullable"`
	AcceptedHostPeakGB   *float64 `json:"accepted_host_peak_gb" jsonschema:"nullable"`
	HostErrorGB          *float64 `json:"host_error_gb" jsonschema:"nullable"`
	ReservedDeviceGB     *float64 `json:"reserved_device_gb" jsonschema:"nullable"`
	AcceptedDevicePeakGB *float64 `json:"accepted_device_peak_gb" jsonschema:"nullable"`
	DeviceErrorGB        *float64 `json:"device_error_gb" jsonschema:"nullable"`
	PredictedSeconds     *float64 `json:"predicted_seconds" jsonschema:"nullable"`
	ActualSeconds        *float64 `json:"actual_seconds" jsonschema:"nullable"`
	DurationErrorSeconds *float64 `json:"duration_error_seconds" jsonschema:"nullable"`
}

// A queue-clock receipt snapshot at the first outcome, kept across later
// cleanup replays. Nil means there was no fresh accepted host report.
type AttemptStopObservation struct {
	IOPressure               *IOPressure `json:"io_pressure" jsonschema:"nullable"`
	At                       time.Time   `json:"at"`
	HostSampleAgeSeconds     float64     `json:"host_sample_age_seconds"`
	HeadroomGB               *float64    `json:"headroom_gb" jsonschema:"nullable"`
	PhysicalUsedGB           *float64    `json:"physical_used_gb" jsonschema:"nullable"`
	MemoryPressure           *float64    `json:"memory_pressure" jsonschema:"nullable"`
	PressureUnit             string      `json:"pressure_unit"`
	PressureAgeSeconds       *float64    `json:"pressure_age_seconds" jsonschema:"nullable"`
	SwapGrowthBytesPerSecond *float64    `json:"swap_growth_bytes_per_second" jsonschema:"nullable"`
}

func (q *Queue) attemptStopObservation(machine, session string) *AttemptStopObservation {
	m := q.s.Machines[machine]
	now := q.clock.Now()
	if m == nil || m.Session != session || m.Gone || now.Before(m.LastSeen) || !now.Before(m.LastSeen.Add(q.machineLossTimeout(m.ID))) {
		return nil
	}
	o := &AttemptStopObservation{At: now, HostSampleAgeSeconds: now.Sub(m.LastSeen).Seconds(), PressureUnit: "unknown"}
	if p := safeIOPressure(m.IOPressure); p != nil {
		p.ReceivedAt = m.IOPressure.ReceivedAt
		p.AgeSeconds += max(0, now.Sub(p.ReceivedAt).Seconds())
		if p.Unavailable == "" && (p.ReceivedAt.IsZero() || now.Before(p.ReceivedAt) || p.AgeSeconds >= q.machineLossTimeout(m.ID).Seconds()) {
			p.Unavailable = "stale"
		}
		o.IOPressure = p
	}
	if m.MemoryObservation != nil {
		headroom := m.MemoryObservation.TotalGB - m.MemoryObservation.UsedGB
		used := m.MemoryObservation.UsedGB
		o.HeadroomGB, o.PhysicalUsedGB = &headroom, &used
	}
	if h := m.HostHealth; h != nil && len(h.History) > 0 {
		sample := h.History[len(h.History)-1]
		age := o.HostSampleAgeSeconds + h.AgeSeconds
		if sample.Error == "" && age >= 0 && age < q.machineLossTimeout(m.ID).Seconds() {
			pressure, swap := sample.Pressure, sample.SwapGrowth
			o.MemoryPressure, o.SwapGrowthBytesPerSecond, o.PressureAgeSeconds = &pressure, &swap, &age
			if m.CapabilityProfile != nil {
				switch m.CapabilityProfile.OS {
				case "linux":
					o.PressureUnit = "memory_psi_some_avg10_percent"
				case "darwin":
					o.PressureUnit = "memory_pressure_level"
				}
			}
		}
	}
	return o
}

type WastedAttempt struct {
	Resources       *AttemptResources       `json:"resources" jsonschema:"nullable"`
	StopObservation *AttemptStopObservation `json:"stop_observation" jsonschema:"nullable"`
	JobID           string                  `json:"job_id"`
	Attempt         int                     `json:"attempt"`
	Host            string                  `json:"host"`
	Kind            string                  `json:"kind"`
	EndedAt         time.Time               `json:"ended_at"`
	Suspect         *bool                   `json:"suspect" jsonschema:"nullable"`
	Pressure        *MemoryPressureEvidence `json:"pressure" jsonschema:"nullable"`
	HeadroomGB      *float64                `json:"headroom_gb" jsonschema:"nullable"`
	IOStatus        string                  `json:"io_status"`
}
type EfficiencySignal struct {
	Observed        bool     `json:"observed"`
	Subject         string   `json:"subject"`
	Metric          string   `json:"metric"`
	Value           float64  `json:"value"`
	BaselineSamples int      `json:"baseline_samples"`
	UpperBound      *float64 `json:"upper_bound" jsonschema:"nullable"`
	Active          bool     `json:"active"`
}
type EfficiencyReport struct {
	At                      time.Time              `json:"at"`
	Hosts                   []EfficiencyHost       `json:"hosts"`
	Programs                []EfficiencyAllocation `json:"programs"`
	Jobs                    []EfficiencyWait       `json:"jobs"`
	Projects                []EfficiencyWait       `json:"projects"`
	Stranded                []StrandedDemand       `json:"stranded"`
	Predictions             []PredictionError      `json:"predictions"`
	Wasted                  []WastedAttempt        `json:"wasted_attempts"`
	Signals                 []EfficiencySignal     `json:"signals"`
	CPUStatus               string                 `json:"cpu_status"`
	ResourceAttemptsDropped int                    `json:"resource_attempts_dropped"`
	Coverage                string                 `json:"coverage"`
}

func (e *JobEfficiency) advance(now time.Time) {
	if !now.After(e.At) {
		return
	}
	seconds := now.Sub(e.At).Seconds()
	if e.Blocker != "" {
		if e.WaitSeconds == nil {
			e.WaitSeconds = map[string]float64{}
		}
		e.WaitSeconds[e.Blocker] += seconds
	}
	e.addRate(e.Rate, now, seconds)
	for _, rate := range e.RetainedRates {
		e.addRate(rate, now, seconds)
	}
	e.At = now
}
func (e *JobEfficiency) addRate(rate efficiencyRate, now time.Time, seconds float64) {
	if rate.Host != "" {
		row := EfficiencyAllocation{Host: rate.Host, Work: e.Work, Basis: rate.Basis}
		row.Seconds, row.ReservedMemoryGBSeconds, row.ReservedCPUSeconds = seconds, seconds*rate.ReservedMemoryGB, seconds*rate.ReservedCPUCores
		covered := max(0, min(now.Sub(e.At), rate.ValidUntil.Sub(e.At)).Seconds())
		if rate.MeasuredMemoryGB != nil {
			row.MemoryCoveredSeconds, row.MeasuredMemoryGBSeconds = covered, covered**rate.MeasuredMemoryGB
			row.ReservedCoveredMemoryGBSeconds = covered * rate.ReservedMemoryGB
		}
		row.ReservedDeviceGBSeconds = seconds * rate.ReservedDeviceGB
		deviceCovered := max(0, min(now.Sub(e.At), rate.DeviceValidUntil.Sub(e.At)).Seconds())
		if rate.MeasuredDeviceGB != nil {
			row.DeviceCoveredSeconds = deviceCovered
			row.MeasuredDeviceGBSeconds = deviceCovered * *rate.MeasuredDeviceGB
		}
		i := slices.IndexFunc(e.Allocations, func(a EfficiencyAllocation) bool { return a.Host == row.Host && a.Basis == row.Basis })
		if i < 0 {
			e.Allocations = append(e.Allocations, row)
		} else {
			e.Allocations[i].ResourceIntegral.add(row.ResourceIntegral)
		}
	}
}

func (h *EfficiencyHost) advance(now time.Time) {
	if !now.After(h.At) {
		return
	}
	seconds := now.Sub(h.At).Seconds()
	h.Seconds += seconds
	h.ReservedMemoryGBSeconds += seconds * h.Rate.ReservedMemoryGB
	h.ReservedCPUSeconds += seconds * h.Rate.ReservedCPUCores
	covered := max(0, min(now.Sub(h.At), h.Rate.ValidUntil.Sub(h.At)).Seconds())
	if h.Rate.MeasuredMemoryGB != nil {
		h.ReservedCoveredMemoryGBSeconds += covered * h.Rate.ReservedMemoryGB
		h.MemoryCoveredSeconds += covered
		h.MeasuredMemoryGBSeconds += covered * *h.Rate.MeasuredMemoryGB
	}
	h.ReservedDeviceGBSeconds += seconds * h.Rate.ReservedDeviceGB
	deviceCovered := max(0, min(now.Sub(h.At), h.Rate.DeviceValidUntil.Sub(h.At)).Seconds())
	if h.Rate.MeasuredDeviceGB != nil {
		h.DeviceCoveredSeconds += deviceCovered
		h.MeasuredDeviceGBSeconds += deviceCovered * *h.Rate.MeasuredDeviceGB
	}
	h.At = now
}

// projectEfficiency owns its counters; advancing a read cannot mutate the
// accounting anchor or a previously published read generation.
func (q *Queue) projectEfficiency(j *Job) {
	if j.Efficiency == nil || !mutableJob(j) {
		return
	}
	copy := cloneJob(&Job{Efficiency: j.Efficiency})
	copy.Efficiency.advance(q.clock.Now())
	j.Efficiency = copy.Efficiency
}

// Account only at a rate or blocker transition. Reads advance detached copies,
// so the durable anchor reconstructs elapsed time after a restart without a
// write for every waiting job on every heartbeat.
func (e *JobEfficiency) transition(old JobEfficiency, now time.Time) {
	if old.Blocker == e.Blocker && reflect.DeepEqual(old.Rate, e.Rate) && reflect.DeepEqual(old.RetainedRates, e.RetainedRates) {
		return
	}
	rate, retained, blocker := e.Rate, e.RetainedRates, e.Blocker
	e.Rate, e.RetainedRates, e.Blocker = old.Rate, old.RetainedRates, old.Blocker
	e.advance(now)
	e.Rate, e.RetainedRates, e.Blocker = rate, retained, blocker
}
func efficiencyBasis(j *Job) string {
	if j.MemoryEnvelope != nil && j.MemoryEnvelope.Basis != "" {
		return j.MemoryEnvelope.Basis
	}
	if j.Spec.Needs.Memory != "auto" && (j.Spec.Needs.HostMemoryGB > 0 || j.Spec.Needs.MemoryGB > 0) {
		return "declared"
	}
	return "unknown"
}
func (q *Queue) refreshEfficiency() {
	defer q.bodyPhaseFinish("efficiency", q.bodyPhaseStart())
	now := q.clock.Now()
	releases := map[string][]HostRelease{}
	for _, r := range q.s.HostReleases {
		if !r.Preparation {
			releases[r.Job] = append(releases[r.Job], r)
		}
	}
	for _, j := range q.liveJobs() {
		q.refreshJobEfficiency(j, releases[j.ID], now)
	}
	if q.s.EfficiencyHosts == nil {
		q.s.EfficiencyHosts = map[string]EfficiencyHost{}
	}
	seen := map[string]bool{}
	owners := map[string]*Machine{}
	hostMachines := map[string][]*Machine{}
	reservedDevices := map[string]float64{}
	for _, m := range q.s.Machines {
		if m.Replacement != nil {
			continue
		}
		host := hostID(m)
		hostMachines[host] = append(hostMachines[host], m)
		old := owners[host]
		if old == nil || m.LastSeen.After(old.LastSeen) || m.LastSeen.Equal(old.LastSeen) && m.ID < old.ID {
			owners[host] = m
		}
	}
	// Aggregate each live ledger once; heartbeats must not scan all jobs and
	// inventory again for every host.
	for _, j := range q.liveJobs() {
		if owner := q.s.Machines[j.Machine]; owner != nil && batchReservesCapacity(j) {
			reservedDevices[hostID(owner)] += j.Spec.Needs.MemoryGB
		}
	}
	for _, slot := range q.s.ModelSlots {
		if owner := q.s.Machines[slot.Machine]; owner != nil && slotReservesMemory(slot) {
			reservedDevices[hostID(owner)] += q.slotMemory(slot)
		}
	}
	for _, r := range q.s.HostReleases {
		if r.CPUOnly || r.Preparation {
			continue
		}
		j := q.s.Jobs[r.Job]
		if j != nil && j.Attempt == r.Attempt && j.Session == r.Session && batchReservesCapacity(j) {
			continue
		}
		reservedDevices[r.Host] += r.MemoryGB * float64(max(1, len(r.Devices)))
	}
	for host, m := range owners {
		seen[host] = true
		identity := efficiencyHostIdentity(hostMachines[host])
		h, ok := q.s.EfficiencyHosts[host]
		if ok && q.hostUnreported(hostMachines[host]) {
			// After a restart the effective cores and budget are rebuilt only by
			// the next check-in. Absent telemetry is not a hardware change.
			identity = h.Identity
		}
		old := h
		if !ok || h.Identity != identity {
			h = EfficiencyHost{Host: host, Since: now, At: now, Identity: identity}
		}
		h.Rate = efficiencyRate{Host: host, ReservedMemoryGB: m.HostMemoryInUseGB, ReservedCPUCores: float64(m.CPUInUseCores)}
		h.TotalMemoryGB, h.PhysicalUsedMemoryGB = nil, nil
		h.IOStatus = "unknown"
		if o := m.MemoryObservation; o != nil && !m.Gone && now.Before(m.LastSeen.Add(q.machineLossTimeout(m.ID))) {
			total, used := o.TotalGB, o.UsedGB
			h.TotalMemoryGB, h.PhysicalUsedMemoryGB = &total, &used
			if !o.AttributionUnknown {
				gb := o.MyceliumGB
				h.Rate.MeasuredMemoryGB = &gb
				h.Rate.ValidUntil = m.LastSeen.Add(q.machineLossTimeout(m.ID))
			}
		}
		// Device memory is separate on discrete hosts; on unified systems this
		// remains a diagnostic dimension and is never added to host physical use.
		var deviceUsed float64
		deviceKnown := true
		deviceCount := 0
		deviceUntil := time.Time{}
		for _, owner := range hostMachines[host] {
			if until := owner.LastSeen.Add(q.machineLossTimeout(owner.ID)); deviceUntil.IsZero() || until.Before(deviceUntil) {
				deviceUntil = until
			}
			if owner.Devices != nil {
				for _, d := range owner.Devices {
					deviceCount++
					deviceKnown = deviceKnown && d.MemoryMeasured && d.Reason == "" && !owner.Gone && now.Before(owner.LastSeen.Add(q.machineLossTimeout(owner.ID)))
					deviceUsed += max(0, d.TotalMemoryGB-d.FreeMemoryGB)
				}
			} else if owner.GPU != "cpu" {
				deviceCount++
				deviceKnown = deviceKnown && owner.MemoryError == "" && owner.TotalMemoryGB > 0 && !owner.Gone && now.Before(owner.LastSeen.Add(q.machineLossTimeout(owner.ID)))
				deviceUsed += owner.MemoryUsedGB
			}
		}
		h.Rate.ReservedDeviceGB = reservedDevices[host]
		if deviceCount > 0 && deviceKnown {
			h.Rate.MeasuredDeviceGB = &deviceUsed
			h.Rate.DeviceValidUntil = deviceUntil
		}
		if old.Identity == h.Identity && !reflect.DeepEqual(old.Rate, h.Rate) {
			old.advance(now)
			h.ResourceIntegral, h.At = old.ResourceIntegral, old.At
		}
		q.s.EfficiencyHosts[host] = h
	}
	for host := range q.s.EfficiencyHosts {
		if !seen[host] {
			delete(q.s.EfficiencyHosts, host)
		}
	}
}

func (q *Queue) hostUnreported(machines []*Machine) bool {
	for _, m := range machines {
		if !q.unreported[m.ID] {
			return false
		}
	}
	return len(machines) > 0
}

func (q *Queue) refreshJobEfficiency(j *Job, releases []HostRelease, now time.Time) {
	previous := j.Efficiency
	next := JobEfficiency{Since: now, At: now, WaitSeconds: map[string]float64{}}
	if previous != nil {
		next = *previous
	} else {
		next.Work = profileMemoryIdentity(j)
	}
	e := &next
	defer func() {
		if previous != nil && previous.Blocker == e.Blocker && reflect.DeepEqual(previous.Rate, e.Rate) && reflect.DeepEqual(previous.RetainedRates, e.RetainedRates) {
			return
		}
		q.touchJob(j.ID)
		copy := cloneJob(&Job{Efficiency: e})
		if previous != nil {
			copy.Efficiency.transition(*previous, now)
		}
		j.Efficiency = copy.Efficiency
	}()
	previousBasis := e.Rate.Basis
	e.Rate = efficiencyRate{}
	if j.State == "queued" && !cancelled(j) {
		if e.Blocker == "" {
			e.Blocker = "unknown"
		}
	} else if j.State == "paused" {
		e.Blocker = "holds"
	} else {
		e.Blocker = ""
	}
	oldRetained := e.RetainedRates
	e.RetainedRates = nil
	for _, release := range releases {
		if batchReservesCapacity(j) && release.Machine == j.Machine && release.Session == j.Session && release.Attempt == j.Attempt {
			continue // The current execution already carries this charge.
		}
		basis := previousBasis
		for _, old := range oldRetained {
			if old.Release == releaseKey(release) {
				basis = old.Basis
				break
			}
		}
		if basis == "" {
			basis = efficiencyBasis(j)
		}
		rate := efficiencyRate{Release: releaseKey(release), Host: release.Host, Basis: basis, ReservedMemoryGB: release.HostChargeGB, ReservedCPUCores: float64(max(1, release.CPUCores)), ReservedDeviceGB: release.MemoryGB * float64(max(1, len(release.Devices)))}
		q.efficiencySample(&rate, release.Machine, attemptKey{release.Job, release.Attempt, release.Session}, now)
		e.RetainedRates = append(e.RetainedRates, rate)
	}
	slices.SortFunc(e.RetainedRates, func(a, b efficiencyRate) int { return strings.Compare(a.Release, b.Release) })
	if !batchReservesCapacity(j) {
		return
	}
	m := q.s.Machines[j.Machine]
	if m == nil {
		return
	}
	e.Rate = efficiencyRate{Host: hostID(m), Basis: efficiencyBasis(j), ReservedMemoryGB: q.hostJobCharge(j), ReservedCPUCores: float64(j.Spec.Needs.cores()), ReservedDeviceGB: j.Spec.Needs.MemoryGB}
	q.efficiencySample(&e.Rate, j.Machine, jobKey(*j), now)
}

func (q *Queue) efficiencySample(rate *efficiencyRate, machine string, key attemptKey, now time.Time) {
	sample := q.hostMemorySamples[machine][key]
	if !sample.deviceAt.IsZero() && now.Before(sample.deviceAt.Add(deviceMemorySampleFreshFor)) {
		gb := sample.deviceGB
		rate.MeasuredDeviceGB, rate.DeviceValidUntil = &gb, sample.deviceAt.Add(deviceMemorySampleFreshFor)
	}
	if !sample.at.IsZero() && now.Before(sample.at.Add(sample.freshFor())) {
		gb := sample.gb
		rate.MeasuredMemoryGB, rate.ValidUntil = &gb, sample.at.Add(sample.freshFor())
	}
}

func efficiencyHostIdentity(machines []*Machine) string {
	// Reporter recency is not a hardware change. CPU/GPU siblings contribute
	// their stable inventory identities in a deterministic order.
	type member struct {
		ID, Hardware, CPU string
		Cores             int
		RAM               float64
	}
	inventory := make([]member, 0, len(machines))
	for _, m := range machines {
		inventory = append(inventory, member{m.ID, m.ExtensionFingerprint, m.CPUExtensionFingerprint, m.EffectiveCPUCores, m.EffectiveHostMemoryBudgetGB})
	}
	slices.SortFunc(inventory, func(a, b member) int { return strings.Compare(a.ID, b.ID) })
	return identityHash(inventory)
}

// Stranded means the actual current fit succeeds after removing ONLY these
// authored placement/policy exclusions. Capabilities, qualification, recipes,
// parity, live resources, custody and preparation all remain required. Targets
// are author intent; this report neither changes them nor asserts output parity.
func (q *Queue) strandedDemand(j *Job) []StrandedDemand {
	var rows []StrandedDemand
	// Called on a detached maintenance observation. Replace only this map;
	// restore it before returning, and never mutate its shared policy values.
	policies := q.s.Policies
	q.s.Policies = maps.Clone(policies)
	if policy, ok := policies[j.Spec.Project]; ok {
		policy.AllowedMachines = nil
		q.s.Policies[j.Spec.Project] = policy
	}
	defer func() { q.s.Policies = policies }()
	scan := q.placementScan()
	fences := q.hostBackfillFences(scan)
	seenHosts := map[string]bool{}
	for _, m := range q.plannerMachines() {
		if seenHosts[hostID(m)] || !q.inventoryFresh(q.s.Machines[m.ID]) {
			continue
		}
		if fence := q.groupDemandFence(m); fence != nil && j.ID != fence.ID && q.comparePriority(j.Spec, fence.Spec) >= 0 {
			continue
		}
		fields := []string{}
		if !machineAllowed(j.Spec.AllowedMachines, m.ID) {
			fields = append(fields, "allowed_machines")
		}
		if contains(j.Spec.ExcludedMachines, m.ID) {
			fields = append(fields, "excluded_machines")
		}
		if !machineAllowed(policies[j.Spec.Project].AllowedMachines, m.ID) {
			fields = append(fields, "policy.projects.allowed_machines")
		}
		targetProbe := *j
		targetProbe.Spec.Targets = nil
		if !executionTargetMatches(j.Spec, m) && executionTargetMatches(targetProbe.Spec, m) {
			fields = append(fields, "targets")
		}
		if len(fields) == 0 {
			continue
		}
		probe := *j
		probe.Spec.AllowedMachines, probe.Spec.ExcludedMachines, probe.Spec.Targets = nil, nil, nil
		candidate := q.batchCandidateWithResources(&probe, m, nil)
		if candidate == nil || q.immediateCandidate(candidate, m) == nil || !hostFits(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB) || !q.batchSlotFreeForEfficiency(candidate, m) || !q.hostBackfillAllows(candidate, m, fences[hostID(m)]) {
			continue
		}
		seenHosts[hostID(m)] = true
		rows = append(rows, StrandedDemand{JobID: j.ID, Project: j.Spec.Project, Host: hostID(m), Fields: fields, MemoryGB: hostNeed(m, candidate.Spec.Needs.MemoryGB, candidate.Spec.Needs.HostMemoryGB), CPUCores: candidate.Spec.Needs.cores()})
	}
	slices.SortFunc(rows, func(a, b StrandedDemand) int { return strings.Compare(a.JobID+a.Host, b.JobID+b.Host) })
	return rows
}
func (q *Queue) batchSlotFreeForEfficiency(j *Job, m *Machine) bool {
	return j.Spec.Needs.cpuOnly() || q.batchSlotFree(m, j)
}

// The existing detached read capture uses immutable cold summaries and copies
// live planning inputs. Diagnostics run once on maintenance, never per GET.
// Exact-job fences protect categories; monotonic captures protect publication.
// Ordinary heartbeats must not starve diagnostics by invalidating the whole fleet.
type efficiencyBlockerObservation struct {
	Category string
	Attempt  int
	Session  string
	Queued   time.Time
}
type efficiencyDiagnostics struct {
	blockers map[string]efficiencyBlockerObservation

	report    *EfficiencyReport
	stranded  []StrandedDemand
	baselines map[string]efficiencyBaseline
	signals   []EfficiencySignal
	applied   bool
}

func (q *Queue) prepareEfficiencyDiagnostics(ctx context.Context) (*efficiencyDiagnostics, error) {
	if q.simulation {
		return nil, nil
	}
	v, _, err := q.captureObservation("", ctx)
	if err != nil {
		return nil, err
	}
	d := &efficiencyDiagnostics{blockers: map[string]efficiencyBlockerObservation{}}
	v.memoryPlanning = newMemoryPlanningCache()
	for _, j := range v.liveJobs() {
		if j.State == "queued" && !cancelled(j) {
			d.blockers[j.ID] = efficiencyBlockerObservation{Category: v.waitHold(j).efficiencyCategory(), Attempt: j.Attempt, Session: j.Session, Queued: j.Queued}
			if j.Efficiency != nil {
				j.Efficiency.advance(v.clock.Now())
				j.Efficiency.Blocker = d.blockers[j.ID].Category
			}
			d.stranded = append(d.stranded, v.strandedDemand(j)...)
		}
	}
	v.s.StrandedDemand = d.stranded
	d.report = v.efficiencyReport()
	d.baselines, d.signals = efficiencySignals(d.report, v.s.EfficiencyHealth)
	slices.SortFunc(d.signals, func(a, b EfficiencySignal) int { return strings.Compare(a.Subject+a.Metric, b.Subject+b.Metric) })
	d.report.Signals = d.signals
	return d, nil
}
func (q *Queue) applyEfficiencyDiagnostics(d *efficiencyDiagnostics) {
	if d == nil || d.report.At.Before(q.s.EfficiencyAt) {
		return
	}
	for id, blocker := range d.blockers {
		if j := q.s.Jobs[id]; j != nil && j.State == "queued" && j.Attempt == blocker.Attempt && j.Session == blocker.Session && j.Queued.Equal(blocker.Queued) && j.Efficiency != nil {
			if j.Efficiency.Blocker != blocker.Category {
				q.touchJob(j.ID)
				j.Efficiency.advance(q.clock.Now())
				j.Efficiency.Blocker = blocker.Category
			}
		}
	}
	q.s.EfficiencyAt = d.report.At
	q.s.StrandedDemand = d.stranded
	q.s.EfficiencyHealth = d.baselines
	d.applied = true
	seen := map[string]bool{}
	for _, s := range d.signals {
		key := "efficiency/" + s.Subject + "/" + s.Metric
		seen[key] = true
		if !s.Observed {
			continue
		}
		event := HealthAlert{Kind: "efficiency_regression", Reason: s.Metric}
		if _, ok := q.s.EfficiencyHosts[s.Subject]; ok {
			event.Host = s.Subject
		}
		if project, ok := strings.CutPrefix(s.Subject, "project:"); ok {
			event.Project = project
		}
		q.observeCurrentHealth(key, s.Active, event)
	}
	for key := range q.s.Health.Conditions {
		if strings.HasPrefix(key, "efficiency/") && !seen[key] {
			delete(q.s.Health.Conditions, key)
		}
	}
}
func (q *Queue) publishEfficiencyDiagnostics(d *efficiencyDiagnostics) {
	if d == nil || !d.applied {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.efficiencyView == nil || !d.report.At.Before(q.efficiencyView.At) {
		q.efficiencyView = d.report
		// The commit captured the prior report; expose this accepted immutable
		// generation to status and inspection without waiting for another commit.
		q.republishReads()
	}
}

// Test/explicit maintenance entry point; ordinary Tick shares its existing commit.
func (q *Queue) updateEfficiencyDiagnostics(ctx context.Context) error {
	d, err := q.prepareEfficiencyDiagnostics(ctx)
	if err != nil {
		return err
	}
	err = q.updateRequest(ctx, func() error { q.applyEfficiencyDiagnostics(d); return nil }, nil)
	if err == nil {
		q.publishEfficiencyDiagnostics(d)
	}
	return err
}

func (q *Queue) efficiencyReport() *EfficiencyReport {
	now := q.clock.Now()
	r := &EfficiencyReport{At: now, CPUStatus: "unknown", Coverage: "queue-clock reservation/memory integrals since first observation; measured memory covers fresh samples only; CPU/I/O counters cover retained attempt windows (elapsed coverage is not the reservation horizon), may be lower bounds, and exclude dropped attempts; per-program memory is nonreclaimable attribution, host memory includes worker and managed descendants; retained jobs only"}
	programs := map[string]EfficiencyAllocation{}
	resourceHosts := map[string]ResourceIntegral{}
	resourceAttempts, cpuAttempts, cpuPartial := 0, 0, false
	ioPressure := q.efficiencyIOPressure(now)
	projects := map[string]map[string]float64{}
	for _, h := range q.s.EfficiencyHosts {
		h.advance(now)
		h.IOPressure = ioPressure[h.Host]
		if h.IOPressure != nil {
			if h.IOPressure.Unavailable != "" {
				h.IOStatus = "unknown: " + h.IOPressure.Unavailable
			} else if h.IOPressure.Some != nil {
				h.IOStatus = "measured host pressure"
			}
		}
		r.Hosts = append(r.Hosts, h)
	}
	for _, j := range q.s.Jobs {
		if e := j.Efficiency; e != nil {
			copy := *e
			copy.WaitSeconds = maps.Clone(e.WaitSeconds)
			copy.Allocations = slices.Clone(e.Allocations)
			if mutableJob(j) {
				copy.advance(now)
			}
			if len(copy.WaitSeconds) > 0 {
				r.Jobs = append(r.Jobs, EfficiencyWait{JobID: j.ID, Project: j.Spec.Project, Seconds: copy.WaitSeconds, Active: j.State == "queued" || j.State == "paused", Blocker: copy.Blocker})
				if projects[j.Spec.Project] == nil {
					projects[j.Spec.Project] = map[string]float64{}
				}
				for k, s := range copy.WaitSeconds {
					projects[j.Spec.Project][k] += s
				}
			}
			for _, a := range copy.Allocations {
				key := a.Host + "/" + a.Work + "/" + a.Basis
				row := programs[key]
				row.Host, row.Work, row.Basis = a.Host, a.Work, a.Basis
				row.ResourceIntegral.add(a.ResourceIntegral)
				programs[key] = row
			}
		}
		r.ResourceAttemptsDropped += j.ResourceAttemptsDropped
		if j.Attempt > 0 && len(j.ResourceAttempts) == 0 {
			cpuPartial = true
		}
		for _, f := range j.ResourceAttempts {
			resourceAttempts++
			a := q.efficiencyResourceAllocation(j, f)
			cpuAttempts += a.CPUObservedAttempts
			cpuPartial = cpuPartial || a.CPULowerBound
			key := a.Host + "/" + a.Work + "/" + a.Basis
			row := programs[key]
			row.Host, row.Work, row.Basis = a.Host, a.Work, a.Basis
			row.ResourceIntegral.add(a.ResourceIntegral)
			programs[key] = row
			h := resourceHosts[a.Host]
			h.add(a.ResourceIntegral)
			resourceHosts[a.Host] = h
		}
		r.Predictions = append(r.Predictions, efficiencyPredictions(j)...)
		r.Wasted = append(r.Wasted, efficiencyWasted(j)...)
	}
	for i := range r.Hosts {
		r.Hosts[i].ResourceIntegral.add(resourceHosts[r.Hosts[i].Host])
	}
	if cpuAttempts > 0 {
		r.CPUStatus = "measured retained attempts"
		if cpuPartial || cpuAttempts < resourceAttempts || r.ResourceAttemptsDropped > 0 {
			r.CPUStatus = "partial retained attempts"
		}
	}
	for _, a := range programs {
		r.Programs = append(r.Programs, a)
	}
	for project, seconds := range projects {
		r.Projects = append(r.Projects, EfficiencyWait{Project: project, Seconds: seconds})
	}
	r.Stranded = slices.Clone(q.s.StrandedDemand)
	for key, b := range q.s.EfficiencyHealth {
		r.Signals = append(r.Signals, b.signal(key))
	}
	slices.SortFunc(r.Hosts, func(a, b EfficiencyHost) int { return strings.Compare(a.Host, b.Host) })
	slices.SortFunc(r.Programs, func(a, b EfficiencyAllocation) int {
		if (a.MemoryCoveredSeconds > 0) != (b.MemoryCoveredSeconds > 0) {
			if a.MemoryCoveredSeconds > 0 {
				return -1
			}
			return 1
		}
		if d := b.ReservedCoveredMemoryGBSeconds - b.MeasuredMemoryGBSeconds - (a.ReservedCoveredMemoryGBSeconds - a.MeasuredMemoryGBSeconds); d > 0 {
			return 1
		} else if d < 0 {
			return -1
		}
		return strings.Compare(a.Host+a.Work+a.Basis, b.Host+b.Work+b.Basis)
	})
	slices.SortFunc(r.Jobs, func(a, b EfficiencyWait) int { return strings.Compare(a.JobID, b.JobID) })
	slices.SortFunc(r.Projects, func(a, b EfficiencyWait) int { return strings.Compare(a.Project, b.Project) })
	slices.SortFunc(r.Predictions, func(a, b PredictionError) int {
		return strings.Compare(fmt.Sprintf("%s/%09d", a.JobID, a.Attempt), fmt.Sprintf("%s/%09d", b.JobID, b.Attempt))
	})
	slices.SortFunc(r.Wasted, func(a, b WastedAttempt) int {
		return strings.Compare(fmt.Sprintf("%s/%09d", a.JobID, a.Attempt), fmt.Sprintf("%s/%09d", b.JobID, b.Attempt))
	})
	slices.SortFunc(r.Signals, func(a, b EfficiencySignal) int { return strings.Compare(a.Subject+a.Metric, b.Subject+b.Metric) })
	return r
}
func efficiencyPredictions(j *Job) []PredictionError {
	var rows []PredictionError
	for _, f := range j.MemoryAttempts {
		// LearningReason is the acceptance classifier, not an inference from exit 0.
		if f.MemoryEnvelope == nil || f.MemoryPeak == nil || f.LearningReason != "eligible" {
			continue
		}
		p := f.MemoryPeak
		if p.constrained() || p.PressureUnavailable {
			continue
		}
		e := f.MemoryEnvelope
		row := PredictionError{JobID: j.ID, Work: profileMemoryIdentity(j), Attempt: f.Attempt, Basis: e.Basis}
		if j.Efficiency != nil {
			row.Work = j.Efficiency.Work
		}
		if p.HostGB != nil || p.CgroupGB != nil || p.CombinedHostGB != nil {
			peak := hostMemoryPeakGB(p)
			reserved := e.HostGB
			delta := reserved - peak
			row.ReservedHostGB, row.AcceptedHostPeakGB, row.HostErrorGB = &reserved, &peak, &delta
		}
		if p.DeviceGB != nil {
			reserved, peak := e.DeviceGB, *p.DeviceGB
			delta := reserved - peak
			row.ReservedDeviceGB, row.AcceptedDevicePeakGB, row.DeviceErrorGB = &reserved, &peak, &delta
		}
		if t := j.Timing; t != nil && t.Execution.Attempt == f.Attempt && t.LastAcceptedSeconds != nil && t.ActualSeconds != nil {
			delta := *t.LastAcceptedSeconds - *t.ActualSeconds
			row.PredictedSeconds, row.ActualSeconds, row.DurationErrorSeconds = t.LastAcceptedSeconds, t.ActualSeconds, &delta
		}
		rows = append(rows, row)
	}
	// Timing can exist without memory telemetry (including older workers).
	if t := j.Timing; t != nil && t.LastAcceptedSeconds != nil && t.ActualSeconds != nil && !slices.ContainsFunc(rows, func(r PredictionError) bool { return r.Attempt == t.Execution.Attempt }) {
		delta := *t.LastAcceptedSeconds - *t.ActualSeconds
		work := profileMemoryIdentity(j)
		if j.Efficiency != nil {
			work = j.Efficiency.Work
		}
		rows = append(rows, PredictionError{JobID: j.ID, Work: work, Attempt: t.Execution.Attempt, Basis: efficiencyBasis(j), PredictedSeconds: t.LastAcceptedSeconds, ActualSeconds: t.ActualSeconds, DurationErrorSeconds: &delta})
	}
	return rows
}
func efficiencyWasted(j *Job) []WastedAttempt {
	var rows []WastedAttempt
	for _, o := range j.AttemptHistory {
		kind := ""
		if o.Interrupted {
			kind = "lost"
		}
		if o.Forced || o.ExecutionCause != nil && (o.ExecutionCause.Class == "killed" || o.ExecutionCause.Class == "signal" || o.ExecutionCause.ExitCode != nil && *o.ExecutionCause.ExitCode == 137) {
			kind = "killed"
		}
		if o.PoliteStop {
			kind = "guard_stopped"
		}
		if o.OutOfMemory {
			kind = "oom"
		}
		if kind == "" || o.OperatorStop || o.PriorityStop != nil {
			continue
		}
		row := WastedAttempt{JobID: j.ID, Attempt: o.Attempt, Host: o.Machine, Kind: kind, EndedAt: o.EndedAt, IOStatus: "unknown", StopObservation: o.StopObservation}
		for _, f := range j.ResourceAttempts {
			if f.Attempt == o.Attempt && f.Machine == o.Machine && f.Session == o.Session {
				row.Resources, row.IOStatus = f.Resources, attemptIOStatus(f.Resources)
				break
			}
		}
		if o.Placement != nil {
			row.Host = o.Placement.Host
		}
		for _, f := range j.MemoryAttempts {
			if f.Attempt != o.Attempt || f.Machine != o.Machine || f.Session != o.Session || f.MemoryPeak == nil {
				continue
			}
			p := f.MemoryPeak
			if p.Pressure != nil {
				pressure := *p.Pressure
				pressure.Reason = "attempt_memory_window"
				row.Pressure = &pressure
			}
			if p.Pressure != nil && !p.PressureUnavailable {
				e := p.Pressure
				if e.MinAvailableGB != nil {
					gb := *e.MinAvailableGB - e.AvailableFloorGB
					row.HeadroomGB = &gb
				}
				if e.ElapsedUS > 0 && e.HostStallUS != nil && e.CgroupStallUS != nil && row.HeadroomGB != nil {
					suspect := *row.HeadroomGB > 0 && *e.HostStallUS == 0 && *e.CgroupStallUS == 0 && e.HostOOMDelta == 0 && e.HostPressureLevel <= 1 && e.HostSwapGrowthBytes == 0 && e.HostSwapOutBytes == 0
					row.Suspect = &suspect
				}
			}
		}
		if o.StopObservation != nil && o.StopObservation.MemoryPressure != nil {
			observed := o.StopObservation
			adverse := observed.PressureUnit == "memory_psi_some_avg10_percent" && *observed.MemoryPressure > 0 || observed.PressureUnit == "memory_pressure_level" && *observed.MemoryPressure > 1 || observed.SwapGrowthBytesPerSecond != nil && *observed.SwapGrowthBytesPerSecond > 0
			if adverse {
				suspect := false
				row.Suspect = &suspect
			} else if row.Suspect == nil && observed.HeadroomGB != nil && observed.SwapGrowthBytesPerSecond != nil && (observed.PressureUnit == "memory_psi_some_avg10_percent" || observed.PressureUnit == "memory_pressure_level") {
				// Zero PSI and Darwin's normal-pressure enum are physical
				// meanings, not a deployment-tuned efficiency threshold.
				suspect := *observed.HeadroomGB > 0
				row.Suspect = &suspect
				row.HeadroomGB = observed.HeadroomGB
			}
		}
		rows = append(rows, row)
	}
	return rows
}

type efficiencyBaseline struct {
	Observed   bool        `json:"observed"`
	Identity   string      `json:"identity,omitempty"`
	At         time.Time   `json:"at"`
	History    alertSeries `json:"history"`
	Adverse    int         `json:"adverse"`
	Recovering int         `json:"recovering"`
	Active     bool        `json:"active"`
	Value      float64     `json:"value"`
}

func (b efficiencyBaseline) signal(key string) EfficiencySignal {
	subject, metric, _ := strings.Cut(key, "/")
	s := EfficiencySignal{Observed: b.Observed, Subject: subject, Metric: metric, Value: b.Value, BaselineSamples: len(b.History), Active: b.Active}
	if len(b.History) >= alertWarmup {
		_, hi := b.History.band()
		s.UpperBound = &hi
	}
	return s
}
func efficiencySignals(r *EfficiencyReport, old map[string]efficiencyBaseline) (map[string]efficiencyBaseline, []EfficiencySignal) {
	values := map[string]float64{}
	identities := map[string]string{}
	for _, h := range r.Hosts {
		for _, metric := range []string{"reservation_gap_gb", "stranded_demand", "wasted_attempts", "suspect_attempts", "io_pressure_some_percent"} {
			identities[h.Host+"/"+metric] = h.Identity
		}
	}
	for _, h := range r.Hosts {
		if h.Rate.MeasuredMemoryGB != nil {
			values[h.Host+"/reservation_gap_gb"] = max(0, h.Rate.ReservedMemoryGB-*h.Rate.MeasuredMemoryGB)
		}
		if h.IOPressure != nil && h.IOPressure.Unavailable == "" && h.IOPressure.Some != nil {
			values[h.Host+"/io_pressure_some_percent"] = h.IOPressure.Some.Avg10
		}
	}
	for _, p := range r.Jobs {
		for _, category := range []string{"memory_reservation", "authored_platform_targets", "environment", "priority_fairness", "preparation", "capacity", "holds", "dependency", "unknown"} {
			key := "project:" + p.Project + "/wait_" + category + "_seconds"
			identities[key] = ""
			if !p.Active {
				continue
			}
			if p.Blocker == category {
				values[key] = max(values[key], p.Seconds[category])
			} else if _, ok := values[key]; !ok {
				values[key] = 0
			}
		}
	}
	predictionSums := map[string]float64{}
	predictionCounts := map[string]int{}
	for _, p := range r.Predictions {
		subject := "work:" + p.Work + ":" + p.Basis
		if p.AcceptedHostPeakGB != nil && *p.AcceptedHostPeakGB > 0 && p.HostErrorGB != nil {
			key := subject + "/memory_prediction_relative_error"
			predictionSums[key] += absHealth(*p.HostErrorGB / *p.AcceptedHostPeakGB)
			predictionCounts[key]++
		}
		if p.ActualSeconds != nil && *p.ActualSeconds > 0 && p.DurationErrorSeconds != nil {
			key := subject + "/duration_prediction_relative_error"
			predictionSums[key] += absHealth(*p.DurationErrorSeconds / *p.ActualSeconds)
			predictionCounts[key]++
		}
	}
	for key, sum := range predictionSums {
		values[key] = sum / float64(predictionCounts[key])
	}
	for _, h := range r.Hosts {
		values[h.Host+"/wasted_attempts"] = 0
		values[h.Host+"/suspect_attempts"] = 0
	}
	for _, a := range r.Wasted {
		values[a.Host+"/wasted_attempts"]++
		if a.Suspect != nil && *a.Suspect {
			values[a.Host+"/suspect_attempts"]++
		}
	}

	for _, s := range r.Stranded {
		values[s.Host+"/stranded_demand"]++
	}
	// Preserve a zero baseline for enabled hosts before an exclusion appears.
	for _, h := range r.Hosts {
		if _, ok := values[h.Host+"/stranded_demand"]; !ok {
			values[h.Host+"/stranded_demand"] = 0
		}
	}
	next := map[string]efficiencyBaseline{}
	var signals []EfficiencySignal
	for key, value := range values {
		b := old[key]
		if b.Identity != identities[key] {
			b = efficiencyBaseline{Identity: identities[key]}
		}
		if !b.At.IsZero() && !r.At.After(b.At) {
			next[key] = b
			signals = append(signals, b.signal(key))
			continue
		}
		b.At = r.At
		b.Observed = true
		b.History = slices.Clone(b.History)
		b.Value = value
		if b.History.high(value) {
			b.Adverse++
			b.Recovering = 0
		} else {
			b.Adverse = 0
			b.Recovering++
			b.History.add(value)
		}
		if b.Adverse >= alertConfirmation {
			b.Active = true
		}
		if b.Recovering >= alertConfirmation {
			b.Active = false
		}
		next[key] = b
		signals = append(signals, b.signal(key))
	}
	// Missing host attribution cannot train zero use or establish recovery.
	// Keep its bounded local evidence until the host/identity leaves inventory.
	for key, b := range old {
		if _, ok := next[key]; ok {
			continue
		}
		identity, present := identities[key]
		if present && identity == b.Identity {
			b.Observed = false
			if strings.HasPrefix(key, "project:") {
				// No live demand resolves the condition without teaching idle
				// zeros over the learned history of actual queue episodes.
				b.Observed, b.Active, b.Value = true, false, 0
				b.Adverse, b.Recovering = 0, 0
			}
			next[key] = b
			signals = append(signals, b.signal(key))
		}
	}
	return next, signals
}

// A concise overview, with no made-up CPU/I/O measurement or ranked zero use.
func (r *EfficiencyReport) Lines() []string {
	if r == nil {
		return nil
	}
	lines := []string{fmt.Sprintf("efficiency at %s: %d stranded placements; %d wasted attempts; CPU use %s", r.At.UTC().Format(time.RFC3339), len(r.Stranded), len(r.Wasted), r.CPUStatus)}
	for _, h := range r.Hosts {
		used := "unknown"
		if h.Rate.MeasuredMemoryGB != nil {
			used = fmt.Sprintf("%.1f GiB", *h.Rate.MeasuredMemoryGB)
		}
		physical := "unknown"
		if h.PhysicalUsedMemoryGB != nil {
			physical = fmt.Sprintf("%.1f GiB", *h.PhysicalUsedMemoryGB)
		}
		lines = append(lines, fmt.Sprintf("  %s: reserved %.1f GiB; measured Mycelium %s; physical used %s; %s; %s; %s; host I/O %s", h.Host, h.Rate.ReservedMemoryGB, used, physical, efficiencyMemoryWords(h.ResourceIntegral), efficiencyCPUWords(h.ResourceIntegral), efficiencyIOWords(h.ResourceIntegral), h.IOStatus))
	}
	for _, p := range r.Projects {
		for _, category := range slices.Sorted(maps.Keys(p.Seconds)) {
			lines = append(lines, fmt.Sprintf("  project %s: %s wait %.1f hours", p.Project, category, p.Seconds[category]/3600))
		}
	}
	strandedGroups := map[string]int{}
	for _, s := range r.Stranded {
		strandedGroups[s.Host+": excluded by "+strings.Join(s.Fields, ", ")]++
	}
	for _, group := range slices.Sorted(maps.Keys(strandedGroups)) {
		lines = append(lines, fmt.Sprintf("  stranded %d fitting jobs on %s", strandedGroups[group], group))
	}
	seen := map[string]bool{}
	for _, p := range r.Programs {
		key := p.Host + "/" + p.Basis
		if seen[key] {
			continue
		}
		seen[key] = true
		lines = append(lines, fmt.Sprintf("  sizing %s %s: work %s; %s; %s; %s", p.Host, p.Basis, p.Work, efficiencyMemoryWords(p.ResourceIntegral), efficiencyCPUWords(p.ResourceIntegral), efficiencyIOWords(p.ResourceIntegral)))
	}
	if r.ResourceAttemptsDropped > 0 {
		lines = append(lines, fmt.Sprintf("  resource coverage: %d attempt records dropped by owning-job retention", r.ResourceAttemptsDropped))
	}
	type predictionSummary struct {
		count, memoryCount, durationCount int
		memory, duration                  float64
	}
	byBasis := map[string]predictionSummary{}
	for _, p := range r.Predictions {
		a := byBasis[p.Basis]
		a.count++
		if p.HostErrorGB != nil {
			a.memoryCount++
			a.memory += *p.HostErrorGB
		}
		if p.DurationErrorSeconds != nil {
			a.durationCount++
			a.duration += *p.DurationErrorSeconds
		}
		byBasis[p.Basis] = a
	}
	for _, basis := range slices.Sorted(maps.Keys(byBasis)) {
		a := byBasis[basis]
		memory, duration := "unknown", "unknown"
		if a.memoryCount > 0 {
			memory = fmt.Sprintf("%+.2f GiB", a.memory/float64(a.memoryCount))
		}
		if a.durationCount > 0 {
			duration = fmt.Sprintf("%+.1fs", a.duration/float64(a.durationCount))
		}
		lines = append(lines, fmt.Sprintf("  predictions %s: %d accepted comparisons; mean envelope error %s; mean duration error %s", basis, a.count, memory, duration))
	}
	suspect, unknown := 0, 0
	for _, a := range r.Wasted {
		if a.Suspect == nil {
			unknown++
		} else if *a.Suspect {
			suspect++
		}
	}
	if len(r.Wasted) > 0 {
		lines = append(lines, fmt.Sprintf("  stops: %d suspect; %d with unknown pressure", suspect, unknown))
	}
	for _, s := range r.Signals {
		if s.Active {
			lines = append(lines, "  efficiency regression: "+s.Subject+" "+s.Metric)
		}
	}

	return lines
}

func efficiencyMemoryWords(a ResourceIntegral) string {
	measured := "unknown"
	if a.MemoryCoveredSeconds > 0 {
		measured = fmt.Sprintf("%.1f GiB-hours", a.MeasuredMemoryGBSeconds/3600)
	}
	return fmt.Sprintf("integrated %.1f reserved GiB-hours / %s measured (%.0fs coverage)", a.ReservedMemoryGBSeconds/3600, measured, a.MemoryCoveredSeconds)
}
