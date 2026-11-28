package myce

import (
	"slices"
	"strings"
	"time"
)

const memoryStopHeader = "X-Myce-Memory-Stop"
const memoryStopProtocol = "memory-stop-v1"
const memoryStopSampleAgeHeader = "X-Myce-Memory-Stop-Sample-Age"

// Unified CUDA needs a qualified attribution rule before R4 can split
// available RAM changes into attempts and other owners (probe 0b-1).
const unifiedStopAttributionQualified = false

func usesAttemptStopForecast(m *Machine, pool string) bool {
	return m.MemoryShadow != nil && m.MemoryShadow.stopForecast &&
		(pool != "ram" || !hostHasUnifiedCUDA(m) || unifiedStopAttributionQualified)
}

func (q *Queue) stopShortMemoryPool(m *Machine, received time.Time, period time.Duration) {
	report := m.MemoryShadow
	host := hostID(m)
	now := q.clock.Now()
	if report == nil || !report.stopForecast || m.Gone || m.Leaving || m.Replacement != nil ||
		q.unreported[m.ID] || !q.workerFresh(m, now) || report.sampleAge < 0 {
		return
	}
	at := received.Add(-report.sampleAge)
	if now.Sub(at) >= 2*checkinPeriod(period) || !at.After(q.poolLastStop[host]) {
		return
	}
	for _, pool := range report.Pools {
		cardID, cardPool := strings.CutPrefix(pool.Name, "card/")
		var card *Machine
		if pool.Name != "ram" {
			index := slices.IndexFunc(m.Devices, func(d MachineDevice) bool { return d.ID == cardID && !d.UnifiedMemory })
			if !cardPool || index == -1 {
				continue
			}
			target := m.Devices[index].machine(*m)
			card = &target
		}
		trigger := pool.shortage()
		if trigger == "" || !usesAttemptStopForecast(m, pool.Name) {
			continue
		}
		for _, j := range q.liveJobs() {
			if owner := q.s.Machines[j.Machine]; owner != nil && hostID(owner) == host &&
				j.State == "stopping" && batchReservesCapacity(j) {
				return
			}
		}
		o := shadowObservation{candidates: make(map[Execution]shadowCandidate, len(pool.Attempts))}
		production := 0
		for _, a := range pool.Attempts {
			j := q.s.Jobs[a.ID]
			if j == nil || j.Attempt != a.Attempt || j.Machine != m.ID || j.Session != m.Session ||
				j.State != "running" || j.StartedAt == nil || !batchReservesCapacity(j) || card != nil && !batchOnTarget(j, card) {
				continue
			}
			if jobClass(j.Spec) == "production" {
				production++
				if !j.Spec.Resumable {
					continue
				}
			}
			if !cancelled(j) && !plannedStop(j) {
				o.candidates[Execution{ID: j.ID, Attempt: j.Attempt}] = shadowCandidate{q.priorityMatch(j.Spec).Index, j.Spec.Resumable, *j.StartedAt}
			}
		}
		a, _, ok := o.victim(pool)
		if !ok {
			continue
		}
		j := q.s.Jobs[a.ID]
		if jobClass(j.Spec) == "production" && production < 2 {
			continue
		}
		stop := PriorityPreemption{Demand: "host/" + host, MemorySample: &memoryStopSample{
			ExpectedGB: a.ExpectedGB, PeakGB: max(a.HighWaterGB, *a.FootprintGB),
		}}
		free := 0.0
		if pool.FreeGB != nil {
			free = *pool.FreeGB
		}
		if pool.Name == "ram" {
			stop.MemorySample.OwnersGB = pool.OwnersGB
			stop.MemorySample.OwnersPeakGB = max(pool.OwnersGB, pool.OwnersForecastGB)
			grace := q.preemptionGrace().Seconds()
			r := &MemoryPoolStop{ObservedAt: at, AvailableGB: free + pool.KeepGB, ReserveGB: pool.KeepGB,
				SampleSeconds: max(0, report.ReactionSeconds-grace), GraceSeconds: grace, Trigger: trigger}
			if pool.ForecastFreeGB != nil {
				r.GraceLossGB = max(0, free-*pool.ForecastFreeGB)
			}
			if trigger == "host_pressure" {
				r.HostPressure = 2 // The report carries only the warning bit.
			}
			stop.MemoryPool = r
		} else {
			stop.Demand = "device/" + cardID
			stop.DevicePool = &DevicePoolStop{ObservedAt: at, BudgetGB: pool.CapacityGB, UsedGB: pool.FootprintGB,
				FreeGB: free + pool.KeepGB, MemoryMeasured: pool.FreeGB != nil, Trigger: trigger,
				OverageGB: max(0, *a.FootprintGB-a.ExpectedGB)}
		}
		q.stopForPoolPressure(j, stop)
		if q.poolLastStop == nil {
			q.poolLastStop = map[string]time.Time{}
		}
		q.poolLastStop[host] = now
		return
	}
}
