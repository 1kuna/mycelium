package myce

import (
	"reflect"
	"strings"
	"time"
)

// Reuse an explanation while its scheduling inputs are unchanged. The immutable
// record tree lets unchanged jobs/learning catalogs compare by pointer, without
// decoding or walking retained history. Only base state and small runtime maps
// need value comparisons. This work also runs outside the writer mutex.
func sameAdmissionWaitInputs(a, b *admissionWaitCapture) bool {
	if a == nil || a.index == nil || b.index == nil ||
		!a.refresh.IsZero() && !b.index.capture.source.clock.Now().Before(a.refresh) {
		return false
	}
	x, y := a.index.capture, b.index.capture
	if !sameAdmissionDecisions(a.admissions, b.admissions) || !reflect.DeepEqual(a.decisions, b.decisions) ||
		!reflect.DeepEqual(x.models, y.models) || !reflect.DeepEqual(x.environments, y.environments) ||
		!reflect.DeepEqual(x.runtime, y.runtime) || !sameWaitLedgers(x.ledgers, y.ledgers) ||
		x.source.networkQuiet != y.source.networkQuiet || x.source.nasAvailable != y.source.nasAvailable ||
		!reflect.DeepEqual(x.source.evidenceRetention, y.source.evidenceRetention) ||
		!sameWaitRecords(a.index.root, b.index.root) {
		return false
	}
	left, err := readGet(a.index.root, "state").decode("state")
	if err != nil {
		return false
	}
	right, err := readGet(b.index.root, "state").decode("state")
	if err != nil {
		return false
	}
	s, t := *(left.(*state)), *(right.(*state))
	if len(s.Machines) != len(t.Machines) {
		return false
	}
	for id, m := range s.Machines {
		n := t.Machines[id]
		if n == nil || x.fresh[id].unreported != y.fresh[id].unreported ||
			waitHealth(x, m) != waitHealth(y, n) || !reflect.DeepEqual(waitMachine(*m), waitMachine(*n)) {
			return false
		}
	}
	if !sameWaitSamples(x.source.hostMemorySamples, y.source.hostMemorySamples) {
		return false
	}
	return reflect.DeepEqual(waitState(s), waitState(t))
}

func sameWaitLedgers(a, b map[string]observationMachineLedger) bool {
	if len(a) != len(b) {
		return false
	}
	for id, left := range a {
		right, ok := b[id]
		if !ok {
			return false
		}
		// Delivery order and receipt age matter through the eligibility reason
		// below; a fresh identical health report does not change a blocker.
		left.healthDelivery, right.healthDelivery = nil, nil
		if !reflect.DeepEqual(left, right) {
			return false
		}
	}
	return true
}

func waitHealth(c *committedReadCapture, m *Machine) string {
	copy := *m
	copy.LastSeen = c.fresh[m.ID].lastSeen
	if m.HostHealth != nil {
		health := *m.HostHealth
		health.delivery = c.ledgers[m.ID].healthDelivery
		copy.HostHealth = &health
	}
	return c.source.hostHygieneReasonAt(&copy, c.source.clock.Now())
}

func sameWaitRecords(a, b *readNode) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || a.key != b.key {
		return false
	}
	if a.key != "state" && !strings.HasPrefix(a.key, "health_events/") && a.record != b.record {
		return false
	}
	return sameWaitRecords(a.left, b.left) && sameWaitRecords(a.right, b.right)
}

// Receipt times and diagnostic resource histories do not change placement.
// Keep all capacity, qualification, custody, policy and device-fault fields.
func waitMachine(m Machine) Machine {
	m.LastSeen, m.DiskReceivedAt = time.Time{}, time.Time{}
	m.IOPressure, m.HygieneCollection, m.HostHealth = nil, nil, nil
	m.MemoryObservation, m.ExternalMemory = nil, nil
	m.Release, m.Fleet, m.Enrollment = nil, nil, nil
	return m
}

// These bookkeeping/diagnostic clocks do not change a blocker. Tier/order
// decisions themselves are compared above, including not_scheduler_turn.
func waitState(s state) state {
	s.Machines = nil
	s.EfficiencyAt, s.LastWorkerContact, s.UsageAt, s.UsageMaintenance = time.Time{}, time.Time{}, time.Time{}, time.Time{}
	s.EfficiencyHosts, s.EfficiencyHealth, s.StrandedDemand = nil, nil, nil
	s.FairUsage = nil
	s.Health = healthAlerts{}
	return s
}

func sameWaitSamples(a, b map[string]map[attemptKey]hostMemorySample) bool {
	if len(a) != len(b) {
		return false
	}
	for host, samples := range a {
		if len(samples) != len(b[host]) {
			return false
		}
		for key, sample := range samples {
			other, ok := b[host][key]
			if !ok {
				return false
			}
			// Refreshing identical telemetry is not a blocker transition. The
			// earliest freshness expiry still forces a new observation.
			sample.id, other.id = 0, 0
			sample.receivedAt, other.receivedAt = time.Time{}, time.Time{}
			sample.at, other.at = time.Time{}, time.Time{}
			sample.deviceAt, other.deviceAt = time.Time{}, time.Time{}
			if sample != other {
				return false
			}
		}
	}
	return true
}
