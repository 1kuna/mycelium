package myce

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// InventoryObservation dates discovery independently of worker contact. At is
// rebased onto queue time on receipt; cached reports never renew the scan.
type InventoryObservation struct {
	ScanAt         time.Time         `json:"scan_at,omitempty"` // Stable worker start, used only within its session.
	At             time.Time         `json:"at"`
	AgeSeconds     float64           `json:"age_seconds"`
	Probe          *ProbeObservation `json:"probe,omitempty"`
	State          string            `json:"state"`
	ErrorClass     string            `json:"error_class,omitempty"`
	Operation      string            `json:"operation,omitempty"`
	PendingSeconds float64           `json:"pending_seconds,omitempty"`
}

func inventoryObservationFresh(p *InventoryObservation, now time.Time) bool {
	return p != nil && p.Probe.valid() && validMemoryValue(p.AgeSeconds) &&
		!now.Before(p.At) && now.Sub(p.At) < p.Probe.freshness() &&
		(p.State == "ready" || p.State == "partial")
}

// A nil observation is the predecessor protocol. Its explicit per-device
// reasons still fence admission. New workers must supply a fresh observation.
func inventoryWaitingReason(m *Machine) string {
	if m.GPU == "cpu" && len(m.Devices) == 0 {
		return ""
	}
	if p := m.InventoryProbe; p != nil && p.State != "ready" && p.State != "partial" {
		p = safeInventoryObservation(p)
		why := "waiting for fresh accelerator inventory: " + p.State
		if p.ErrorClass != "" {
			why += " (" + p.ErrorClass + ")"
		}
		if p.Operation != "" {
			why += "; pending " + p.Operation
		}
		return why
	}
	if m.MemoryError == "device_probe_unavailable" {
		return "waiting for fresh accelerator inventory: unavailable"
	}
	return ""
}

func safeInventoryObservation(p *InventoryObservation) *InventoryObservation {
	if p == nil {
		return nil
	}
	out := *p
	switch out.State {
	case "pending", "ready", "partial", "failed", "stale":
	default:
		out.State = "failed"
	}
	switch out.ErrorClass {
	case "", "deadline", "cancelled", "gpu_driver_missing", "gpu_driver_unavailable", "gpu_inventory_invalid", "gpu_inventory_unavailable", "probe_failed":
	default:
		out.ErrorClass = "probe_failed"
	}
	switch out.Operation {
	case "", "runtime_inventory", "nvidia_identity", "nvidia_memory", "runtime_topology", "kernel_identity":
	default:
		out.Operation = "runtime_inventory"
	}
	return &out
}

func (q *Queue) expireInventoryObservations() {
	for _, m := range q.s.Machines {
		p := m.InventoryProbe
		if p != nil && (p.State == "ready" || p.State == "partial") && !inventoryObservationFresh(p, q.clock.Now()) {
			q.touchMachine(m.ID)
			p.State = "stale"
		}
	}
}

type inventoryProgressKey struct{}
type inventoryProgress struct {
	mu        sync.Mutex
	operation string
	at        time.Time
	clock     Clock
}

// Fixed operation names only; argv, subprocess output and paths never escape.
func noteInventoryOperation(ctx context.Context, name string, args []string) {
	p, _ := ctx.Value(inventoryProgressKey{}).(*inventoryProgress)
	if p == nil {
		return
	}
	op := "runtime_inventory"
	switch name {
	case "nvidia-smi":
		op = "nvidia_identity"
		if strings.Contains(strings.Join(args, " "), "memory.") {
			op = "nvidia_memory"
		}
	case "python3":
		op = "runtime_topology"
	case "uname":
		op = "kernel_identity"
	}
	p.mu.Lock()
	p.operation, p.at = op, p.clock.Now()
	p.mu.Unlock()
}

func inventoryErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	}
	var pf *PreflightError
	if errors.As(err, &pf) {
		switch pf.Code {
		case "gpu_driver_missing", "gpu_driver_unavailable", "gpu_inventory_invalid", "gpu_inventory_unavailable":
			return pf.Code
		}
	}
	return "probe_failed"
}

func (m *hostInventoryObserver) observation() *InventoryObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observationLocked()
}

func (m *hostInventoryObserver) observationLocked() *InventoryObservation {
	lane := &m.lane
	now := lane.clock.Now()
	p := &InventoryObservation{At: lane.at, Probe: lane.observation, State: "pending", ErrorClass: inventoryErrorClass(lane.err)}
	if !lane.at.IsZero() {
		p.AgeSeconds = max(0, now.Sub(lane.at).Seconds())
		p.State = "failed"
		if lane.fresh(now) {
			p.State = "ready"
		} else if lane.err == nil {
			p.State = "stale"
		} else {
			var partial *acceleratorProbeError
			if errors.As(lane.err, &partial) && lane.observation.valid() && !now.Before(lane.at) && now.Sub(lane.at) < lane.observation.freshness() {
				p.State = "partial"
			}
		}
	}
	if lane.pending != nil {
		m.progress.mu.Lock()
		p.Operation = m.progress.operation
		if !m.progress.at.IsZero() {
			p.PendingSeconds = max(0, now.Sub(m.progress.at).Seconds())
		}
		m.progress.mu.Unlock()
	}
	return p
}

// A launch probe is newer negative evidence than a cached discovery result.
// Only a completed scan begun after that refusal can reopen the affected card.
func (q *Queue) noteInventoryRefusal(j *Job) {
	c := j.ExecutionCause
	m := q.s.Machines[j.Machine]
	if c == nil || c.Predicate != "device_probe_unavailable" || m == nil || m.Session != j.Session || m.InventoryProbe == nil {
		return
	}
	q.touchMachine(m.ID)
	now := q.clock.Now()
	for i := range m.Devices {
		d := &m.Devices[i]
		for _, id := range j.AllocatedDevices {
			if d.ID == id {
				d.InventoryProbeAfter = &now
				d.InventoryProbeFailedScan = &m.InventoryProbe.ScanAt
				d.Reason, d.FreeMemoryGB = "device_probe_unavailable", 0
			}
		}
	}
}

func retainInventoryRefusals(m, old *Machine) {
	for i := range m.Devices {
		d := &m.Devices[i]
		d.InventoryProbeAfter = nil // Queue-owned; never trust a worker fence.
		d.InventoryProbeFailedScan = nil
		if old == nil || old.Session != m.Session {
			continue
		}
		for _, previous := range old.Devices {
			if previous.ID != d.ID || previous.Identity != d.Identity || previous.InventoryProbeAfter == nil {
				continue
			}
			if m.InventoryProbe == nil || !inventoryObservationFresh(m.InventoryProbe, m.InventoryProbe.At) || !m.InventoryProbe.At.After(*previous.InventoryProbeAfter) || previous.InventoryProbeFailedScan == nil || !m.InventoryProbe.ScanAt.After(*previous.InventoryProbeFailedScan) {
				d.InventoryProbeAfter = previous.InventoryProbeAfter
				d.InventoryProbeFailedScan = previous.InventoryProbeFailedScan
				d.Reason, d.FreeMemoryGB = "device_probe_unavailable", 0
			}
		}
	}
}
