package myce

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"
)

// A normal result is positive vendor evidence, never command success alone.
type DeviceHealthProbe func(context.Context, MachineDevice) (bool, error)

const deviceRecoveryProtocol = "device-recovery-v1"

func legacyDeviceRecoveryCheckin(c Checkin) Checkin {
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	for i := range c.Machine.Devices {
		c.Machine.Devices[i].Recovery = nil
	}
	return c
}

func hasDeviceRecovery(c Checkin) bool {
	return slices.ContainsFunc(c.Machine.Devices, func(d MachineDevice) bool { return d.Recovery != nil })
}

func (w Worker) deviceRecoveryCheckin(c Checkin) Checkin {
	if r := w.unifiedMemoryWire; r != nil && r.recoverySupported && r.machine == c.Machine.ID && r.session == c.Machine.Session {
		return c
	}
	return legacyDeviceRecoveryCheckin(c)
}

type DeviceRecovery struct {
	ObservedAt         time.Time `json:"observed_at,omitempty"` // Queue-owned receipt of a quarantined-to-healthy transition.
	FaultEvent         string    `json:"fault_event"`
	HealthySince       time.Time `json:"healthy_since"`
	ClearedAt          time.Time `json:"cleared_at"`
	ObservationSeconds float64   `json:"observation_seconds"`
	Probe              string    `json:"probe"`
}

// A worker's wall clock cannot order its clear against a queue-owned loss.
// Replays preserve receipt time; a historical clear first seen on a healthy
// registration is not a newly observed recovery.
func (q *Queue) receiveDeviceRecovery(m, old *Machine) {
	for i := range m.Devices {
		d := &m.Devices[i]
		var previous *DeviceRecovery
		var priorReason string
		if old != nil {
			for _, prior := range old.Devices {
				if prior.Platform == d.Platform && prior.Identity == d.Identity {
					previous, priorReason = prior.Recovery, prior.Reason
					break
				}
			}
		}
		// Negotiation/downgrade can omit the optional field. Retain the old
		// receipt as historical evidence so that replay never renews it.
		if d.Recovery == nil {
			if previous != nil {
				copy := *previous
				d.Recovery = &copy
			}
			continue
		}
		r := *d.Recovery
		if !sha256Name.MatchString(r.FaultEvent) || !validMemoryValue(r.ObservationSeconds) || r.ObservationSeconds <= 0 || r.HealthySince.IsZero() || r.ClearedAt.Sub(r.HealthySince).Seconds() < r.ObservationSeconds {
			d.Recovery = nil
			if previous != nil {
				copy := *previous
				d.Recovery = &copy
			}
			continue
		}
		r.Probe = deviceHealthProbeName(d.Platform)
		r.ObservedAt = time.Time{}
		if previous != nil && previous.FaultEvent == r.FaultEvent && previous.ClearedAt == r.ClearedAt {
			r.ObservedAt = previous.ObservedAt
		} else if d.Reason == "" && deviceFaultReason(priorReason) {
			r.ObservedAt = q.clock.Now()
		}
		d.Recovery = &r
	}
}

type deviceHealthObservation struct {
	event  string
	at     time.Time
	normal bool
}

type deviceRecoveryObservation struct {
	event       string
	since, last time.Time
	window      time.Duration
}

type deviceHealthDue struct {
	event string
	at    time.Time
}

func (w *Worker) quarantineHealth(ctx context.Context, root *workerStore) map[string]deviceHealthObservation {
	root.vendorProbeStart.setBusy(w.vendorGPUActive)
	for key, h := range w.faultHealth {
		f := w.faults.Devices[key]
		if f.Reason == "" || f.RepeatedTeardown || h.event != f.Event {
			delete(w.faultHealth, key)
		}
	}
	if len(w.faultHealth) > 0 {
		return w.faultHealth // Await a kernel scan after these observations.
	}
	if w.DeviceHealthProbe == nil || len(w.faults.Devices) == 0 {
		return nil
	}
	devices, faults, probe, clock := slices.Clone(w.Machine.Devices), maps.Clone(w.faults.Devices), w.DeviceHealthProbe, w.Clock
	devices = slices.DeleteFunc(devices, func(d MachineDevice) bool {
		key := deviceFaultKey(d)
		fault, due := faults[key], w.faultHealthDue[key]
		// A quarantined card's vendor tool can initialize every sibling. Wait
		// for accelerator custody to end, while kernel fault scans continue.
		return fault.Reason == "" || fault.RepeatedTeardown || d.Platform == "xpu" && w.vendorGPUActive ||
			due.event == fault.Event && clock.Now().Before(due.at)
	})
	// Collect an already completed asynchronous read even if no new card is
	// due. A nil devices slice otherwise prevents empty health retry loops.
	root.deviceHealth.mu.Lock()
	pending := root.deviceHealth.pending != nil
	root.deviceHealth.mu.Unlock()
	if len(devices) == 0 && !pending {
		return nil
	}
	read := func() (map[string]deviceHealthObservation, error) {
		out := map[string]deviceHealthObservation{}
		for _, d := range devices {
			key := deviceFaultKey(d)
			f := faults[key]
			// Skip known custody between queries. The production command runner
			// also orders actual process creation against custody acquisition.
			if f.Reason == "" || f.RepeatedTeardown || d.Platform == "xpu" && root.vendorProbeStart.blocked() {
				continue
			}
			probeCtx := ctx
			if d.Platform == "xpu" {
				probeCtx = withVendorProbeStart(ctx, &root.vendorProbeStart)
			}
			normal, err := probe(probeCtx, d)
			if errors.Is(err, errVendorProbeDeferred) {
				continue // No query ran; do not postpone an idle recovery decision.
			}
			out[key] = deviceHealthObservation{f.Event, clock.Now(), normal && err == nil}
		}
		return out, nil
	}
	health, err := root.deviceHealth.run("quarantine-health", read)
	if err != nil {
		return nil
	}
	w.faultHealth = health
	if w.faultHealthDue == nil {
		w.faultHealthDue = map[string]deviceHealthDue{}
	}
	for key, h := range health {
		fault := w.faults.Devices[key]
		if fault.Event != h.event {
			continue
		}
		// No clear can occur before its existing quiet window. Rechecking
		// inside that span buys no recovery decision. Failed observations
		// extend it by the measured time still unrecovered, without a host-
		// tuned retry duration or a maximum recovery delay.
		window := max(teardownFaultWindow, time.Duration(w.faults.RecoveryWindows[key]*float64(time.Second)))
		if !fault.At.IsZero() && h.at.After(fault.At) {
			window = max(window, h.at.Sub(fault.At))
		}
		if r := w.faultRecovery[key]; r.event == fault.Event {
			window = max(window, r.window)
		}
		w.faultHealthDue[key] = deviceHealthDue{event: h.event, at: h.at.Add(window)}
	}
	return w.faultHealth
}

// Agent default (#1142, MY-34): observe at least a complete local probe gap
// after the first normal reading. A slower measured recovery or earlier clear
// lengthens that window. Nothing here is calibrated to a particular fleet.
func (w *Worker) autoClearDeviceFaults(next *deviceQuarantine, health map[string]deviceHealthObservation, snapshot KernelLogSnapshot, cadence time.Duration) bool {
	changed := false
	for _, d := range w.Machine.Devices {
		key := deviceFaultKey(d)
		fault := next.Devices[key]
		if fault.Reason == "" || fault.RepeatedTeardown {
			delete(w.faultRecovery, key)
			delete(health, key)
			continue // Escalated quarantine still requires an operator.
		}
		pending := slices.ContainsFunc(w.faultPending, func(line KernelLogLine) bool {
			match := w.DeviceFaultMatchers[d.Platform]
			return match != nil && match(d, line.Message) != ""
		})
		h, ok := health[key]
		if pending || ok && (!h.normal || h.event != fault.Event) {
			delete(w.faultRecovery, key)
			delete(health, key)
			continue
		}
		if !ok {
			continue
		}
		if h.at.Before(fault.At) || h.at.Before(w.faultHealthAfter) {
			delete(health, key)
			continue
		}
		if snapshot.started.Before(h.at) {
			continue
		}
		delete(health, key) // Consumed only after a subsequent kernel observation.
		r := w.faultRecovery[key]
		if r.event != fault.Event {
			latency := time.Duration(0)
			if !fault.At.IsZero() {
				latency = h.at.Sub(fault.At)
			}
			r = deviceRecoveryObservation{event: fault.Event, since: h.at, last: h.at,
				window: max(cadence, teardownFaultWindow, latency, time.Duration(next.RecoveryWindows[key]*float64(time.Second)))}
			w.faultRecovery[key] = r
			continue
		}
		if !h.at.After(r.last) {
			continue // Repeated/stale probe results cannot age into proof.
		}
		r.window = max(r.window, cadence, h.at.Sub(r.last))
		r.last = h.at
		w.faultRecovery[key] = r
		if h.at.Sub(r.since) < r.window {
			continue
		}
		next.Cleared[key] = DeviceRecovery{FaultEvent: fault.Event, HealthySince: r.since, ClearedAt: h.at, ObservationSeconds: r.window.Seconds(), Probe: deviceHealthProbeName(d.Platform)}
		next.RecoveryWindows[key] = r.window.Seconds()
		delete(next.Devices, key)
		delete(w.faultRecovery, key)
		changed = true
	}
	return changed
}

func deviceHealthProbeName(platform string) string {
	switch platform {
	case "xpu":
		return "xpu-smi health"
	case "cuda":
		return "nvidia-smi recovery action"
	default:
		return "vendor health"
	}
}

// DeviceHealth is read-only and targets the physical vendor identity. Unsupported
// tools/fields, malformed output and unknown health retain quarantine.
func (p MachineProbe) DeviceHealth(ctx context.Context, d MachineDevice) (bool, error) {
	if p.Command == nil {
		return false, errors.New("device health probe unavailable")
	}
	switch d.Platform {
	case "xpu":
		if d.pciAddress == "" {
			return false, errors.New("device health identity unavailable")
		}
		data, err := p.Command(ctx, "xpu-smi", "health", "-d", d.pciAddress, "-j")
		if err != nil {
			return false, err
		}
		var health map[string]json.RawMessage
		if err := json.Unmarshal(data, &health); err != nil {
			return false, err
		}
		if _, failed := health["error"]; failed {
			return false, nil
		}
		for _, component := range []string{"core_temperature", "power", "memory"} {
			var reading struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(health[component], &reading) != nil || reading.Status != "OK" {
				return false, nil
			}
		}
		// Optional components can be unsupported, but an explicit warning or
		// critical result on any component prevents clearance.
		for _, data := range health {
			var reading struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(data, &reading) == nil && reading.Status != "" && reading.Status != "OK" && reading.Status != "Unknown" {
				return false, nil
			}
		}
		return true, nil
	case "cuda":
		data, err := p.Command(ctx, "nvidia-smi", "-q", "-x", "-i", d.Identity)
		if err != nil {
			return false, err
		}
		var report struct {
			GPUs []struct {
				UUID     string `xml:"uuid"`
				Recovery string `xml:"gpu_recovery_action"`
			} `xml:"gpu"`
		}
		if err := xml.Unmarshal(data, &report); err != nil {
			return false, err
		}
		return len(report.GPUs) == 1 && report.GPUs[0].UUID == d.Identity && strings.TrimSpace(report.GPUs[0].Recovery) == "None", nil
	default:
		return false, errors.New("device health probe unsupported")
	}
}
