package myce

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

// A normal result is positive vendor evidence, never command success alone.
type DeviceHealthProbe func(context.Context, MachineDevice) (bool, error)

// DeviceHealthReportProbe preserves component evidence for same-batch model
// capability comparison. Legacy/custom boolean probes retain their contract.
type DeviceHealthReportProbe func(context.Context, MachineDevice) (DeviceHealthReport, error)
type DeviceHealthReport struct {
	Components map[string]string
}

var requiredXPUHealth = []string{"core_temperature", "power", "memory"}

func (r DeviceHealthReport) proof(sibling *DeviceHealthReport) ([]string, error) {
	var unsupported []string
	for _, component := range requiredXPUHealth {
		status := r.Components[component]
		if status == "OK" {
			continue
		}
		code := "health_component_failed"
		if status == "" {
			code = "health_component_missing"
		} else if status == "Unknown" {
			code = "health_component_unknown"
			if sibling != nil && sibling.Components[component] == "Unknown" {
				unsupported = append(unsupported, component)
				continue
			}
		}
		return unsupported, &DeviceHealthFailure{Code: code, Component: component}
	}
	return unsupported, nil
}

const deviceRecoveryProtocol = "device-recovery-v1"
const deviceHealthProtocol = "device-health-v1"

// DeviceHealthFailure contains only worker-owned classifications, never vendor
// output, error strings or paths. ObservedAt identifies the last rejected proof.
type DeviceHealthFailure struct {
	Code       string    `json:"code"`
	Component  string    `json:"component,omitempty"`
	ExitStatus *int      `json:"exit_status,omitempty"`
	ObservedAt time.Time `json:"observed_at,omitempty"`
}

func (f *DeviceHealthFailure) Error() string {
	s := f.Code
	if f.Component != "" {
		s += ": " + f.Component
	}
	if f.ExitStatus != nil {
		s += fmt.Sprintf(": exit_status=%d", *f.ExitStatus)
	}
	return s
}

func deviceHealthFailure(normal bool, err error) *DeviceHealthFailure {
	if normal && err == nil {
		return nil
	}
	var failure *DeviceHealthFailure
	if errors.As(err, &failure) && failure != nil {
		// Even injected probes cannot publish arbitrary labels or error text.
		switch failure.Code {
		case "health_component_missing", "health_component_unknown", "health_component_failed":
			switch failure.Component {
			case "core_temperature", "power", "memory", "memory_temperature", "frequency", "fabric_port", "optional":
				return &DeviceHealthFailure{Code: failure.Code, Component: failure.Component}
			}
		case "health_probe_invalid_output", "health_probe_error", "health_probe_identity_unavailable", "health_probe_identity_mismatch", "health_probe_unsupported", "health_probe_permission_denied", "health_probe_unavailable", "health_probe_cancelled", "health_probe_failed", "health_probe_unhealthy", "health_probe_comparison_stale", "health_probe_monitor_gap":
			return &DeviceHealthFailure{Code: failure.Code}
		case "health_probe_exit":
			if failure.ExitStatus != nil {
				status := *failure.ExitStatus
				return &DeviceHealthFailure{Code: failure.Code, ExitStatus: &status}
			}
		}
	}
	code := "health_probe_unhealthy"
	switch {
	case errors.Is(err, os.ErrPermission):
		code = "health_probe_permission_denied"
	case errors.Is(err, exec.ErrNotFound), errors.Is(err, os.ErrNotExist):
		code = "health_probe_unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = "health_probe_cancelled"
	case err != nil:
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			status := exit.ExitCode()
			return &DeviceHealthFailure{Code: "health_probe_exit", ExitStatus: &status}
		}
		code = "health_probe_failed"
	}
	return &DeviceHealthFailure{Code: code}
}

func legacyDeviceRecoveryCheckin(c Checkin) Checkin {
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	for i := range c.Machine.Devices {
		c.Machine.Devices[i].Recovery = nil
		c.Machine.Devices[i].RecoveryBlocked = nil
		c.Machine.Devices[i].UnsupportedOnDevice = nil
	}
	return c
}

func hasDeviceRecovery(c Checkin) bool {
	return slices.ContainsFunc(c.Machine.Devices, func(d MachineDevice) bool {
		return d.Recovery != nil || d.RecoveryBlocked != nil || len(d.UnsupportedOnDevice) > 0
	})
}

func (w Worker) deviceRecoveryCheckin(c Checkin) Checkin {
	r := w.unifiedMemoryWire
	if r == nil || r.machine != c.Machine.ID || r.session != c.Machine.Session {
		return legacyDeviceRecoveryCheckin(c)
	}
	c.Machine.Devices = slices.Clone(c.Machine.Devices)
	for i := range c.Machine.Devices {
		if !r.recoverySupported {
			c.Machine.Devices[i].Recovery = nil
		}
		if !r.healthSupported {
			c.Machine.Devices[i].RecoveryBlocked = nil
			c.Machine.Devices[i].UnsupportedOnDevice = nil
		}
	}
	return c
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
	event                     string
	at                        time.Time // Target query completion, never renewed by comparison.
	started                   time.Time
	siblingStarted, siblingAt time.Time
	normal                    bool
	failure                   *DeviceHealthFailure
	unsupported               []string
	sibling, model            string
	siblingTeardown           string
}

func earliestHealthTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
func latestHealthTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
func (h deviceHealthObservation) predates(after time.Time) bool {
	start := h.started
	if start.IsZero() {
		start = h.at
	} // Compatibility for existing injected observations.
	return start.Before(after) || !h.siblingStarted.IsZero() && h.siblingStarted.Before(after)
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
	for key, h := range w.faultHealth {
		f := w.faults.Devices[key]
		if f.Reason == "" || f.RepeatedTeardown || h.event != f.Event {
			delete(w.faultHealth, key)
		}
	}
	if len(w.faultHealth) > 0 {
		return w.faultHealth // Await a kernel scan after these observations.
	}
	if w.DeviceHealthProbe == nil && w.DeviceHealthReportProbe == nil || len(w.faults.Devices) == 0 {
		return nil
	}
	devices, faults, probe, clock := slices.Clone(w.Machine.Devices), maps.Clone(w.faults.Devices), w.DeviceHealthProbe, w.Clock
	comparisonDevices := slices.Clone(devices) // Keep healthy siblings outside the due-target filter.
	devices = slices.DeleteFunc(devices, func(d MachineDevice) bool {
		key := deviceFaultKey(d)
		fault, due := faults[key], w.faultHealthDue[key]
		return fault.Reason == "" || fault.RepeatedTeardown ||
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

	reportProbe := w.DeviceHealthReportProbe
	teardowns := maps.Clone(w.faults.Teardowns)
	// Compare within one control interval plus the prior kernel-cost prediction.
	// Freeze the allowance before starting: a stalled query cannot enlarge its own validity window.
	comparisonWindow := root.faultTiming.controlPeriod() + time.Duration(root.faultTiming.cost.upper()*float64(time.Second))
	read := func() (map[string]deviceHealthObservation, error) {
		out := map[string]deviceHealthObservation{}
		type reading struct {
			report      DeviceHealthReport
			err         error
			started, at time.Time
		}
		reports := map[string]reading{}
		readReport := func(d MachineDevice) reading {
			key := deviceFaultKey(d)
			if r, ok := reports[key]; ok {
				return r
			}
			r := reading{started: clock.Now()}
			r.report, r.err = reportProbe(ctx, d)
			r.at = clock.Now()
			reports[key] = r
			return r
		}
		for _, d := range devices {
			key := deviceFaultKey(d)
			f := faults[key]
			if f.Reason == "" || f.RepeatedTeardown {
				continue
			}
			h := deviceHealthObservation{event: f.Event, started: clock.Now()}
			var normal bool
			var err error
			if reportProbe != nil && d.Platform == "xpu" {
				target := readReport(d)
				r, errRead := target.report, target.err
				h.started, h.at = target.started, target.at
				err = errRead
				if err == nil {
					_, err = r.proof(nil)
					if _, comparisonErr := r.proof(&r); err != nil && comparisonErr == nil && d.Fingerprint != "" {
						for _, sibling := range comparisonDevices {
							if sibling.Platform != d.Platform || sibling.Identity == d.Identity || sibling.Fingerprint != d.Fingerprint || sibling.Reason != "" || faults[deviceFaultKey(sibling)].Reason != "" {
								continue
							}
							comparison := readReport(sibling)
							s, siblingErr := comparison.report, comparison.err
							if siblingErr != nil {
								continue
							}
							// A healthy comparator may lack a capability, but must
							// positively pass every component it can determine.
							if _, siblingErr = s.proof(&s); siblingErr != nil {
								continue
							}
							span := latestHealthTime(target.at, comparison.at).Sub(earliestHealthTime(target.started, comparison.started))
							if span < 0 || span > comparisonWindow {
								err = &DeviceHealthFailure{Code: "health_probe_comparison_stale"}
								continue
							}
							h.siblingStarted, h.siblingAt = comparison.started, comparison.at
							h.unsupported, err = r.proof(&s)
							h.sibling, h.model = deviceFaultKey(sibling), d.Fingerprint
							h.siblingTeardown = teardowns[h.sibling].Event
							if err == nil {
								break
							}
						}
					}
				}
				normal = err == nil
			} else if probe != nil {
				normal, err = probe(ctx, d)
				h.at = clock.Now()
			} else {
				err = exec.ErrNotFound
			}
			h.normal, h.failure = normal && err == nil, deviceHealthFailure(normal, err)
			out[key] = h
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
	for key, h := range health {
		f := w.faults.Devices[key]
		if f.Reason == "" || f.RepeatedTeardown || f.Event != h.event {
			continue
		}
		if h.predates(w.faultHealthAfter) {
			h.normal, h.failure, h.unsupported = false, &DeviceHealthFailure{Code: "health_probe_monitor_gap"}, nil
			health[key] = h
		}
		if w.faultRejected == nil {
			w.faultRejected = map[string]deviceHealthObservation{}
		}
		previous := w.faultRejected[key]
		if h.failure == nil {
			w.faultRejected[key] = h
			continue
		}
		w.faultRejected[key] = h
		if previous.event != h.event || previous.failure == nil || previous.failure.Error() != h.failure.Error() {
			log.Printf("device recovery proof rejected: device=%s reason=%s", key, h.failure)
		}
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
		if ok && len(h.unsupported) > 0 {
			validSibling := slices.ContainsFunc(w.Machine.Devices, func(s MachineDevice) bool {
				return d.Fingerprint == h.model && deviceFaultKey(s) == h.sibling && s.Fingerprint == h.model && s.Reason == "" && next.Devices[h.sibling].Reason == "" && next.Teardowns[h.sibling].Event == h.siblingTeardown &&
					!slices.ContainsFunc(w.faultPending, func(line KernelLogLine) bool {
						match := w.DeviceFaultMatchers[s.Platform]
						return match != nil && match(s, line.Message) != ""
					})
			})
			if !validSibling {
				h.normal = false
				h.failure = &DeviceHealthFailure{Code: "health_component_unknown", Component: h.unsupported[0]}
				h.unsupported = nil
				w.faultRejected[key] = h
				log.Printf("device recovery proof rejected: device=%s reason=%s", key, h.failure)
			}
		}
		if pending || ok && (!h.normal || h.event != fault.Event) {
			delete(w.faultRecovery, key)
			delete(health, key)
			continue
		}
		if !ok {
			continue
		}
		if h.predates(fault.At) || h.predates(w.faultHealthAfter) {
			delete(health, key)
			continue
		}
		if snapshot.started.Before(latestHealthTime(h.at, h.siblingAt)) {
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
		if next.Unsupported == nil {
			next.Unsupported = map[string][]string{}
		}
		next.Unsupported[key] = slices.Clone(h.unsupported)
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
		return false, exec.ErrNotFound
	}
	switch d.Platform {
	case "xpu":
		r, err := p.XPUHealthReport(ctx, d)
		if err != nil {
			return false, err
		}
		_, err = r.proof(nil)
		return err == nil, err

	case "cuda":
		data, err := p.Command(ctx, "nvidia-smi", "-q", "-x", "-i", d.Identity)
		if err != nil {
			return false, deviceHealthFailure(false, err)
		}
		var report struct {
			GPUs []struct {
				UUID     string `xml:"uuid"`
				Recovery string `xml:"gpu_recovery_action"`
			} `xml:"gpu"`
		}
		if err := xml.Unmarshal(data, &report); err != nil {
			return false, &DeviceHealthFailure{Code: "health_probe_invalid_output"}
		}
		if len(report.GPUs) != 1 || report.GPUs[0].UUID != d.Identity {
			return false, &DeviceHealthFailure{Code: "health_probe_identity_mismatch"}
		}
		return strings.TrimSpace(report.GPUs[0].Recovery) == "None", nil
	default:
		return false, &DeviceHealthFailure{Code: "health_probe_unsupported"}
	}
}

// XPUHealthReport keeps Unknown separate from malformed/missing evidence.
// Only explicit undeterminable readings can qualify a sibling comparison.
func (p MachineProbe) XPUHealthReport(ctx context.Context, d MachineDevice) (DeviceHealthReport, error) {
	if p.Command == nil {
		return DeviceHealthReport{}, exec.ErrNotFound
	}
	if d.pciAddress == "" {
		return DeviceHealthReport{}, &DeviceHealthFailure{Code: "health_probe_identity_unavailable"}
	}
	data, err := p.Command(ctx, "xpu-smi", "health", "-d", d.pciAddress, "-j")
	if err != nil {
		return DeviceHealthReport{}, deviceHealthFailure(false, err)
	}
	var health map[string]json.RawMessage
	if err := json.Unmarshal(data, &health); err != nil {
		return DeviceHealthReport{}, &DeviceHealthFailure{Code: "health_probe_invalid_output"}
	}
	if _, failed := health["error"]; failed {
		return DeviceHealthReport{}, &DeviceHealthFailure{Code: "health_probe_error"}
	}
	report := DeviceHealthReport{Components: map[string]string{}}
	for _, component := range []struct{ field, name string }{
		{"core_temperature", "core_temperature"}, {"power_health", "power"}, {"memory_health", "memory"},
	} {
		data, ok := health[component.field]
		if !ok {
			continue
		}
		var reading struct{ Status, Description string }
		status := "Invalid"
		if json.Unmarshal(data, &reading) == nil {
			switch {
			case reading.Status == "OK":
				status = "OK"
			case strings.EqualFold(strings.TrimSpace(reading.Status), "Unknown"), strings.EqualFold(strings.TrimSpace(reading.Status), "cannot be determined"):
				status = "Unknown"
			case reading.Status == "" && strings.Contains(strings.ToLower(reading.Description), "cannot be determined"):
				status = "Unknown"
			}
		}
		report.Components[component.name] = status
	}
	// Optional components can be unsupported, but an explicit warning or
	// critical result on any component prevents clearance.
	for _, field := range slices.Sorted(maps.Keys(health)) {
		if field == "core_temperature" || field == "power_health" || field == "memory_health" {
			continue
		}
		data := health[field]
		var reading struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(data, &reading) == nil && reading.Status != "" && reading.Status != "OK" && reading.Status != "Unknown" {
			component := "optional"
			switch field {
			case "memory_temperature":
				component = field
			case "frequency_health":
				component = "frequency"
			case "fabric_port_health":
				component = "fabric_port"
			}
			return DeviceHealthReport{}, &DeviceHealthFailure{Code: "health_component_failed", Component: component}
		}
	}
	return report, nil
}
