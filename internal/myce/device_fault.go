package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// KernelLogLine has a stable, boot-local identity (kernel monotonic timestamp).
// At is its wall time, zero when the source has none.
// Raw messages are inspected locally and never persisted or sent to the queue.
type KernelLogLine struct {
	ID, Message string
	At          time.Time
	evidence    map[string]*FaultEvidence
}
type KernelLogSnapshot struct {
	BootID             string
	Lines              []KernelLogLine
	started, completed time.Time
}

// Without a cursor of its own, a source reads records from since onward.
type DeviceFaultSource func(ctx context.Context, since time.Time) (KernelLogSnapshot, error)
type DeviceFaultMatcher func(MachineDevice, string) string

const deviceFaultFile = "device-quarantine.json"

var workerDeviceFaultSource = localDeviceFaultSource

type deviceQuarantine struct {
	AllocationThrough map[string]uint64           `json:"allocation_through,omitempty"`
	Evidence          map[string][]*FaultEvidence `json:"evidence,omitempty"`
	Unsupported       map[string][]string         `json:"unsupported_on_device,omitempty"`
	BootID            string                      `json:"boot_id"`
	Seen              map[string]bool             `json:"seen"`
	Devices           map[string]deviceFault      `json:"devices"`
	// Last fault per device attributed to teardown rather than quarantined.
	Teardowns       map[string]deviceFault    `json:"teardowns,omitempty"`
	Cleared         map[string]DeviceRecovery `json:"cleared,omitempty"`
	RecoveryWindows map[string]float64        `json:"recovery_windows,omitempty"` // Historical calibration, never current clear authority.
}

type deviceFault struct {
	At               time.Time `json:"at,omitempty"`
	Reason           string    `json:"reason"`
	Event            string    `json:"event"`
	RepeatedTeardown bool      `json:"repeated_teardown,omitempty"`
}
type deviceFaultClear struct {
	Device string `json:"device"`
	BootID string `json:"boot_id"`
	Event  string `json:"event"`
}

func deviceFaultKey(d MachineDevice) string { return d.Platform + ":" + d.Identity }
func deviceFaultReason(s string) bool {
	for _, backend := range acceleratorBackends {
		if slices.Contains(backend.faultReasons, s) {
			return true
		}
	}
	return false
}

func (w *Worker) initDeviceFaults(ctx context.Context, root *workerStore) error {
	w.faults = &deviceQuarantine{}
	// Read from worker start, not the whole boot (#595). Faults while no worker
	// ran are not replayed; quarantine already recorded persists below.
	w.faultSince = w.Clock.Now()
	w.faultTeardowns = map[string][]time.Time{}
	w.faultRecovery = map[string]deviceRecoveryObservation{}
	w.faultHealth = map[string]deviceHealthObservation{}
	w.faultHealthDue = map[string]deviceHealthDue{}
	w.faultRejected = map[string]deviceHealthObservation{}
	w.faultHealthAfter = w.Clock.Now()
	data, err := os.ReadFile(filepath.Join(root.dir, deviceFaultFile))
	if err == nil {
		err = decodeWorkerJSON(data, w.faults)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if w.DeviceFaultMatchers == nil {
		w.DeviceFaultMatchers = map[string]DeviceFaultMatcher{}
		for _, backend := range acceleratorBackends {
			if backend.faultMatcher != nil {
				w.DeviceFaultMatchers[backend.platform] = backend.faultMatcher
			}
		}
	}
	if w.DeviceFaultSource == nil {
		w.DeviceFaultSource = workerDeviceFaultSource()
	}
	if w.DeviceHealthProbe == nil {
		if w.DeviceHealthReportProbe == nil {
			w.DeviceHealthReportProbe = func(ctx context.Context, d MachineDevice) (DeviceHealthReport, error) {
				if probe := backendFor(d.Platform).healthReport; probe != nil {
					return probe(workerMachineProbe(), ctx, d)
				}
				return DeviceHealthReport{}, &DeviceHealthFailure{Code: "health_probe_unsupported"}
			}
		}
		// Resolve host discovery only when a health check runs (#1152): a
		// worker without a fault source never consults the platform probe.
		w.DeviceHealthProbe = func(ctx context.Context, d MachineDevice) (bool, error) {
			return workerMachineProbe().DeviceHealth(ctx, d)
		}
	}
	// Establish the first cursor before offering inventory. Cancellation owns
	// startup; no fixed deadline assumes how long this host's journal takes.
	previousBudget := root.deviceFaults.timeout
	root.deviceFaults.timeout = -1
	defer func() { root.deviceFaults.timeout = previousBudget }()
	return w.refreshDeviceFaults(ctx, root)
}

func (w *Worker) applyDeviceQuarantine() {
	if w.faults == nil {
		return
	}
	for i := range w.Machine.Devices {
		d := &w.Machine.Devices[i]
		d.FaultEvidence = w.faults.Evidence[deviceFaultKey(*d)]
		d.Recovery = nil
		d.RecoveryBlocked = nil
		d.UnsupportedOnDevice = nil
		if recovery, ok := w.faults.Cleared[deviceFaultKey(*d)]; ok {
			copy := recovery
			d.Recovery = &copy
			d.UnsupportedOnDevice = slices.Clone(w.faults.Unsupported[deviceFaultKey(*d)])
		}
		d.RepeatedTeardownFault = w.faults.Devices[deviceFaultKey(*d)].RepeatedTeardown
		if fault := w.faults.Devices[deviceFaultKey(*d)]; fault.Reason != "" {
			d.Reason, d.FreeMemoryGB = fault.Reason, 0
			if h := w.faultRejected[deviceFaultKey(*d)]; !fault.RepeatedTeardown && h.event == fault.Event && h.failure != nil {
				failure := *h.failure
				failure.ObservedAt = h.at
				d.RecoveryBlocked = &failure
			}
			if h := w.faultRejected[deviceFaultKey(*d)]; !fault.RepeatedTeardown && h.event == fault.Event {
				d.UnsupportedOnDevice = slices.Clone(h.unsupported)
			}
		} else if deviceFaultReason(d.Reason) {
			// Capacity returns only with the next successful inventory probe.
			d.Reason = ""
		}
	}
}

func (w *Worker) refreshDeviceFaults(ctx context.Context, root *workerStore) error {
	if w.faults == nil {
		return nil
	}
	defer w.applyDeviceQuarantine()
	if w.DeviceFaultSource == nil {
		return nil
	} // Apple has no cheap qualified source.
	root.faultTiming.base, root.faultTiming.cores = w.Interval, w.Machine.EffectiveCPUCores
	// Health work has its own single-flight lane: a stuck vendor tool cannot
	// prevent kernel fault detection on the other cards or block heartbeats.
	health := w.quarantineHealth(ctx, root)
	source, since, clock := w.DeviceFaultSource, w.faultSince, w.Clock
	read := func() (KernelLogSnapshot, error) {
		started := clock.Now()
		snapshot, err := source(ctx, since)
		snapshot.started, snapshot.completed = started, clock.Now()
		return snapshot, err
	}
	var snapshot KernelLogSnapshot
	var err error
	if root.deviceFaults.timeout < 0 {
		snapshot, err = root.deviceFaults.runContext(ctx, "kernel", read)
	} else {
		snapshot, err = root.deviceFaults.run("kernel", read)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !errors.Is(err, errCheckpointIOPending) && !snapshot.started.IsZero() {
		root.faultTiming.observe(snapshot.started, snapshot.completed, err != nil || len(snapshot.Lines) > 0)
	}
	if errors.Is(err, errCheckpointIOPending) {
		return nil
	}
	if err != nil || snapshot.BootID == "" {
		clear(w.faultRecovery) // A monitoring gap cannot prove a quiet window.
		clear(w.faultHealth)
		w.faultHealthAfter = w.Clock.Now()
		// Status shows the gap; optional observation cannot disable unrelated devices.
		if w.Machine.FaultMonitorError == "" {
			log.Print("accelerator fault monitor unavailable: kernel log not readable")
		}
		w.Machine.FaultMonitorError = "kernel log not readable"
		return nil
	}
	if w.Machine.FaultMonitorError != "" {
		log.Print("accelerator fault monitor recovered")
	}
	w.Machine.FaultMonitorError = ""
	next := deviceQuarantine{Evidence: map[string][]*FaultEvidence{}, AllocationThrough: map[string]uint64{}, BootID: snapshot.BootID, Seen: map[string]bool{}, Devices: map[string]deviceFault{}, Teardowns: map[string]deviceFault{}, Cleared: map[string]DeviceRecovery{}, RecoveryWindows: map[string]float64{}, Unsupported: map[string][]string{}}
	lines := snapshot.Lines
	if w.faults.BootID == snapshot.BootID {
		maps.Copy(next.AllocationThrough, w.faults.AllocationThrough)
		maps.Copy(next.Evidence, w.faults.Evidence)
		maps.Copy(next.Seen, w.faults.Seen)
		maps.Copy(next.Devices, w.faults.Devices)
		maps.Copy(next.Teardowns, w.faults.Teardowns)
		maps.Copy(next.Cleared, w.faults.Cleared)
		maps.Copy(next.RecoveryWindows, w.faults.RecoveryWindows)
		maps.Copy(next.Unsupported, w.faults.Unsupported)
		for key, clear := range next.Cleared {
			next.RecoveryWindows[key] = max(next.RecoveryWindows[key], clear.ObservationSeconds)
		}
		lines = append(w.faultPending, lines...)
	} else {
		clear(w.faultRecovery)
		clear(w.faultHealth)
		w.faultHealthAfter = w.Clock.Now()
	}
	w.faultPending = nil
	changed := next.BootID != w.faults.BootID
	now := w.Clock.Now()
	observations := map[string]*FaultEvidence{}
	for _, line := range lines {
		sum := sha256.Sum256([]byte(line.ID + "\x00" + line.Message))
		key := hex.EncodeToString(sum[:])
		if next.Seen[key] {
			continue
		}
		for _, d := range w.Machine.Devices {
			match := w.DeviceFaultMatchers[d.Platform]
			if match == nil {
				continue
			}
			reason := match(d, line.Message)
			kind, attribution := allocationKernelFault(d, line.Message)
			if kind == "" && reason != "" {
				kind, attribution = reason, "pci"
			}
			if kind == "" {
				continue
			}
			monotonic, _ := strconv.ParseUint(line.ID, 10, 64)
			if reason == "" && monotonic > 0 && monotonic <= next.AllocationThrough[deviceFaultKey(d)] {
				continue
			}
			if line.evidence == nil {
				line.evidence = map[string]*FaultEvidence{}
			}
			e := line.evidence[deviceFaultKey(d)]
			if e == nil {
				base := observations[deviceFaultKey(d)]
				if base == nil {
					base = w.captureFault(d, line, key, kind, attribution)
					observations[deviceFaultKey(d)] = base
				}
				e = faultAtLine(base, line, key, kind, attribution)
				line.evidence[deviceFaultKey(d)] = e
			}
			next.Evidence[deviceFaultKey(d)] = retainFaultEvidence(next.Evidence[deviceFaultKey(d)], e)
			changed = true
			if reason == "" {
				if monotonic > 0 {
					next.AllocationThrough[deviceFaultKey(d)] = monotonic
				} else {
					next.Seen[key] = true
				}
				continue // Allocation evidence never quarantines.
			}
			// An exit just after the fault may not be recorded yet; decide next scan.
			if !line.At.IsZero() && now.Sub(line.At) < teardownFaultWindow {
				w.faultPending = append(w.faultPending, line)
				continue
			}
			fault := deviceFault{Reason: reason, Event: key, At: line.At}
			if fault.At.IsZero() || fault.At.After(now) {
				fault.At = now
			}
			delete(w.faultRecovery, deviceFaultKey(d))
			delete(health, deviceFaultKey(d)) // Even teardown is a new matching fault.
			next.Seen[key], changed = true, true
			if !line.At.IsZero() && w.control.teardownNear(d.ID, line.At) {
				if next.Devices[deviceFaultKey(d)].Reason != "" || !w.repeatedTeardownFault(deviceFaultKey(d), line.At) {
					next.Teardowns[deviceFaultKey(d)] = fault
					log.Printf("device fault attributed to teardown: device=%s pci=%s reason=%s fault_event=%s", d.ID, d.pciAddress, reason, key)
					continue
				}
				log.Printf("repeated teardown faults escalated to quarantine: device=%s pci=%s count=%d period=%s", d.ID, d.pciAddress, teardownFaultLimit, teardownFaultPeriod)
				fault.RepeatedTeardown = true
			}
			// Further lines from the same reset must retain the escalation fact.
			fault.RepeatedTeardown = fault.RepeatedTeardown || next.Devices[deviceFaultKey(d)].RepeatedTeardown
			next.Devices[deviceFaultKey(d)] = fault
			delete(next.Cleared, deviceFaultKey(d)) // A newer fault invalidates the previous clear.
			delete(next.Unsupported, deviceFaultKey(d))
			delete(w.faultRecovery, deviceFaultKey(d))
		}
	}
	if w.autoClearDeviceFaults(&next, health, snapshot, root.faultTiming.controlPeriod()) {
		changed = true
	}
	// Consume clear requests only after observing the log. Seen hashes retain
	// acknowledged historical faults so a restart/fallback cannot replay them.
	var clears []string
	for _, d := range w.Machine.Devices {
		name := "clear-device-quarantine-" + d.ID + ".json"
		data, err := os.ReadFile(filepath.Join(root.dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var clear deviceFaultClear
		if err := decodeWorkerJSON(data, &clear); err != nil {
			return err
		}
		if clear.Device == deviceFaultKey(d) && clear.BootID == next.BootID && clear.Event == next.Devices[clear.Device].Event {
			delete(next.Devices, clear.Device)
			changed = true
		}
		clears = append(clears, name)
	}
	if changed {
		if err := saveWorkerJSON(root.dir, deviceFaultFile, next); err != nil {
			return err
		}
		for _, d := range w.Machine.Devices {
			before, after := w.faults.Devices[deviceFaultKey(d)], next.Devices[deviceFaultKey(d)]
			if before == after {
				continue
			}
			if after.Reason != "" {
				log.Printf("device quarantined: device=%s pci=%s reason=%s fault_event=%s", d.ID, d.pciAddress, after.Reason, after.Event)
			} else {
				log.Printf("device quarantine cleared: device=%s pci=%s reason=%s fault_event=%s", d.ID, d.pciAddress, before.Reason, before.Event)
			}
		}
	}
	w.faults = &next
	for _, name := range clears {
		if err := os.Remove(filepath.Join(root.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// RequestDeviceQuarantineClear queues a local maintenance request for this exact
// device identity. The running worker remains the sole owner of quarantine state.
func RequestDeviceQuarantineClear(stateDir, id string) error {
	if stateDir == "" || !canonicalDeviceID(id) {
		return errors.New("clear-quarantine requires state-dir and a numeric device ID")
	}
	data, err := os.ReadFile(filepath.Join(stateDir, "devices.json"))
	if err != nil {
		return errors.New("worker inventory unavailable")
	}
	var devices []MachineDevice
	if err := decodeWorkerJSON(data, &devices); err != nil {
		return err
	}
	if err := validateInventory(devices); err != nil {
		return err
	}
	data, err = os.ReadFile(filepath.Join(stateDir, deviceFaultFile))
	if err != nil {
		return errors.New("device quarantine state unavailable")
	}
	var state deviceQuarantine
	if err := decodeWorkerJSON(data, &state); err != nil {
		return err
	}
	for _, d := range devices {
		if d.ID == id {
			fault := state.Devices[deviceFaultKey(d)]
			if fault.Event == "" {
				return errors.New("device is not quarantined")
			}
			return saveWorkerJSON(stateDir, "clear-device-quarantine-"+id+".json", deviceFaultClear{Device: deviceFaultKey(d), BootID: state.BootID, Event: fault.Event})
		}
	}
	return errors.New("device ID is absent from worker inventory")
}

var kernelPCI = regexp.MustCompile(`(?i)\b(?:([0-9a-f]{4,8}):)?([0-9a-f]{2}):([0-9a-f]{2})(?:\.([0-7]))?\b`)

func kernelLineOnDevice(d MachineDevice, line string) bool {
	address := d.pciAddress
	if d.pciFact != nil {
		address = d.pciFact.address
	}
	expected := kernelPCI.FindStringSubmatch(strings.ToLower(address))
	if expected == nil {
		return false
	}
	number := func(s string) uint64 { n, _ := strconv.ParseUint(s, 16, 32); return n }
	for _, m := range kernelPCI.FindAllStringSubmatch(strings.ToLower(line), -1) {
		if (m[1] == "" || number(m[1]) == number(expected[1])) && m[2] == expected[2] && m[3] == expected[3] && (m[4] == expected[4] || m[4] == "" && expected[4] == "0") {
			return true
		}
	}
	return false
}
func intelKernelFault(d MachineDevice, line string) string {
	if !kernelLineOnDevice(d, line) {
		return ""
	}
	s := strings.ToLower(line)
	driver := ""
	if strings.Contains(s, "xe ") {
		driver = "xe"
	} else if strings.Contains(s, "i915 ") {
		driver = "i915"
	}
	if driver == "" {
		return ""
	}
	for _, fault := range []string{"engine memory cat error", "fault response: unsuccessful", "gpu hang", "gpu fault", "vm fault", "gt reset", "resetting chip", "resetting gt", "resetting engine", "engine reset", "schedule disable timeout", "schedule disable failed to respond", "xe device coredump has been created", "trying reset", "reset queued", "reset started", "reset done"} {
		if strings.Contains(s, fault) {
			return "device_fault_" + driver
		}
	}
	return ""
}
func cudaKernelFault(d MachineDevice, line string) string {
	s := strings.ToLower(line)
	if strings.Contains(s, "nvrm: xid") && kernelLineOnDevice(d, line) {
		return "device_fault_nvidia_xid"
	}
	return ""
}

// #595: a fault within this window of a same-device stop request, signal or
// exit is teardown, not a card fault. Observed: 273 ms after TERM and 1 ms
// before the exit was recorded (#300); a card fault quarantine would cut capacity.
const teardownFaultWindow = time.Second

// Released attempts keep evidence past the five-second scan interval and a late
// journal read; the cost is a few timestamps per released accelerator attempt.
const teardownRetention = time.Minute

// The window also matches a card fault that crashes its job within a second, or
// a card that faults at every exit; repeats on one card escalate to quarantine.
// Observed teardown faults were days apart per card (#300), so a third incident
// within an hour is not ordinary teardown. A clear restarts the count.
const (
	teardownFaultLimit  = 3
	teardownFaultPeriod = time.Hour
)

// Fault lines within teardownFaultWindow of the last counted one are the same
// incident: an xe fault and its engine resets arrive within milliseconds.
func (w *Worker) repeatedTeardownFault(device string, at time.Time) bool {
	recent := slices.DeleteFunc(w.faultTeardowns[device], func(t time.Time) bool { return at.Sub(t) > teardownFaultPeriod })
	if n := len(recent); n > 0 && at.Sub(recent[n-1]) <= teardownFaultWindow {
		w.faultTeardowns[device] = recent
		return false
	}
	if recent = append(recent, at); len(recent) >= teardownFaultLimit {
		delete(w.faultTeardowns, device)
		return true
	}
	w.faultTeardowns[device] = recent
	return false
}

type teardownMark struct {
	devices []string
	times   []time.Time
	until   time.Time
}

// Stop request (injected clock) plus recorded signal and exit wall times.
func (p *process) teardownTimes() []time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	var times []time.Time
	add := func(o memberOutcome) {
		for _, s := range o.Signals {
			times = append(times, s.At)
		}
		if o.Exit != nil && !o.Exit.ObservedAt.IsZero() {
			times = append(times, o.Exit.ObservedAt)
		}
	}
	if !p.stopRequested.IsZero() {
		times = append(times, p.stopRequested)
	}
	add(p.outcome)
	if p.native != nil {
		for _, m := range p.native.members {
			add(m.memberOutcome)
		}
	}
	return times
}

// Caller holds c.mu.
func (c *processControl) pruneTeardowns() {
	now := c.clock.Now()
	c.retired = slices.DeleteFunc(c.retired, func(m teardownMark) bool { return now.After(m.until) })
}

func (c *processControl) teardownNear(device string, at time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneTeardowns()
	marks := slices.Clone(c.retired)
	for _, p := range c.processes {
		if !p.job.Spec.Needs.cpuOnly() {
			marks = append(marks, teardownMark{devices: p.job.AllocatedDevices, times: p.teardownTimes()})
		}
	}
	for _, m := range marks {
		if !slices.Contains(m.devices, device) {
			continue
		}
		for _, t := range m.times {
			if d := at.Sub(t); d <= teardownFaultWindow && d >= -teardownFaultWindow {
				return true
			}
		}
	}
	return false
}

// Called inside Checkin's existing transaction; all kernel I/O stays in workers.
func (q *Queue) quarantineDeviceAttempts(m *Machine) {
	for _, j := range q.liveJobs() {
		if j.Machine != m.ID || j.Session != m.Session || j.State != "running" || j.Spec.Needs.cpuOnly() || cancelled(j) {
			continue
		}
		if !slices.ContainsFunc(m.Devices, func(d MachineDevice) bool {
			return deviceFaultReason(d.Reason) && slices.Contains(j.AllocatedDevices, d.ID)
		}) {
			continue
		}
		q.touchJob(j.ID)
		q.retainRelease(j)
		if q.launchCapacityPending(j) {
			// Diagnostic verification may still be outside the queue lock. A
			// later device fault cannot turn its known refusal into a loss.
			for i := range j.Copies {
				j.Copies[i].Recoverable = false
			}
			continue
		}
		j.Losses++
		q.chargeLoss(j)
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
		losses := []DeviceFaultLoss{}
		for _, d := range m.Devices {
			if deviceFaultReason(d.Reason) && slices.Contains(j.AllocatedDevices, d.ID) {
				losses = append(losses, DeviceFaultLoss{Host: hostID(m), Platform: d.Platform, Devices: []string{d.Identity}, At: q.clock.Now()})
			}
		}
		attempt := j.Attempt
		q.requeue(j, "accelerator driver fault; device quarantined", true)
		for i := range j.AttemptHistory {
			if h := &j.AttemptHistory[i]; h.Attempt == attempt && h.Machine == m.ID && h.Session == m.Session && len(losses) > 0 {
				loss := losses[0] // One execution target has one backend.
				for _, other := range losses[1:] {
					loss.Devices = append(loss.Devices, other.Devices...)
				}
				h.DeviceFault = &loss
			}
		}
	}
}
