package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// KernelLogLine has a stable, boot-local identity (kernel monotonic timestamp).
// Raw messages are inspected locally and never persisted or sent to the queue.
type KernelLogLine struct{ ID, Message string }
type KernelLogSnapshot struct {
	BootID string
	Lines  []KernelLogLine
}
type DeviceFaultSource func(context.Context) (KernelLogSnapshot, error)
type DeviceFaultMatcher func(MachineDevice, string) string

const deviceFaultFile = "device-quarantine.json"

var workerDeviceFaultSource = localDeviceFaultSource

type deviceQuarantine struct {
	BootID  string                 `json:"boot_id"`
	Seen    map[string]bool        `json:"seen"`
	Devices map[string]deviceFault `json:"devices"`
}

type deviceFault struct {
	Reason string `json:"reason"`
	Event  string `json:"event"`
}
type deviceFaultClear struct {
	Device string `json:"device"`
	BootID string `json:"boot_id"`
	Event  string `json:"event"`
}

func deviceFaultKey(d MachineDevice) string { return d.Platform + ":" + d.Identity }
func deviceFaultReason(s string) bool {
	return s == "device_fault_xe" || s == "device_fault_i915" || s == "device_fault_nvidia_xid"
}

func (w *Worker) initDeviceFaults(ctx context.Context, root *workerStore) error {
	w.faults = &deviceQuarantine{}
	data, err := os.ReadFile(filepath.Join(root.dir, deviceFaultFile))
	if err == nil {
		err = decodeWorkerJSON(data, w.faults)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if w.DeviceFaultMatchers == nil {
		w.DeviceFaultMatchers = map[string]DeviceFaultMatcher{"xpu": intelKernelFault, "cuda": cudaKernelFault}
	}
	if w.DeviceFaultSource == nil {
		w.DeviceFaultSource = workerDeviceFaultSource()
	}
	// Finish the initial boot scan before offering inventory. Later scans use
	// the short heartbeat I/O budget and retain late completion.
	previousBudget := root.deviceFaults.timeout
	root.deviceFaults.timeout = 2100 * time.Millisecond
	defer func() { root.deviceFaults.timeout = previousBudget }()
	return w.refreshDeviceFaults(ctx, root)
}

func (w *Worker) applyDeviceQuarantine() {
	if w.faults == nil {
		return
	}
	for i := range w.Machine.Devices {
		d := &w.Machine.Devices[i]
		if fault := w.faults.Devices[deviceFaultKey(*d)]; fault.Reason != "" {
			d.Reason, d.FreeMemoryGB = fault.Reason, 0
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
	source := w.DeviceFaultSource
	snapshot, err := root.deviceFaults.run("kernel", func() (KernelLogSnapshot, error) {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return source(readCtx)
	})
	if errors.Is(err, errCheckpointIOPending) {
		return nil
	}
	if err != nil || snapshot.BootID == "" {
		if !w.faultMonitorUnavailable {
			log.Print("accelerator fault monitor unavailable: kernel log not readable")
		}
		w.faultMonitorUnavailable = true
		return nil // Optional observation cannot disable unrelated devices.
	}
	w.faultMonitorUnavailable = false
	next := deviceQuarantine{BootID: snapshot.BootID, Seen: map[string]bool{}, Devices: map[string]deviceFault{}}
	if w.faults.BootID == snapshot.BootID {
		for k, v := range w.faults.Seen {
			next.Seen[k] = v
		}
		for k, v := range w.faults.Devices {
			next.Devices[k] = v
		}
	}
	changed := next.BootID != w.faults.BootID
	for _, line := range snapshot.Lines {
		sum := sha256.Sum256([]byte(line.ID + "\x00" + line.Message))
		key := hex.EncodeToString(sum[:])
		if next.Seen[key] {
			continue
		}
		for _, d := range w.Machine.Devices {
			if match := w.DeviceFaultMatchers[d.Platform]; match != nil {
				if reason := match(d, line.Message); reason != "" {
					next.Devices[deviceFaultKey(d)], next.Seen[key], changed = deviceFault{Reason: reason, Event: key}, true, true
				}
			}
		}
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
	for _, fault := range []string{"engine memory cat error", "fault response: unsuccessful", "gpu hang", "gpu fault", "vm fault", "gt reset", "resetting chip", "resetting gt", "resetting engine"} {
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

// Called inside Checkin's existing transaction; all kernel I/O stays in workers.
func (q *Queue) quarantineDeviceAttempts(m *Machine) {
	for _, j := range q.s.Jobs {
		if j.Machine != m.ID || j.Session != m.Session || j.State != "running" || j.Spec.Needs.cpuOnly() || cancelled(j) {
			continue
		}
		if !slices.ContainsFunc(m.Devices, func(d MachineDevice) bool {
			return deviceFaultReason(d.Reason) && slices.Contains(j.AllocatedDevices, d.ID)
		}) {
			continue
		}
		q.retainRelease(j)
		j.Losses++
		q.chargeLoss(j)
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
		q.requeue(j, "accelerator driver fault; device quarantined", true)
	}
}
