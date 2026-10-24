package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MachineDevice is one accelerator's capacity, never an additional host pool.
type MachineDevice struct {
	pciFact              *pciAccelerator    // Physical facts from the shared probe.
	pciAddress           string             // Local probe fact; never part of the queue protocol.
	Attempts             []Execution        `json:"attempts,omitempty"`
	Models               []ModelSlot        `json:"models,omitempty"`
	Releases             []HostRelease      `json:"releases,omitempty"`
	ExtensionFingerprint string             `json:"extension_fingerprint,omitempty"`
	ExtensionReports     []ExtensionReport  `json:"extension_reports,omitempty"`
	ID                   string             `json:"id"`
	Platform             string             `json:"platform"`
	Identity             string             `json:"identity"`
	Fingerprint          string             `json:"fingerprint"`
	Selector             string             `json:"selector"`
	MemoryMode           string             `json:"memory_mode"`
	TotalMemoryGB        float64            `json:"total_memory_gb"`
	ReserveMemoryGB      float64            `json:"reserve_memory_gb"`
	BudgetGB             float64            `json:"budget_gb"`
	FreeMemoryGB         float64            `json:"free_memory_gb"`
	MemoryMeasured       bool               `json:"memory_measured,omitempty"`
	UnifiedMemory        bool               `json:"unified_memory,omitempty"`
	Reason               string             `json:"reason,omitempty"`
	CapabilityProfile    *CapabilityProfile `json:"capability_profile,omitempty"`
}

func singleDeviceMask(device string) error {
	if strings.ContainsAny(device, ",;") {
		return reject("gpu_count_unsupported", errors.New("multiple GPU devices are deferred to #105"))
	}
	return nil
}
func canonicalDeviceID(id string) bool {
	n, err := strconv.ParseUint(id, 10, 32)
	return err == nil && strconv.FormatUint(n, 10) == id
}
func validateInventory(devices []MachineDevice) error {
	ids, identities := map[string]bool{}, map[string]bool{}
	for _, d := range devices {
		if !canonicalDeviceID(d.ID) || ids[d.ID] || d.Platform == "cpu" || !validGPU(d.Platform) || d.Identity == "" || identities[d.Platform+":"+d.Identity] || d.Selector == "" {
			return currentProtocolError("invalid or ambiguous machine device inventory")
		}
		if err := singleDeviceMask(d.Selector); err != nil {
			return err
		}
		for _, value := range []float64{d.TotalMemoryGB, d.ReserveMemoryGB, d.BudgetGB, d.FreeMemoryGB} {
			if !validMemoryValue(value) {
				return currentProtocolError("invalid device memory evidence")
			}
		}
		ids[d.ID], identities[d.Platform+":"+d.Identity] = true, true
	}
	return nil
}

func (d MachineDevice) machine(base Machine) Machine {
	base.GPU, base.Device = d.Platform, d.ID
	base.ExtensionFingerprint, base.ExtensionReports = d.ExtensionFingerprint, d.ExtensionReports
	// The old planner's total is its effective capacity. Physical totals stay in
	// the inventory; neither telemetry nor scalar compatibility adds allowance.
	base.FreeMemoryGB, base.TotalMemoryGB, base.ReservedMemoryGB = min(d.FreeMemoryGB, d.BudgetGB), d.BudgetGB, d.ReserveMemoryGB
	base.UnifiedMemory, base.MemoryMeasured, base.MemoryError = d.UnifiedMemory, d.MemoryMeasured, d.Reason
	if d.CapabilityProfile != nil {
		profile := *d.CapabilityProfile
		if shared := base.CapabilityProfile; shared != nil {
			profile.LogicalCPUs = shared.LogicalCPUs
			profile.SystemMemoryBytes = shared.SystemMemoryBytes
			profile.OS, profile.Arch = shared.OS, shared.Arch
			profile.Availability = shared.Availability
		}
		base.CapabilityProfile = &profile
	}
	if d.UnifiedMemory {
		// Uncapped unified BudgetGB follows live free RAM. Keep that admission
		// telemetry out of the capacity shared by observation keys and curves.
		capacity := d.TotalMemoryGB
		if capacity == 0 && base.CapabilityProfile != nil {
			capacity = float64(base.CapabilityProfile.SystemMemoryBytes) / (1 << 30)
		}
		base.TotalMemoryGB = max(0, capacity-d.ReserveMemoryGB)
	}
	return base
}

// A worker allowance can include several cards; each attempt still gets one.
func deviceAllowance(value string) ([]string, error) {
	if value == "" {
		return nil, nil
	}
	out := []string{}
	for _, selector := range strings.Split(value, ",") {
		selector = strings.TrimSpace(selector)
		if selector == "" || strings.ContainsAny(selector, ";\r\n") || slices.Contains(out, selector) {
			return nil, reject("gpu_selector_invalid", errors.New("device allowance requires distinct nonempty selectors"))
		}
		out = append(out, selector)
	}
	return out, nil
}

func (w *Worker) initInventory(ctx context.Context, s *workerStore) error {
	var err error
	w.inventoryAllowance, err = deviceAllowance(w.Machine.Device)
	if err != nil {
		return err
	}
	if w.InventoryProbe == nil {
		if w.Machine.GPU == "fake" {
			d := MachineDevice{ID: "0", Platform: "fake", Identity: "fake-0", Fingerprint: "fake", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: w.Machine.FreeMemoryGB, BudgetGB: w.Machine.FreeMemoryGB, FreeMemoryGB: w.Machine.FreeMemoryGB}
			w.InventoryProbe = func(context.Context) ([]MachineDevice, error) { return []MachineDevice{d}, nil }
		} else {
			// Capture operator configuration before any scalar device projection.
			w.InventoryProbe = w.localInventory
		}
	}
	devices := slices.Clone(w.Machine.Devices)
	var probeErr error
	if devices == nil {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		devices, probeErr = w.InventoryProbe(probeCtx)
		cancel()
	}
	var previous []MachineDevice
	data, err := os.ReadFile(filepath.Join(s.dir, "devices.json"))
	if err == nil {
		if err = decodeWorkerJSON(data, &previous); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := validateInventory(previous); err != nil {
		return err
	}
	w.Machine.Devices = previous
	if err := w.refreshInventory(ctx, devices, probeErr, s); err != nil {
		return err
	}
	if w.inventoryReason == "device_selector_unmatched" && len(w.Machine.Devices) == 0 {
		return reject(w.inventoryReason, errors.New(rejectionMessages[w.inventoryReason]))
	}
	for _, d := range w.Machine.Devices {
		if d.Reason == "" {
			w.Machine = d.machine(w.Machine)
			w.DeviceBinding = d.Selector
			break
		}
	}
	return nil
}

// Probe health is current evidence, never durable identity. Persist the saved
// mapping, but recompute all withholding on every observation (including reopen).
func (w *Worker) refreshInventory(ctx context.Context, observed []MachineDevice, probeErr error, s *workerStore) error {
	// A heartbeat's short I/O wait is not the probe's deadline. Keep the last
	// completed observation while it runs; apply its success or failure when
	// collected. Launch verification still probes the selected device directly.
	if errors.Is(probeErr, errCheckpointIOPending) {
		return nil
	}
	var partial *acceleratorProbeError
	familyFailure := errors.As(probeErr, &partial)
	if probeErr == nil || familyFailure {
		if err := validateInventory(observed); err != nil {
			probeErr, familyFailure = err, false
		}
	}
	w.inventoryReason = ""
	var devices []MachineDevice
	if !familyFailure && (probeErr != nil || len(observed) == 0 && w.Machine.GPU != "cpu") {
		w.inventoryReason = "device_probe_unavailable"
		devices = slices.Clone(w.Machine.Devices)
		for i := range devices {
			devices[i].Reason = w.inventoryReason
			devices[i].FreeMemoryGB = 0
		}
	} else {
		devices = reconcileDevices(w.Machine.Devices, observed)
		if familyFailure {
			w.inventoryReason = partial.Error()
			for i := range devices {
				if partial.failed(devices[i].Platform) {
					devices[i].Reason, devices[i].FreeMemoryGB = "device_probe_unavailable", 0
				}
			}
		}
	}
	matchedSelectors := map[string]bool{}
	devices = slices.DeleteFunc(devices, func(d MachineDevice) bool {
		if len(w.inventoryAllowance) == 0 {
			return false
		}
		allowed := false
		for _, selector := range w.inventoryAllowance {
			matches := selector == d.Identity || selector == d.Selector
			if canonicalDeviceID(selector) {
				matches = selector == d.ID
			}
			if matches {
				matchedSelectors[selector], allowed = true, true
			}
		}
		return !allowed
	})
	if devices == nil {
		devices = []MachineDevice{}
	}
	if probeErr == nil && w.Machine.GPU != "cpu" && len(matchedSelectors) < len(w.inventoryAllowance) {
		w.inventoryReason = "device_selector_unmatched"
	}
	// Saved per-device settings are an independent expectation even when
	// --device was omitted (whole-machine installation).
	for _, setting := range w.DeviceSettings {
		found := false
		for _, d := range observed {
			if setting.Selector == d.Selector || setting.Selector == d.Identity || setting.Selector == d.ID {
				found = true
				break
			}
		}
		if !found && w.inventoryReason == "" {
			w.inventoryReason = "device_selector_unmatched"
		}
	}
	for i := range devices {
		d := &devices[i]
		if d.UnifiedMemory && d.MemoryMeasured && d.BudgetGB == 0 && d.Reason == "" {
			d.BudgetGB = d.FreeMemoryGB
		}
		if d.BudgetGB <= 0 && d.Reason == "" {
			d.Reason = "device_budget_unavailable"
		}
		if d.CapabilityProfile == nil {
			m := d.machine(w.Machine)
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			profile := workerProfileProbe().Detect(probeCtx, m, s.dir, w.Clock.Now())
			cancel()
			applyFixedAcceleratorMemory(&profile, m, d.MemoryMode == "fixed")
			d.CapabilityProfile = &profile
		}
	}
	if err := validateInventory(devices); err != nil {
		return err
	}
	if !slices.EqualFunc(w.Machine.Devices, devices, func(a, b MachineDevice) bool {
		return a.ID == b.ID && a.Identity == b.Identity && a.Selector == b.Selector && a.Reason == b.Reason && a.BudgetGB == b.BudgetGB && a.Fingerprint == b.Fingerprint
	}) {
		if err := saveWorkerJSON(s.dir, "devices.json", devices); err != nil {
			return err
		}
	}
	w.Machine.Devices = devices
	w.applyDeviceQuarantine()
	return nil
}

// Verify both the full physical mapping and the selected runtime view before
// each member exec. Probes run with private env; the worker env never changes.
func (w Worker) verifyDevice() error {
	if w.cpuIsolation {
		if w.CPUIsolationProbe != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := w.CPUIsolationProbe(ctx); err != nil {
				return reject("cpu_accelerator_isolation_unavailable", errors.New("runtime select-none visibility is unqualified"))
			}
			return nil
		}
		return w.verifyCPUIsolation()
	}
	if w.selectedDevice == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	devices, err := w.InventoryProbe(ctx)
	expected := *w.selectedDevice
	matches := 0
	currentReason := ""
	for _, d := range devices {
		if d.Platform == expected.Platform && d.Selector == expected.Selector && d.Identity == expected.Identity {
			matches++
			currentReason = d.Reason
		}
	}
	if err != nil {
		var partial *acceleratorProbeError
		if !errors.As(err, &partial) || partial.failed(expected.Platform) {
			return reject("device_probe_unavailable", errors.New("device inventory probe unavailable before launch"))
		}
	}
	if expected.Reason != "" {
		return reject(expected.Reason, errors.New(rejectionMessages[expected.Reason]))
	}
	if matches != 1 {
		return reject("device_identity_mismatch", errors.New("device runtime mapping no longer matches saved identity"))
	}
	if currentReason != "" {
		return reject(currentReason, errors.New(rejectionMessages[currentReason]))
	}
	return nil
}

func (w Worker) forAttempt(j Job) (Worker, error) {
	if j.Spec.Needs.cpuOnly() {
		w.Machine.GPU, w.Machine.Device, w.DeviceBinding = "cpu", "", ""
		if w.Machine.CPUExtensionFingerprint != "" {
			w.Machine.ExtensionFingerprint = w.Machine.CPUExtensionFingerprint
		}
		w.extensionFingerprint = w.Machine.ExtensionFingerprint
		w.MemorySource = nil
		w.selectedDevice = nil
		w.cpuIsolation = true
		if w.CPUIsolationProbe != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := w.CPUIsolationProbe(ctx); err != nil {
				return w, reject("cpu_accelerator_isolation_unavailable", errors.New("runtime select-none visibility is unqualified"))
			}
		} else if err := w.verifyCPUIsolation(); err != nil {
			return w, err
		}
		return w, nil
	}
	ids := j.AllocatedDevices
	if len(ids) > 1 {
		return w, reject("gpu_count_unsupported", errors.New("one GPU per attempt is supported"))
	}
	if len(ids) == 0 {
		// M2's unchanged one-attempt queue selects its compatibility target.
		ids = []string{w.Machine.Device}
		if ids[0] == "" && len(w.Machine.Devices) == 1 {
			ids[0] = w.Machine.Devices[0].ID
		}
	}
	for _, d := range w.Machine.Devices {
		if d.ID != ids[0] {
			continue
		}
		w.selectedDevice = &d
		w.Machine = d.machine(w.Machine)
		w.extensionFingerprint = w.Machine.ExtensionFingerprint
		w.DeviceBinding = d.Selector
		if err := w.verifyDevice(); err != nil {
			return w, err
		}
		budget, reserve := d.BudgetGB, 0.0
		if d.UnifiedMemory {
			budget, reserve = d.BudgetGB+d.ReserveMemoryGB, d.ReserveMemoryGB
		}
		source, err := NewDeviceMemorySource(d.Platform, d.Selector, budget, reserve, d.MemoryMode)
		if err != nil {
			return w, err
		}
		if d.Platform != "fake" || len(w.Machine.Devices) != 1 || w.MemorySource == nil {
			w.MemorySource, w.MemoryMeasured = source, d.MemoryMeasured
		}
		return w, nil
	}
	return w, reject("device_identity_mismatch", errors.New("assigned device is not in the machine inventory"))
}

func (w Worker) localInventory(ctx context.Context) ([]MachineDevice, error) {
	return w.localInventoryWithProbe(ctx, workerMachineProbe())
}

func (w Worker) localInventoryWithProbe(ctx context.Context, probe MachineProbe) ([]MachineDevice, error) {
	probe = probe.withFilesystem()
	if w.Machine.GPU == "cpu" {
		return []MachineDevice{}, nil
	}
	physical, err := probe.Accelerators(ctx)
	var partial *acceleratorProbeError
	if err != nil && !errors.As(err, &partial) {
		return nil, err
	}
	for i := range physical {
		d := &physical[i]
		if d.Reason != "" {
			continue
		}
		if (d.Platform == "cuda" || d.Platform == "xpu") && (d.pciFact == nil || !d.pciFact.topologyKnown) {
			d.Reason = "device_memory_unavailable"
			continue
		}
		configured := w
		configuredMode := "auto"
		if len(w.DeviceSettings) > 0 {
			found := false
			for _, setting := range w.DeviceSettings {
				if setting.Selector == d.Selector || setting.Selector == d.Identity || setting.Selector == d.ID {
					if setting.Detection.Platform != d.Platform {
						return nil, migrationError("machine_migration_conflict", "device platform changed")
					}
					configured.Machine.FreeMemoryGB = setting.Detection.MemoryGB
					configured.Machine.ReservedMemoryGB = setting.Detection.ReserveGB
					configured.FixedAcceleratorMemory = setting.Detection.MemoryMode == "fixed"
					configuredMode = setting.Detection.MemoryMode
					found = true
					break
				}
			}
			if !found {
				d.Reason = "device_budget_unavailable"
				continue
			}
		}
		d.MemoryMode = "auto"
		if len(w.DeviceSettings) > 0 && configured.FixedAcceleratorMemory {
			d.MemoryMode = "fixed"
			d.TotalMemoryGB = configured.Machine.FreeMemoryGB
			d.ReserveMemoryGB = configured.Machine.ReservedMemoryGB
			d.BudgetGB = max(0, d.TotalMemoryGB-d.ReserveMemoryGB)
		} else if d.Platform == "xpu" && configured.FixedAcceleratorMemory {
			d.MemoryMode, d.BudgetGB = "fixed", configured.Machine.FreeMemoryGB
			d.TotalMemoryGB, d.ReserveMemoryGB = configured.Machine.FreeMemoryGB+configured.Machine.ReservedMemoryGB, configured.Machine.ReservedMemoryGB
		} else {
			detected, e := probe.detectAccelerator(ctx, *d)
			if e != nil {
				d.Reason = "device_memory_unavailable"
				continue
			}
			d.MemoryMode, d.TotalMemoryGB, d.ReserveMemoryGB = detected.MemoryMode, detected.MemoryGB, detected.ReserveGB
			if len(w.DeviceSettings) > 0 {
				d.ReserveMemoryGB = configured.Machine.ReservedMemoryGB
				if configuredMode != "auto" && detected.MemoryMode != "unified" {
					d.MemoryMode = configuredMode
				}
				capacity := detected.MemoryGB
				if configured.Machine.FreeMemoryGB > 0 {
					capacity = min(capacity, configured.Machine.FreeMemoryGB)
				}
				d.BudgetGB = max(0, capacity-d.ReserveMemoryGB)
			} else {
				d.BudgetGB = max(0, detected.MemoryGB-detected.ReserveGB)
				if configured.Machine.FreeMemoryGB > 0 {
					d.BudgetGB = min(d.BudgetGB, configured.Machine.FreeMemoryGB)
				}
			}
		}
		if d.pciFact != nil {
			if d.pciFact.integrated {
				d.MemoryMode = "unified"
			} else if d.MemoryMode == "unified" {
				d.Reason = "device_memory_unavailable"
				continue
			}
		}
		d.UnifiedMemory = d.MemoryMode == "unified"
		if d.UnifiedMemory {
			// Zero is an uncapped measured allowance, as on GB10 and Metal.
			d.TotalMemoryGB = configured.Machine.FreeMemoryGB
			d.BudgetGB = max(0, configured.Machine.FreeMemoryGB-d.ReserveMemoryGB)
		}
		budget, reserve := d.BudgetGB, 0.0
		if d.UnifiedMemory {
			budget, reserve = configured.Machine.FreeMemoryGB, d.ReserveMemoryGB
		}
		if d.MemoryMode == "fixed" && budget <= 0 || d.UnifiedMemory && budget > 0 && budget <= reserve {
			d.Reason = "device_budget_unavailable"
			continue
		}
		source, e := newDeviceMemorySource(d.Platform, d.Selector, budget, reserve, d.MemoryMode, probe.OS, probe.ReadFile, probe.Command)
		if e != nil {
			d.Reason = "device_memory_unavailable"
			continue
		}
		d.FreeMemoryGB, e = source(ctx)
		if e != nil {
			d.Reason = "device_memory_unavailable"
			d.FreeMemoryGB = 0
		}
		d.MemoryMeasured = d.MemoryMode != "fixed" && d.Platform != "fake"
		if d.UnifiedMemory && configured.Machine.FreeMemoryGB == 0 {
			d.BudgetGB = d.FreeMemoryGB
		}
	}
	return physical, err
}

// M2 projects exactly one inventory component into the existing planner. M3
// enumerates targets; scalar diagnostics never add another allocation pool.
func inventoryTarget(m *Machine) error {
	fingerprint, reports := m.ExtensionFingerprint, m.ExtensionReports
	defer func() { m.ExtensionFingerprint, m.ExtensionReports = fingerprint, reports }()
	if m.Devices == nil {
		return nil
	}
	if err := validateInventory(m.Devices); err != nil {
		return err
	}
	for _, d := range m.Devices {
		if d.Reason == "" {
			*m = d.machine(*m)
			return nil
		}
	}
	// Preserve a CPU worker's diagnostic selector for qualification matching;
	// an empty inventory still contributes no accelerator capacity.
	if m.GPU != "cpu" {
		m.Device = ""
	}
	if len(m.Devices) > 0 {
		m.MemoryError = m.Devices[0].Reason
	}
	m.GPU, m.TotalMemoryGB, m.FreeMemoryGB = "cpu", 0, 0
	return nil
}

func (w Worker) deviceEnvironment() (map[string]string, error) {
	selector := w.Machine.Device
	if w.DeviceBinding != "" {
		selector = w.DeviceBinding
	}
	m, err := WithModelDevice(Model{}, w.Machine.GPU, selector)
	return m.DeviceEnv, err
}

// Preserve IDs by identity. A changed XPU ordinal is withheld, never followed.
func reconcileDevices(previous, current []MachineDevice) []MachineDevice {
	out := []MachineDevice{}
	used := map[string]bool{}
	matched := map[int]bool{}
	for _, old := range previous {
		used[old.ID] = true
		found := false
		for i, d := range current {
			if d.Platform != old.Platform || d.Identity != old.Identity {
				continue
			}
			found = true
			matched[i] = true
			d.ID = old.ID
			if d.CapabilityProfile == nil && d.Fingerprint == old.Fingerprint {
				d.CapabilityProfile = old.CapabilityProfile
			}
			if d.Selector != old.Selector {
				old.Reason = "device_identity_mismatch"
				d = old
			}
			out = append(out, d)
			break
		}
		if !found {
			old.Reason = "device_identity_mismatch"
			out = append(out, old)
		}
	}
	for i, d := range current {
		if matched[i] {
			continue
		}
		// A replacement at an owned runtime selector cannot become a new target.
		if slices.ContainsFunc(previous, func(old MachineDevice) bool { return old.Platform == d.Platform && old.Selector == d.Selector }) {
			continue
		}
		if used[d.ID] {
			// Discovery IDs are ordinals; an addition must not steal a saved ID.
			for next := 0; ; next++ {
				id := strconv.Itoa(next)
				if !used[id] {
					d.ID = id
					break
				}
			}
		}
		used[d.ID] = true
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b MachineDevice) int { an, _ := strconv.Atoi(a.ID); bn, _ := strconv.Atoi(b.ID); return an - bn })
	return out
}
