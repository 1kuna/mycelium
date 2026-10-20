package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MachineDevice is one accelerator's capacity, never an additional host pool.
type MachineDevice struct {
	ID                string             `json:"id"`
	Platform          string             `json:"platform"`
	Identity          string             `json:"identity"`
	Fingerprint       string             `json:"fingerprint"`
	Selector          string             `json:"selector"`
	MemoryMode        string             `json:"memory_mode"`
	TotalMemoryGB     float64            `json:"total_memory_gb"`
	ReserveMemoryGB   float64            `json:"reserve_memory_gb"`
	BudgetGB          float64            `json:"budget_gb"`
	FreeMemoryGB      float64            `json:"free_memory_gb"`
	MemoryMeasured    bool               `json:"memory_measured,omitempty"`
	UnifiedMemory     bool               `json:"unified_memory,omitempty"`
	Reason            string             `json:"reason,omitempty"`
	CapabilityProfile *CapabilityProfile `json:"capability_profile,omitempty"`
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
	return base
}

func (w *Worker) initInventory(ctx context.Context, s *workerStore) error {
	if err := singleDeviceMask(w.Machine.Device); err != nil {
		return err
	}
	if w.InventoryProbe == nil {
		if w.Machine.GPU == "fake" {
			w.InventoryProbe = func(context.Context) ([]MachineDevice, error) {
				return []MachineDevice{{ID: "0", Platform: "fake", Identity: "fake-0", Fingerprint: "fake", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: w.Machine.FreeMemoryGB, BudgetGB: w.Machine.FreeMemoryGB, FreeMemoryGB: w.Machine.FreeMemoryGB}}, nil
			}
		} else {
			w.InventoryProbe = w.localInventory
		}
	}
	devices := slices.Clone(w.Machine.Devices)
	if devices == nil {
		var err error
		devices, err = w.InventoryProbe(ctx)
		if err != nil {
			return err
		}
	}
	if devices == nil {
		devices = []MachineDevice{}
	}
	if err := validateInventory(devices); err != nil {
		return err
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
	devices = reconcileDevices(previous, devices)
	if err := validateInventory(devices); err != nil {
		return err
	}
	for i := range devices {
		d := &devices[i]
		if d.BudgetGB <= 0 || d.Fingerprint == "" {
			d.Reason = "device_identity_mismatch"
		}
		if d.CapabilityProfile == nil {
			m := d.machine(w.Machine)
			profile := LocalProfileProbe().Detect(ctx, m, s.dir, w.Clock.Now())
			applyFixedAcceleratorMemory(&profile, m, d.MemoryMode == "fixed")
			d.CapabilityProfile = &profile
		}
	}
	w.Machine.Devices = devices
	for _, d := range devices {
		if w.Machine.Device == "" || w.Machine.Device == d.ID || w.DeviceBinding == d.Selector {
			w.Machine = d.machine(w.Machine)
			w.DeviceBinding = d.Selector
			break
		}
	}
	return saveWorkerJSON(s.dir, "devices.json", devices)
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
	for _, d := range devices {
		if d.Platform == expected.Platform && d.Selector == expected.Selector && d.Identity == expected.Identity && d.Fingerprint == expected.Fingerprint {
			matches++
		}
	}
	if err != nil || matches != 1 || expected.Reason != "" {
		return reject("device_identity_mismatch", errors.New("device runtime mapping no longer matches saved identity"))
	}
	return nil
}

func (w Worker) forAttempt(j Job) (Worker, error) {
	if j.Spec.Needs.cpuOnly() {
		w.Machine.GPU, w.Machine.Device, w.DeviceBinding = "cpu", "", ""
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
		w.DeviceBinding = d.Selector
		source, err := NewDeviceMemorySource(d.Platform, d.Selector, d.BudgetGB, 0, d.MemoryMode)
		if err != nil {
			return w, err
		}
		w.MemorySource, w.MemoryMeasured = source, d.MemoryMeasured
		if err := w.verifyDevice(); err != nil {
			return w, err
		}
		return w, nil
	}
	return w, reject("device_identity_mismatch", errors.New("assigned device is not in the machine inventory"))
}

func (w Worker) verifyCPUIsolation() error {
	probe := LocalMachineProbe()
	devices := w.Machine.Devices
	simulated := len(devices) > 0
	for _, d := range devices {
		simulated = simulated && d.Platform == "fake"
	}
	if !simulated && len(devices) == 0 {
		if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
			return reject("cpu_accelerator_isolation_unavailable", errors.New("Metal has no qualified select-none control"))
		}
		if _, err := os.Stat("/dev/nvidiactl"); err == nil {
			devices = append(devices, MachineDevice{Platform: "cuda"})
		}
		if nodes, _ := filepath.Glob("/dev/dri/renderD*"); len(nodes) > 0 {
			devices = append(devices, MachineDevice{Platform: "xpu"})
		}
	}
	for _, d := range devices {
		switch d.Platform {
		case "fake":
		case "cuda":
			env, _ := WithModelDevice(Model{}, "cpu", "")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			b, err := probe.CommandEnv(ctx, env.DeviceEnv, "python3", "-c", `import ctypes as c; r=c.CDLL("libcudart.so"); n=c.c_int(); e=r.cudaGetDeviceCount(c.byref(n)); assert e in (0,100) and n.value==0; print("none")`)
			cancel()
			if err != nil || strings.TrimSpace(string(b)) != "none" {
				return reject("cpu_accelerator_isolation_unavailable", errors.New("CUDA select-none runtime enumeration failed"))
			}
		case "xpu":
			env, _ := WithModelDevice(Model{}, "cpu", "")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			b, err := probe.CommandEnv(ctx, env.DeviceEnv, "sycl-ls")
			cancel()
			if err != nil || strings.Contains(strings.ToLower(string(b)), "gpu") {
				return reject("cpu_accelerator_isolation_unavailable", errors.New("XPU select-none runtime enumeration failed"))
			}
		default:
			return reject("cpu_accelerator_isolation_unavailable", errors.New("accelerator runtime has no qualified select-none control"))
		}
	}
	return nil
}

func (w Worker) localInventory(ctx context.Context) ([]MachineDevice, error) {
	if w.Machine.GPU == "cpu" {
		return []MachineDevice{}, nil
	}
	probe := LocalMachineProbe()
	physical, err := probe.Accelerators(ctx)
	if err != nil {
		return nil, err
	}
	for i := range physical {
		d := &physical[i]
		d.MemoryMode = "auto"
		if d.Platform == "xpu" && w.FixedAcceleratorMemory {
			d.MemoryMode, d.BudgetGB = "fixed", w.Machine.FreeMemoryGB
			d.TotalMemoryGB, d.ReserveMemoryGB = w.Machine.FreeMemoryGB+w.Machine.ReservedMemoryGB, w.Machine.ReservedMemoryGB
		} else {
			selectedProbe := probe
			if d.Platform == "xpu" {
				run := probe.Command
				selectedProbe.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if name == "nvidia-smi" {
						return nil, ErrNoGPU
					}
					return run(ctx, name, args...)
				}
			}
			detected, e := selectedProbe.DetectDevice(ctx, d.Selector)
			if e != nil {
				return nil, e
			}
			d.MemoryMode, d.TotalMemoryGB, d.ReserveMemoryGB = detected.MemoryMode, detected.MemoryGB, detected.ReserveGB
			d.BudgetGB = max(0, detected.MemoryGB-detected.ReserveGB)
			if w.Machine.FreeMemoryGB > 0 {
				d.BudgetGB = min(d.BudgetGB, w.Machine.FreeMemoryGB)
			}
		}
		d.UnifiedMemory = d.MemoryMode == "unified"
		if d.UnifiedMemory {
			d.BudgetGB = w.Machine.FreeMemoryGB
		}
		source, e := NewDeviceMemorySource(d.Platform, d.Selector, d.BudgetGB, 0, d.MemoryMode)
		if e != nil {
			return nil, e
		}
		d.FreeMemoryGB, e = source(ctx)
		if e != nil {
			d.Reason = "device_memory_unavailable"
			d.FreeMemoryGB = 0
		}
		d.MemoryMeasured = d.MemoryMode != "fixed" && d.Platform != "fake"
	}
	return physical, nil
}

// M2 projects exactly one inventory component into the existing planner. M3
// enumerates targets; scalar diagnostics never add another allocation pool.
func inventoryTarget(m *Machine) error {
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
	m.GPU, m.Device, m.TotalMemoryGB, m.FreeMemoryGB = "cpu", "", 0, 0
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
			if old.Reason == "device_identity_mismatch" || d.Fingerprint != old.Fingerprint || d.Selector != old.Selector {
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
		if used[d.ID] {
			for n := 0; ; n++ {
				id := strconv.Itoa(n)
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
