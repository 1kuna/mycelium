package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// HostMemoryBudget is the one host allowance, always UNQUALIFIED until a
// separately authorized native measurement. GiB is a configuration convenience;
// Bytes is the validated effective limit used at launch and in status.
type HostMemoryBudget struct {
	GiB           float64 `json:"gib"`
	Bytes         int64   `json:"memory_allowance_bytes"`
	Qualification string  `json:"qualification"`
}

func hostMemoryAllowance(gib float64) (HostMemoryBudget, error) {
	n, err := MemoryCeilingConfig(gib, 0, 0)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	if gib > 0 && n == 0 {
		return HostMemoryBudget{}, errors.New("host allowance is smaller than one byte")
	}
	return HostMemoryBudget{GiB: float64(n) / (1 << 30), Bytes: n, Qualification: "UNQUALIFIED"}, nil
}

func sharedHostDirectory(dir string) (string, error) {
	if dir == "" {
		return HostStateDirectory()
	}
	if legacyUnitParent(dir) != "" {
		return "", errors.Join(ErrHostState, legacyState(legacyUnitParent(dir)))
	}
	return dir, nil
}

func EffectiveHostMemoryBudget(dir string, fallback float64) (HostMemoryBudget, string, error) {
	dir, err := sharedHostDirectory(dir)
	if err != nil {
		return HostMemoryBudget{}, "", err
	}
	state, err := ReadHostIntent(dir)
	if err != nil {
		return HostMemoryBudget{}, "", err
	}
	if state.MemoryAllowanceBytes > 0 {
		return HostMemoryBudget{GiB: float64(state.MemoryAllowanceBytes) / (1 << 30), Bytes: state.MemoryAllowanceBytes, Qualification: "UNQUALIFIED"}, hostStateFile, nil
	}
	// Preserve the old file in place as inert migration input. Never select its
	// independent generation at runtime or silently replace a conservative limit.
	if _, err := os.Lstat(filepath.Join(dir, "host-memory-budget.json")); err == nil {
		return HostMemoryBudget{}, "", errors.New("legacy memory policy requires explicit CLOSED host-memory-budget import")
	} else if !errors.Is(err, os.ErrNotExist) {
		return HostMemoryBudget{}, "", err
	}
	budget, err := hostMemoryAllowance(fallback)
	if err == nil && budget.Bytes > 0 {
		err = errors.New("install-time allowance is UNQUALIFIED migration input; explicitly configure it while CLOSED")
	}
	return budget, "install-time --host-memory-budget-gb (UNQUALIFIED)", err
}

// SetHostMemoryBudget is explicit CLOSED-host reconfiguration. The existing
// host controller owns persistence and serialization; open/close preserve it.
func SetHostMemoryBudget(dir string, gib float64) (HostMemoryBudget, error) {
	return setHostMemoryBudget(dir, gib, false)
}

func setHostMemoryBudget(dir string, gib float64, importOnly bool) (HostMemoryBudget, error) {
	budget, err := hostMemoryAllowance(gib)
	if err != nil || budget.Bytes <= 0 {
		return HostMemoryBudget{}, errors.New("host allowance must be positive finite GiB")
	}
	dir, err = sharedHostDirectory(dir)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	// Same lock order as launch: launch then intake. No daemon/network work here.
	unlock, err := hostOperationLock(dir, "launch.lock", true)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	defer unlock()
	intake, err := hostOperationLock(dir, "intake.lock", true)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	defer intake()
	state, err := ReadHostIntent(dir)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	if state.Desired != "closed" {
		return HostMemoryBudget{}, errors.New("host must be CLOSED before allowance reconfiguration")
	}
	if importOnly && state.MemoryAllowanceBytes != 0 {
		return HostMemoryBudget{}, errors.New("host allowance is already configured")
	}
	state.MemoryAllowanceBytes = budget.Bytes
	return budget, atomicHostJSON(dir, hostStateFile, state, nil)
}

// ImportHostMemoryBudget is an explicit one-time conversion of the former
// file's value. The source bytes remain intact, inert, and UNQUALIFIED.
func ImportHostMemoryBudget(dir string) (HostMemoryBudget, error) {
	dir, err := sharedHostDirectory(dir)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	state, err := ReadHostIntent(dir)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	if state.MemoryAllowanceBytes != 0 {
		return HostMemoryBudget{}, errors.New("host allowance is already configured")
	}
	data, err := readHostRegular(filepath.Join(dir, "host-memory-budget.json"), uint32(os.Geteuid()), 4096)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	var old struct {
		GiB float64 `json:"gib"`
	}
	if json.Unmarshal(data, &old) != nil {
		return HostMemoryBudget{}, errors.New("invalid legacy allowance input")
	}
	return setHostMemoryBudget(dir, old.GiB, true)
}

func (w *Worker) refreshHostMemoryBudget() error {
	budget, _, err := EffectiveHostMemoryBudget(w.HostStateDir, w.Machine.HostMemoryBudgetGB)
	if err != nil {
		return err
	}
	w.Machine.HostMemoryBudgetGB = budget.GiB
	return nil
}

func (w Worker) checkHostMemoryBudget(gpu, host float64) error {
	if w.HostStateDir == "" {
		return nil
	}
	budget, _, err := EffectiveHostMemoryBudget(w.HostStateDir, w.Machine.HostMemoryBudgetGB)
	if err != nil {
		return fmt.Errorf("%w: host allowance: %v", errWorkerMemory, err)
	}
	reported, err := hostMemoryAllowance(w.Machine.HostMemoryBudgetGB)
	if err != nil || budget.Bytes != reported.Bytes {
		return fmt.Errorf("%w: host allowance changed since queue admission", errWorkerMemory)
	}
	if budget.Bytes > 0 && (host <= 0 || hostNeed(&w.Machine, gpu, host) > budget.GiB) {
		return fmt.Errorf("%w: allocation exceeds host allowance %d bytes", errWorkerMemory, budget.Bytes)
	}
	return nil
}

func (m Machine) HostMemoryBudgetStatus() string {
	if m.HostMemoryPolicyMismatch {
		return "host allowance policy mismatch"
	}
	if m.EffectiveHostMemoryBudgetGB > 0 && m.HostMemoryInUseGB > m.EffectiveHostMemoryBudgetGB {
		return fmt.Sprintf("over budget: %g GiB in use > %g GiB", m.HostMemoryInUseGB, m.EffectiveHostMemoryBudgetGB)
	}
	return ""
}
