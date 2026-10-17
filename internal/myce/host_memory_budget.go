package myce

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

type HostMemoryBudget struct {
	GiB        float64 `json:"gib"`
	Generation uint64  `json:"generation"`
}

func sharedHostDirectory(dir string) (string, error) {
	if dir == "" {
		return HostStateDirectory()
	}
	if legacyUnitParent(dir) != "" {
		return "", legacyHostStateError()
	}
	return dir, nil
}

func EffectiveHostMemoryBudget(dir string, fallback float64) (HostMemoryBudget, string, error) {
	dir, err := sharedHostDirectory(dir)
	if err != nil {
		return HostMemoryBudget{}, "", err
	}
	path := filepath.Join(dir, "host-memory-budget.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return HostMemoryBudget{GiB: fallback}, "install-time --host-memory-budget-gb", nil
	}
	if err != nil {
		return HostMemoryBudget{}, path, err
	}
	var budget HostMemoryBudget
	if err = json.Unmarshal(data, &budget); err != nil {
		return budget, path, errors.New("invalid shared host memory budget file")
	}
	if budget.Generation == 0 || budget.GiB <= 0 {
		return budget, path, errors.New("invalid shared host memory budget generation/value")
	}
	if _, err = MemoryCeilingConfig(budget.GiB, 0, 0); err != nil {
		return budget, path, err
	}
	return budget, path, nil
}

func SetHostMemoryBudget(dir string, gib float64) (HostMemoryBudget, error) {
	if gib <= 0 {
		return HostMemoryBudget{}, errors.New("host memory budget must be positive GiB")
	}
	if _, err := MemoryCeilingConfig(gib, 0, 0); err != nil {
		return HostMemoryBudget{}, err
	}
	dir, err := sharedHostDirectory(dir)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return HostMemoryBudget{}, err
	}
	// Serialize with every instance's final launch gate, even when DISABLED exists.
	unlock, err := hostOperationLock(dir, "launch.lock", true)
	if err != nil {
		return HostMemoryBudget{}, err
	}
	defer unlock()
	old, _, err := EffectiveHostMemoryBudget(dir, 0)
	if err != nil {
		return old, err
	}
	if old.Generation == math.MaxUint64 {
		return old, errors.New("host memory budget generation exhausted")
	}
	budget := HostMemoryBudget{GiB: gib, Generation: old.Generation + 1}
	// Existing atomic rename + file and directory fsync preserve one complete generation.
	if err = saveWorkerJSON(dir, "host-memory-budget.json", budget); err != nil {
		return budget, err
	}
	return budget, nil
}

func (w *Worker) refreshHostMemoryBudget() error {
	budget, _, err := EffectiveHostMemoryBudget(w.HostStateDir, w.Machine.HostMemoryBudgetGB)
	if err != nil {
		return err
	}
	if budget.Generation < w.Machine.HostMemoryBudgetGeneration {
		return errors.New("shared host memory budget generation regressed")
	}
	w.Machine.HostMemoryBudgetGB, w.Machine.HostMemoryBudgetGeneration = budget.GiB, budget.Generation
	return nil
}

func (w Worker) checkHostMemoryBudget(gpu, host float64) error {
	if w.HostStateDir == "" {
		return nil
	}
	budget, _, err := EffectiveHostMemoryBudget(w.HostStateDir, w.Machine.HostMemoryBudgetGB)
	if err != nil {
		return fmt.Errorf("%w: shared host memory budget: %v", errWorkerMemory, err)
	}
	if budget.Generation != w.Machine.HostMemoryBudgetGeneration {
		return fmt.Errorf("%w: shared host memory budget changed since queue admission", errWorkerMemory)
	}
	if budget.GiB > 0 && (host <= 0 || hostNeed(&w.Machine, gpu, host) > budget.GiB) {
		return fmt.Errorf("%w: allocation exceeds shared host memory budget %.3g GiB", errWorkerMemory, budget.GiB)
	}
	return nil
}

func (m Machine) HostMemoryBudgetStatus() string {
	if m.EffectiveHostMemoryBudgetGB > 0 && m.HostMemoryInUseGB > m.EffectiveHostMemoryBudgetGB {
		return fmt.Sprintf("over budget: %g GiB in use > %g GiB", m.HostMemoryInUseGB, m.EffectiveHostMemoryBudgetGB)
	}
	return ""
}
