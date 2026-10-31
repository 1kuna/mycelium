package myce

import (
	"errors"
	"fmt"
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

func (w Worker) checkHostMemoryBudget(gpu, host float64) error {
	bytes, err := MemoryCeilingConfig(w.Machine.HostMemoryBudgetGB, 0, 0)
	if err != nil || bytes > 0 && (host <= 0 || hostNeed(&w.Machine, gpu, host) > w.Machine.HostMemoryBudgetGB) {
		return refuseAdmission("configured_host_allowance", "GiB", hostNeed(&w.Machine, gpu, host), w.Machine.HostMemoryBudgetGB, err != nil || host <= 0)
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
