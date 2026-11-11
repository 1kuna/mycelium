package myce

import (
	"errors"
	"fmt"
)

// HostMemoryBudget is the one host allowance, always UNQUALIFIED until a
// separately authorized native measurement. GiB is a configuration convenience;
// Bytes is the validated effective limit used at launch and in status.
type HostMemoryBudget struct {
	Measured      bool    `json:"measured,omitempty"`
	GiB           float64 `json:"gib"`
	Bytes         int64   `json:"memory_allowance_bytes"`
	Qualification string  `json:"qualification"`
}

// HostMemoryDerivation exposes the authority and its evidence, including fallback.
type HostMemoryDerivation struct {
	Source             string               `json:"source"`
	Reason             string               `json:"reason,omitempty"`
	FallbackGB         float64              `json:"fallback_gb"`
	TotalGB            float64              `json:"total_gb,omitempty"`
	BaselineMaxGB      float64              `json:"baseline_max_gb,omitempty"`
	Dedicated          bool                 `json:"dedicated,omitempty"`
	Role               string               `json:"role,omitempty"`
	BaselineQuantileGB float64              `json:"baseline_quantile_gb,omitempty"`
	ReserveSource      string               `json:"reserve_source,omitempty"`
	ReserveGB          float64              `json:"reserve_gb,omitempty"`
	AllowanceGB        float64              `json:"allowance_gb"`
	History            ExternalMemoryWindow `json:"history_7d"`
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
	if err != nil || (bytes > 0 || w.Machine.HostMemoryBudgetMeasured) && (host <= 0 || hostNeed(&w.Machine, gpu, host) > w.Machine.HostMemoryBudgetGB) {
		return refuseAdmission("configured_host_allowance", "GiB", hostNeed(&w.Machine, gpu, host), w.Machine.HostMemoryBudgetGB, err != nil || host <= 0)
	}
	return nil
}

func (m Machine) HostMemoryBudgetStatus() string {
	if (m.EffectiveHostMemoryBudgetGB > 0 || m.HostMemoryDerivation != nil) && m.HostMemoryInUseGB > m.EffectiveHostMemoryBudgetGB {
		return fmt.Sprintf("over budget: %g GiB in use > %g GiB", m.HostMemoryInUseGB, m.EffectiveHostMemoryBudgetGB)
	}
	return ""
}
