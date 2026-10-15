package hostbridge

import (
	"encoding/csv"
	"strconv"
	"strings"
)

// NVIDIAHeat consumes every GPU row. NVIDIA T.Limit is signed headroom to
// maximum operating temperature, NOT an absolute temperature limit. Match the
// Linux probe's five-degree recovery margin. Unsupported/malformed rows veto
// nominal coverage, while any known hot GPU still vetoes work.
func NVIDIAHeat(raw string) string {
	rows, err := csv.NewReader(strings.NewReader(raw)).ReadAll()
	if err != nil || len(rows) == 0 {
		return "unknown"
	}
	heat, unknown := "nominal", false
	for _, row := range rows {
		if len(row) != 2 {
			unknown = true
			continue
		}
		temp, e1 := strconv.Atoi(strings.TrimSpace(row[0]))
		margin, e2 := strconv.Atoi(strings.TrimSpace(row[1]))
		if e1 != nil || e2 != nil || temp < -40 || temp > 200 || margin < -200 || margin > 200 {
			unknown = true
			continue
		}
		if margin <= 0 {
			heat = "serious"
		} else if margin < 5 && heat != "serious" {
			heat = "fair"
		}
	}
	if heat == "serious" {
		return heat
	}
	if unknown {
		return "unknown"
	}
	return heat
}

// SetGPUHeat never certifies CPU/host coverage from a GPU or an unidentified
// ACPI zone. A reliable hot GPU can veto work despite missing CPU coverage.
func (s *Signals) SetGPUHeat(heat string) {
	s.GPUHeat, s.HeatCoverage = heat, "gpu-only"
	if heat == "fair" || heat == "serious" {
		s.Heat = heat
	} else {
		s.Heat = "unknown"
	}
}
