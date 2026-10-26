package myce

import (
	"syscall"

	"mycelium/internal/probeguard"
)

func localProcessMemory() *processMemoryProbe {
	return &processMemoryProbe{
		Resident: func(groups map[int]bool) (uint64, int, error) {
			probeguard.Check()
			out, err := processPS("-ax", "-o", "pid=,pgid=,rss=")
			if err != nil {
				return 0, 0, err
			}
			return processGroupResident(out, groups)
		},
	}
}

// Darwin reports ru_maxrss in bytes.
func rusageMaxRSSBytes(ru *syscall.Rusage) float64 { return float64(ru.Maxrss) }
