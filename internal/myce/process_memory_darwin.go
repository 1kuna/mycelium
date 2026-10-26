package myce

import (
	"encoding/binary"
	"errors"
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
		Signals: func() (memorySignals, error) {
			probeguard.Check()
			// 1 normal, 2 warning, 4 critical (NOTE_MEMORYSTATUS_PRESSURE_*).
			level, err := syscall.SysctlUint32("kern.memorystatus_vm_pressure_level")
			if err != nil {
				return memorySignals{}, err
			}
			// struct xsw_usage: total, avail, used (uint64), then page size.
			raw, err := syscall.Sysctl("vm.swapusage")
			if err != nil {
				return memorySignals{}, err
			}
			if len(raw) < 24 {
				return memorySignals{}, errors.New("short vm.swapusage")
			}
			return memorySignals{Pressure: level, SwapUsed: binary.LittleEndian.Uint64([]byte(raw[16:24]))}, nil
		},
	}
}

// Darwin reports ru_maxrss in bytes.
func rusageMaxRSSBytes(ru *syscall.Rusage) float64 { return float64(ru.Maxrss) }
