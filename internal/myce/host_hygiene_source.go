package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"mycelium/internal/probeguard"
)

var workerHostHealthSource = localHostHealth
var workerForeignConsumers = localForeignConsumers

func localHostHealth(ctx context.Context) (HostHealthReading, error) {
	probeguard.Check()
	r := HostHealthReading{}
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return r, err
		}
		fields := strings.Fields(string(b))
		if len(fields) == 0 {
			return r, errors.New("load unavailable")
		}
		r.Load, err = strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return r, err
		}
		b, err = os.ReadFile("/proc/pressure/memory")
		if err != nil {
			return r, err
		}
		r.Pressure, err = sharedMemoryPSI(b)
		if err != nil {
			return r, err
		}
		b, err = os.ReadFile("/proc/meminfo")
		if err != nil {
			return r, err
		}
		values := map[string]float64{}
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				values[f[0]], _ = strconv.ParseFloat(f[1], 64)
			}
		}
		r.SwapBytes = max(0, values["SwapTotal:"]-values["SwapFree:"]) * 1024
	} else if runtime.GOOS == "darwin" {
		b, err := exec.CommandContext(ctx, "/usr/sbin/sysctl", "-n", "vm.loadavg", "kern.memorystatus_vm_pressure_level", "vm.swapusage").Output()
		if err != nil {
			return r, err
		}
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(lines) != 3 {
			return r, errors.New("host counters unavailable")
		}
		f := strings.Fields(lines[0])
		if len(f) < 2 {
			return r, errors.New("load unavailable")
		}
		r.Load, err = strconv.ParseFloat(f[1], 64)
		if err != nil {
			return r, err
		}
		r.Pressure, err = strconv.ParseFloat(strings.TrimSpace(lines[1]), 64)
		if err != nil {
			return r, err
		}
		f = strings.Fields(lines[2])
		found := false
		for i := 0; i+2 < len(f); i++ {
			if f[i] == "used" && f[i+1] == "=" {
				value := f[i+2]
				if len(value) < 2 {
					break
				}
				n, e := strconv.ParseFloat(value[:len(value)-1], 64)
				if e != nil {
					return r, e
				}
				scale := map[byte]float64{'K': 1024, 'M': 1 << 20, 'G': 1 << 30}[value[len(value)-1]]
				if scale == 0 {
					break
				}
				r.SwapBytes, found = n*scale, true
			}
		}
		if !found {
			return r, errors.New("swap unavailable")
		}
	} else {
		return r, errors.New("host counters unsupported")
	}
	// Timer lateness measures scheduler latency without spinning. The OS wait
	// measures elapsed sensor time; decisions use injected-clock samples.
	started := time.Now()
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return r, ctx.Err()
	case <-timer.C:
	}
	r.WakeSeconds = max(0, time.Since(started).Seconds()-time.Millisecond.Seconds())
	return r, nil
}

type healthProcess struct {
	pid, parent int
	cpu         float64
	rss         uint64
	executable  string
}

func foreignHealthProcesses(b []byte, owned map[int]bool) []ForeignConsumer {
	var processes []healthProcess
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		pid, e1 := strconv.Atoi(f[0])
		parent, e2 := strconv.Atoi(f[1])
		cpu, e3 := strconv.ParseFloat(f[2], 64)
		rss, e4 := strconv.ParseUint(f[3], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || !validMemoryValue(cpu) {
			continue
		}
		processes = append(processes, healthProcess{pid, parent, cpu, rss * 1024, filepath.Base(strings.Join(f[4:], " "))})
	}
	for changed := true; changed; {
		changed = false
		for _, p := range processes {
			if owned[p.parent] && !owned[p.pid] {
				owned[p.pid], changed = true, true
			}
		}
	}
	var out []ForeignConsumer
	for _, p := range processes {
		if !owned[p.pid] {
			out = append(out, ForeignConsumer{PID: p.pid, Executable: p.executable, CPUPercent: p.cpu, RSSBytes: p.rss})
		}
	}
	slices.SortFunc(out, func(a, b ForeignConsumer) int {
		if a.CPUPercent > b.CPUPercent {
			return -1
		}
		if a.CPUPercent < b.CPUPercent {
			return 1
		}
		if a.RSSBytes > b.RSSBytes {
			return -1
		}
		if a.RSSBytes < b.RSSBytes {
			return 1
		}
		return a.PID - b.PID
	})
	// Show both CPU and RSS leaders. These display bounds never affect admission.
	result := slices.Clone(out[:min(5, len(out))])
	slices.SortFunc(out, func(a, b ForeignConsumer) int {
		if a.RSSBytes > b.RSSBytes {
			return -1
		}
		if a.RSSBytes < b.RSSBytes {
			return 1
		}
		return a.PID - b.PID
	})
	for _, p := range out[:min(5, len(out))] {
		if !slices.ContainsFunc(result, func(v ForeignConsumer) bool { return v.PID == p.PID }) {
			result = append(result, p)
		}
	}
	return result
}

func localForeignConsumers(ctx context.Context, dir string) ([]ForeignConsumer, error) {
	probeguard.Check()
	owned, err := hostHealthOwnedPIDs(dir)
	if err != nil {
		return nil, err
	}
	b, err := exec.CommandContext(ctx, "ps", "-axo", "pid=,ppid=,pcpu=,rss=,comm=").Output()
	if err != nil {
		return nil, err
	}
	out := foreignHealthProcesses(b, owned)
	if runtime.GOOS == "darwin" {
		data, err := exec.CommandContext(ctx, "launchctl", "list").Output()
		if err != nil {
			return out, fmt.Errorf("service inventory unavailable")
		}
		for _, line := range strings.Split(string(data), "\n") {
			f := strings.Fields(line)
			if len(f) == 3 {
				pid, _ := strconv.Atoi(f[0])
				for i := range out {
					if out[i].PID == pid {
						out[i].Service = f[2]
					}
				}
			}
		}
	} else if runtime.GOOS == "linux" {
		for i := range out {
			b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", out[i].PID))
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(b), "\n") {
				for _, component := range strings.Split(line, "/") {
					if strings.HasSuffix(component, ".service") || strings.HasSuffix(component, ".scope") {
						out[i].Service = component
					}
				}
			}
		}
	}
	return out, nil
}

// Include surviving exact-attempt groups even after reparenting. A PID alone
// is never ownership: a matching recorded kernel birth is required.
func hostHealthOwnedPIDs(dir string) (map[int]bool, error) {
	owned := map[int]bool{os.Getpid(): true}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if name != "state.json" && name != "legacy-state.json" && !(strings.HasPrefix(name, "state-attempt-") && strings.HasSuffix(name, ".json")) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var state workerState
		if err = decodeWorkerState(b, &state); err != nil {
			return nil, err
		}
		if state.PID > 0 && state.Birth != "" {
			birth, err := processBirth(state.PID)
			if err == nil && birth == state.Birth {
				owned[state.PID] = true
			}
		}
		for _, members := range state.Groups {
			for pid, expected := range members {
				if expected != "" {
					birth, err := processBirth(pid)
					if err == nil && birth == expected {
						owned[pid] = true
					}
				}
			}
		}
	}
	return owned, nil
}
