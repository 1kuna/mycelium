package myce

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const deviceUnifiedResidentSource = "unified RSS/cgroup upper estimate with host reconciliation"
const deviceUnifiedUncertain = "unified device attribution uncertain; exclusive execution required"
const deviceUnifiedStarting = "unified device sample pending first sensor cycle"

func sharedDeviceSampleReady(s hostMemorySample, now time.Time) bool {
	return !s.exclusive && !s.deviceAt.IsZero() && !now.Before(s.deviceAt) && now.Sub(s.deviceAt) < deviceMemorySampleFreshFor
}

func unifiedDeviceSampleStarting(j *Job, s hostMemorySample, now time.Time) bool {
	// A started process checks in before the first periodic probe. Reuse the
	// sensor's existing one-missed-cycle freshness allowance, not a fleet-tuned
	// startup delay. It cannot admit another tenant or excuse explicit unknown
	// evidence, and expires even if the first sample never arrives.
	return j.StartedAt != nil && s.deviceAt.IsZero() && !s.exclusive && !now.Before(*j.StartedAt) && now.Sub(*j.StartedAt) < deviceMemorySampleFreshFor
}

// RSS can count shared pages repeatedly and resident cgroup pages include CPU
// allocations. Their maximum is deliberately an upper estimate, not CUDA bytes.
// An outside-root host drop cannot be divided among GPU co-tenants. Charge it
// to an exclusive attempt only, and keep that attempt exclusive while it lasts.
func (a *memoryAttempt) unifiedDeviceMemory(ids map[int]bool, foreign bool) (float64, bool, error) {
	if a == nil || a.hostBaseline == nil || a.rootBaseline == nil {
		return 0, true, errors.New(deviceUnifiedUncertain)
	}
	resident, ok := cgroupResidentGB(a.fs, a.path)
	root, rootOK := cgroupResidentGB(a.fs, filepath.Dir(a.path))
	host, err := readGPUHostState(a.fs)
	if !ok || !rootOK || err != nil {
		return 0, true, errors.New(deviceUnifiedUncertain)
	}
	rss := 0.0
	for pid := range ids {
		b, err := a.fs.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "status"))
		if err != nil {
			return 0, true, errors.New(deviceUnifiedUncertain)
		}
		found := false
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) == 3 && f[0] == "VmRSS:" && f[2] == "kB" {
				n, e := strconv.ParseUint(f[1], 10, 64)
				if e != nil {
					return 0, true, errors.New(deviceUnifiedUncertain)
				}
				rss += float64(n) / (1 << 20)
				found = true
			}
		}
		if !found {
			return 0, true, errors.New(deviceUnifiedUncertain)
		}
	}
	// RSS outside cgroup charging can explain this attempt's host drop. Never
	// subtract its charged pages twice. Sibling exits retain their signed delta.
	outside := max(0, a.hostBaseline.available-host.available-(root-*a.rootBaseline)-max(0, rss-resident))
	page := a.pageSize
	if page <= 0 {
		return 0, true, errors.New(deviceUnifiedUncertain)
	}
	uncertain := outside > float64(page)/(1<<30)
	if foreign || uncertain {
		return max(resident, rss) + outside, true, nil
	}
	return max(resident, rss), false, nil
}
