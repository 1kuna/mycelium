package myce

import (
	"os"
	"path/filepath"
	"time"
)

// Reserve room for the live WAL's worst-case checkpoint growth plus the next
// prepared turn or recently observed shared-WAL extent growth. This physical
// byte budget has no fixed disk percentage or GiB cap. Probe outside q.mu.
type controlDiskBudget struct{ Available, WALBytes uint64 }

func controlStorageProbe(path string) func() (controlDiskBudget, error) {
	return func() (controlDiskBudget, error) {
		disk, err := workerDiskProbe(filepath.Dir(path))
		if err != nil {
			return controlDiskBudget{}, err
		}
		out := controlDiskBudget{Available: disk.FreeBytes}
		st, err := os.Stat(path + "-wal")
		if os.IsNotExist(err) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out.WALBytes = uint64(max(0, st.Size()))
		return out, nil
	}
}
func (c *controlRecorder) storagePermit(nextBytes int) bool {
	var disk controlDiskBudget
	var err error
	if c.storageProbe != nil {
		disk, err = c.storageProbe()
	}
	c.mu.Lock()
	pressure := c.pressure
	c.mu.Unlock()
	pressured := false
	if pressure != nil {
		pressured = pressure.observedPressure.Load()
	}
	if disk.WALBytes > c.lastWALBytes {
		c.observedGrowth = max(c.observedGrowth, disk.WALBytes-c.lastWALBytes)
	}
	c.lastWALBytes = disk.WALBytes
	reserve := addDiskBytes(disk.WALBytes, max(uint64(max(0, nextBytes)), c.observedGrowth))
	c.mu.Lock()
	defer c.mu.Unlock()
	reason := ""
	switch {
	case err != nil:
		reason = "capacity_unknown"
	case c.storageProbe != nil && disk.Available <= reserve:
		reason = "checkpoint_headroom"
	case pressured:
		reason = "control_pressure"
	case c.commitPressure:
		reason = "commit_cost"
	}
	c.storageReason, c.storageDeferred = reason, reason != ""
	return !c.storageDeferred
}

// Only healthy control commits train the prediction band. A slow diagnostic
// commit defers diagnostic writes until actual control commits show recovery.
// No diagnostic retry timer or installation-specific delay is introduced.
func (c *controlRecorder) observeCommit(d time.Duration, diagnostic bool, operations ...string) {
	if c == nil || d <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cost := float64(d)
	if diagnostic {
		if c.diagnosticCommitCost.Mean == 0 || cost <= c.diagnosticCommitCost.upper() {
			c.diagnosticCommitCost.observe(cost)
		} else {
			c.commitPressure, c.pressureOperation = true, "diagnostic"
		}
		return
	}
	operation := "control"
	if len(operations) > 0 {
		operation = operations[0]
	}
	if c.controlCommitCosts == nil {
		c.controlCommitCosts = map[string]probeEstimate{}
	}
	estimate := c.controlCommitCosts[operation]
	if estimate.Mean == 0 || cost <= estimate.upper() {
		estimate.observe(cost)
		c.controlCommitCosts[operation] = estimate
		if c.pressureOperation == "" || c.pressureOperation == "diagnostic" || c.pressureOperation == operation {
			c.commitPressure, c.pressureOperation = false, ""
		}
	} else {
		c.commitPressure, c.pressureOperation = true, operation
	}
}
