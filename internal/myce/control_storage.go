package myce

import (
	"os"
	"path/filepath"
	"time"
)

// Reserve live WAL checkpoint space plus the prepared transaction's WAL and
// possible database growth. Probe outside q.mu; no historical growth latch.
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

// Bootstrap physical growth from the actual prepared mutations, not JSON bytes
// alone. SQLite's B-tree cursor depth is bounded at 20 (BTCURSOR_MAX_DEPTH).
// Allow four pages per level per mutated tree entry for the path, splits,
// freelist/pointer-map bookkeeping, plus overflow payload pages. Events and
// totals count both their table and sequence/index mutations. This intentionally
// generous allowance is checked against real WAL growth in the SQLite regression;
// it is not a guarantee against concurrent foreign writes or filesystem overhead.
func (c *controlRecorder) transactionGrowth(payload uint64, mutations uint64) uint64 {
	page := uint64(c.pageBytes)
	if page == 0 {
		page = 4096
	} // SQLite's default, before initialization only.
	pages := addDiskBytes(payload/page+1, mutations*4*20)
	// WAL frame header is 24 bytes; the file header is 32 bytes.
	return addDiskBytes(32, pages*(page+24))
}
func (c *controlRecorder) storagePermit(growth uint64) bool {
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
	// New WAL frames also reserve space for their possible checkpoint growth.
	reserve := addDiskBytes(disk.WALBytes, addDiskBytes(growth, growth))
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

// Learn each write class, including outliers, so sustained latency changes adapt.
// Recovery accumulates actual healthy control-commit service time, across classes,
// up to the baseline service cost that triggered deferral. A tiny unrelated write
// cannot immediately erase a larger stall; recurring healthy writes can recover
// without repeating an administrative operation. Read-only commits never call here.
func (c *controlRecorder) observeCommit(d time.Duration, diagnostic bool, operations ...string) {
	if c == nil || d <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cost := float64(d)
	var estimate probeEstimate
	operation := "control"
	if diagnostic {
		estimate = c.diagnosticCommitCost
	} else {
		if len(operations) > 0 {
			operation = operations[0]
		}
		if c.controlCommitCosts == nil {
			c.controlCommitCosts = map[string]probeEstimate{}
		}
		estimate = c.controlCommitCosts[operation]
	}
	learned := estimate.Mean > 0
	slow := learned && cost > estimate.upper()
	if slow {
		c.commitPressure = true
		c.recoveryRemaining = max(c.recoveryRemaining, estimate.Mean)
	} else if !diagnostic && learned && c.commitPressure {
		c.recoveryRemaining -= cost
		if c.recoveryRemaining <= 0 {
			c.commitPressure = false
			c.recoveryRemaining = 0
		}
	}
	estimate.observe(cost)
	if diagnostic {
		c.diagnosticCommitCost = estimate
	} else {
		c.controlCommitCosts[operation] = estimate
	}
}
