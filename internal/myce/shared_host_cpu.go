package myce

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The worker is already in root/worker after memory delegation. Only attempt
// descendants use root/shared-execution: cpu.max never throttles control threads.
// This reuses existing cgroup custody/recovery rather than another process tree.
func sharedCPUCeilings(c *memoryCeilings, cores int) (*memoryCeilings, error) {
	if c == nil || cores <= 0 || int64(cores) > math.MaxInt64/100000 {
		return nil, errors.New("shared CPU requires delegated cgroup v2 and positive allowance")
	}
	b, err := c.fs.ReadFile(filepath.Join(c.root, "cgroup.controllers"))
	if err != nil || !strings.Contains(" "+strings.Join(strings.Fields(string(b)), " ")+" ", " cpu ") {
		return nil, errors.New("CPU controller not delegated")
	}
	if err = c.fs.WriteFile(filepath.Join(c.root, "cgroup.subtree_control"), []byte("+cpu")); err != nil {
		return nil, err
	}
	root := filepath.Join(c.root, "shared-execution")
	if err = c.fs.Mkdir(root); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	quota := strconv.FormatInt(int64(cores)*100000, 10) + " 100000"
	for _, v := range []struct{ name, value string }{{"cpu.weight", "10"}, {"cpu.max", quota}, {"cgroup.subtree_control", "+memory"}} {
		path := filepath.Join(root, v.name)
		if err = c.fs.WriteFile(path, []byte(v.value)); err != nil {
			return nil, err
		}
		b, err = c.fs.ReadFile(path)
		got := strings.TrimSpace(string(b))
		if err != nil || v.name != "cgroup.subtree_control" && got != v.value || v.name == "cgroup.subtree_control" && !strings.Contains(" "+strings.TrimPrefix(got, "+")+" ", " memory ") {
			return nil, fmt.Errorf("shared CPU %s not confirmed", v.name)
		}
	}
	return &memoryCeilings{fs: c.fs, root: root, pageSize: c.pageSize, clock: c.clock}, nil
}

func (w Worker) executionCeilings() *memoryCeilings {
	if w.Machine.SharedHost != nil && w.sharedExecutionCeilings != nil {
		return w.sharedExecutionCeilings
	}
	return w.memoryCeilings
}

func sharedCPUEnforcementStatus(m *Machine) string {
	if m.SharedHost == nil {
		return ""
	}
	value := m.SharedCPUEnforcement
	if value == fmt.Sprintf("cgroup v2 cpu.weight=10 cpu.max=%d/100000", m.CPUBudgetCores*100000) ||
		value == "background task policy; declared-core cap; CPU time unenforced" ||
		value == "CPU time unenforced: dev admission-only" ||
		value == "CPU time unenforced: delegated CPU controller unavailable" {
		return value
	}
	return "CPU time unenforced: worker did not confirm policy"
}

// Cooperative runtime concurrency complements the aggregate declared-core
// ledger on Darwin. Payloads can ignore these variables; never claim a hard cap.
func sharedCPUEnvironment(env map[string]string, p *SharedHostProfile, cores int) {
	if p == nil {
		return
	}
	for _, key := range []string{"GOMAXPROCS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS", "RAYON_NUM_THREADS"} {
		n, err := strconv.Atoi(env[key])
		if err != nil || n <= 0 || n > cores {
			env[key] = strconv.Itoa(max(1, cores))
		}
	}
}
