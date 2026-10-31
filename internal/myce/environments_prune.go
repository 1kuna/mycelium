package myce

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

type EnvironmentPruneEntry struct {
	Key        string        `json:"key"`
	Age        time.Duration `json:"age_ns"`
	Bytes      int64         `json:"bytes"`
	Referenced bool          `json:"referenced"`
	Locked     bool          `json:"locked"`
	Recent     bool          `json:"recent"`
	Removed    bool          `json:"removed"`
}

type EnvironmentPruneReport struct {
	Apply          bool                    `json:"apply"`
	Entries        []EnvironmentPruneEntry `json:"entries"`
	ReclaimedBytes int64                   `json:"reclaimed_bytes"`
}

// Both pressure admission and standalone prune enumerate ready prefixes oldest
// first and use evict's lock/age recheck before retiring anything.
func (c *EnvironmentCache) evictionCandidates(exclude string) ([]EnvironmentPruneEntry, error) {
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return nil, err
	}
	out := []EnvironmentPruneEntry{}
	for _, d := range entries {
		key := d.Name()
		if !d.IsDir() || key == exclude || (FileRef{SHA256: key}).validate() != nil {
			continue
		}
		st, err := os.Stat(filepath.Join(c.Root, key, "ready.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, EnvironmentPruneEntry{Key: key, Age: c.Clock.Now().Sub(st.ModTime())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Age > out[j].Age })
	return out, nil
}

// evict runs under admission. The optional check is called with exclusive use
// and build locks held, immediately before retirement. It must fail closed if
// reference state cannot be read. Filesystem work never holds the queue mutex.
func (c *EnvironmentCache) evict(key, reason string, check func() (bool, error)) (bool, error) {
	use, err := c.tryLock(key + ".use")
	if err != nil || use == nil {
		return false, err
	}
	defer unlockEnvironment(use)
	build, err := c.tryLock(key + ".build")
	if err != nil || build == nil {
		return false, err
	}
	defer unlockEnvironment(build)
	path := filepath.Join(c.Root, key)
	if c.recent(path) {
		return false, nil
	}
	if check != nil {
		allowed, err := check()
		if err != nil || !allowed {
			return false, err
		}
	}
	if err = c.trash(path); err != nil {
		return false, err
	}
	log.Printf("environment eviction key=%s reason=%s", key, reason)
	return true, nil
}

// Prune retains the same 24-hour minimum as pressure eviction. references is a
// fresh authoritative query, also repeated under each candidate's local locks.
// Dry-run only inspects prefixes; it never retires or purges environments.
func (c *EnvironmentCache) Prune(ctx context.Context, apply bool, references func(context.Context, string) (bool, error)) (report EnvironmentPruneReport, err error) {
	report.Apply = apply
	if references == nil {
		return report, errors.New("environment prune requires queue reference state")
	}
	if err = c.init(); err != nil {
		return report, err
	}
	gate, err := c.lock(ctx, "admission", syscall.LOCK_EX)
	if err != nil {
		return report, err
	}
	defer unlockEnvironment(gate)
	report.Entries, err = c.evictionCandidates("")
	if err != nil {
		return report, err
	}
	// Include downloads in the inode union: deleting an installed hardlink does
	// not reclaim blocks still owned by the shared package cache.
	usage := func() (int64, error) {
		entries, err := os.ReadDir(c.Root)
		if err != nil {
			return 0, err
		}
		paths := []string{filepath.Join(c.Root, ".Trash"), filepath.Join(c.Root, ".downloads")}
		for _, d := range entries {
			if d.IsDir() && (len(d.Name()) == 64 || filepath.Ext(d.Name()) == ".partial") {
				paths = append(paths, filepath.Join(c.Root, d.Name()))
			}
		}
		return environmentDiskUsage(paths...)
	}
	before, err := usage()
	if err != nil {
		return report, err
	}
	var audit *os.File
	if apply {
		audit, err = os.OpenFile(filepath.Join(c.Root, "prune-audit.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return report, err
		}
		defer audit.Close()
		defer func() {
			after, measureErr := usage()
			if measureErr == nil && before > after {
				report.ReclaimedBytes = before - after
			}
			record := struct {
				At time.Time `json:"at"`
				EnvironmentPruneReport
				Error string `json:"error,omitempty"`
			}{At: c.Clock.Now(), EnvironmentPruneReport: report}
			if err != nil {
				record.Error = err.Error()
			}
			err = errors.Join(err, measureErr, json.NewEncoder(audit).Encode(record), audit.Sync())
		}()
	}
	for i := range report.Entries {
		v := &report.Entries[i]
		if err = ctx.Err(); err != nil {
			return report, err
		}
		v.Bytes, err = environmentDiskUsage(filepath.Join(c.Root, v.Key))
		if err != nil {
			return report, err
		}
		v.Referenced, err = references(ctx, v.Key)
		if err != nil {
			return report, err
		}
		v.Recent = v.Age < environmentRecentWindow
		for _, suffix := range []string{".use", ".build"} {
			lock, lockErr := c.tryLock(v.Key + suffix)
			if lockErr != nil {
				return report, lockErr
			}
			v.Locked = v.Locked || lock == nil
			unlockEnvironment(lock)
		}
		if !apply || v.Referenced || v.Recent || v.Locked {
			continue
		}
		v.Removed, err = c.evict(v.Key, "operator_prune", func() (bool, error) {
			v.Referenced, err = references(ctx, v.Key)
			if err != nil || v.Referenced {
				return false, err
			}
			// Write intent before mutation, so interrupted pruning is auditable.
			if err := json.NewEncoder(audit).Encode(struct {
				At    time.Time `json:"at"`
				Key   string    `json:"key"`
				Event string    `json:"event"`
			}{c.Clock.Now(), v.Key, "retire"}); err != nil {
				return false, err
			}
			return true, audit.Sync()
		})
		if err != nil {
			return report, err
		}
		if v.Removed {
			if _, err = c.purgeRetiredKey(v.Key); err != nil {
				return report, err
			}
		}
	}
	return report, nil
}

// environmentReferenced reads only queue metadata. Conservatively protect all
// platform variants, sidecars, retained attempt custody and preparation offers.
func (q *Queue) environmentReferenced(ctx context.Context, key string) bool {
	unlock := requestReadLock(ctx, &q.mu)
	defer unlock()
	for _, slot := range q.s.ModelSlots {
		if slot.EnvironmentRecipe != nil && slot.EnvironmentRecipe.Key == key {
			return true
		}
	}
	for _, call := range q.calls {
		if model, ok := q.model(call.Model); ok {
			for _, recipe := range q.environments.Environments[model.Environment] {
				if recipe.Key == key {
					return true
				}
			}
		}
	}
	held := map[string]bool{}
	for _, r := range q.s.HostReleases {
		held[r.Job] = true
	}
	for _, p := range q.s.PreparationPlans {
		if p.Kind == "environment" && p.Key == key {
			return true
		}
		held[p.Demand] = true
	}
	for _, j := range q.s.Jobs {
		if !mutableJob(j) && !held[j.ID] {
			continue
		}
		if j.Spec.Environment.Key() == key {
			return true
		}
		for _, r := range j.EnvironmentRecipes {
			if r.Key == key {
				return true
			}
		}
		for _, recipes := range j.SidecarEnvironmentRecipes {
			for _, r := range recipes {
				if r.Key == key {
					return true
				}
			}
		}
		for _, e := range j.Extensions {
			// Setup/assets have their own prepared prefix in this same cache.
			// Derive its key from inventory only, without running the planner.
			for _, m := range q.s.Machines {
				if extensionPreparationKey(e, m) == key || extensionPreparationKey(e, cpuMachineView(m)) == key {
					return true
				}
				for _, d := range m.Devices {
					target := d.machine(*m)
					if extensionPreparationKey(e, &target) == key {
						return true
					}
				}
			}
			for _, v := range e.Variants {
				for _, r := range v.Environments {
					if r.Key == key {
						return true
					}
				}
			}
		}
	}
	return false
}
