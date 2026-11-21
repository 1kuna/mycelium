package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Startup holds the worker spool lock and has loaded all saved attempts. An
// unknown pin is not a release receipt. Follow an exact detached owner only
// when the custodian's own finished result and OS release evidence survive.
func (w Worker) reconcileFinishedCompilerPins(ctx context.Context, root *workerStore, attempts []*workerStore) {
	if w.compilerCache == nil {
		return
	}
	cache := &w.compilerCache.cache
	key := identityHash("go-build")
	pins := filepath.Join(cache.Root, "pins", key)
	pinned, err := sharedInputPinned(cache.Root, key)
	if err != nil || !pinned {
		return
	}
	lock, err := cache.tryLock(key + ".use")
	if err != nil || lock == nil {
		return
	}
	defer unlockEnvironment(lock)
	active := map[string]bool{}
	for _, attempt := range append([]*workerStore{root}, attempts...) {
		if attempt.state.Job != nil || attempt.state.Result != nil || attempt.state.LaunchPending || attempt.state.InputWriterPending || attempt.state.Preparation != "" || attempt.state.StorageDirectory != "" {
			active[attempt.state.Custodian] = true
			active[attempt.state.Identity] = true
		}
	}
	dir := custodianDir(root.dir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		name, found := strings.CutPrefix(entry.Name(), "custodian-")
		id, snapshot := strings.CutSuffix(name, ".snapshot.json")
		if !found || !snapshot || !validCustodianID(id) || active[id] || !entry.Type().IsRegular() {
			continue
		}
		owner := workerStore{dir: root.dir, stateName: custodianName(id, "state")}
		pin := filepath.Join(pins, identityHash(owner.sharedInputOwnerID()))
		if info, err := os.Lstat(pin); err != nil || !info.Mode().IsRegular() {
			continue
		}
		w.reconcileFinishedCompilerPin(ctx, dir, id, &owner, cache.Root, key, pin, active)
	}
}

func (w Worker) reconcileFinishedCompilerPin(ctx context.Context, dir, id string, owner *workerStore, cache, key, pin string, active map[string]bool) {
	unlock, err := commandLock(dir, "custodian-"+id+".lock", false)
	if err != nil {
		return
	}
	defer unlock()
	var req custodianRequest
	var snap custodianSnapshot
	if readCustodianJSON(dir, custodianName(id, "request"), &req) != nil ||
		readCustodianJSON(dir, custodianName(id, "snapshot"), &snap) != nil ||
		req.ID != id || req.Protocol != custodianProtocol || req.State.Job == nil || req.Machine.ID != w.Machine.ID ||
		req.State.Job.Machine != w.Machine.ID || req.State.Job.Session != req.State.Session ||
		!validCustodianID(req.State.Identity) || req.State.Identity != snap.State.Identity || active[snap.State.Identity] {
		return
	}
	ownerSnapshot := attemptCustodian{id: id}
	st := snap.State
	if !snap.Finished || snap.PID <= 0 || snap.Birth == "" || ownerSnapshot.acceptFinished(snap, *req.State.Job) != nil ||
		st.Result.ReleasePending || st.LaunchPending || st.InputWriterPending || st.Preparation != "" || st.StorageDirectory != "" {
		return
	}
	birthOf := w.custodianBirth
	if birthOf == nil {
		birthOf = processBirth
	}
	birth, err := birthOf(snap.PID)
	if err != nil && !errors.Is(err, os.ErrNotExist) || err == nil && (birth == "" || birth == snap.Birth) {
		return
	}
	absent := w.custodianAbsent
	if absent == nil {
		absent = custodianManagerAbsent
	}
	if absent(ctx, id) != nil || ctx.Err() != nil {
		return
	}
	uses, err := owner.readSharedInputUses()
	if err != nil {
		return
	}
	for _, use := range uses {
		if use.Root == cache && use.Key == key {
			_ = removeSharedInputPin(pin)
			return
		}
	}
}
