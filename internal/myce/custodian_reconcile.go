package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const legacyCustodianRefusal = "systemd worker invocation has no allowed worker cgroup"

// ReconcileCustodianStartup settles only the old, pre-entry Linux refusal. It
// neither contacts the queue nor launches/signals anything. The stopped worker
// subsequently adopts the finished no-execution result through normal replay.
func ReconcileCustodianStartup(ctx context.Context, dir, id string, privilegedInspection bool) error {
	if runtime.GOOS != "linux" {
		return errors.New("custodian startup recovery is Linux-only")
	}
	if os.Geteuid() == 0 {
		return ErrHostIdentity
	}
	fence := fenceCustodianStartup
	if privilegedInspection {
		fence = privilegedCustodianStartupFence
	}
	return reconcileCustodianStartup(ctx, dir, id, fence)
}

func reconcileCustodianStartup(ctx context.Context, dir, id string, fence func(context.Context, string, string) error) error {
	if dir == "" || !validCustodianID(id) {
		return errors.New("exact worker spool and custodian identity required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Never create an unrelated spool, and exclude the worker's assignment actors.
	for _, root := range []string{dir, custodianDir(dir)} {
		info, err := os.Lstat(root)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("existing ordinary worker/custody directories required")
		}
	}
	unlock, err := commandLock(dir, "worker.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	unlockOwner, err := commandLock(custodianDir(dir), "custodian-"+id+".lock", false)
	if err != nil {
		return err
	}
	defer unlockOwner()
	var req custodianRequest
	if err := readCustodianJSON(custodianDir(dir), custodianName(id, "request"), &req); err != nil {
		return err
	}
	if req.ID != id || req.Protocol != custodianProtocol || req.State.Job == nil {
		return errors.New("custodian request identity mismatch")
	}
	j := req.State.Job
	if j.ID == "" || !validCustodianID(j.ID) || j.Attempt <= 0 || j.Session == "" || j.Machine == "" {
		return errors.New("invalid custodian assignment")
	}
	a := attemptRecordFor(j, j.Machine)
	paths, err := filepath.Glob(filepath.Join(dir, "state-attempt-*.json"))
	if err != nil {
		return err
	}
	matches := 0
	for _, path := range append(paths, filepath.Join(dir, "state.json"), filepath.Join(dir, "legacy-state.json")) {
		b, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var s workerState
		if err := decodeWorkerState(b, &s); err != nil {
			return err
		}
		if s.Custodian != id {
			continue
		}
		matches++
		if !a.ownsState(s) || !custodianUnstarted(s) {
			return errors.New("saved assignment has mismatched or execution custody")
		}
	}
	if matches != 1 {
		return errors.New("exactly one saved custodian assignment required")
	}
	if !req.unstartedFor(id, a) {
		return errors.New("request retains execution custody")
	}
	// Lstat deliberately rejects even a dangling symlink or corrupt observation.
	for _, path := range []string{
		filepath.Join(custodianDir(dir), custodianName(id, "snapshot")),
		filepath.Join(custodianDir(dir), custodianName(id, "state")),
		filepath.Join(custodianDir(dir), custodianName(id, "gate")),
		attemptStorageRoot(*j, dir),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			return errors.New("custodian has possible execution evidence; custody retained")
		}
	}
	b, err := os.ReadFile(filepath.Join(custodianDir(dir), "custodian-"+id+".log"))
	if err != nil {
		return err
	}
	// The legacy CLI logs exactly one timestamped fatal error before entering
	// runCustodian. Substrings, extra output and multiple invocations are unknown.
	line := strings.TrimSuffix(string(b), "\n")
	if len(line) != 20+len(legacyCustodianRefusal) || line[19] != ' ' || line[20:] != legacyCustodianRefusal {
		return errors.New("exact legacy pre-entry refusal missing; custody retained")
	}
	if _, err := time.Parse("2006/01/02 15:04:05", line[:19]); err != nil {
		return errors.New("invalid legacy refusal timestamp")
	}
	if err := fence(ctx, dir, id); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return RecordCustodianStartupFailure(dir, id)
}
