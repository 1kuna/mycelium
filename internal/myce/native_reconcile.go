package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ReconcileNativeAttempt verifies dead, offline custody without signalling or
// launching anything. The installed worker must subsequently replay the result;
// this command has no queue credentials or maintenance authority.
func ReconcileNativeAttempt(ctx context.Context, dir, identity string) error {
	if os.Geteuid() == 0 {
		return ErrHostIdentity
	}
	return reconcileNativeAttempt(ctx, dir, identity, workerGroupMembers, processBirth, nativeScratchEmpty)
}

func reconcileNativeAttempt(ctx context.Context, dir, identity string, members func(int) ([]int, error), birth func(int) (string, error), scratchEmpty func(context.Context, string) error) error {
	if dir == "" || identity == "" || identity == "." || identity == ".." || filepath.Base(identity) != identity {
		return errors.New("resolve-native requires exact worker state and attempt identity")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Do not create a new spool or adopt/migrate unrelated attempts.
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("worker spool is not a directory")
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return err
	}
	unlock, err := commandLock(dir, "worker.lock", false)
	if err != nil {
		return err
	}
	defer unlock()
	paths, err := filepath.Glob(filepath.Join(dir, "state-attempt-*.json"))
	if err != nil {
		return err
	}
	var selected *workerStore
	for _, path := range append(paths, filepath.Join(dir, "state.json"), filepath.Join(dir, "legacy-state.json")) {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var state workerState
		if err := decodeWorkerState(data, &state); err != nil {
			return err
		}
		if state.Identity != identity || state.Job == nil {
			continue
		}
		if selected != nil {
			return errors.New("duplicate native attempt custody")
		}
		selected = &workerStore{dir: dir, stateName: filepath.Base(path), state: state}
	}
	if selected == nil {
		return errors.New("native attempt does not match saved worker custody")
	}
	s := selected
	j := s.state.Job
	if j.Spec.Run.Docker || len(j.Spec.Run.DockerSidecars) != 0 || j.ID == "" || filepath.Base(j.ID) != j.ID || j.ID == "." || j.ID == ".." || j.Attempt <= 0 || s.state.Session == "" || j.Session != "" && j.Session != s.state.Session {
		return errors.New("invalid native attempt custody")
	}
	// recoverResult fences both copies by job, attempt, worker and session.
	if s.state.Result == nil || s.state.Result.Machine == "" || s.state.Result.Machine != j.Machine {
		return errors.New("native reconciliation requires a saved finished result")
	}
	if err := s.recoverResult(s.state.Result.Machine); err != nil {
		return err
	}
	var custody workerState
	data, err := os.ReadFile(filepath.Join(dir, "attempt-"+identity+".json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &custody); err != nil {
		return err
	}
	if custody.Identity != identity || s.state.PID != 0 && s.state.PID != custody.PID {
		return errors.New("native process custody unavailable or mismatched")
	}
	if custody.Cgroup == "" {
		custody.Cgroup = s.state.Cgroup // Ordinary PID-only receipts keep it in state.
	} else if s.state.Cgroup != "" && s.state.Cgroup != custody.Cgroup {
		return errors.New("mismatched native cgroup custody")
	}
	groups := custody.Groups
	if len(groups) == 0 {
		// Ordinary commands persist only the supervisor PID. Absence of both
		// that PID and its process group is proof; no birth assertion is needed.
		if custody.PID <= 0 {
			return errors.New("native process custody unavailable")
		}
		groups = map[int]map[int]string{custody.PID: nil}
	}
	pids := map[int]bool{}
	for group, recorded := range groups {
		if group <= 0 || len(recorded) == 0 && group != custody.PID {
			return errors.New("invalid native process group custody")
		}
		live, err := members(group)
		if err != nil {
			return err
		}
		if len(live) != 0 {
			return errors.New("native process group remains live; custody retained")
		}
		pids[group] = true
		for pid, start := range recorded {
			if pid <= 0 || start == "" {
				return errors.New("invalid native process birth custody")
			}
			pids[pid] = true
		}
	}
	if custody.PID > 0 {
		pids[custody.PID] = true
	}
	var native struct {
		Identity string
		Members  []nativeMember
	}
	data, err = os.ReadFile(filepath.Join(dir, "native-"+identity+".json"))
	if err == nil {
		if json.Unmarshal(data, &native) != nil || native.Identity != identity {
			return errors.New("mismatched native sidecar custody")
		}
		for _, m := range native.Members {
			if m.Cgroup != "" {
				leaf := "main"
				if m.Name != "main" {
					leaf = "sidecar-" + m.Name
				}
				if custody.Cgroup == "" || m.Cgroup != filepath.Join(custody.Cgroup, leaf) {
					return errors.New("mismatched native member cgroup custody")
				}
			}
			if m.PID > 0 {
				pids[m.PID] = true
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) || len(j.Spec.Run.Sidecars) > 0 || j.Spec.Run.MemoryGB > 0 {
		return errors.New("native sidecar custody unavailable")
	}
	// Check recorded PIDs even if they escaped their original group. A reused
	// PID is deliberately a hold too; never infer death from a birth mismatch.
	for pid := range pids {
		if _, err := birth(pid); !errors.Is(err, os.ErrNotExist) {
			return errors.New("native process identity live or unknown; custody retained")
		}
	}
	if custody.Cgroup != "" {
		if filepath.Base(custody.Cgroup) != "attempt-"+identity {
			return errors.New("mismatched native cgroup custody")
		}
		if _, err := os.Stat(custody.Cgroup); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			populated, err := (&memoryAttempt{path: custody.Cgroup, fs: osMemoryFS{}}).populated()
			if err != nil || populated {
				return errors.New("native cgroup population unconfirmed; custody retained")
			}
		}
	}
	if err := scratchEmpty(ctx, filepath.Join(dir, "job-files", j.ID, strconv.Itoa(j.Attempt))); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Receipt first: startup reads it before state.json. Retain all outcome,
	// scratch and process evidence; only the exact release hold is settled.
	released := *s.state.Result
	released.ReleasePending = false
	if err := saveWorkerJSON(dir, identity+".result.json", released); err != nil {
		return err
	}
	s.state.Result, s.state.PID = &released, 0
	return s.save()
}

func nativeScratchEmpty(ctx context.Context, dir string) error {
	// Resolve existing ancestors even when the scratch directory was unlinked.
	// OS descriptor paths use canonical names (e.g. /private/var on macOS).
	for ancestor := dir; ; ancestor = filepath.Dir(ancestor) {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			rel, err := filepath.Rel(ancestor, dir)
			if err != nil {
				return err
			}
			dir = filepath.Join(resolved, rel)
			break
		}
		if !errors.Is(err, os.ErrNotExist) || ancestor == filepath.Dir(ancestor) {
			return err
		}
	}
	// lsof is the OS handle inspector (included on macOS). Missing tools,
	// warnings, incomplete inspection or deadline expiry all retain custody.
	// Inventory descriptors, rather than walking +D: unlinked open files no
	// longer appear in a directory walk but still belong to a live process.
	cmd := exec.CommandContext(ctx, "lsof", "-nP", "-u", strconv.Itoa(os.Geteuid()), "-Fpn")
	cmd.WaitDelay = 100 * time.Millisecond
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err != nil || stdout.Len() == 0 || stderr.Len() != 0 || ctx.Err() != nil {
		return errors.New("native scratch handles remain open or inspection failed; custody retained")
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		if !strings.HasPrefix(line, "n") {
			continue
		}
		name := strings.TrimSuffix(line[1:], " (deleted)")
		if name == dir || strings.HasPrefix(name, dir+string(os.PathSeparator)) {
			return errors.New("native scratch handles remain open; custody retained")
		}
	}
	return nil
}
