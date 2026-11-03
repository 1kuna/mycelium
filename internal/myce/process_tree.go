package myce

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Outer commands own an OS session. Nested workers/engines keep that session
// even when they start new process groups (#579). This is cleanup custody, not
// a sandbox against payloads deliberately creating their own sessions.
const processSessionVariable = "MYCE_PROCESS_SESSION"

func commandProcessAttributes() *syscall.SysProcAttr {
	if os.Getenv(processSessionVariable) != "" {
		return &syscall.SysProcAttr{Setpgid: true}
	}
	return &syscall.SysProcAttr{Setsid: true}
}

func inheritedProcessSessionEnvironment(env []string) []string {
	if os.Getenv(processSessionVariable) == "" {
		return env
	}
	return processSessionEnvironment(env)
}

func processSessionEnvironment(env []string) []string {
	return append(env, processSessionVariable+"=1")
}

// Snapshot the whole session before group delivery can orphan its descendants.
// Individual birth checks fence PID reuse, and the ordinary release inspector
// verifies all remaining session members, including separate process groups.
func signalWorkerSession(group int, signal syscall.Signal) error {
	members, err := workerGroupMembers(group)
	if err != nil {
		return err
	}
	births := map[int]string{}
	for _, pid := range members {
		birth, err := processBirth(pid)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		births[pid] = birth
	}
	result := signalWorkerGroupUsing(group, signal, syscall.Kill, workerGroupMembers)
	for pid, saved := range births {
		birth, err := processBirth(pid)
		if errors.Is(err, os.ErrNotExist) || err == nil && birth != saved {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		session, sessionErr := unix.Getsid(pid)
		pgid, groupErr := syscall.Getpgid(pid)
		if sessionErr != nil || groupErr != nil {
			if !errors.Is(sessionErr, syscall.ESRCH) && !errors.Is(groupErr, syscall.ESRCH) {
				result = errors.Join(result, sessionErr, groupErr)
			}
			continue
		}
		if pgid == group || session != group {
			continue
		}
		if err := syscall.Kill(pid, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
			result = errors.Join(result, err)
		}
	}
	return result
}

// Persist the current session members before signals can orphan them. Recovery
// uses these kernel birth identities rather than mutable argv/environment.
func recordWorkerGroup(state *workerState, group int) error {
	members, err := workerGroupMembers(group)
	if err != nil {
		return err
	}
	if state.Groups == nil {
		state.Groups = map[int]map[int]string{}
	}
	if state.Groups[group] == nil {
		state.Groups[group] = map[int]string{}
	}
	for _, pid := range members {
		birth, err := processBirth(pid)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		state.Groups[group][pid] = birth
	}
	return nil
}
