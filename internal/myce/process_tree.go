package myce

import (
	"errors"
	"os"
	"syscall"
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

// Snapshot the whole session and every descendant (including setsid escapers,
// #1322) before group delivery can orphan them, then signal each snapshot
// member outside the group by exact PID. Birth checks fence PID reuse, and the
// release inspector verifies the same set, so release waits for escapers too.
// Failed inspection still signals the owned group, while retaining the error
// so missing descendant custody can never be mistaken for confirmed release.
func signalWorkerSession(group int, signal syscall.Signal, delivered ...func()) error {
	members, err := workerGroupMembers(group)
	if err != nil {
		return errors.Join(err, signalWorkerGroupUsing(group, signal, syscall.Kill, workerGroupMembers, delivered...))
	}
	births := map[int]string{}
	for _, pid := range members {
		birth, err := processBirth(pid)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.Join(err, signalWorkerGroupUsing(group, signal, syscall.Kill, workerGroupMembers, delivered...))
		}
		births[pid] = birth
	}
	result := signalWorkerGroupUsing(group, signal, syscall.Kill, workerGroupMembers, delivered...)
	for pid, saved := range births {
		birth, err := processBirth(pid)
		if errors.Is(err, os.ErrNotExist) || err == nil && birth != saved {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		pgid, groupErr := syscall.Getpgid(pid)
		if groupErr != nil {
			if !errors.Is(groupErr, syscall.ESRCH) {
				result = errors.Join(result, groupErr)
			}
			continue
		}
		if pgid == group {
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
