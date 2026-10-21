package myce

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Read only: never clean, adopt, or merge execution custody during conversion.
// Process receipts from production are sparse workerState, not heartbeat state.
func settledMigrationSpool(dir string, snapshots ...Snapshot) error {
	lock, err := os.OpenFile(filepath.Join(dir, "worker.lock"), os.O_RDWR, 0)
	if err == nil {
		defer lock.Close()
		if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return migrationError("machine_migration_incomplete", "spool still in use")
		}
		defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return inspectMigrationSpool(dir, snapshots...)
}

func inspectMigrationSpool(dir string, snapshots ...Snapshot) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var problems []string
	refuse := func(name, reason string) {
		problems = append(problems, name+": "+reason)
	}
	for _, entry := range entries {
		name := entry.Name()
		heartbeat := name == "state.json" || name == "legacy-state.json" || strings.HasPrefix(name, "state-attempt-") && strings.HasSuffix(name, ".json")
		process := strings.HasSuffix(name, ".json") && (strings.HasPrefix(name, "attempt-") || strings.HasPrefix(name, "engine-"))
		result := strings.HasSuffix(name, ".result.json")
		docker := strings.HasSuffix(name, ".docker.json")
		marker := name == "operator-stop.json"
		if !heartbeat && !process && !result && !docker && !marker {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			refuse(name, "spool_unreadable")
			continue
		}
		if !info.Mode().IsRegular() {
			refuse(name, "spool_not_regular")
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			refuse(name, "spool_unreadable")
			continue
		}
		if result {
			var r Result
			if err = decodeWorkerJSON(data, &r); err != nil {
				refuse(name, "spool_invalid")
			} else if reason := migrationAttemptRefusal(r, snapshots); reason != "" {
				refuse(name, reason)
			}
			continue
		}
		if docker {
			if reason := migrationDockerRefusal(dir, name, data, snapshots); reason != "" {
				refuse(name, reason)
			}
			continue
		}
		var s workerState
		if heartbeat || marker {
			err = decodeWorkerState(data, &s)
		} else {
			err = decodeWorkerJSON(data, &s)
		}
		if err != nil {
			refuse(name, "spool_invalid")
			continue
		}
		if s.Job != nil || s.Result != nil || s.LaunchPending {
			refuse(name, "spool_active_state")
		}
		// A retained operator-stop marker is harmless once the live heartbeat was
		// cleared. A keyed marker's attempts must also have cleared state files.
		if marker {
			for _, attempt := range s.Attempts {
				if filepath.Base(attempt) != attempt {
					refuse(name, "operator_stop_unsettled")
					continue
				}
				if _, e := os.Stat(filepath.Join(dir, attempt)); e == nil {
					refuse(name, "operator_stop_unsettled")
					continue
				} else if !errors.Is(e, os.ErrNotExist) {
					refuse(name, "spool_unreadable")
				}
			}
		}
		groups := map[int]bool{}
		if s.PID > 0 {
			groups[s.PID] = true
		}
		for group := range s.Groups {
			groups[group] = true
		}
		for group := range groups {
			members, e := workerGroupMembers(group)
			if e != nil {
				refuse(name, "process_group_unreadable")
				continue
			}
			if len(members) > 0 {
				refuse(name, "process_group_live")
			}
		}
		if s.Cgroup != "" {
			if _, e := os.Stat(s.Cgroup); !errors.Is(e, os.ErrNotExist) {
				refuse(name, "process_cgroup_unsettled")
			}
		}
	}
	if len(problems) != 0 {
		return migrationError("machine_migration_incomplete", "unresolved spool custody: "+strings.Join(problems, "; "))
	}
	return nil
}

// An exact queue release is authority; age, success, an ack file, or a larger
// attempt number alone is not. Superseded executions need their own settled copy.
func migrationAttemptRefusal(r Result, snapshots []Snapshot) string {
	if r.ID == "" || r.Attempt <= 0 || r.Machine == "" || r.Session == "" {
		return "queue_attempt_identity_missing"
	}
	for _, snapshot := range snapshots {
		for _, j := range snapshot.Jobs {
			if j == nil || j.ID != r.ID {
				continue
			}
			if j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
				if !terminal(j.State) {
					return "queue_attempt_live"
				}
				if !j.ReleaseConfirmed {
					return "queue_release_unsettled"
				}
				return ""
			}
			for _, c := range j.Copies {
				if c.Attempt == r.Attempt && c.Machine == r.Machine && c.Session == r.Session {
					if !c.ReleaseConfirmed {
						return "queue_release_unsettled"
					}
					return ""
				}
			}
		}
	}
	return "queue_attempt_unknown"
}

func migrationDockerRefusal(dir, name string, data []byte, snapshots []Snapshot) string {
	var d dockerExecution
	if decodeWorkerJSON(data, &d) != nil {
		return "spool_invalid"
	}
	identity := strings.TrimSuffix(name, ".docker.json")
	if d.Identity != identity {
		return "docker_identity_mismatch"
	}
	if !d.Released || d.Pending || d.OperatorConfirmed {
		return "docker_custody_unsettled"
	}
	// Production's ack is a retention timer, not exact attempt release authority.
	// ErrStale can clear the heartbeat after queue settlement without writing it.
	var ack dockerAcknowledgement
	receipt, err := os.ReadFile(filepath.Join(dir, identity+".docker-ack.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "docker_ack_unreadable"
	}
	if err == nil && (json.Unmarshal(receipt, &ack) != nil || ack.Worker == "" || ack.At.IsZero()) {
		return "docker_ack_invalid"
	}
	owner := d.Owner
	// Older writers without Owner can still bind custody via the same identity's
	// retained result. Never reconstruct an execution from an opaque filename.
	var result Result
	data, err = os.ReadFile(filepath.Join(dir, identity+".result.json"))
	if err == nil {
		if decodeWorkerJSON(data, &result) != nil {
			return "spool_invalid"
		}
		if owner != nil && (owner.Execution != result.Execution || owner.Machine != result.Machine || owner.Session != result.Session) {
			return "docker_owner_mismatch"
		}
		owner = &result
	} else if !errors.Is(err, os.ErrNotExist) {
		return "spool_unreadable"
	}
	if owner == nil {
		return "queue_attempt_identity_missing"
	}
	return migrationAttemptRefusal(*owner, snapshots)
}
