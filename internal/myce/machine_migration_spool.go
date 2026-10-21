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
	refuse := func(name string) error {
		return migrationError("machine_migration_incomplete", "unresolved spool custody: "+name)
	}
	acknowledged := func(r Result) bool {
		for _, snap := range snapshots {
			for _, j := range snap.Jobs {
				if j.ID == r.ID && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session && terminal(j.State) && j.ReleaseConfirmed {
					return true
				}
			}
		}
		return false
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
			return err
		}
		if !info.Mode().IsRegular() {
			return refuse(name)
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if result {
			var r Result
			if err = decodeWorkerJSON(data, &r); err != nil {
				return err
			}
			if !acknowledged(r) {
				return refuse(name)
			}
			continue
		}
		if docker {
			var d dockerExecution
			if err = decodeWorkerJSON(data, &d); err != nil {
				return err
			}
			var ack dockerAcknowledgement
			receipt, e := os.ReadFile(filepath.Join(dir, strings.TrimSuffix(name, ".docker.json")+".docker-ack.json"))
			if !d.Released || d.Pending || d.OperatorConfirmed || d.Identity != strings.TrimSuffix(name, ".docker.json") || e != nil || json.Unmarshal(receipt, &ack) != nil || ack.Worker == "" || ack.At.IsZero() {
				return refuse(name)
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
			return err
		}
		if s.Job != nil || s.Result != nil || s.LaunchPending {
			return refuse(name)
		}
		// A retained operator-stop marker is harmless once the live heartbeat was
		// cleared. A keyed marker's attempts must also have cleared state files.
		if marker {
			for _, attempt := range s.Attempts {
				if filepath.Base(attempt) != attempt {
					return refuse(name)
				}
				if _, e := os.Stat(filepath.Join(dir, attempt)); e == nil {
					return refuse(name)
				} else if !errors.Is(e, os.ErrNotExist) {
					return e
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
				return e
			}
			if len(members) > 0 {
				return refuse(name)
			}
		}
		if s.Cgroup != "" {
			if _, e := os.Stat(s.Cgroup); !errors.Is(e, os.ErrNotExist) {
				return refuse(name)
			}
		}
	}
	return nil
}
