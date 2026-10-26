package myce

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func validAttemptIdentity(identity string) bool {
	if len(identity) != 32 {
		return false
	}
	_, err := hex.DecodeString(identity)
	return err == nil
}

// pruneAttemptState uses the cleared custody record as the deletion authority.
// Its mtime is the acknowledgement/save time, so even a long-running attempt
// gets a full log-retention window after release. Delete the authority last so
// interrupted cleanup retries. Unknown/orphan records remain for recovery.
func pruneAttemptState(dir string, now time.Time, age time.Duration) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	protected := map[string]bool{}
	// The root index can briefly retain a completed actor until its loop joins it.
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err == nil {
		var st workerState
		if err := decodeWorkerState(data, &st); err != nil {
			return err
		}
		for _, name := range st.Attempts {
			protected[name] = true
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var result error
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "state-attempt-") || !strings.HasSuffix(name, ".json") || protected[name] {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if !info.Mode().IsRegular() || info.ModTime().After(now.Add(-age)) {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		var st workerState
		if err := decodeWorkerState(data, &st); err != nil {
			result = errors.Join(result, err)
			continue
		}
		if st.Job != nil || st.Result != nil || st.Pending != 0 || st.PID != 0 || st.LaunchPending || !validAttemptIdentity(st.Identity) {
			continue
		}
		// Docker custody and acknowledgement belong to its existing stopped-container
		// collector; preserve evidence until that collector has finished successfully.
		held := false
		for _, suffix := range []string{".docker.json", ".docker-ack.json"} {
			if _, err := os.Lstat(filepath.Join(dir, st.Identity+suffix)); err == nil {
				held = true
			} else if !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
				held = true
			}
		}
		if held {
			continue
		}
		var files []string
		for _, candidate := range entries {
			n := candidate.Name()
			owned := n == "attempt-"+st.Identity+".json" || n == "native-"+st.Identity+".json"
			for _, suffix := range []string{".result.json", ".outcome.json", ".stop.json", ".priority-stop.json", ".observation-loss.json", ".usage.json", ".docker-evidence.json", ".docker-events.json", ".cid"} {
				owned = owned || n == st.Identity+suffix
			}
			owned = owned || strings.HasPrefix(n, st.Identity+"-") && strings.HasSuffix(n, ".cid")
			if !owned {
				continue
			}
			info, err := candidate.Info()
			if err != nil {
				result = errors.Join(result, err)
				held = true
				continue
			}
			if !info.Mode().IsRegular() || info.ModTime().After(now.Add(-age)) {
				held = true
				continue
			}
			files = append(files, filepath.Join(dir, n))
		}
		if held {
			continue
		}
		var failure error
		for _, file := range files {
			if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
				failure = errors.Join(failure, err)
			}
		}
		if failure == nil {
			failure = os.Remove(path)
		}
		result = errors.Join(result, failure)
	}
	return result
}
