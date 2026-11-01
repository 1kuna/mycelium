package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// These records are decode-only compatibility for retained queue and worker
// state. The unfinished implementation is preserved on park/storage-protection.
// No backend, activation, collection or worker-local delivery runs on main.
const parkedStorageReason = "storage protection is parked; use legacy file-store outputs or async_nas"

type OutputDestination struct {
	Protocol string `json:"protocol" yaml:"protocol"`
	Kind     string `json:"kind" yaml:"kind"`
	ID       string `json:"id" yaml:"id"`
}

type TaskOutputCustody struct {
	State                 string       `json:"state"`
	Allocation            string       `json:"allocation"`
	Files                 FileManifest `json:"files"`
	StoredAt              time.Time    `json:"stored_at"`
	CollectedAt           *time.Time   `json:"collected_at,omitempty"`
	CollectionRequestedAt *time.Time   `json:"collection_requested_at,omitempty"`
}

type StorageAllocation struct {
	ID            string    `json:"id"`
	Execution     Execution `json:"execution"`
	Machine       string    `json:"machine"`
	Session       string    `json:"session"`
	Backing       string    `json:"backing"`
	Bytes         uint64    `json:"bytes"`
	Files         uint64    `json:"files"`
	WorkerCustody bool      `json:"worker_custody"`
	At            time.Time `json:"at"`
}

func parkedStorage(j *Job) bool {
	return j != nil && (j.StorageAllocation != nil || j.LocalOutputs != nil || j.Spec.StorageProtocol != "" || j.Spec.OutputDestination != nil)
}

// Historical protocol fields remain as provenance after collection. The exact
// persisted collection acknowledgement retires worker custody, so ordinary
// owner release, retention and history expiry may drain the remaining records.
// A delivery receipt alone never authorizes deleting the worker's last copy.
func parkedStorageCustody(j *Job) bool {
	if !parkedStorage(j) {
		return false
	}
	c, a, d := j.LocalOutputs, j.StorageAllocation, j.Delivery
	return c == nil || c.State != "collected" || c.CollectedAt == nil ||
		a == nil || c.Allocation != a.ID || a.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || a.Machine != j.Machine || a.Session != j.Session ||
		d == nil || d.State != "verified" || d.VerifiedAt == nil || !sameDelivery(j, *j)
}

// A crash can leave a synced protected child before its root discovery record.
// Read those small state files before changing the root session/index. Unsettled
// custody needs the parked implementation to reconcile it; never claim empty
// custody or start an unprotected replacement. Cleared child records may drain.
func clearedParkedStorageChildren(dir string) (map[string]string, error) {
	cleared := map[string]string{}
	physical, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	base := filepath.Join(physical, "protected-attempts")
	entries, err := os.ReadDir(base)
	if errors.Is(err, os.ErrNotExist) {
		return cleared, nil
	}
	if err != nil {
		return nil, err
	}
	if err = noModelSymlinks(base, false); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 64 {
			return nil, errors.New(parkedStorageReason)
		}
		children, err := os.ReadDir(filepath.Join(base, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			if !strings.HasPrefix(child.Name(), "state-attempt-") || !strings.HasSuffix(child.Name(), ".json") {
				continue
			}
			path := filepath.Join(base, entry.Name(), child.Name())
			if err = noModelSymlinks(path, false); err != nil {
				return nil, err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			var saved workerState
			if err = decodeWorkerState(data, &saved); err != nil {
				return nil, err
			}
			if saved.StorageDirectory != entry.Name() || saved.Job != nil || saved.Result != nil || saved.PID != 0 || saved.LaunchPending {
				return nil, errors.New(parkedStorageReason)
			}
			cleared[entry.Name()+"/"+child.Name()] = saved.Session
		}
	}
	return cleared, nil
}

func taskOutputFiles(j Job) FileManifest {
	if j.LocalOutputs != nil {
		return j.LocalOutputs.Files
	}
	return j.OutputRefs
}
