package myce

import "time"

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

func taskOutputFiles(j Job) FileManifest {
	if j.LocalOutputs != nil {
		return j.LocalOutputs.Files
	}
	return j.OutputRefs
}
