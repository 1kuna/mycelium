package myce

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// PhysicalStorageStatus remains shadow until the explicit qualified migration.
// Existing allocations already reduce statfs free space; only unwritten
// commitments are deducted again. ProtectedFreeBytes is zero without active,
// freshly verified backing/control/custody and write confinement.
type PhysicalStorageStatus struct {
	Mode                 string               `json:"mode"`
	SampledAt            time.Time            `json:"sampled_at"`
	DiskFreeBytes        uint64               `json:"disk_free_bytes"`
	RemainingUploadBytes uint64               `json:"remaining_upload_bytes"`
	ReservedOutputBytes  uint64               `json:"reserved_output_bytes"`
	UncommittedBytes     uint64               `json:"uncommitted_bytes" jsonschema:"description=Upper bound only: physical free minus remaining tus lengths and known output commitments. Excludes unqualified control expansion and worker backing; not usable for protected admission."`
	ProtectedFreeBytes   uint64               `json:"protected_free_bytes"`
	Control              ControlStorageStatus `json:"control"`
	UploadMetadataBytes  uint64               `json:"upload_metadata_bytes"`
	Qualification        string               `json:"qualification"`
	Error                string               `json:"error,omitempty"`
}

// ControlStorageStatus measures the current SQLite working set, including job
// and environment logs in the database/WAL. It is not a bound on future writes,
// a historical-burst floor, or a claim that sync/checkpoint expansion is reserved.
type ControlStorageStatus struct {
	DatabaseBytes  uint64 `json:"database_bytes"`
	WALBytes       uint64 `json:"wal_bytes"`
	SharedBytes    uint64 `json:"shared_bytes"`
	AllocatedBytes uint64 `json:"allocated_bytes"`
}

type controlStorageFiles struct {
	database, wal, shared string
}

// storageAllocation counts allocated blocks, not sparse logical maxima. The
// inode key deduplicates aliases/hardlinks without exposing private identifiers.
// Missing WAL/SHM files are normal before use or after SQLite checkpointing.
func storageAllocation(ctx context.Context, path string, optional bool, seen map[[2]uint64]bool) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if optional && errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Blocks < 0 {
		return 0, errors.New("storage allocation unavailable")
	}
	key := [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}
	if seen[key] {
		return 0, nil
	}
	seen[key] = true
	if uint64(stat.Blocks) > math.MaxUint64/512 {
		return math.MaxUint64, nil
	}
	return uint64(stat.Blocks) * 512, nil
}

func (s *FileStore) physicalStorageStatus(ctx context.Context, status FileStoreStatus, payloads map[string]storedPayload, now time.Time) *PhysicalStorageStatus {
	v := &PhysicalStorageStatus{Mode: "shadow", SampledAt: now, DiskFreeBytes: status.DiskFreeBytes,
		ReservedOutputBytes: status.ReservedOutputBytes,
		Qualification:       "unqualified: native write denial, control expansion/reserves, worker custody backing and write confinement require proof"}
	if status.Error != "" {
		v.Error = "physical storage accounting unavailable"
		return v
	}
	for _, p := range payloads {
		v.RemainingUploadBytes = addDiskBytes(v.RemainingUploadBytes, p.length-p.size)
	}
	remaining := addDiskBytes(v.RemainingUploadBytes, v.ReservedOutputBytes)
	v.UncommittedBytes = v.DiskFreeBytes - min(v.DiskFreeBytes, remaining)
	// A fixed set of SQLite files, and metadata for uploads already inventoried
	// by capacity(). No directory traversal or hashing, and no q.mu held here.
	seen := map[[2]uint64]bool{}
	var err error
	for _, file := range []struct {
		path     string
		optional bool
		bytes    *uint64
	}{{s.controlFiles.database, false, &v.Control.DatabaseBytes}, {s.controlFiles.wal, true, &v.Control.WALBytes}, {s.controlFiles.shared, true, &v.Control.SharedBytes}} {
		if file.path == "" {
			err = errors.New("control files unavailable")
			break
		}
		*file.bytes, err = storageAllocation(ctx, file.path, file.optional, seen)
		if err != nil {
			break
		}
		v.Control.AllocatedBytes = addDiskBytes(v.Control.AllocatedBytes, *file.bytes)
	}
	if err == nil {
		for hash, p := range payloads {
			if !p.upload {
				continue
			}
			var bytes uint64
			bytes, err = storageAllocation(ctx, filepath.Join(s.root, "uploads", hash+".info"), true, seen)
			if err != nil {
				break
			}
			v.UploadMetadataBytes = addDiskBytes(v.UploadMetadataBytes, bytes)
		}
	}
	if err != nil {
		// Never publish OS errors containing private paths. Partial measurements
		// remain diagnostic, but an incomplete observation grants no headroom.
		v.Error = "physical storage accounting unavailable"
		v.UncommittedBytes = 0
	}
	if status.LimitBytes == 0 && status.Error == "" && v.Error == "" {
		v.Mode, v.ProtectedFreeBytes = "active", status.FreeBytes
		v.Qualification = "verified backing/control/confinement; admitted unwritten limits remain charged"
	}
	return v
}
