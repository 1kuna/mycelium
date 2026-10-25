package myce

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// DiskNeeds is an optional hint. Measured manifests take precedence per dimension.
type DiskNeeds struct {
	InputBytes  uint64 `json:"input_bytes,omitempty" yaml:"input_bytes,omitempty"`
	OutputBytes uint64 `json:"output_bytes,omitempty" yaml:"output_bytes,omitempty"`
}

func addDiskBytes(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}
func manifestDiskBytes(files FileManifest) uint64 {
	var size uint64
	for _, f := range files {
		if f.SizeBytes > 0 {
			size = addDiskBytes(size, uint64(f.SizeBytes))
		}
	}
	return size
}
func projectedDiskNeed(j *Job) uint64 {
	files := jobInputFiles(j)
	input, output := manifestDiskBytes(files), manifestDiskBytes(j.OutputRefs)
	if j.Spec.Disk != nil {
		if len(files) == 0 {
			input = j.Spec.Disk.InputBytes
		}
		if len(j.OutputRefs) == 0 {
			output = j.Spec.Disk.OutputBytes
		}
	}
	// An uncached input occupies the immutable file cache and a staged workdir
	// copy. Reserve both; an already-cached input may need less, but the queue
	// cannot prove a cache hit from a manifest alone.
	return addDiskBytes(addDiskBytes(input, input), output)
}
func diskFreeFloor(total uint64) uint64 {
	// Leave 1% for filesystem metadata and other writers; on small volumes keep
	// at least 256 MiB for control logs/state. This reserves headroom, not a job
	// size cap: larger volumes and measured job needs raise the required free space.
	return max(total/100, 256<<20)
}
func diskHeadroomReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	need := projectedDiskNeed(j)
	for _, volume := range m.DiskVolumes {
		if slices.Contains(volume.Roots, "checkpoint") && len(volume.Roots) == 1 && !needsCheckpoint(j) {
			continue
		}
		if slices.Contains(volume.Roots, "environment") && len(volume.Roots) == 1 && j.Spec.Environment.Key() == "" && !j.Spec.extensionJob() {
			continue
		}
		floor := diskFreeFloor(volume.TotalBytes)
		// Environment sizes aren't declared by jobs; protect that volume's floor.
		volumeNeed := need
		if len(volume.Roots) == 1 && volume.Roots[0] == "environment" {
			volumeNeed = 0
		}
		if volume.Error != "" || volume.TotalBytes == 0 || volume.FreeBytes < floor || volumeNeed > volume.FreeBytes-floor {
			return fmt.Sprintf("disk_headroom: host %s %s free=%d bytes floor=%d bytes need=%d bytes", hostID(m), strings.Join(volume.Roots, "+"), volume.FreeBytes, floor, volumeNeed)
		}
	}
	return ""
}
