package myce

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

// DiskNeeds is an optional hint. Measured manifests take precedence per dimension.
type DiskNeeds struct {
	InputBytes      uint64 `json:"input_bytes,omitempty" yaml:"input_bytes,omitempty"`
	OutputBytes     uint64 `json:"output_bytes,omitempty" yaml:"output_bytes,omitempty"`
	CheckpointBytes uint64 `json:"checkpoint_bytes,omitempty" yaml:"checkpoint_bytes,omitempty"`
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
func diskInputOutput(j *Job) (input, output uint64) {
	files := jobInputFiles(j)
	input, output = manifestDiskBytes(files), manifestDiskBytes(j.OutputRefs)
	unknownInput := len(files) == 0
	for _, f := range files {
		unknownInput = unknownInput || f.SizeBytes <= 0
	}
	if j.Spec.Disk != nil {
		// Zero/unknown sizes cannot suppress an explicit fallback hint. Actual
		// positive manifest sizes remain authoritative, including smaller sizes.
		if unknownInput {
			input = max(input, j.Spec.Disk.InputBytes)
		}
		if output == 0 {
			output = j.Spec.Disk.OutputBytes
		}
	}
	return
}
func projectedDiskNeed(j *Job) uint64 {
	input, output := diskInputOutput(j)
	return addDiskBytes(addDiskBytes(input, input), output)
}
func diskVolumeNeed(j *Job, v DiskVolume) (uint64, bool) {
	input, output := diskInputOutput(j)
	var need uint64
	relevant := false
	for _, root := range v.Roots {
		switch root {
		case "data":
			relevant = true
			need = addDiskBytes(need, addDiskBytes(input, output))
		case "file-cache":
			relevant = relevant || input > 0
			need = addDiskBytes(need, input)
		case "checkpoint":
			if j.Spec.Resumable || j.ResumeFiles != nil || j.Spec.Disk != nil && j.Spec.Disk.CheckpointBytes > 0 {
				relevant = true
				checkpoint := uint64(0)
				if j.ResumeFiles != nil {
					checkpoint = manifestDiskBytes(j.ResumeFiles.Files)
				}
				if checkpoint == 0 && j.Spec.Disk != nil {
					checkpoint = j.Spec.Disk.CheckpointBytes
				}
				need = addDiskBytes(need, checkpoint)
			}
		case "environment":
			relevant = relevant || j.Spec.Environment.Key() != "" || j.Spec.extensionJob()
		case "model-cache":
			relevant = relevant || j.Spec.Needs.Model != ""
		}
	}
	return need, relevant
}
func diskFreeFloor(total uint64) uint64 {
	// Leave 1% for filesystem metadata and other writers; on small volumes keep
	// at least 256 MiB for control logs/state. This is headroom, not a job-size cap.
	return max(total/100, 256<<20)
}
func diskVolumeReason(m *Machine, v DiskVolume, need, reserved uint64) string {
	label := fmt.Sprintf("disk_headroom: host %s %s", hostID(m), strings.Join(v.Roots, "+"))
	if v.Error != "" && !v.Retained || v.TotalBytes == 0 {
		return fmt.Sprintf("%s disk reading unavailable (last sample age %.1fs); need=%d bytes", label, v.AgeSeconds, need)
	}
	floor := diskFreeFloor(v.TotalBytes)
	required := addDiskBytes(need, reserved)
	if v.FreeBytes < floor || required > v.FreeBytes-floor {
		return fmt.Sprintf("%s free=%d bytes floor=%d bytes need=%d bytes reserved=%d bytes total=%d bytes", label, v.FreeBytes, floor, need, reserved, v.TotalBytes)
	}
	return ""
}
func diskHeadroomReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	for _, v := range m.DiskVolumes {
		if need, relevant := diskVolumeNeed(j, v); relevant {
			if reason := diskVolumeReason(m, v, need, 0); reason != "" {
				return reason
			}
		}
	}
	return ""
}

// Retained readings bridge a slow observation for three heartbeats. Beyond
// that window, neither a retained value nor a still-checking-in worker proves
// current headroom. The explicit age also remains visible in status.
func (q *Queue) currentDiskVolume(v DiskVolume) DiskVolume {
	interval := v.SampleIntervalSeconds
	if interval <= 0 {
		interval = 1
	}
	age := q.clock.Now().Sub(v.SampledAt).Seconds()
	v.AgeSeconds = max(v.AgeSeconds, age)
	if v.SampledAt.IsZero() || v.AgeSeconds > 3*interval || age < -3*interval {
		v.Error = "disk reading unavailable"
		v.Retained = false
	}
	return v
}
func (q *Queue) diskHeadroomReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	for _, v := range m.DiskVolumes {
		need, relevant := diskVolumeNeed(j, v)
		if !relevant {
			continue
		}
		v = q.currentDiskVolume(v)
		reserved := uint64(0)
		for _, other := range q.s.Jobs {
			if other.ID == j.ID || other.ReleaseConfirmed || other.State != "running" && other.State != "stopping" {
				continue
			}
			owner := q.s.Machines[other.Machine]
			if owner == nil || hostID(owner) != hostID(m) {
				continue
			}
			for _, ov := range owner.DiskVolumes {
				same := v.ID != "" && ov.ID == v.ID || v.ID == "" && owner.ID == m.ID && slices.Equal(ov.Roots, v.Roots)
				if same {
					n, _ := diskVolumeNeed(other, ov)
					reserved = addDiskBytes(reserved, n)
					break
				}
			}
		}
		if reason := diskVolumeReason(m, v, need, reserved); reason != "" {
			return reason
		}
	}
	return ""
}

// Capacity preserves measured totals while removing transient use and errors.
func diskCapacityVolumes(volumes []DiskVolume) []DiskVolume {
	out := []DiskVolume{}
	for _, v := range volumes {
		if v.TotalBytes > 0 {
			v.FreeBytes = v.TotalBytes
			v.Error = ""
			out = append(out, v)
		}
	}
	return out
}
func (q *Queue) diskCapacityRefusal(j *Job) string {
	if !q.hasDiskReports("") {
		return ""
	}
	reasons := []string{}
	for _, m := range q.plannerMachines() {
		if !q.jobAllowsMachine(j, m.ID) || !executionTargetMatches(j.Spec, m) || !deviceAllowed(j, m) || !j.Spec.Needs.cpuOnly() && (m.GPU == "cpu" || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU)) {
			continue
		}
		copy := *m
		copy.DiskVolumes = diskCapacityVolumes(m.DiskVolumes)
		if reason := diskHeadroomReason(j, &copy); reason != "" {
			reasons = append(reasons, reason)
		} else {
			return ""
		}
	}
	slices.Sort(reasons)
	return strings.Join(slices.Compact(reasons), "; ")
}
func (q *Queue) diskWaitingReason(j *Job, host string) string {
	if !q.hasDiskReports(host) {
		return ""
	}
	reasons := []string{}
	for _, m := range q.plannerMachines() {
		if !q.jobAllowsMachine(j, m.ID) || host != "" && hostID(m) != host {
			continue
		}
		if !executionTargetMatches(j.Spec, m) || !deviceAllowed(j, m) || !j.Spec.Needs.cpuOnly() && (m.GPU == "cpu" || len(j.Spec.Needs.Platform.Required) > 0 && !slices.Contains(j.Spec.Needs.Platform.Required, m.GPU)) {
			continue
		}
		if reason := q.diskHeadroomReason(j, m); reason != "" {
			reasons = append(reasons, reason)
		} else {
			return ""
		}
	}
	slices.Sort(reasons)
	return strings.Join(slices.Compact(reasons), "; ")
}

func (q *Queue) hasDiskReports(host string) bool {
	for _, m := range q.s.Machines {
		if len(m.DiskVolumes) > 0 && (host == "" || hostID(m) == host) {
			return true
		}
	}
	return false
}
