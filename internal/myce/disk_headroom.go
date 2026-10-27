package myce

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
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
func diskInputCacheBytes(j *Job) uint64 {
	// NAS/path inputs are staged directly. Only immutable manifest inputs use
	// the file cache; a hint alone does not create a second physical copy.
	if len(jobInputFiles(j)) == 0 {
		return 0
	}
	input, _ := diskInputOutput(j)
	return input
}
func projectedDiskNeed(j *Job) uint64 {
	input, output := diskInputOutput(j)
	return addDiskBytes(addDiskBytes(input, output), diskInputCacheBytes(j))
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
			cache := diskInputCacheBytes(j)
			relevant = relevant || cache > 0 || len(j.Spec.SharedInputs) > 0
			need = addDiskBytes(need, cache)
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
func diskVolumeReason(m *Machine, v DiskVolume, need uint64) string {
	label := fmt.Sprintf("disk_headroom: host %s %s", hostID(m), strings.Join(v.Roots, "+"))
	if v.Error != "" && !v.Retained || v.TotalBytes == 0 {
		return fmt.Sprintf("%s disk reading unavailable (last sample age %.1fs); need=%d bytes", label, v.AgeSeconds, need)
	}
	floor := diskFreeFloor(v.TotalBytes)
	if v.FreeBytes < floor || need > v.FreeBytes-floor {
		return fmt.Sprintf("%s free=%d bytes floor=%d bytes need=%d bytes total=%d bytes", label, v.FreeBytes, floor, need, v.TotalBytes)
	}
	return ""
}
func diskHeadroomReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	for _, v := range m.DiskVolumes {
		if need, relevant := diskVolumeNeed(j, v); relevant {
			if reason := diskVolumeReason(m, v, need); reason != "" {
				return reason
			}
		}
	}
	return ""
}

// Retained readings bridge three heartbeats plus the sampling budget. Beyond
// that window, neither a retained value nor a still-checking-in worker proves
// current headroom. The explicit age also remains visible in status.
func (q *Queue) currentDiskVolume(v DiskVolume, receivedAt time.Time) DiskVolume {
	interval := v.SampleIntervalSeconds
	if interval <= 0 {
		interval = 1
	}
	// Faster control ticks cannot shorten the bounded disk-observation cycle:
	// four local probes plus one NAS probe can consume these existing budgets.
	// Otherwise healthy readings expire during ordinary preparation/handoff.
	samplingBudget := (4*localObservationBudget + heartbeatFilesystemBudget).Seconds()
	interval = max(interval, samplingBudget)
	// Workers timestamp before probing up to five roots, so a healthy sample
	// can already be 850 ms old on receipt. Add that budget to three cycles
	// to avoid false disk holds (3.85 s at a 1 s heartbeat), while still aging
	// missing observations out after this bounded allowance.
	window := 3*interval + samplingBudget
	// SampledAt belongs to the worker clock. Age is measured there, then
	// advanced only by queue time since receipt; clock skew cannot age a sample.
	v.AgeSeconds = max(0, v.AgeSeconds) + max(0, q.clock.Now().Sub(receivedAt).Seconds())
	if receivedAt.IsZero() || v.SampledAt.IsZero() || v.AgeSeconds > window {
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
		v = q.currentDiskVolume(v, m.DiskReceivedAt)
		if reason := diskVolumeReason(m, v, need); reason != "" {
			return reason
		}
	}
	return ""
}

// The start fence refuses only a current or retained observation below the free
// floor. Need was admitted at dispatch and staged inputs may already be
// reflected in free space; an expired reading is not evidence of exhaustion (s029).
func (q *Queue) startDiskFloorReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	for _, v := range m.DiskVolumes {
		if _, relevant := diskVolumeNeed(j, v); !relevant {
			continue
		}
		v = q.currentDiskVolume(v, m.DiskReceivedAt)
		if v.Error != "" && !v.Retained || v.TotalBytes == 0 {
			continue
		}
		if reason := diskVolumeReason(m, v, 0); reason != "" {
			return reason
		}
	}
	return ""
}

// Model loads only write a cache copy when verified files are not present.
// Calls to ready/loading slots and restarts with cached/local files write none.
func (q *Queue) modelLoadDiskHeadroomReason(model Model, m *Machine) string {
	if model.DirectNAS || model.LocalSource != "" || slices.Contains(m.CachedModels, model.Name) {
		return ""
	}
	if slot := q.s.ModelSlots[targetSlotKey(m, model.Name)]; slot != nil && slot.Session == m.Session && (slot.State == "ready" || slot.State == "loading") {
		return ""
	}
	for _, v := range m.DiskVolumes {
		if slices.Contains(v.Roots, "model-cache") {
			v = q.currentDiskVolume(v, m.DiskReceivedAt)
			if reason := diskVolumeReason(m, v, uint64(max(0, model.SizeBytes))); reason != "" {
				return reason
			}
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
		reason := ""
		if call := q.calls[j.ID]; call != nil {
			model, _ := q.model(call.Model)
			reason = q.modelLoadDiskHeadroomReason(model, m)
		} else {
			reason = q.diskHeadroomReason(j, m)
		}
		if reason != "" {
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
