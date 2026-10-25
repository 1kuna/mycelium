package myce

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"mycelium/internal/probeguard"
)

// DiskVolume contains no local paths or filesystem IDs. Roots name uses of the
// same volume; rate is signed free-space change, nil until two samples exist.
type DiskVolume struct {
	Roots              []string  `json:"roots"`
	TotalBytes         uint64    `json:"total_bytes"`
	FreeBytes          uint64    `json:"free_bytes"`
	ChangeBytesPerHour *float64  `json:"change_bytes_per_hour,omitempty"`
	SampledAt          time.Time `json:"sampled_at"`
	Error              string    `json:"error,omitempty"`
}
type DiskReading struct {
	Volume                string
	TotalBytes, FreeBytes uint64
}

var workerDiskProbe = localDiskReading

func localDiskReading(path string) (DiskReading, error) {
	probeguard.Check()
	// Cache/checkpoint directories may not exist yet. Their nearest existing
	// ancestor identifies the target volume without creating anything.
	for {
		var st syscall.Statfs_t
		err := syscall.Statfs(path, &st)
		if os.IsNotExist(err) {
			parent := filepath.Dir(path)
			if parent != path {
				path = parent
				continue
			}
		}
		if err != nil {
			return DiskReading{}, err
		}
		return DiskReading{Volume: fmt.Sprint(st.Fsid), TotalBytes: st.Blocks * uint64(st.Bsize), FreeBytes: st.Bavail * uint64(st.Bsize)}, nil
	}
}

type diskObservation struct {
	reading DiskReading
	at      time.Time
}
type workerDiskSampler struct {
	probes   [3]checkpointIO[diskObservation]
	previous map[string]diskObservation
}

func (w *Worker) sampleDisks(data, checkpoint string) []DiskVolume {
	if w.diskSampler == nil {
		w.diskSampler = &workerDiskSampler{previous: map[string]diskObservation{}}
	}
	sampler := w.diskSampler
	probe := w.DiskProbe
	if probe == nil {
		probe = workerDiskProbe
	}
	now := w.Clock.Now()
	out := []DiskVolume{}
	seen := map[string]int{}
	for i, root := range []struct{ name, path string }{{"data", data}, {"environment", w.EnvironmentRoot}, {"checkpoint", checkpoint}} {
		if root.path == "" {
			continue
		}
		// A stalled statfs (e.g. NAS) consumes at most one outstanding syscall per
		// root. The existing 25 ms observation budget costs <=75 ms per heartbeat,
		// avoiding a filesystem stall blocking stop delivery or unbounded goroutines.
		observation, err := sampler.probes[i].run(root.path, func() (diskObservation, error) {
			r, err := probe(root.path)
			return diskObservation{reading: r, at: now}, err
		})
		if err != nil {
			out = append(out, DiskVolume{Roots: []string{root.name}, Error: "disk reading unavailable"})
			continue
		}
		r := observation.reading
		if index, ok := seen[r.Volume]; ok {
			out[index].Roots = append(out[index].Roots, root.name)
			continue
		}
		seen[r.Volume] = len(out)
		v := DiskVolume{Roots: []string{root.name}, TotalBytes: r.TotalBytes, FreeBytes: r.FreeBytes, SampledAt: observation.at}
		if old, ok := sampler.previous[r.Volume]; ok && r.TotalBytes == old.reading.TotalBytes && observation.at.After(old.at) {
			delta := float64(r.FreeBytes-min(r.FreeBytes, old.reading.FreeBytes)) - float64(old.reading.FreeBytes-min(r.FreeBytes, old.reading.FreeBytes))
			rate := delta / observation.at.Sub(old.at).Hours()
			v.ChangeBytesPerHour = &rate
		}
		out = append(out, v)
	}
	next := map[string]diskObservation{}
	// Only completed observations form the next baseline. Failed probes cannot
	// retain stale free bytes as current evidence.
	for volume, index := range seen {
		v := out[index]
		next[volume] = diskObservation{reading: DiskReading{Volume: volume, TotalBytes: v.TotalBytes, FreeBytes: v.FreeBytes}, at: v.SampledAt}
	}
	sampler.previous = next
	return out
}
