package myce

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"mycelium/internal/probeguard"
)

// DiskVolume contains no local paths or raw filesystem IDs. ID is an opaque,
// stable host/volume hash. Roots name uses of the same volume; rate is signed
// free-space change, nil until two samples exist.
type DiskVolume struct {
	ID                    string    `json:"id,omitempty"`
	Roots                 []string  `json:"roots"`
	TotalBytes            uint64    `json:"total_bytes"`
	FreeBytes             uint64    `json:"free_bytes"`
	ChangeBytesPerHour    *float64  `json:"change_bytes_per_hour,omitempty"`
	SampledAt             time.Time `json:"sampled_at" jsonschema:"description=Worker-clock sample timestamp; freshness uses reported age plus queue time since receipt."`
	SampleIntervalSeconds float64   `json:"sample_interval_seconds,omitempty"`
	AgeSeconds            float64   `json:"age_seconds,omitempty" jsonschema:"description=Sample age on the worker at check-in; status adds queue-clock time since receipt."`
	Retained              bool      `json:"retained,omitempty"`
	Error                 string    `json:"error,omitempty"`
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
		return DiskReading{Volume: fmt.Sprint(st.Fsid), TotalBytes: st.Blocks * diskBlockSize(&st), FreeBytes: st.Bavail * diskBlockSize(&st)}, nil
	}
}

type diskObservation struct {
	reading DiskReading
	at      time.Time
}
type diskRootSample struct {
	probe checkpointIO[diskObservation]
	path  string
	last  diskObservation
}
type workerDiskSampler struct {
	roots    [5]diskRootSample
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
	interval := w.Interval.Seconds()
	if interval <= 0 {
		interval = 1
	}
	fileCache, modelCache := w.FileCacheRoot, w.CacheRoot
	if fileCache == "" {
		fileCache = filepath.Join(data, "file-cache")
	}
	if modelCache == "" {
		modelCache = filepath.Join(data, "model-cache")
	}
	out := []DiskVolume{}
	seen := map[string]int{}
	next := map[string]diskObservation{}
	for i, root := range []struct{ name, path string }{{"data", data}, {"environment", w.EnvironmentRoot}, {"checkpoint", checkpoint}, {"file-cache", fileCache}, {"model-cache", modelCache}} {
		if root.path == "" {
			continue
		}
		state := &sampler.roots[i]
		if state.path != root.path {
			state.path, state.last = root.path, diskObservation{}
		}
		// Keep one outstanding syscall per root. Local roots cost at most 4*25ms;
		// NAS uses the shared 750ms heartbeat budget, so network metadata latency
		// doesn't masquerade as lost storage or accumulate unbounded probe calls.
		state.probe.timeout = localObservationBudget
		if root.name == "checkpoint" {
			state.probe.timeout = heartbeatFilesystemBudget
		}
		observation, err := state.probe.run(root.path, func() (diskObservation, error) {
			r, err := probe(root.path)
			return diskObservation{reading: r, at: now}, err
		})
		if err == nil {
			state.last = observation
		} else {
			observation = state.last
		}
		r := observation.reading
		v := DiskVolume{Roots: []string{root.name}, TotalBytes: r.TotalBytes, FreeBytes: r.FreeBytes, SampledAt: observation.at, SampleIntervalSeconds: interval}
		if !observation.at.IsZero() {
			v.ID = identityHash(struct{ Host, Volume string }{hostID(&w.Machine), r.Volume})
			v.AgeSeconds = max(0, now.Sub(observation.at).Seconds())
		}
		if err != nil {
			v.Error = "disk reading unavailable"
			v.Retained = !observation.at.IsZero()
		}
		if old, ok := sampler.previous[r.Volume]; ok && r.TotalBytes == old.reading.TotalBytes && observation.at.After(old.at) {
			delta := float64(r.FreeBytes-min(r.FreeBytes, old.reading.FreeBytes)) - float64(old.reading.FreeBytes-min(r.FreeBytes, old.reading.FreeBytes))
			rate := delta / observation.at.Sub(old.at).Hours()
			v.ChangeBytesPerHour = &rate
		}
		if !observation.at.IsZero() {
			if previous, ok := next[r.Volume]; !ok || observation.at.After(previous.at) {
				next[r.Volume] = observation
			}
			if index, ok := seen[v.ID]; ok {
				roots := append(out[index].Roots, root.name)
				if v.SampledAt.After(out[index].SampledAt) || v.SampledAt.Equal(out[index].SampledAt) && v.Error == "" {
					out[index] = v
				}
				out[index].Roots = roots
				continue
			}
			seen[v.ID] = len(out)
		}
		out = append(out, v)
	}
	sampler.previous = next
	return out
}
