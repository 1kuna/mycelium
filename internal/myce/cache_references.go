package myce

import (
	"context"
	"errors"
	"net/http"
)

type cacheReferenceRequest struct {
	Key   string         `json:"key"`
	Model *ModelSnapshot `json:"model,omitempty"`
}

type workerDiskDemandRequest struct {
	Machine string `json:"machine"`
	Session string `json:"session"`
}
type workerDiskDemand struct {
	Roots []string `json:"roots"`
	Bytes uint64   `json:"bytes"`
}

func (a API) registerCacheReferences(mux *requestMux) {
	mux.HandleFunc("POST /api/workers/disk-demand", func(w http.ResponseWriter, r *http.Request) {
		var request workerDiskDemandRequest
		if !decode(w, r, &request) {
			return
		}
		respond(w, a.Queue.workerDiskDemand(r.Context(), request), nil)
	})
	mux.HandleFunc("POST /api/workers/cache-reference", func(w http.ResponseWriter, r *http.Request) {
		var request cacheReferenceRequest
		if !decode(w, r, &request) {
			return
		}
		if (FileRef{SHA256: request.Key}).validate() != nil {
			respond(w, nil, errors.New("invalid cache key"))
			return
		}
		respond(w, map[string]bool{"referenced": a.Queue.cacheReferenced(r.Context(), request)}, nil)
	})
}

// Read only the existing queue manifests/capacity hints. This does not grant
// placement, release unique data or inspect worker files under the mutex.
func (q *Queue) workerDiskDemand(ctx context.Context, request workerDiskDemandRequest) []workerDiskDemand {
	unlock := requestReadLock(ctx, &q.mu)
	defer unlock()
	m := q.s.Machines[request.Machine]
	if m == nil || m.Session != request.Session {
		return nil
	}
	var out []workerDiskDemand
	for _, volume := range m.DiskVolumes {
		floor := diskFreeFloor(volume.TotalBytes)
		if volume.TotalBytes <= floor {
			continue
		}
		var smallest uint64
		for _, j := range q.s.Jobs {
			if j.State != "queued" || !q.jobAllowsMachine(j, m.ID) || !executionTargetMatches(j.Spec, m) || !deviceAllowed(j, m) {
				continue
			}
			need, relevant := q.diskVolumeNeed(j, volume, m)
			if call := q.calls[j.ID]; call != nil {
				if model, ok := q.model(call.Model); ok && !model.DirectNAS && model.LocalSource == "" {
					for _, role := range volume.Roots {
						if role == "model-cache" {
							need = addDiskBytes(need, uint64(max(0, model.SizeBytes)))
							relevant = true
						}
					}
				}
			}
			if relevant && need > 0 && need <= volume.TotalBytes-floor && (volume.FreeBytes < floor || need > volume.FreeBytes-floor) && (smallest == 0 || need < smallest) {
				smallest = need
			}
		}
		if smallest > 0 {
			out = append(out, workerDiskDemand{Roots: volume.Roots, Bytes: smallest})
		}
	}
	return out
}

func (w Worker) reclaimPendingDisk(ctx context.Context) {
	if w.diskBudget == nil {
		return
	}
	var demands []workerDiskDemand
	if err := w.Client.Call(ctx, "POST", "/api/workers/disk-demand", workerDiskDemandRequest{Machine: w.Machine.ID, Session: w.Machine.Session}, &demands); err != nil {
		return
	}
	paths := map[string]string{"data": w.diskBudget.inventoryDir}
	if w.fileCache != nil {
		paths["file-cache"] = w.fileCache.Root
	}
	if w.environmentCache != nil {
		paths["environment"] = w.environmentCache.Root
	}
	for _, root := range w.diskBudget.rootSnapshot() {
		if root.name == "models" {
			paths["model-cache"] = root.path
		}
	}
	for _, demand := range demands {
		for _, role := range demand.Roots {
			if path := paths[role]; path != "" {
				release, err := w.diskBudget.reserve(ctx, path, demand.Bytes, 0)
				if err == nil {
					release()
				}
				break
			}
		}
	}
}

func (q *Queue) cacheReferenced(ctx context.Context, r cacheReferenceRequest) bool {
	unlock := requestReadLock(ctx, &q.mu)
	defer unlock()
	held := map[string]bool{}
	for _, release := range q.s.HostReleases {
		held[release.Job] = true
	}
	for _, plan := range q.s.PreparationPlans {
		held[plan.Demand] = true
	}
	for _, job := range q.s.Jobs {
		if !mutableJob(job) && !held[job.ID] {
			continue
		}
		for _, input := range job.Spec.SharedInputs {
			if input.key() == r.Key {
				return true
			}
		}
		if r.Model != nil {
			for _, model := range job.Spec.ModelSnapshots {
				if model.Repo == r.Model.Repo && model.Revision == r.Model.Revision {
					return true
				}
			}
		}
	}
	return false
}

func (c *SharedInputCache) queueReferenced(ctx context.Context, key string, recovery *sharedRecoverySource) (bool, error) {
	request := cacheReferenceRequest{Key: key}
	if recovery != nil {
		request.Model = recovery.Model
	}
	var state struct {
		Referenced bool `json:"referenced"`
	}
	err := c.Client.Call(ctx, "POST", "/api/workers/cache-reference", request, &state)
	return state.Referenced, err
}
