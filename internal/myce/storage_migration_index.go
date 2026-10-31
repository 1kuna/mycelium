package myce

import (
	"sync"
	"sync/atomic"
	"time"
)

// Derived, never persisted separately from jobs. The existing transaction journal
// updates only changed holders. sync.Map permits inventory intersection outside
// q.mu without copying the retained digest set; revision fences torn observations.
type storageMigrationIndex struct {
	outputs               sync.Map // digest -> reference count, single writer under q.mu
	revision              atomic.Uint64
	holders               map[string]storageMigrationContribution
	writers, count, bytes uint64
	period                time.Duration
	periodJob             string
	policyRevision        uint64
}

type storageMigrationContribution struct {
	outputs           map[string]uint64
	writer            bool
	cycle             time.Duration
	queued, collected time.Time
}

func (q *Queue) storageMigrationContribution(j *Job) storageMigrationContribution {
	c := storageMigrationContribution{}
	if j == nil {
		return c
	}
	c.writer = !terminal(j.State) && (j.Spec.StorageProtocol != workerCustodyProtocol || len(j.Spec.Outputs) > 0 && j.Spec.OutputDestination == nil)
	if j.LocalOutputs != nil && j.LocalOutputs.CollectedAt != nil && j.Delivery != nil && j.Delivery.State == "verified" && j.Delivery.VerifiedAt != nil && sameDelivery(j, *j) {
		c.queued, c.collected = j.Queued, *j.LocalOutputs.CollectedAt
		c.cycle = c.collected.Sub(c.queued)
	}
	if q.priorityMatch(j.Spec).Tier == "production" {
		return c
	}
	manifests := []FileManifest{j.OutputRefs, j.PendingOutputRefs}
	for _, a := range j.AttemptHistory {
		if a.Diagnostics != nil {
			manifests = append(manifests, a.Diagnostics.Files)
		}
	}
	if j.FanOut != nil {
		for _, p := range j.FanOut.Results {
			manifests = append(manifests, p.OutputRefs)
		}
	}
	for _, c := range j.FileCheckpoints {
		manifests = append(manifests, c.Files)
	}
	if j.ResumeFiles != nil {
		manifests = append(manifests, j.ResumeFiles.Files)
	}
	for _, r := range j.RecoveredResults {
		manifests = append(manifests, r.Files)
	}
	for _, refs := range manifests {
		for _, ref := range refs {
			if c.outputs == nil {
				c.outputs = map[string]uint64{}
			}
			c.outputs[ref.SHA256] = uint64(max(0, ref.SizeBytes))
		}
	}
	return c
}

func (q *Queue) indexStorageMigration(id string, j *Job) {
	x := q.records.migration
	old := x.holders[id]
	next := q.storageMigrationContribution(j)
	if old.writer {
		x.writers--
	}
	if next.writer {
		x.writers++
	}
	for hash, size := range old.outputs {
		if _, keeps := next.outputs[hash]; keeps {
			continue
		}
		n, _ := x.outputs.Load(hash)
		if n.(uint64) == 1 {
			x.outputs.Delete(hash)
			x.count--
			x.bytes -= size
		} else {
			x.outputs.Store(hash, n.(uint64)-1)
		}
	}
	for hash, size := range next.outputs {
		if _, kept := old.outputs[hash]; kept {
			continue
		}
		if n, exists := x.outputs.Load(hash); exists {
			x.outputs.Store(hash, n.(uint64)+1)
		} else {
			x.outputs.Store(hash, uint64(1))
			x.count++
			x.bytes = addDiskBytes(x.bytes, size)
		}
	}
	if j == nil {
		delete(x.holders, id)
	} else {
		x.holders[id] = next
	}
	if next.cycle > x.period {
		x.period, x.periodJob = next.cycle, id
	}
}

// All expensive metadata is already serialized by persistState. Borrow those
// immutable bytes, plus constant-size journal aggregates, instead of traversing
// retained jobs, custody or shadow evidence in the status lock.
func (q *Queue) captureStorageMigration() storageMigrationObservation {
	x := q.records.migration
	return storageMigrationObservation{base: q.persisted["state"], continuity: q.storageShadowContinuity, verified: q.storageProtectionVerified, index: x, revision: x.revision.Load(), writers: x.writers, referencedCount: x.count, referencedBytes: x.bytes}
}
