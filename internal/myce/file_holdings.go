package myce

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"
)

// ProjectFileHoldings explains the same deduplicated attribution as status.
// StoredBytes measures observed payload, including the written portion of
// uploads. RetainedBytes includes the declared sizes of claims and pending
// outputs even when their transfers are incomplete.
type ProjectFileHoldings struct {
	Project       string                      `json:"project"`
	CapturedAt    time.Time                   `json:"captured_at"`
	RetainedBytes uint64                      `json:"retained_bytes"`
	StoredBytes   uint64                      `json:"stored_bytes"`
	SharedBytes   uint64                      `json:"shared_bytes"`
	ByKind        map[string]FileHoldingUsage `json:"by_kind"`
	Files         []ProjectHeldFile           `json:"files"`
}

type FileHoldingUsage struct {
	RetainedBytes uint64 `json:"retained_bytes"`
	StoredBytes   uint64 `json:"stored_bytes"`
	FileCount     int    `json:"file_count"`
}

type ProjectHeldFile struct {
	SHA256           string                 `json:"sha256"`
	RetainedBytes    uint64                 `json:"retained_bytes"`
	StoredBytes      uint64                 `json:"stored_bytes"`
	State            string                 `json:"state"` // published, uploading, or absent
	OtherHolderKinds []string               `json:"other_holder_kinds,omitempty"`
	Shared           bool                   `json:"shared"` // held by other projects or queue-wide custody
	Holders          []FileKeepingReference `json:"holders"`
}

type fileHoldingFact struct {
	file  FileRef
	owner FileKeepingReference
}

func (q *Queue) ProjectFileHoldings(ctx context.Context, project string) (ProjectFileHoldings, error) {
	if !validFileProject(project) {
		return ProjectFileHoldings{}, &HTTPError{Status: http.StatusBadRequest, Code: "file_holdings_invalid", Message: "holdings requires a project"}
	}
	at, facts, payloads, err := q.captureProjectFileHoldings(ctx)
	if err != nil {
		return ProjectFileHoldings{}, err
	}
	return summarizeProjectFileHoldings(project, at, facts, payloads), nil
}

func (q *Queue) captureProjectFileHoldings(ctx context.Context) (time.Time, []fileHoldingFact, map[string]storedPayload, error) {
	// The lifecycle fence prevents collection between the custody and payload
	// reads. Publication can proceed. Only copy references under the queue lock.
	if err := lockUploadContext(ctx, q.fileLifecycle.RLock, q.fileLifecycle.RUnlock); err != nil {
		return time.Time{}, nil, nil, err
	}
	defer q.fileLifecycle.RUnlock()
	unlock, err := requestObservationLock(ctx, &q.mu)
	if err != nil {
		return time.Time{}, nil, nil, err
	}
	store, at := q.files, q.clock.Now()
	if store == nil {
		unlock()
		return time.Time{}, nil, nil, fileHoldingsUnavailable()
	}
	var facts []fileHoldingFact
	q.collectFileReferences(true, nil, func(file FileRef, owner FileKeepingReference) {
		if file.SHA256 != "" {
			facts = append(facts, fileHoldingFact{file, owner})
		}
	})
	unlock()
	payloads, err := store.inventoryUnlocked(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return time.Time{}, nil, nil, ctx.Err()
		}
		return time.Time{}, nil, nil, &HTTPError{Status: http.StatusServiceUnavailable, Code: "file_store_unavailable", Message: "queue file store inventory unavailable"}
	}
	// No mutation uses the snapshot. A worker heartbeat or a later job/claim
	// update cannot invalidate the committed custody observation at captured_at.
	// Do recheck that the payloads came from the still-configured store.
	unlock = requestReadLock(ctx, &q.mu)
	current := q.files == store
	unlock()
	if !current {
		return time.Time{}, nil, nil, fileHoldingsUnavailable()
	}
	return at, facts, payloads, nil
}

func fileHoldingsUnavailable() error {
	return &HTTPError{Status: http.StatusServiceUnavailable, Code: "file_holdings_unavailable", Message: "file store custody observation unavailable; retry the read"}
}

func validFileProject(project string) bool {
	return strings.TrimSpace(project) != "" && !strings.ContainsAny(project, "\r\n\x00")
}

func summarizeProjectFileHoldings(project string, at time.Time, facts []fileHoldingFact, payloads map[string]storedPayload) ProjectFileHoldings {
	out := ProjectFileHoldings{Project: project, CapturedAt: at, ByKind: map[string]FileHoldingUsage{}, Files: []ProjectHeldFile{}}
	type heldFile struct {
		ProjectHeldFile
		holders map[FileKeepingReference]struct{}
		kinds   map[string]uint64
	}
	files := map[string]*heldFile{}
	for _, f := range facts {
		if f.owner.Project != project || f.owner.Kind == "worker_local_custody" {
			continue
		}
		hash := f.file.SHA256
		item := files[hash]
		if item == nil {
			item = &heldFile{ProjectHeldFile: ProjectHeldFile{SHA256: hash, State: "absent"}, holders: map[FileKeepingReference]struct{}{}, kinds: map[string]uint64{}}
			if p, ok := payloads[hash]; ok {
				item.StoredBytes, item.State = p.size, "published"
				if p.upload {
					item.State = "uploading"
				}
			}
			files[hash] = item
		}
		var charge uint64
		if projectFileCharge(f.owner) {
			charge = uint64(f.file.SizeBytes)
		}
		item.RetainedBytes = max(item.RetainedBytes, charge)
		item.kinds[f.owner.Kind] = max(item.kinds[f.owner.Kind], charge)
		// Exclude duplicated provenance (for example a checkpoint in both the
		// locally accepted manifest and the resume pointer).
		item.holders[f.owner] = struct{}{}
	}
	for _, f := range facts {
		if item := files[f.file.SHA256]; item != nil && f.owner.Project != project {
			item.Shared = true
			item.OtherHolderKinds = append(item.OtherHolderKinds, f.owner.Kind)
		}
	}
	for _, item := range files {
		for holder := range item.holders {
			item.Holders = append(item.Holders, holder)
		}
		slices.SortFunc(item.Holders, compareFileHolders)
		slices.Sort(item.OtherHolderKinds)
		item.OtherHolderKinds = slices.Compact(item.OtherHolderKinds)
		for kind, charge := range item.kinds {
			s := out.ByKind[kind]
			s.RetainedBytes = addDiskBytes(s.RetainedBytes, charge)
			s.StoredBytes = addDiskBytes(s.StoredBytes, item.StoredBytes)
			s.FileCount++
			out.ByKind[kind] = s
		}
		out.RetainedBytes = addDiskBytes(out.RetainedBytes, item.RetainedBytes)
		out.StoredBytes = addDiskBytes(out.StoredBytes, item.StoredBytes)
		if item.Shared {
			out.SharedBytes = addDiskBytes(out.SharedBytes, item.StoredBytes)
		}
		out.Files = append(out.Files, item.ProjectHeldFile)
	}
	slices.SortFunc(out.Files, func(a, b ProjectHeldFile) int { return strings.Compare(a.SHA256, b.SHA256) })
	return out
}

func compareFileHolders(a, b FileKeepingReference) int {
	if c := strings.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	if c := strings.Compare(a.JobID, b.JobID); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
}
