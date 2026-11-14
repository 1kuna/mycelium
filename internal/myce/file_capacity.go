package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
	tus "github.com/tus/tusd/v2/pkg/handler"
)

// Payload bytes, including incomplete uploads. FreeBytes is the space available
// for new payloads after declared tus lengths and the physical volume floor.
// Store is a public role name, never a private filesystem path.
type FileStoreStatus struct {
	Physical                         *PhysicalStorageStatus  `json:"physical,omitempty"`
	UnidentifiedWritesMayUseReserve  bool                    `json:"unidentified_writes_may_use_reserve"`
	UnidentifiedReserveAdmittedBytes uint64                  `json:"unidentified_reserve_admitted_bytes"`
	ProductionReserveBytes           uint64                  `json:"production_reserve_bytes"`
	ReservedOutputBytes              uint64                  `json:"reserved_output_bytes,omitempty"`
	Projects                         []ProjectFileStoreUsage `json:"projects,omitempty"`
	Store                            string                  `json:"store"`
	UsedBytes                        uint64                  `json:"used_bytes"`
	FreeBytes                        uint64                  `json:"free_bytes"`
	ReclaimableBytes                 uint64                  `json:"reclaimable_bytes"`
	Collection                       *FileCollectionStatus   `json:"collection,omitempty"`
	LimitBytes                       uint64                  `json:"limit_bytes"`
	DiskFreeBytes                    uint64                  `json:"disk_free_bytes"`
	FloorBytes                       uint64                  `json:"floor_bytes"`
	Error                            string                  `json:"error,omitempty"`
	// Which bound set FreeBytes, for refusal explanations; not public status.
	allowanceFree, volumeFree, diskTotal uint64
}

// Published-blob collection since this queue process opened its store. Passes
// growing while reclaimed bytes stay flat and pending_candidates stays nonzero
// is a stalled collector.
type FileCollectionStatus struct {
	Passes uint64 `json:"passes"`
	// Stopped after a batch because a control request was waiting on the queue.
	YieldedPasses uint64 `json:"yielded_passes"`
	// Stopped because an upload or publication held the file fence.
	DeferredPasses    uint64     `json:"deferred_passes"`
	ReclaimedBlobs    uint64     `json:"reclaimed_blobs"`
	ReclaimedBytes    uint64     `json:"reclaimed_bytes"`
	PendingCandidates int        `json:"pending_candidates"`
	LastReclaimAt     *time.Time `json:"last_reclaim_at,omitempty"`
}

func (s *FileStore) noteCollection(at time.Time, blobs, bytes uint64, deferred, yielded bool, pending int) {
	s.collectionMu.Lock()
	defer s.collectionMu.Unlock()
	c := &s.collection
	c.Passes++
	if deferred {
		c.DeferredPasses++
	}
	if yielded {
		c.YieldedPasses++
	}
	c.ReclaimedBlobs += blobs
	c.ReclaimedBytes = addDiskBytes(c.ReclaimedBytes, bytes)
	c.PendingCandidates = pending
	if blobs > 0 {
		c.LastReclaimAt = &at
	}
}

func (s *FileStore) collectionStatus() *FileCollectionStatus {
	s.collectionMu.Lock()
	defer s.collectionMu.Unlock()
	c := s.collection
	if c.LastReclaimAt != nil {
		at := *c.LastReclaimAt
		c.LastReclaimAt = &at
	}
	return &c
}

type storedPayload struct {
	size, length uint64
	modified     time.Time
	upload       bool
}

// Caller fences file-pair creation and internal bundle writes.
func (s *FileStore) inventoryUnlocked(ctx context.Context) (map[string]storedPayload, error) {
	out := map[string]storedPayload{}
	for _, dir := range []string{"blobs", "uploads"} {
		entries, err := os.ReadDir(filepath.Join(s.root, dir))
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			hash := entry.Name()
			if (FileRef{SHA256: hash}).validate() != nil {
				continue
			}
			st, err := entry.Info()
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if !st.Mode().IsRegular() {
				return nil, errors.New("invalid store payload")
			}
			// Published uploads can briefly retain a second hard link. Count once.
			if _, ok := out[hash]; ok {
				continue
			}
			p := storedPayload{size: uint64(st.Size()), length: uint64(st.Size()), modified: st.ModTime(), upload: dir == "uploads"}
			if p.upload {
				if _, err := os.Lstat(filepath.Join(s.root, dir, hash+".info")); os.IsNotExist(err) {
					if blob, e := os.Lstat(s.blob(FileRef{SHA256: hash})); e == nil && blob.Mode().IsRegular() {
						p = storedPayload{size: uint64(blob.Size()), length: uint64(blob.Size()), modified: blob.ModTime()}
					}
					out[hash] = p
					continue
				} else if err != nil {
					return nil, err
				}
				upload, err := filestore.New(filepath.Join(s.root, dir)).GetUpload(ctx, hash)
				if err == nil {
					info, err := upload.GetInfo(ctx)
					if err != nil {
						return nil, err
					}
					if info.Size < st.Size() {
						return nil, errors.New("invalid upload length")
					}
					p.length = uint64(info.Size)
					meta, err := os.Lstat(filepath.Join(s.root, dir, hash+".info"))
					if os.IsNotExist(err) {
						if blob, e := os.Lstat(s.blob(FileRef{SHA256: hash})); e == nil && blob.Mode().IsRegular() {
							out[hash] = storedPayload{size: uint64(blob.Size()), length: uint64(blob.Size()), modified: blob.ModTime()}
							continue
						}
					}
					if err != nil {
						return nil, err
					}
					if !meta.Mode().IsRegular() {
						return nil, errors.New("invalid upload metadata")
					}
					if meta.ModTime().After(p.modified) {
						p.modified = meta.ModTime()
					}
				} else if !os.IsNotExist(err) {
					// Publication may have removed the tus paths after the directory
					// snapshot. Its authoritative blob must exist before that removal.
					if errors.Is(err, tus.ErrNotFound) {
						blob, statErr := os.Lstat(s.blob(FileRef{SHA256: hash}))
						if statErr == nil && blob.Mode().IsRegular() {
							p = storedPayload{size: uint64(blob.Size()), length: uint64(blob.Size()), modified: blob.ModTime()}
						} else {
							return nil, err
						}
					} else {
						return nil, err
					}
				}
			}
			out[hash] = p
		}
	}
	return out, nil
}

func (s *FileStore) diskCapacity(ctx context.Context) (FileStoreStatus, map[string]storedPayload, error) {
	// Keep physical free space and the inventory on the same side of a bundle
	// write. Otherwise its bytes can be deducted by both disk-free and the
	// completion ledger. No queue callbacks run under this filesystem fence.
	if err := lockUploadContext(ctx, s.uploadFilesMu.RLock, s.uploadFilesMu.RUnlock); err != nil {
		return FileStoreStatus{}, nil, err
	}
	defer s.uploadFilesMu.RUnlock()
	status := FileStoreStatus{Store: "queue-file-store", LimitBytes: uint64(s.limit), ProductionReserveBytes: s.productionReserve, UnidentifiedWritesMayUseReserve: s.productionReserve > 0, UnidentifiedReserveAdmittedBytes: s.unidentifiedReserveAdmitted.Load()}
	payloads, err := s.inventoryUnlocked(ctx)
	if err != nil {
		return status, nil, err
	}
	var allocated, remaining uint64
	for _, p := range payloads {
		status.UsedBytes = addDiskBytes(status.UsedBytes, p.size)
		allocated = addDiskBytes(allocated, p.length)
		remaining = addDiskBytes(remaining, p.length-p.size)
	}
	probe := s.diskProbe
	if probe == nil {
		probe = workerDiskProbe
	}
	disk, err := probe(s.root)
	if err != nil {
		return status, payloads, err
	}
	if disk.TotalBytes == 0 {
		return status, payloads, errors.New("disk capacity unavailable")
	}
	status.DiskFreeBytes, status.FloorBytes = disk.FreeBytes, diskFreeFloor(disk.TotalBytes)
	storeFree := status.LimitBytes - min(status.LimitBytes, allocated)
	unavailable := addDiskBytes(status.FloorBytes, remaining)
	status.allowanceFree, status.volumeFree, status.diskTotal = storeFree, disk.FreeBytes-min(disk.FreeBytes, unavailable), disk.TotalBytes
	status.FreeBytes = min(status.allowanceFree, status.volumeFree)
	return status, payloads, nil
}

func (s *FileStore) capacity(ctx context.Context) (FileStoreStatus, map[string]storedPayload, error) {
	view := s.snapshotCapacity(ctx)
	defer view.release()
	s.mu.Lock()
	status, payloads := view.current()
	s.mu.Unlock()
	if status.Error != "" {
		return status, payloads, errors.New(status.Error)
	}
	return status, payloads, nil
}

func fileStoreRefusal(status FileStoreStatus, need uint64) error {
	if status.Error != "" {
		return &HTTPError{Status: http.StatusServiceUnavailable, Code: "file_store_unavailable", Message: "queue-file-store capacity unavailable; retry when storage is healthy"}
	}
	if need > status.FreeBytes {
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "file_store_capacity", Message: fmt.Sprintf("queue-file-store: need=%d bytes free=%d bytes shortfall=%d bytes used=%d bytes disk_free=%d bytes floor=%d bytes%s; release retained files, increase --file-store-bytes or free space on the queue file-store volume", need, status.FreeBytes, max(need-min(need, status.FreeBytes), status.FloorBytes-min(status.FloorBytes, status.DiskFreeBytes)), status.UsedBytes, status.DiskFreeBytes, status.FloorBytes, status.capacityBound())}
	}
	return nil
}

// Snapshot custody before walking disk. Status is read-only and never runs GC.
type fileStoreObservation struct {
	store                     *FileStore
	expired, keep, referenced map[string]bool
	markers                   map[string]time.Time
	bundles                   map[string]time.Time
	now                       time.Time
	retention                 time.Duration
	publicationHorizon        time.Duration
	expiredGrace              time.Duration
	projects                  projectFileFacts
	outputs                   []statusOutputReservation
}

type statusOutputReservation struct {
	bytes uint64
	refs  FileManifest
}

// The caller holds q.mu. Reference maps are newly allocated and timestamps are
// values; the FileStore owns its separate lifecycle/inventory synchronization.
func (q *Queue) captureFileStoreStatus() fileStoreObservation {
	// One reference walk serves capacity and per-project retained bytes.
	projects := projectFileFacts{retained: make([]projectFileFact, 0, 4*len(q.s.Jobs))}
	expired, keep, referenced := q.collectFileReferences(true, nil, projects.retain)
	q.noteProjectFileAdditions(&projects, q.s.Jobs)
	q.noteProjectFileAdditions(&projects, q.retiredSubmissionJobs("", nil))
	q.noteInputClaimFacts(&projects, "")
	for name, p := range q.s.Policies {
		if p.MaxRetainedFileBytes > 0 || p.MaxAddedFileBytes24h > 0 || p.ReservedFileBytes > 0 {
			if projects.limits == nil {
				projects.limits = map[string]ProjectPolicy{}
			}
			projects.limits[name] = p
		}
	}
	markers := make(map[string]time.Time, len(q.s.JobInputUploads)+len(q.s.AttemptOutputUploads))
	bundles := make(map[string]time.Time, len(q.s.BundleUploads))
	for hash, at := range q.jobUploadMarkers() {
		markers[hash] = at
	}
	for hash, at := range q.s.BundleUploads {
		bundles[hash] = at
		if _, exists := markers[hash]; !exists {
			markers[hash] = at
		}
	}
	var outputs []statusOutputReservation
	for _, j := range q.liveJobs() {
		if j.OutputAdmissionPending || terminal(j.State) || j.Spec.OutputDelivery != asyncNAS {
			continue
		}
		outputs = append(outputs, statusOutputReservation{j.Spec.Disk.OutputBytes, maps.Clone(j.PendingOutputRefs)})
	}
	return fileStoreObservation{q.files, expired, keep, referenced, markers, bundles, q.clock.Now(), q.fileRetention, q.s.PublicationHorizon, q.expiredFilePublicationGrace(), projects, outputs}
}

func (q *Queue) fileStoreStatus(ctx context.Context) *FileStoreStatus {
	if err := lockUploadContext(ctx, q.mu.RLock, q.mu.RUnlock); err != nil {
		return &FileStoreStatus{Error: "file store capacity unavailable"}
	}
	view := q.captureFileStoreStatus()
	q.mu.RUnlock()
	return view.status(ctx)
}

func (v fileStoreObservation) status(ctx context.Context) *FileStoreStatus {
	store := v.store
	expired, keep, referenced, markers := v.expired, v.keep, v.referenced, v.markers
	now, retention := v.now, v.retention
	if store == nil {
		return nil
	}
	if err := lockUploadContext(ctx, store.lifecycle.RLock, store.lifecycle.RUnlock); err != nil {
		return &FileStoreStatus{Error: "file store capacity unavailable"}
	}
	defer store.lifecycle.RUnlock()
	status, payloads, err := store.capacity(ctx)
	status.Projects = v.projects.usage(now)
	status.Collection = store.collectionStatus()
	if err != nil {
		status.Error = "file store capacity unavailable"
		status.Physical = store.physicalStorageStatus(ctx, status, nil, now)
		return &status
	}
	for hash, p := range payloads {
		// Mirror the collector: only marked orphans and bundles keep the full
		// horizon; an expired job's files use the grace capped at retention.
		_, bundle := v.bundles[hash]
		_, marker := markers[hash]
		grace := v.expiredGrace
		if bundle || marker && !referenced[hash] {
			grace = v.publicationHorizon
		}
		if keep[hash] || p.modified.After(now.Add(-grace)) {
			continue
		}
		if at, tracked := v.bundles[hash]; tracked && (at.After(now.Add(-retention)) || p.modified.After(now.Add(-retention))) {
			continue
		}
		reclaimable := !p.upload && expired[hash]
		if at, marked := markers[hash]; retention > 0 && marked && !referenced[hash] && !p.modified.After(now.Add(-retention)) {
			reclaimable = p.upload || !at.IsZero() && !at.After(now.Add(-retention))
		}
		if reclaimable {
			status.ReclaimableBytes = addDiskBytes(status.ReclaimableBytes, p.size)
		}
	}
	seen := map[string]bool{}
	for _, output := range v.outputs {
		if output.refs == nil {
			status.ReservedOutputBytes = addDiskBytes(status.ReservedOutputBytes, output.bytes)
			continue
		}
		for _, ref := range output.refs {
			if seen[ref.SHA256] {
				continue
			}
			seen[ref.SHA256] = true
			if p, ok := payloads[ref.SHA256]; !ok || p.length != uint64(ref.SizeBytes) {
				status.ReservedOutputBytes = addDiskBytes(status.ReservedOutputBytes, uint64(ref.SizeBytes))
			}
		}
	}
	status.FreeBytes -= min(status.FreeBytes, status.ReservedOutputBytes)
	status.Physical = store.physicalStorageStatus(ctx, status, payloads, now)
	return &status
}

// PreflightFileStore checks the whole manifest before its first upload. Existing
// complete blobs and the already-declared part of tus uploads need no new space.
// This is advisory, not a reservation; POST upload creation repeats admission.
func (q *Queue) PreflightFileStore(ctx context.Context, files FileManifest) error {
	if err := files.validate(); err != nil {
		return err
	}
	if err := q.reclaimJobFilesContext(ctx); err != nil {
		log.Printf("file retention deferred: %v", err)
	}
	requestUnlock1 := requestReadLock(ctx, &q.mu)
	store := q.files
	requestUnlock1()
	if store == nil {
		return &HTTPError{Status: 503, Code: "file_store_unavailable", Message: "queue-file-store is not configured"}
	}
	growth, err := q.preflightAdmission(ctx, store, files)
	if !fileStoreSpaceRefusal(err) {
		return err
	}
	// Reclaim what retention or an owner release already authorized before
	// refusing. The admission fence is released so collection can take it.
	if q.reclaimForAdmission(ctx, growth) {
		if _, err = q.preflightAdmission(ctx, store, files); !fileStoreSpaceRefusal(err) {
			return err
		}
	}
	return q.explainFileStoreRefusal(ctx, err)
}

// Holds the store lifecycle fence only for one capacity reading and admission.
// Returns the manifest's new bytes.
func (q *Queue) preflightAdmission(ctx context.Context, store *FileStore, files FileManifest) (uint64, error) {
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	status, payloads, err := store.capacity(ctx)
	if err != nil {
		status.Error = "file store capacity unavailable"
	}
	var need uint64
	seen := map[string]bool{}
	for _, ref := range files {
		if seen[ref.SHA256] {
			continue
		}
		seen[ref.SHA256] = true
		p, exists := payloads[ref.SHA256]
		if exists && p.length == uint64(ref.SizeBytes) {
			continue
		}
		need = addDiskBytes(need, uint64(ref.SizeBytes))
	}
	// Simulate the manifest's allocation so declared output reservations are
	// counted once, while other queued/running tasks retain their budget.
	if payloads == nil {
		payloads = map[string]storedPayload{}
	}
	for _, ref := range files {
		payloads[ref.SHA256] = storedPayload{length: uint64(ref.SizeBytes)}
	}
	growth := need
	need = addDiskBytes(need, store.reservedOutputs(payloads))
	// Admission also refuses an over-quota project before it uploads inputs
	// submit would refuse.
	_, err = store.admit(ctx, status, payloads, files, need, growth)
	return growth, err
}

func (c Client) PreflightFileStore(ctx context.Context, files FileManifest) error {
	return c.Call(ctx, http.MethodPost, "/api/files/preflight", files, nil)
}

// Refresh progress using the queue clock, including a zero-byte tus creation.
// Interrupted requests retain their durable offset but receive a new idle window.
func (s *FileStore) refreshUpload(hash string) error {
	for _, suffix := range []string{"", ".info"} {
		path := filepath.Join(s.root, "uploads", hash+suffix)
		if err := s.refreshBlob(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Only positively marked job-input uploads are disposable. Unknown/imported
// files stay for operator recovery. The publication fence excludes every active
// PATCH and verify/commit interval while custody and idle time are rechecked.
func (q *Queue) reclaimInterruptedUploads(requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	store := q.files
	_, _, referenced := q.fileReferences(false)
	revision := q.fileReferenceRevision
	markers := map[string]time.Time{}
	for hash, at := range q.jobUploadMarkers() {
		if at.IsZero() && !referenced[hash] {
			markers[hash] = at
		}
	}
	if q.fileRetention == 0 {
		requestUnlock1()
		return nil
	}
	cutoff := q.clock.Now().Add(-max(q.fileRetention, q.s.PublicationHorizon))
	requestUnlock1()
	if store == nil || len(markers) == 0 {
		return nil
	}
	attempted := false
	for hash, at := range markers {
		if (FileRef{SHA256: hash}).validate() != nil {
			continue
		}
		// Each upload commits its own transaction; under control pressure a pass
		// still settles one before yielding, so a busy queue cannot stall it.
		if requestCtx.Err() != nil || attempted && q.fileReclaimUnderPressure() {
			break
		}
		attempted = true
		if !q.fileLifecycle.TryLock() {
			return nil
		}
		requestUnlock2 := requestReadLock(requestCtx, &q.mu)
		if revision != q.fileReferenceRevision {
			_, _, referenced = q.fileReferences(false)
			revision = q.fileReferenceRevision
		}
		current, marked := q.jobUploadMarker(hash)
		remove := q.files == store && marked && current == at && !referenced[hash]
		requestUnlock2()
		err := func() error {
			if !remove {
				return nil
			}
			var published time.Time
			if st, err := os.Lstat(store.blob(FileRef{SHA256: hash})); err == nil {
				if !st.Mode().IsRegular() {
					return nil
				}
				// Publication can crash before the SQLite marker changes. Recover
				// its original freshness, rather than granting a new idle window.
				published = st.ModTime()
			} else if !os.IsNotExist(err) {
				return err
			}
			path := filepath.Join(store.root, "uploads", hash)
			for _, suffix := range []string{"", ".info"} {
				st, err := os.Lstat(path + suffix)
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					return err
				}
				if !st.Mode().IsRegular() || st.ModTime().After(cutoff) {
					return nil
				}
			}
			// Remove metadata first: leftover data cannot be resumed or refreshed,
			// and a later sweep can finish cleanup after a crash.
			for _, suffix := range []string{".info", ""} {
				if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
			if err := store.syncDir(filepath.Join(store.root, "uploads")); err != nil {
				return err
			}
			return q.updateRequest(requestCtx, func() error {
				if published.IsZero() {
					q.forgetFileInputClaims(hash, cutoff)
					delete(q.s.JobInputUploads, hash)
					delete(q.s.AttemptOutputUploads, hash)
				} else {
					if _, owned := q.s.AttemptOutputUploads[hash]; owned {
						q.s.AttemptOutputUploads[hash] = published
					} else {
						q.s.JobInputUploads[hash] = published
					}
				}
				return nil
			}, nil)
		}()
		q.fileLifecycle.Unlock()
		if err != nil {
			log.Printf("file retention deferred upload=%s error=%q; retrying next tick", hash, err)
		}
	}
	return nil
}

// Match public capacity codes without relaying arbitrary upstream bodies.
func fileCapacityCode(code string) bool {
	return code == "file_store_capacity" || code == "file_store_unavailable"
}

// File-store refusals whose code and reason reach HTTP callers intact.
func fileStoreRefusalCode(code string) bool {
	return fileCapacityCode(code) || code == "project_file_store_quota" || code == "file_input_claim_required" || code == "input_blob_missing" || code == "input_blob_corrupt" || code == "input_blob_unavailable" || code == "file_blob_unavailable"
}
