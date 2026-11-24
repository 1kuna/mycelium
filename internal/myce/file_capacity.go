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
	"strings"
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
	// Present published candidates without a keeper, including publication grace.
	PendingCollectionBytes uint64 `json:"pending_collection_bytes"`
	// Present published payloads with neither a keeper nor current collection
	// authority. Includes recent tracked orphans and untracked imports.
	UnreferencedPublishedBytes uint64                `json:"unreferenced_published_bytes"`
	OpenUploadBytes            uint64                `json:"open_upload_bytes"`
	HeldPublishedBytes         uint64                `json:"held_published_bytes"`
	Collection                 *FileCollectionStatus `json:"collection,omitempty"`
	LimitBytes                 uint64                `json:"limit_bytes"`
	DiskFreeBytes              uint64                `json:"disk_free_bytes"`
	FloorBytes                 uint64                `json:"floor_bytes"`
	Error                      string                `json:"error,omitempty"`
	// Which bound set FreeBytes, for refusal explanations; not public status.
	allowanceFree, volumeFree, diskTotal uint64
	diskVolume                           string
	uploadRemaining                      uint64
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
	jobInput     bool
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
					p.jobInput = jobInputUpload(info, hash)
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
	status.diskVolume = disk.Volume
	status.FreeBytes = min(status.allowanceFree, status.volumeFree)
	status.uploadRemaining = remaining
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
	outputMarkers             map[string]time.Time
	bundles                   map[string]time.Time
	claims                    map[string][]FileInputClaim
	age                       filePublicationAge
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
	expired, keep, referenced := q.collectDurableFileReferences(true, nil, projects.retain)
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
	var outputs []statusOutputReservation
	for _, j := range q.liveJobs() {
		if j.OutputAdmissionPending || terminal(j.State) || j.Spec.OutputDelivery != asyncNAS {
			continue
		}
		outputs = append(outputs, statusOutputReservation{j.Spec.Disk.OutputBytes, maps.Clone(j.PendingOutputRefs)})
	}
	return fileStoreObservation{
		store: q.files, expired: expired, keep: keep, referenced: referenced,
		markers: q.jobUploadMarkers(), outputMarkers: maps.Clone(q.s.AttemptOutputUploads), bundles: maps.Clone(q.s.BundleUploads),
		claims: q.unsettledInputClaims(), age: q.filePublicationAge(q.clock.Now()), projects: projects, outputs: outputs,
	}
}

func (v fileStoreObservation) status(ctx context.Context) *FileStoreStatus {
	store := v.store
	expired, keep, referenced, markers := v.expired, v.keep, v.referenced, v.markers
	now, retention := v.age.now, v.age.retention
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
		at, marked := markers[hash]
		claims := v.claims[hash]
		if p.upload {
			status.OpenUploadBytes = addDiskBytes(status.OpenUploadBytes, p.size)
			_, output := v.outputMarkers[hash]
			owned := marked && at.IsZero() || !marked && p.jobInput && allocatedInputClaim(claims, int64(p.length))
			if owned && !referenced[hash] && (!output || len(claims) == 0) && v.age.interruptedUploadIdle(p.modified, claims) {
				status.ReclaimableBytes = addDiskBytes(status.ReclaimableBytes, p.size)
			}
			continue
		}
		if keep[hash] || len(claims) != 0 {
			status.HeldPublishedBytes = addDiskBytes(status.HeldPublishedBytes, p.size)
			continue
		}
		// Mirror the collector's candidate enrollment separately from its age
		// check. A candidate may still be in publication grace or deferred.
		bundleAt, bundle := v.bundles[hash]
		if !marked && bundle {
			at, marked = bundleAt, true
		}
		cutoff := now.Add(-retention)
		orphan := retention > 0 && !referenced[hash] &&
			(marked && !at.IsZero() && !at.After(cutoff) || bundle && !bundleAt.After(cutoff))
		if !expired[hash] && !orphan {
			status.UnreferencedPublishedBytes = addDiskBytes(status.UnreferencedPublishedBytes, p.size)
			continue
		}
		status.PendingCollectionBytes = addDiskBytes(status.PendingCollectionBytes, p.size)
		markedCandidate := bundle || orphan
		if !v.age.protects(p.modified, !markedCandidate) &&
			(!markedCandidate || expired[hash] || !p.modified.After(cutoff)) {
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

func jobInputUpload(info tus.FileInfo, hash string) bool {
	return info.ID == hash && info.MetaData["sha256"] == hash && info.MetaData["purpose"] == jobInputUploadPurpose && !info.SizeIsDeferred
}

func (a filePublicationAge) interruptedUploadIdle(modified time.Time, claims []FileInputClaim) bool {
	cutoff := a.now.Add(-a.retention)
	if a.retention == 0 || modified.After(cutoff) {
		return false
	}
	for _, claim := range claims {
		if claim.lastIssuance().After(cutoff) {
			return false
		}
	}
	// Transfer activity sets the idle deadline for a claimed input. A measured
	// delay from other claims to submission still protects published bytes and
	// unclaimed interrupted transfers under their existing rules.
	return len(claims) != 0 || !a.protects(modified, false)
}

// The publication fence excludes active transfers and verify/commit intervals
// while custody and idle time are rechecked.
func (q *Queue) reclaimInterruptedUploads(requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	store := q.files
	if store == nil || q.fileRetention == 0 {
		requestUnlock1()
		return nil
	}
	_, _, referenced := q.collectDurableFileReferences(false, nil, nil)
	claims := q.unsettledInputClaims()
	revision := q.fileReferenceRevision
	age := q.filePublicationAge(q.clock.Now())
	candidates := map[string]bool{}
	markers := q.jobUploadMarkers()
	for hash, at := range markers {
		if at.IsZero() && !referenced[hash] {
			candidates[hash] = true
		}
	}
	// Older queues dropped the marker as soon as a claim kept the identity.
	// Verify the original purpose and exact allocation from tus metadata below.
	legacy := map[string]bool{}
	for hash, receipts := range claims {
		if _, marked := markers[hash]; !marked && !referenced[hash] && age.interruptedUploadIdle(time.Time{}, receipts) {
			for _, claim := range receipts {
				if claim.Allocated {
					legacy[hash] = true
					break
				}
			}
		}
	}
	requestUnlock1()
	// Completed legacy claims can outnumber interrupted transfers. Intersect
	// with the upload directory before taking the write fence for any of them.
	if len(legacy) != 0 {
		entries, err := os.ReadDir(filepath.Join(store.root, "uploads"))
		if err != nil {
			log.Printf("file retention deferred: upload inventory unavailable: %v", err)
		}
		for _, entry := range entries {
			if hash, ok := strings.CutSuffix(entry.Name(), ".info"); ok && legacy[hash] {
				candidates[hash] = true
			}
		}
	}
	attempted := false
	for hash := range candidates {
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
			_, _, referenced = q.collectDurableFileReferences(false, nil, nil)
			claims = q.unsettledInputClaims()
			revision = q.fileReferenceRevision
		}
		age = q.filePublicationAge(q.clock.Now())
		at, marked := q.jobUploadMarker(hash)
		_, output := q.s.AttemptOutputUploads[hash]
		receipts := claims[hash]
		remove := q.files == store && !referenced[hash] && (!marked || at.IsZero()) &&
			(!output || len(receipts) == 0) && age.interruptedUploadIdle(time.Time{}, receipts)
		requestUnlock2()
		if remove {
			if err := q.reclaimInterruptedUpload(requestCtx, store, hash, age, receipts, marked); err != nil {
				log.Printf("file retention deferred upload=%s error=%q; retrying next tick", hash, err)
			}
		}
		q.fileLifecycle.Unlock()
	}
	return nil
}

// Caller holds the lifecycle write fence. Disk checks and removal never hold q.mu.
func (q *Queue) reclaimInterruptedUpload(ctx context.Context, store *FileStore, hash string, age filePublicationAge, claims []FileInputClaim, marked bool) error {
	var published time.Time
	if st, err := os.Lstat(store.blob(FileRef{SHA256: hash})); err == nil {
		if !marked || !st.Mode().IsRegular() {
			return nil
		}
		// Recover a publish/link crash using the blob's original freshness.
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
		if !st.Mode().IsRegular() || !age.interruptedUploadIdle(st.ModTime(), claims) {
			return nil
		}
	}
	if !marked {
		upload, err := filestore.New(filepath.Dir(path)).GetUpload(ctx, hash)
		if errors.Is(err, tus.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		info, err := upload.GetInfo(ctx)
		if err != nil {
			return err
		}
		if !jobInputUpload(info, hash) || !allocatedInputClaim(claims, info.Size) {
			return nil
		}
		// Persist ownership before consuming the legacy metadata. A restart
		// after either unlink can then finish cleanup and retire the claim.
		if err := q.recordFileUpload(hash, jobInputUploadPurpose, true, ctx); err != nil {
			return err
		}
		unlock := requestReadLock(ctx, &q.mu)
		_, marked = q.s.JobInputUploads[hash]
		unlock()
		if !marked {
			return nil
		}
	}
	// Remove metadata first so leftover data cannot be resumed or refreshed.
	for _, suffix := range []string{".info", ""} {
		if err := os.Remove(path + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := store.syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	return q.updateRequest(ctx, func() error {
		if published.IsZero() {
			cutoff := age.now.Add(-age.retention)
			if len(claims) == 0 {
				cutoff = age.now.Add(-max(age.retention, q.s.PublicationHorizon))
			}
			q.forgetFileInputClaims(hash, cutoff)
			delete(q.s.JobInputUploads, hash)
			delete(q.s.AttemptOutputUploads, hash)
		} else if _, output := q.s.AttemptOutputUploads[hash]; output {
			q.s.AttemptOutputUploads[hash] = published
		} else {
			q.s.JobInputUploads[hash] = published
		}
		return nil
	}, nil)
}

// Match public capacity codes without relaying arbitrary upstream bodies.
func fileCapacityCode(code string) bool {
	return code == "file_store_capacity" || code == "file_store_unavailable"
}

// File-store refusals whose code and reason reach HTTP callers intact.
func fileStoreRefusalCode(code string) bool {
	return fileCapacityCode(code) || code == "project_file_store_quota" || code == "file_input_claim_required" || code == "input_blob_missing" || code == "input_blob_corrupt" || code == "input_blob_unavailable" || code == "file_blob_unavailable" || code == "file_holdings_unavailable"
}
