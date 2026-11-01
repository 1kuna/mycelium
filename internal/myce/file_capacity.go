package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	Physical                         *PhysicalStorageStatus `json:"physical,omitempty"`
	UnidentifiedWritesMayUseReserve  bool                   `json:"unidentified_writes_may_use_reserve"`
	UnidentifiedReserveAdmittedBytes uint64                 `json:"unidentified_reserve_admitted_bytes"`
	ProductionReserveBytes           uint64                 `json:"production_reserve_bytes"`
	ReservedOutputBytes              uint64                 `json:"reserved_output_bytes,omitempty"`
	Store                            string                 `json:"store"`
	UsedBytes                        uint64                 `json:"used_bytes"`
	FreeBytes                        uint64                 `json:"free_bytes"`
	ReclaimableBytes                 uint64                 `json:"reclaimable_bytes"`
	LimitBytes                       uint64                 `json:"limit_bytes"`
	DiskFreeBytes                    uint64                 `json:"disk_free_bytes"`
	FloorBytes                       uint64                 `json:"floor_bytes"`
	Error                            string                 `json:"error,omitempty"`
}

type storedPayload struct {
	size, length uint64
	modified     time.Time
	upload       bool
}

// Callers hold lifecycle.RLock (or its exclusive side for collection). No q.mu.
func (s *FileStore) inventory(ctx context.Context) (map[string]storedPayload, error) {
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

func (s *FileStore) capacity(ctx context.Context) (FileStoreStatus, map[string]storedPayload, error) {
	status := FileStoreStatus{Store: "queue-file-store", LimitBytes: uint64(s.limit), ProductionReserveBytes: s.productionReserve, UnidentifiedWritesMayUseReserve: s.productionReserve > 0, UnidentifiedReserveAdmittedBytes: s.unidentifiedReserveAdmitted.Load()}
	payloads, err := s.inventory(ctx)
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
	status.FreeBytes = min(storeFree, disk.FreeBytes-min(disk.FreeBytes, unavailable))
	return status, payloads, nil
}

func fileStoreRefusal(status FileStoreStatus, need uint64) error {
	if status.Error != "" {
		return &HTTPError{Status: http.StatusServiceUnavailable, Code: "file_store_unavailable", Message: "queue-file-store capacity unavailable; retry when storage is healthy"}
	}
	if need > status.FreeBytes {
		return &HTTPError{Status: http.StatusInsufficientStorage, Code: "file_store_capacity", Message: fmt.Sprintf("queue-file-store: need=%d bytes free=%d bytes shortfall=%d bytes used=%d bytes disk_free=%d bytes floor=%d bytes; wait for retention or increase --file-store-bytes / free space on the queue file-store volume", need, status.FreeBytes, max(need-min(need, status.FreeBytes), status.FloorBytes-min(status.FloorBytes, status.DiskFreeBytes)), status.UsedBytes, status.DiskFreeBytes, status.FloorBytes)}
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
}

// The caller holds q.mu. Reference maps are newly allocated and timestamps are
// values; the FileStore owns its separate lifecycle/inventory synchronization.
func (q *Queue) captureFileStoreStatus() fileStoreObservation {
	expired, keep, referenced := q.fileReferences(true)
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
	return fileStoreObservation{q.files, expired, keep, referenced, markers, bundles, q.clock.Now(), q.fileRetention}
}

func (q *Queue) fileStoreStatus(ctx context.Context) *FileStoreStatus {
	q.mu.RLock()
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
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	status, payloads, err := store.capacity(ctx)
	if err != nil {
		status.Error = "file store capacity unavailable"
		status.Physical = store.physicalStorageStatus(ctx, status, nil, now)
		return &status
	}
	for hash, p := range payloads {
		if keep[hash] || p.modified.After(now.Add(-filePublicationGrace)) {
			continue
		}
		if at, tracked := v.bundles[hash]; tracked && (at.After(now.Add(-retention)) || p.modified.After(now.Add(-retention))) {
			continue
		}
		reclaimable := !p.upload && expired[hash]
		if at, marked := markers[hash]; marked && !referenced[hash] && !p.modified.After(now.Add(-retention)) {
			reclaimable = p.upload || !at.IsZero() && !at.After(now.Add(-retention))
		}
		if reclaimable {
			status.ReclaimableBytes = addDiskBytes(status.ReclaimableBytes, p.size)
		}
	}
	status.ReservedOutputBytes = store.reservedOutputs(payloads)
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
	store.lifecycle.RLock()
	defer store.lifecycle.RUnlock()
	store.mu.Lock()
	defer store.mu.Unlock()
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
	_, err = store.admit(ctx, status, payloads, files, need, growth)
	return err
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
	cutoff := q.clock.Now().Add(-max(q.fileRetention, filePublicationGrace))
	requestUnlock1()
	if store == nil || len(markers) == 0 {
		return nil
	}
	checked := 0
	for hash, at := range markers {
		if (FileRef{SHA256: hash}).validate() != nil {
			continue
		}
		if checked == fileReclaimBatch {
			break
		}
		checked++
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
