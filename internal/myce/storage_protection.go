package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// StorageProtectionBackend is the installer/runtime boundary. No production
// implementation is qualified yet. Implementations must inspect the actual
// native boundary, not infer it from a filesystem name or a configured maximum.
// Calls may allocate, sync and inspect mounts; none runs under q.mu.
// Protect must fund *complete* control and worker backing before Snapshot reports
// shared free space. It must never shrink/reformat a volume containing data.
const boundedStorageCapability = "bounded_storage_v1"

type StorageProtectionBackend interface {
	Protect(context.Context, ControlStorageDemand) error
	Snapshot(context.Context) (StorageProtectionSnapshot, error)
}

// StorageAttemptBoundary is worker-local. Bind verifies/installs the exact
// byte/inode limit over every listed write path and confines all payload members
// (including unsandboxed policies, Docker and sidecars), with inputs/caches read
// only. Reject an unsupported/unconfined path before any preparation or payload
// writes. Publication writes and worker metadata need the same charge. Bind is
// idempotent and cannot raise an existing allocation's limit.
type StorageAttemptBoundary interface {
	Bind(context.Context, StorageAllocation, StorageWritePaths) error
	// Denied reports actual native aggregate block/inode denial. A command exit,
	// sampled threshold or uncontained host ENOSPC is never denial evidence.
	Denied(context.Context, StorageAllocation) (bool, error)
}

type StorageWritePaths struct {
	State, Work, Checkpoint, Publication, Index string
	ReadOnly                                    []string
}

// A pool is a capacity domain, not a path/device alias. For APFS it is the
// container, not the volume device. Backends must canonicalize aliases.
type StoragePool struct {
	ID        string `json:"id"`
	FreeBytes uint64 `json:"free_bytes"`
	FreeFiles uint64 `json:"free_files"`
}

// Allocated backing is already absent from statfs free. A native reserve's
// unused portion may still be included there: subtract only ExcludedFreeBytes.
// Project quotas alone are never availability backing.
type StorageBacking struct {
	ID string `json:"id"`
	// NativeIdentity comes from ObserveStorageBackingIdentity. A different volume
	// at the configured path is replacement; reattaching the same volume is not.
	// Usage/free/reservation measurements never participate in volume identity.
	NativeIdentity    string `json:"native_identity"`
	Pool              string `json:"pool"`
	Kind              string `json:"kind"` // independent, allocated, native-reserve
	CapacityBytes     uint64 `json:"capacity_bytes"`
	CapacityFiles     uint64 `json:"capacity_files"`
	BackedBytes       uint64 `json:"backed_bytes"`
	OverheadBytes     uint64 `json:"overhead_bytes"`
	ExcludedFreeBytes uint64 `json:"excluded_free_bytes"`
	Verified          bool   `json:"verified"`
}

type StorageWorkerBacking struct {
	ProductionBacking string `json:"production_backing,omitempty"`
	Machine           string `json:"machine"`
	Session           string `json:"session"`
	Backing           string `json:"backing"`
	// All managed writable paths, native block/inode denial and unrelated writers
	// must be qualified independently. A quota is not proof of confinement.
	Confined         bool   `json:"confined"`
	ExplorationBytes uint64 `json:"exploration_bytes"`
	ExplorationFiles uint64 `json:"exploration_files"`
}

type StorageProtectionSnapshot struct {
	Pools                    []StoragePool          `json:"pools"`
	Backings                 []StorageBacking       `json:"backings"`
	Workers                  []StorageWorkerBacking `json:"workers"`
	SharedPool               string                 `json:"shared_pool"`
	ControlBacking           string                 `json:"control_backing"`
	ControlIsolated          bool                   `json:"control_isolated"`
	UnrelatedWritersIsolated bool                   `json:"unrelated_writers_isolated"`
}

type ControlStorageDemand struct {
	SQLiteBytes     uint64    `json:"sqlite_bytes"`
	LogBytes        uint64    `json:"log_bytes"`
	SyncBytes       uint64    `json:"sync_bytes"`
	CheckpointBytes uint64    `json:"checkpoint_bytes"`
	MetadataBytes   uint64    `json:"metadata_bytes"`
	Files           uint64    `json:"files"`
	RequiredBytes   uint64    `json:"required_bytes"`
	MeasuredAt      time.Time `json:"measured_at"`
}

// ControlStorageWork names the concurrently live working sets of a control
// operation/rehearsal. Include planned SQLite/WAL, logs, sync staging, checkpoint
// copies and backend metadata, not just the idle database. Backing must cover
// this measured operation before managed capacity can be advertised. No chosen
// percentage, old burst, sparse logical size or fixed byte floor is used.
// Paths are operator-local and never persisted or exposed in status.
type ControlStorageWork struct {
	SQLite, Logs, Sync, Checkpoint, Metadata []string
}

func MeasureControlStorage(ctx context.Context, clock Clock, work ControlStorageWork) (ControlStorageDemand, error) {
	var v ControlStorageDemand
	if clock == nil || len(work.SQLite) == 0 || len(work.Logs) == 0 || len(work.Sync) == 0 || len(work.Checkpoint) == 0 || len(work.Metadata) == 0 {
		return v, errors.New("complete control working-space measurement required")
	}
	seen := map[[2]uint64]bool{}
	for _, category := range []struct {
		roots []string
		bytes *uint64
	}{
		{work.SQLite, &v.SQLiteBytes}, {work.Logs, &v.LogBytes}, {work.Sync, &v.SyncBytes}, {work.Checkpoint, &v.CheckpointBytes}, {work.Metadata, &v.MetadataBytes},
	} {
		for _, root := range category.roots {
			err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if err = ctx.Err(); err != nil {
					return err
				}
				info, err := d.Info()
				if err != nil {
					return err
				}
				st, ok := info.Sys().(*syscall.Stat_t)
				if !ok || st.Blocks < 0 || info.Mode()&os.ModeSymlink != 0 {
					return errors.New("invalid control working set")
				}
				key := [2]uint64{uint64(st.Dev), uint64(st.Ino)}
				if seen[key] {
					if d.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				seen[key] = true
				n := uint64(st.Blocks) * 512
				*category.bytes = addDiskBytes(*category.bytes, n)
				v.RequiredBytes = addDiskBytes(v.RequiredBytes, n)
				v.Files = addDiskBytes(v.Files, 1)
				return nil
			})
			if err != nil {
				return ControlStorageDemand{}, errors.New("control working-space measurement unavailable")
			}
		}
	}
	if v.RequiredBytes == 0 {
		return ControlStorageDemand{}, errors.New("empty control working-space measurement")
	}
	v.MeasuredAt = clock.Now()
	return v, nil
}

type StorageAllocation struct {
	ID            string    `json:"id"`
	Execution     Execution `json:"execution"`
	Machine       string    `json:"machine"`
	Session       string    `json:"session"`
	Backing       string    `json:"backing"`
	Bytes         uint64    `json:"bytes"`
	Files         uint64    `json:"files"`
	WorkerCustody bool      `json:"worker_custody"`
	At            time.Time `json:"at"`
}

type StorageCustody struct {
	Allocation string       `json:"allocation"`
	Files      FileManifest `json:"files"`
	At         time.Time    `json:"at"`
}

type storageProtectionState struct {
	Enabled     bool                         `json:"enabled"`
	Control     ControlStorageDemand         `json:"control"`
	Snapshot    StorageProtectionSnapshot    `json:"snapshot"`
	SampledAt   time.Time                    `json:"sampled_at"`
	Allocations map[string]StorageAllocation `json:"allocations"`
	Custody     map[string]StorageCustody    `json:"custody"`
}

// The installer calls this only after a complete control-operation measurement.
// This is an integration seam, not a CLI activation/migration switch (#509).
// Restart retains commitments but grants zero allowance until fresh verification.
func (q *Queue) ConfigureStorageProtection(ctx context.Context, backend StorageProtectionBackend, control ControlStorageDemand) error {
	q.storageProtectionMu.Lock()
	defer q.storageProtectionMu.Unlock()
	if backend == nil || control.RequiredBytes == 0 || control.Files == 0 || control.MeasuredAt.IsZero() {
		return errors.New("measured control backing required")
	}
	q.mu.Lock()
	q.storageProtectionVerified = false
	q.mu.Unlock()
	if err := backend.Protect(ctx, control); err != nil {
		return errors.New("control backing unavailable")
	}
	snapshot, err := backend.Snapshot(ctx)
	if err != nil {
		return errors.New("storage backing unavailable")
	}
	// Own the observation before publishing it; backend refreshes must not mutate
	// a snapshot already used by concurrent admission/status transactions.
	data, copyErr := json.Marshal(snapshot)
	if copyErr != nil {
		return errors.New("invalid storage backing observation")
	}
	var owned StorageProtectionSnapshot
	if json.Unmarshal(data, &owned) != nil {
		return errors.New("invalid storage backing observation")
	}
	snapshot = owned
	if err = validateStorageProtection(snapshot, control); err != nil {
		return err
	}
	err = q.updateRequest(ctx, func() error {
		if q.s.StorageProtection == nil {
			q.s.StorageProtection = &storageProtectionState{Enabled: true, Allocations: map[string]StorageAllocation{}, Custody: map[string]StorageCustody{}}
		}
		p := q.s.StorageProtection
		// Rebinding existing custody to another disk/session would silently orphan
		// unique outputs. Preserve all allocations and their original backing IDs.
		for _, a := range p.Allocations {
			old, _ := storageBacking(p.Snapshot, a.Backing)
			current, ok := storageBacking(snapshot, a.Backing)
			if !ok || !old.sameBoundary(current) {
				return errors.New("retained storage backing changed")
			}
		}
		// The queue update journal copies base state via JSON. Clone nested mutable
		// maps here as well so read-only projections cannot share later mutations.
		copy := *p
		copy.Control, copy.Snapshot, copy.SampledAt = control, snapshot, q.clock.Now()
		q.s.StorageProtection = &copy
		return nil
	}, nil)
	if err == nil {
		q.mu.Lock()
		q.storageProtectionVerified = true
		q.mu.Unlock()
	}
	return err
}

// Capacity is the installed boundary, not current usage. Funding and overhead
// observations are freshly validated below; unused reservation must be allowed
// to change as the same native volume fills without invalidating retained data.
func (b StorageBacking) sameBoundary(other StorageBacking) bool {
	return b.ID == other.ID && b.NativeIdentity == other.NativeIdentity &&
		b.Pool == other.Pool && b.Kind == other.Kind &&
		b.CapacityBytes == other.CapacityBytes && b.CapacityFiles == other.CapacityFiles
}

func storageBacking(snapshot StorageProtectionSnapshot, id string) (StorageBacking, bool) {
	for _, b := range snapshot.Backings {
		if b.ID == id {
			return b, true
		}
	}
	return StorageBacking{}, false
}
func validateStorageProtection(s StorageProtectionSnapshot, c ControlStorageDemand) error {
	if !s.ControlIsolated || !s.UnrelatedWritersIsolated {
		return errors.New("control/unrelated writer isolation unqualified")
	}
	pools := map[string]StoragePool{}
	for _, p := range s.Pools {
		if !extensionName.MatchString(p.ID) {
			return errors.New("invalid storage pool")
		}
		if old, ok := pools[p.ID]; ok && old != p {
			return errors.New("conflicting aliased storage pool")
		}
		pools[p.ID] = p
	}
	if _, ok := pools[s.SharedPool]; !ok {
		return errors.New("shared storage pool unavailable")
	}
	backings := map[string]StorageBacking{}
	for _, b := range s.Backings {
		nativeID, identityErr := hex.DecodeString(b.NativeIdentity)
		if identityErr != nil || len(nativeID) != sha256.Size || !extensionName.MatchString(b.ID) || !b.Verified || b.CapacityBytes == 0 || b.CapacityFiles == 0 || b.BackedBytes < addDiskBytes(b.CapacityBytes, b.OverheadBytes) || b.ExcludedFreeBytes > b.BackedBytes || (b.Kind != "independent" && b.Kind != "allocated" && b.Kind != "native-reserve") {
			return errors.New("storage availability backing unqualified")
		}
		if _, ok := pools[b.Pool]; !ok {
			return errors.New("backing pool unavailable")
		}
		if old, ok := backings[b.ID]; ok && old != b {
			return errors.New("conflicting aliased storage backing")
		}
		backings[b.ID] = b
	}
	control, ok := backings[s.ControlBacking]
	if !ok || control.CapacityBytes < c.RequiredBytes || control.CapacityFiles < c.Files {
		return errors.New("measured control headroom is not backed")
	}
	seen := map[string]bool{}
	for _, w := range s.Workers {
		b, ok := backings[w.Backing]
		if !ok || w.Backing == s.ControlBacking || w.Machine == "" || w.Session == "" || !w.Confined {
			return errors.New("worker custody/confinement unqualified")
		}
		// One independently backed worker budget cannot be advertised by two workers.
		if w.ProductionBacking != "" {
			prod, ok := backings[w.ProductionBacking]
			if !ok || prod.ID == s.ControlBacking || prod.ID == b.ID || seen[prod.ID] {
				return errors.New("production scratch backing is not independent")
			}
			seen[prod.ID] = true
		}
		if seen[b.ID] {
			return errors.New("worker backing advertised twice")
		}
		seen[b.ID] = true
	}
	return nil
}

func (q *Queue) storageWorker(m *Machine) (StorageWorkerBacking, bool) {
	if m == nil || q.s.StorageProtection == nil || !slices.Contains(m.Capabilities, boundedStorageCapability) {
		return StorageWorkerBacking{}, false
	}
	for _, w := range q.s.StorageProtection.Snapshot.Workers {
		if w.Machine == m.ID && w.Session == m.Session {
			return w, true
		}
	}
	return StorageWorkerBacking{}, false
}

func (q *Queue) storageRequest(j *Job, m *Machine) (StorageAllocation, string) {
	p := q.s.StorageProtection
	if p == nil || !p.Enabled {
		return StorageAllocation{}, ""
	}
	if !q.storageProtectionVerified {
		return StorageAllocation{}, "storage protection: fresh backend verification required"
	}
	w, ok := q.storageWorker(m)
	if !ok || !w.Confined {
		return StorageAllocation{}, "storage protection: worker backing/confinement unqualified"
	}
	if j.Spec.StorageProtocol != workerCustodyProtocol {
		return StorageAllocation{}, "storage protection: versioned worker custody protocol required; frozen policy unchanged"
	}
	if !j.storedFileResults() || !j.FileTransport || j.Spec.Run.Workdir != "" || len(jobInputPaths(j)) > 0 {
		return StorageAllocation{}, "storage protection: writable paths are not confined"
	}
	probe := *j
	if j.State == "queued" && j.Spec.Resumable {
		probe.Attempt = max(j.Starts, j.Attempt) + 1
	}
	envelope := q.storageEnvelope(&probe, m)
	bytes, files := w.ExplorationBytes, w.ExplorationFiles
	if envelope.PredictedNeed != nil && envelope.Files != nil {
		bytes, files = *envelope.PredictedNeed, *envelope.Files
		if envelope.Exploration != "" {
			// Keep accepted prediction/error intact; a fresh, independently
			// admitted exploration is the only way to install a larger limit.
			if w.ExplorationBytes <= bytes {
				return StorageAllocation{}, "storage protection: larger exploration requires fresh backing"
			}
			bytes, files = w.ExplorationBytes, max(files, w.ExplorationFiles)
		}
	}
	if bytes == 0 || files == 0 {
		return StorageAllocation{}, "storage protection: bounded exploration unavailable"
	}
	custody := q.priorityMatch(j.Spec).Tier != "production" && len(j.Spec.Outputs) > 0
	backing := w.Backing
	if q.priorityMatch(j.Spec).Tier == "production" {
		if w.ProductionBacking == "" {
			return StorageAllocation{}, "storage protection: production scratch backing unqualified"
		}
		backing = w.ProductionBacking
	}
	a := StorageAllocation{Machine: m.ID, Session: m.Session, Backing: backing, Bytes: bytes, Files: files, WorkerCustody: custody}
	return a, ""
}

// Retained allocations keep their entire installed boundary charged. A native
// allocation cannot be recycled just because compute stopped or a host vanished.
// The delivery/release owner must prove all writers stopped and custody released.
func (q *Queue) protectedStorageReason(j *Job, m *Machine) string {
	p := q.s.StorageProtection
	if p == nil || !p.Enabled {
		if j.Spec.StorageProtocol != "" {
			return "storage protection: backend unqualified"
		}
		return ""
	}
	request, reason := q.storageRequest(j, m)
	if reason != "" {
		return reason
	}
	b, _ := storageBacking(p.Snapshot, request.Backing)
	var bytes, files uint64
	for _, a := range p.Allocations {
		if a.Backing == b.ID {
			if j.State == "running" && j.StorageAllocation != nil && a.ID == j.StorageAllocation.ID && a.Execution.ID == j.ID && a.Execution.Attempt == j.Attempt && a.Machine == m.ID && a.Session == m.Session {
				continue
			}
			bytes = addDiskBytes(bytes, a.Bytes)
			files = addDiskBytes(files, a.Files)
		}
	}
	if request.Bytes > b.CapacityBytes-min(b.CapacityBytes, bytes) || request.Files > b.CapacityFiles-min(b.CapacityFiles, files) {
		freeBytes, freeFiles := b.CapacityBytes-min(b.CapacityBytes, bytes), b.CapacityFiles-min(b.CapacityFiles, files)
		var holders []string
		for _, a := range p.Allocations {
			if a.Backing == b.ID {
				holders = append(holders, a.Execution.ID)
			}
		}
		slices.Sort(holders)
		return fmt.Sprintf("storage protection: worker custody budget full; backing=%s need=%d bytes/%d files; retained=%d bytes/%d files; free=%d bytes/%d files deficit=%d bytes/%d files holders=%s; wait for verified delivery and legal release or route to another qualified worker", b.ID, request.Bytes, request.Files, bytes, files, freeBytes, freeFiles, request.Bytes-min(request.Bytes, freeBytes), request.Files-min(request.Files, freeFiles), strings.Join(slices.Compact(holders), ","))
	}
	return ""
}

func (q *Queue) assignStorage(j *Job, a StorageAllocation) {
	if q.s.StorageProtection == nil || !q.s.StorageProtection.Enabled {
		j.StorageAllocation = nil
		return
	}

	a.Execution = Execution{ID: j.ID, Attempt: j.Attempt}
	a.ID = identityHash(struct {
		Execution        Execution
		Machine, Session string
	}{a.Execution, a.Machine, a.Session})
	a.At = q.clock.Now()
	p := *q.s.StorageProtection
	p.Allocations = maps.Clone(p.Allocations)
	p.Allocations[a.ID] = a
	q.s.StorageProtection = &p
	j.StorageAllocation = &a
	if j.StorageEnvelope != nil {
		j.StorageEnvelope.HardLimit = nil // Admission is not proof that the worker installed it.
		j.StorageEnvelope.Enforcement = "filesystem boundary required before preparation; native qualification separate"
	}
}

func (w Worker) bindStorage(ctx context.Context, s *workerStore, j Job) error {
	if j.StorageAllocation == nil {
		return nil
	}
	a := *j.StorageAllocation
	if w.StorageBoundary == nil || a.Execution.ID != j.ID || a.Execution.Attempt != j.Attempt || a.Machine != w.Machine.ID || a.Session != j.Session || a.Bytes == 0 || a.Files == 0 {
		return errors.New("storage protection: exact attempt boundary unavailable")
	}
	work := filepath.Join(s.dir, "job-files", j.ID, strconv.Itoa(j.Attempt))
	checkpoint := filepath.Join(s.dir, "checkpoints", j.ID, fmt.Sprintf("attempt-%d", j.Attempt))
	paths := StorageWritePaths{State: s.dir, Work: work, Checkpoint: checkpoint, Publication: s.dir, Index: s.indexRoot, ReadOnly: []string{w.FileCacheRoot, w.CacheRoot, w.EnvironmentRoot, w.ExtensionRoot, w.JobFileRoot}}
	if j.Checkpoint != "" {
		paths.Checkpoint = j.Checkpoint
	}
	return w.StorageBoundary.Bind(ctx, a, paths)
}

// Shared capacity excludes unused native reservations once; allocated backing
// already reduced free space. Worker filling/retention never changes this value.
func (q *Queue) protectedSharedFree() uint64 {
	p := q.s.StorageProtection
	if p == nil || !q.storageProtectionVerified {
		return 0
	}
	var free uint64
	for _, pool := range p.Snapshot.Pools {
		if pool.ID == p.Snapshot.SharedPool {
			free = pool.FreeBytes
			break
		}
	}
	seen := map[string]bool{}
	for _, b := range p.Snapshot.Backings {
		if b.Pool == p.Snapshot.SharedPool && !seen[b.ID] {
			free -= min(free, b.ExcludedFreeBytes)
			seen[b.ID] = true
		}
	}
	return free
}

func storageCustodyRefusal() error {
	return &HTTPError{Status: http.StatusConflict, Code: "worker_custody_required", Message: "unique experiment outputs must remain on their producing worker; destination delivery is pending"}
}

const workerCustodyProtocol = "worker-custody/1"

var errStorageCustodyPending = errors.New("worker-local custody retained; verified destination delivery required")

type StorageCustodyReceipt struct {
	Result     *Result      `json:"result,omitempty"`
	Protocol   string       `json:"protocol"`
	Execution  Execution    `json:"execution"`
	Machine    string       `json:"machine"`
	Session    string       `json:"session"`
	Allocation string       `json:"allocation"`
	Files      FileManifest `json:"files"`
}

// This records custody, never ordinary output_refs, task success, or NAS
// durability. #508 owns destination receipts and the versioned client contract.
func (q *Queue) RecordStorageCustody(ctx context.Context, r StorageCustodyReceipt) error {
	if r.Protocol != workerCustodyProtocol || r.Files.validate() != nil {
		return errors.New("invalid worker custody receipt")
	}
	return q.updateRequest(ctx, func() error {
		if j := q.s.Jobs[r.Execution.ID]; exactTaskOutputs(j, r) {
			return nil // Exact terminal replay, even after collection or worker loss.
		}
		j, err := q.current(Result{Execution: r.Execution, Machine: r.Machine, Session: r.Session})
		p := q.s.StorageProtection
		if err != nil || p == nil || j.StorageAllocation == nil || j.StorageAllocation.ID != r.Allocation || !j.StorageAllocation.WorkerCustody && j.Spec.OutputDestination == nil {
			return ErrStale
		}
		m := q.s.Machines[r.Machine]
		if m == nil || m.Session != r.Session || cancelled(j) {
			return ErrStale
		}
		a, ok := p.Allocations[r.Allocation]
		if !ok || a != *j.StorageAllocation {
			return ErrStale
		}
		if len(r.Files) != len(j.Spec.Outputs) || manifestDiskBytes(r.Files) > a.Bytes || uint64(len(r.Files)) > a.Files {
			return errors.New("custody manifest exceeds installed boundary")
		}
		for name := range j.Spec.Outputs {
			if _, ok := r.Files[name]; !ok {
				return errors.New("complete custody manifest required")
			}
		}
		if previous, ok := p.Custody[a.ID]; ok {
			if !maps.Equal(previous.Files, r.Files) {
				return errors.New("conflicting worker custody receipt")
			}
			return nil
		}
		copy := *p
		copy.Custody = maps.Clone(p.Custody)
		copy.Custody[a.ID] = StorageCustody{Allocation: a.ID, Files: maps.Clone(r.Files), At: q.clock.Now()}
		j.Reason = "worker-local custody pending verified destination delivery"
		q.s.StorageProtection = &copy
		return q.acceptTaskOutputCustody(j, r)
	}, nil)
}
func (a API) storageCustodyHandler(w http.ResponseWriter, r *http.Request) {
	var receipt StorageCustodyReceipt
	if !decode(w, r, &receipt) {
		return
	}
	respondOperation(w, map[string]string{"state": "worker_local_pending_delivery", "protocol": workerCustodyProtocol}, a.Queue.RecordStorageCustody(r.Context(), receipt), "storage_custody_invalid")
}

func (w Worker) retainStorageCustody(j Job, r Result, parent string) (Result, error) {
	// Failed/interrupted progress also stays within the installed boundary. No
	// checkpoint, diagnostic or output upload can spill it into shared storage.
	if !r.Success || r.Interrupted {
		return r, nil
	}
	if j.Spec.OutputDestination != nil {
		return w.finishTaskOutputs(j, r, parent)
	}
	_, files, err := captureDeliverySpool(j, parent, w.storageObserver)
	if err != nil {
		return r, err
	}
	receipt := StorageCustodyReceipt{Protocol: workerCustodyProtocol, Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Allocation: j.StorageAllocation.ID, Files: files}
	// Save custody before queue notification. A lost ACK is replayed against the
	// immutable spool. This marker also survives stale/cancelled actor cleanup.
	if err = saveWorkerJSON(deliverySpoolPath(j, parent), "custody.json", receipt); err != nil {
		return r, err
	}
	if err = w.Client.Call(context.Background(), "POST", "/api/workers/storage-custody", receipt, nil); err != nil {
		return r, err
	}
	r.OutputRefs = nil
	return r, errStorageCustodyPending
}

func (s *workerStore) restoreStorageDirectory(root string) error {
	id := s.state.StorageDirectory
	if len(id) != 64 || filepath.Base(id) != id {
		return errors.New("invalid protected storage directory")
	}
	physicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	dir := filepath.Join(physicalRoot, "protected-attempts", id)
	if err := noModelSymlinks(dir, false); err != nil {
		return errors.New("protected storage directory unavailable")
	}
	data, err := os.ReadFile(filepath.Join(dir, s.stateName))
	if err != nil {
		return err
	}
	var saved workerState
	if err = decodeWorkerState(data, &saved); err != nil {
		return err
	}
	if saved.StorageDirectory != id || saved.Session != s.state.Session {
		return errors.New("conflicting protected storage state")
	}
	s.dir, s.indexRoot, s.state = dir, root, saved
	return nil
}

// File-store admission always combines fresh statfs and tus lengths with these
// independently backed reservations. Existing caps and legacy floor still apply.
func (q *Queue) storageExcludedFree() (uint64, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	p := q.s.StorageProtection
	if p == nil || !p.Enabled {
		return 0, nil
	}
	if !q.storageProtectionVerified {
		return 0, errors.New("storage protection requires fresh verification")
	}
	var excluded uint64
	seen := map[string]bool{}
	for _, b := range p.Snapshot.Backings {
		if b.Pool == p.Snapshot.SharedPool && !seen[b.ID] {
			excluded = addDiskBytes(excluded, b.ExcludedFreeBytes)
			seen[b.ID] = true
		}
	}
	return excluded, nil
}

func (q *Queue) protectedStorageLaunchReason(j *Job) string {
	if reason := taskOutputCapabilityReason(j.Spec, q.s.Machines[j.Machine]); reason != "" {
		return reason
	}
	p := q.s.StorageProtection
	if p == nil || !p.Enabled {
		if j.Spec.OutputDestination != nil {
			return "storage_boundary_unverified"
		}
		return ""
	}
	if !q.storageProtectionVerified || j.StorageAllocation == nil {
		return "storage_boundary_unverified"
	}
	a, ok := p.Allocations[j.StorageAllocation.ID]
	if !ok || a != *j.StorageAllocation || a.Execution.ID != j.ID || a.Execution.Attempt != j.Attempt || a.Machine != j.Machine || a.Session != j.Session {
		return "storage_boundary_stale"
	}
	return ""
}

func recoverUnindexedStorageStores(root *workerStore, stores *[]*workerStore) error {
	physicalRoot, err := filepath.EvalSymlinks(root.dir)
	if err != nil {
		return err
	}
	base := filepath.Join(physicalRoot, "protected-attempts")
	entries, err := os.ReadDir(base)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = noModelSymlinks(base, false); err != nil {
		return err
	}
	known := map[string]bool{}
	for _, s := range *stores {
		if s.state.StorageDirectory != "" {
			known[s.state.StorageDirectory+"/"+s.stateName] = true
		}
	}
	for _, entry := range entries {
		id := entry.Name()
		if !entry.IsDir() || len(id) != 64 {
			return errors.New("unrecognized protected storage custody; recovery required")
		}
		dir := filepath.Join(base, id)
		if err = noModelSymlinks(dir, false); err != nil {
			return err
		}
		paths, err := filepath.Glob(filepath.Join(dir, "state-attempt-*.json"))
		if err != nil {
			return err
		}
		for _, path := range paths {
			name := filepath.Base(path)
			if known[id+"/"+name] {
				continue
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var st workerState
			if err = decodeWorkerState(data, &st); err != nil {
				return err
			}
			if st.StorageDirectory != id || st.Job != nil && (st.Job.StorageAllocation == nil || st.Job.StorageAllocation.ID != id) {
				return errors.New("conflicting protected storage custody")
			}
			if st.Job != nil {
				*stores = append(*stores, &workerStore{dir: dir, indexRoot: root.dir, stateName: name, state: st})
			}
		}
	}
	return nil
}
