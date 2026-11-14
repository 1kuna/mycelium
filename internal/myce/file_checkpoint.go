package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type FileCheckpoint struct {
	Generation int64        `json:"generation"`
	Files      FileManifest `json:"files"`
}
type checkpointPublication struct {
	Result
	Checkpoint FileCheckpoint `json:"checkpoint"`
}

func bestFileCheckpoint(j *Job) *FileCheckpoint {
	bestAttempt := 0
	var best *FileCheckpoint
	for attempt, save := range j.FileCheckpoints {
		if best == nil || save.Generation > best.Generation || (save.Generation == best.Generation && attempt > bestAttempt) {
			value := save
			best = &value
			bestAttempt = attempt
		}
	}
	return best
}
func (q *Queue) verifyManifest(ctx context.Context, m FileManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.files == nil {
		return errors.New("file store unavailable")
	}
	if err := m.validate(); err != nil {
		return err
	}
	for _, ref := range m {
		if err := q.files.verify(ctx, ref); err != nil {
			return errors.New("manifest file unavailable or corrupt")
		}
	}
	// A retry after a directory-sync failure must repeat the barrier, not infer
	// durability from a visible blob name.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := q.files.syncDir(filepath.Join(q.files.root, "blobs")); err != nil {
		return err
	}
	return ctx.Err()
}
func (q *Queue) publishCheckpoint(ctx context.Context, p checkpointPublication) error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	if p.Checkpoint.Generation < 0 || len(p.Checkpoint.Files) == 0 {
		return errors.New("checkpoint requires a complete save")
	}
	if err := q.verifyManifest(ctx, p.Checkpoint.Files); err != nil {
		return err
	}
	// Resolve the policy without retaining a mutable job across bulk verification.
	q.mu.RLock()
	j := q.s.Jobs[p.ID]
	local := j != nil && j.Spec.localFirst()
	q.mu.RUnlock()
	if local {
		if err := verifyLocalFirstStore(q.files, p.Checkpoint); err != nil {
			return err
		}
	}
	generation := int64(-1)
	for name, ref := range p.Checkpoint.Files {
		generation = max(generation, 0)
		if strings.HasSuffix(name, ".json") {
			if ref.SizeBytes > 1<<20 {
				return errors.New("checkpoint JSON metadata exceeds 1 MiB")
			}
			data, err := os.ReadFile(q.files.blob(ref))
			if err != nil {
				return err
			}
			var save map[string]json.RawMessage
			if json.Unmarshal(data, &save) == nil && save["myce_checkpoint"] != nil {
				var meta struct {
					Generation json.RawMessage `json:"generation"`
				}
				if json.Unmarshal(save["myce_checkpoint"], &meta) == nil && meta.Generation != nil {
					var g int64
					if json.Unmarshal(meta.Generation, &g) != nil || g <= 0 {
						return errors.New("invalid checkpoint generation")
					}
					generation = max(generation, g)
				}
			}
		}
	}
	if generation != p.Checkpoint.Generation {
		return errors.New("checkpoint generation does not match saved bytes")
	}
	return q.updateRequest(ctx, func() error {
		j := q.touchJob(p.ID)
		if j == nil || !j.FileTransport || !j.Spec.Resumable || terminal(j.State) || cancelled(j) {
			return ErrStale
		}
		if local && (q.s.Machines[p.Machine] == nil || q.s.Machines[p.Machine].Session != p.Session) {
			return ErrStale
		}
		if local && (j.Attempt != p.Attempt || j.Machine != p.Machine || j.Session != p.Session || (j.State != "running" && j.State != "stopping")) {
			return ErrStale
		}
		allowed := false
		var devices []string
		for _, copy := range j.Copies {
			if copy.Attempt == p.Attempt && copy.Machine == p.Machine && copy.Session == p.Session && (copy.Recoverable || j.Attempt == p.Attempt && j.Machine == p.Machine && j.Session == p.Session) {
				allowed, devices = true, copy.AllocatedDevices
			}
		}
		if !allowed {
			return ErrStale
		}
		old, ok := j.FileCheckpoints[p.Attempt]
		if ok && p.Checkpoint.Generation < old.Generation {
			return errors.New("checkpoint generation moved backwards")
		}
		if ok && p.Checkpoint.Generation == old.Generation && !reflect.DeepEqual(old.Files, p.Checkpoint.Files) {
			return errors.New("checkpoint generation reused with different bytes")
		}
		if j.FileCheckpoints == nil {
			j.FileCheckpoints = map[int]FileCheckpoint{}
		}
		if local {
			if best := bestFileCheckpoint(j); best != nil {
				if p.Checkpoint.Generation < best.Generation {
					return ErrStale
				}
				if p.Checkpoint.Generation == best.Generation && !reflect.DeepEqual(best.Files, p.Checkpoint.Files) {
					return checkpointRefusal("checkpoint_generation_conflict", "accepted generation cannot change bytes")
				}
			}
			if err := q.acceptCheckpointMirror(j, p); err != nil {
				return err
			}
		}
		j.FileCheckpoints[p.Attempt] = p.Checkpoint
		noteCheckpointTarget(j, allocationMachine(q.s.Machines[p.Machine], devices), p.Attempt, p.Checkpoint.Generation)
		return nil
	}, nil)
}
func (a API) checkpointHandler(w http.ResponseWriter, r *http.Request) {
	var p checkpointPublication
	if !decode(w, r, &p) {
		return
	}
	respondOperation(w, map[string]bool{"ok": true}, a.Queue.publishCheckpoint(r.Context(), p), "checkpoint_invalid")
}

// captureCheckpoint copies a complete atomic save to immutable private staging.
// Generations and content are checked again after the copy to reject concurrent
// replacements. Writers use immutable payloads plus one atomic JSON manifest.
func captureCheckpoint(root, parent string, observers ...*storageObserver) (string, FileCheckpoint, error) {
	before, err := checkpointProgress(root)
	if err != nil {
		return "", FileCheckpoint{}, err
	}
	if before < 0 {
		return "", FileCheckpoint{Generation: -1}, nil
	}
	stage, err := os.MkdirTemp(parent, ".file-checkpoint-")
	if err != nil {
		return "", FileCheckpoint{}, err
	}
	defer func() {
		// Capture the simultaneous original and immutable copy before cleanup,
		// including partially copied generations when capture fails.
		for _, observer := range observers {
			observer.sample()
		}
		if err != nil {
			_ = os.RemoveAll(stage)
		}
	}()
	manifest := FileManifest{}
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if strings.HasSuffix(d.Name(), ".tmp") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if len(manifest) >= 1024 {
			return errors.New("checkpoint exceeds 1024-file manifest limit")
		}
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		target := filepath.Join(stage, rel)
		if e = copyJobFile(p, target); e != nil {
			return e
		}
		ref, e := IdentifyFile(target)
		if e != nil {
			return e
		}
		manifest[filepath.ToSlash(rel)] = ref
		return nil
	})
	if err == nil {
		err = manifest.validate()
	}
	if err == nil {
		after, e := checkpointProgress(root)
		if e != nil {
			err = e
		} else if after != before {
			err = errors.New("checkpoint changed during capture; retry")
		}
	}
	if err == nil {
		copied, e := checkpointProgress(stage)
		if e != nil {
			err = e
		} else if copied != before {
			err = errors.New("checkpoint snapshot incomplete; retry")
		}
	}
	if err == nil {
		for name, ref := range manifest {
			if e := verifyFile(context.Background(), filepath.Join(root, name), ref); e != nil {
				err = errors.New("checkpoint changed during capture; retry")
				break
			}
		}
	}
	if err != nil {
		return "", FileCheckpoint{}, err
	}
	return stage, FileCheckpoint{before, manifest}, nil
}
func (w Worker) publishFileCheckpoint(j Job, parent string) (int64, error) {
	if !j.FileTransport || !j.Spec.Resumable {
		return 0, nil
	}
	// Publication is attempt-owned allocation, outside payload write grants.
	// Scan the entire root, including temporary copies, rather than named files.
	stagingRoot := attemptPublicationRoot(j, parent)
	if err := os.MkdirAll(stagingRoot, 0700); err != nil {
		return 0, err
	}
	capture := captureCheckpoint
	if j.Spec.localFirst() {
		capture = captureLocalFirst
	}
	stage, save, err := capture(attemptCheckpoint(j), stagingRoot, w.storageObserver)
	if err != nil {
		return 0, err
	}
	if stage == "" {
		return 0, nil
	}
	defer func() {
		w.storageObserver.sample()
		_ = os.RemoveAll(stage)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx = withFileStoreAttempt(ctx, j)
	for name, ref := range save.Files {
		if err = w.Client.UploadFile(ctx, filepath.Join(stage, name), ref); err != nil {
			return 0, err
		}
	}
	p := checkpointPublication{Result: Result{Machine: w.Machine.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, Checkpoint: save}
	if err = w.Client.Call(ctx, "POST", "/api/files/checkpoint", p, nil); err != nil {
		return 0, err
	}
	if j.Spec.localFirst() {
		if err = writeLocalFirstAck(j, save); err != nil {
			return 0, err
		}
	}
	return save.Generation, nil
}
func (w Worker) prepareFileCheckpoint(j Job, stateDir string) (string, error) {
	if j.Spec.localFirst() && !modelRootLocalFilesystem(stateDir) {
		return "", checkpointRefusal("checkpoint_storage_unsupported", "local_first requires a supported local worker-state filesystem; network and unknown filesystems are refused")
	}
	dest := attemptCheckpoint(j)
	if _, err := os.Stat(dest); err == nil {
		return dest, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	if err := os.MkdirAll(j.Checkpoint, 0700); err != nil {
		return "", err
	}
	stage, err := os.MkdirTemp(stateDir, ".file-resume-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(stage)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if j.ResumeFiles != nil {
		if err = j.ResumeFiles.Files.validate(); err != nil {
			return "", err
		}
		for name, ref := range j.ResumeFiles.Files {
			if err = w.fileCache.copyTo(ctx, ref, filepath.Join(stage, name)); err != nil {
				return "", err
			}
		}
		generation, e := checkpointProgress(stage)
		if e != nil || generation != j.ResumeFiles.Generation {
			return "", errors.New("resume checkpoint generation mismatch")
		}
	}
	if err = syncModelDirectory(stage); err != nil {
		return "", err
	}
	if err = os.Rename(stage, dest); err != nil {
		return "", err
	}
	if err = syncWorkerDir(j.Checkpoint); err != nil {
		return "", err
	}
	if j.Spec.localFirst() && j.ResumeFiles != nil {
		if err = writeLocalFirstAck(j, *j.ResumeFiles); err != nil {
			return "", err
		}
	}
	return dest, nil
}
func (w Worker) finishFileResult(j Job, result Result, parent string, metadata ...string) (finished Result, err error) {
	return w.finishFileResultContext(context.Background(), j, result, parent, metadata...)
}

func (w Worker) finishFileResultContext(ctx context.Context, j Job, result Result, parent string, metadata ...string) (finished Result, err error) {
	done := w.Client.publication.begin(w.Client.clock(), "filesystem/checkpoint_publication")
	defer done()
	step, outputName := "checkpoint_publication", ""
	// Surface retryable publication errors without changing custody or Success.
	defer func() {
		if err != nil && !stale(err) {
			finished = finalizationResult(finished, step, outputName, err, false)
			err = finalizationErrorFor(step, outputName, err)
		}
	}()
	if result.Success && result.ExecutionCause != nil && result.ExecutionCause.Finalization != nil {
		result.ExecutionCause, result.Error = nil, ""
	}
	if result.StoragePeak != nil {
		if w.storageObserver == nil {
			w.storageObserver = publicationStorageObserver(j, result, parent, metadata...)
		}
		defer func() {
			done := w.Client.publication.begin(w.Client.clock(), "filesystem/storage_observation")
			defer done()
			w.storageObserver.sample()
			finished.StoragePeak = w.storageObserver.snapshot()
		}()
	}
	generation, err := w.publishFileCheckpoint(j, parent)
	done()
	if err != nil && !stale(err) {
		return result, err
	}
	if scarceYieldStop(result.PriorityStop) && j.Spec.Resumable && !result.ReleasePending {
		if !j.FileTransport {
			generation, err = checkpointProgress(attemptCheckpoint(j))
			if err != nil {
				generation = -1
			}
		}
		result.FinalCheckpointGeneration = &generation
	}
	if !result.Success || result.Interrupted || len(j.Spec.Outputs) == 0 || !j.storedFileResults() {
		return result, nil
	}
	if j.Spec.OutputDelivery == asyncNAS {
		done := w.Client.publication.begin(w.Client.clock(), "filesystem/output_spool")
		defer done()
		step = "output_spool"
		root, files, err := captureDeliverySpool(j, parent, w.storageObserver)
		done()
		if errors.Is(err, errRequiredDeliveryOutput) {
			return finalizationResult(result, step, "", err, true), nil
		}
		if err != nil {
			return result, err
		}
		ctx = withFileStoreAttempt(ctx, j)
		if j.Spec.OutputDelivery == asyncNAS {
			step = "output_reservation"
			reservation := result
			reservation.OutputRefs = files
			if err = w.Client.Call(ctx, "POST", "/api/files/result", reservation, nil); err != nil {
				return result, err
			}
			step = "output_preflight"
			if err = w.Client.PreflightFileStore(ctx, files); err != nil {
				return result, err
			}
		} else if outputPatterns(j.Spec.Outputs) {
			step = "output_preflight"
			if err = w.Client.PreflightFileStore(ctx, files); fileStoreLimitRefusal(err) {
				return finalizationResult(result, step, "", outputLimitError{err}, true), nil
			} else if err != nil {
				return result, err
			}
		}
		for name, ref := range files {
			step, outputName = "output_upload", name
			if err = w.Client.uploadFile(ctx, filepath.Join(root, name), ref, attemptOutputUploadPurpose); err != nil {
				return result, err
			}
		}
		result.OutputRefs = files
		return result, nil
	}
	// Queue service and output size are not bounded by elapsed wall time. Keep
	// the attempt owner's cancellation; the transfer client detects stalled
	// sockets and reconciles accepted offsets without restarting finalization.
	ctx = withFileStoreAttempt(ctx, j)
	output, terminal, err := w.publishOutputFiles(ctx, j, &step, &outputName)
	if terminal {
		return finalizationResult(result, step, outputName, err, true), nil
	}
	if err != nil {
		return result, err
	}
	result.OutputRefs = output
	return result, nil
}
