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
	if p.Checkpoint.Generation < 0 || len(p.Checkpoint.Files) == 0 {
		return errors.New("checkpoint requires a complete save")
	}
	if err := q.verifyManifest(ctx, p.Checkpoint.Files); err != nil {
		return err
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
	return q.update(func() error {
		j := q.s.Jobs[p.ID]
		if j == nil || !j.FileTransport || !j.Spec.Resumable || terminal(j.State) || cancelled(j) {
			return ErrStale
		}
		allowed := false
		for _, copy := range j.Copies {
			if copy.Attempt == p.Attempt && copy.Machine == p.Machine && copy.Session == p.Session && (copy.Recoverable || j.Attempt == p.Attempt && j.Machine == p.Machine && j.Session == p.Session) {
				allowed = true
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
		j.FileCheckpoints[p.Attempt] = p.Checkpoint
		return nil
	})
}
func (a API) checkpointHandler(w http.ResponseWriter, r *http.Request) {
	var p checkpointPublication
	if !decode(w, r, &p) {
		return
	}
	respond(w, map[string]bool{"ok": true}, a.Queue.publishCheckpoint(r.Context(), p))
}

// captureCheckpoint copies a complete atomic save to immutable private staging.
// Generations and content are checked again after the copy to reject concurrent
// replacements. Writers use immutable payloads plus one atomic JSON manifest.
func captureCheckpoint(root, parent string) (string, FileCheckpoint, error) {
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
		_ = os.RemoveAll(stage)
		return "", FileCheckpoint{}, err
	}
	return stage, FileCheckpoint{before, manifest}, nil
}
func (w Worker) publishFileCheckpoint(j Job, parent string) (int64, error) {
	if !j.FileTransport || !j.Spec.Resumable {
		return 0, nil
	}
	stage, save, err := captureCheckpoint(attemptCheckpoint(j), parent)
	if err != nil {
		return 0, err
	}
	if stage == "" {
		return 0, nil
	}
	defer os.RemoveAll(stage)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for name, ref := range save.Files {
		if err = w.Client.UploadFile(ctx, filepath.Join(stage, name), ref); err != nil {
			return 0, err
		}
	}
	p := checkpointPublication{Result: Result{Machine: w.Machine.ID, Session: j.Session, Execution: Execution{ID: j.ID, Attempt: j.Attempt}}, Checkpoint: save}
	if err = w.Client.Call(ctx, "POST", "/api/files/checkpoint", p, nil); err != nil {
		return 0, err
	}
	return save.Generation, nil
}
func (w Worker) prepareFileCheckpoint(j Job, stateDir string) (string, error) {
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
	return dest, syncWorkerDir(j.Checkpoint)
}
func (w Worker) finishFileResult(j Job, result Result, parent string) (Result, error) {
	if _, err := w.publishFileCheckpoint(j, parent); err != nil && !stale(err) {
		return result, err
	}
	if !result.Success || result.Interrupted || len(j.Spec.Outputs) == 0 || !j.FileTransport {
		return result, nil
	}
	output := FileManifest{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for name := range j.Spec.Outputs {
		src, err := confinedPath(j.Spec.Run.Workdir, name)
		if err != nil {
			result.Success = false
			result.Error = "required output missing or invalid"
			return result, nil
		}
		ref, err := IdentifyFile(src)
		if err != nil {
			result.Success = false
			result.Error = "required output missing or invalid"
			return result, nil
		}
		if err = w.Client.UploadFile(ctx, src, ref); err != nil {
			return result, err
		}
		output[name] = ref
	}
	result.OutputRefs = output
	return result, nil
}
