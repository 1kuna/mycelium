package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const asyncNAS = "async_nas"
const deliveryReceiptName = ".myce-delivery.json"

// Avoid retry storms against a failing destination; costs at most a minute
// after recovery. Outstanding syscalls keep their slot regardless of elapsed time.
const deliveryRetryDelay = time.Minute

// WithOutputDeliveryRoot selects the queue's NAS output destination independently
// of checkpoint mode. An empty root disables new asynchronous delivery tasks.
func WithOutputDeliveryRoot(root string) QueueOption { return func(q *Queue) { q.deliveryRoot = root } }

func deliveryRootID(root string) string {
	sum := sha256.Sum256([]byte(root))
	return hex.EncodeToString(sum[:])
}

type OutputDelivery struct {
	VerificationRound  uint64 `json:"verification_round,omitempty"`
	ProgressOperations uint64 `json:"progress_operations,omitempty"`
	Protocol           string `json:"protocol,omitempty"`
	Kind               string `json:"kind,omitempty"`
	DestinationID      string `json:"destination_id"`
	Execution
	Machine        string     `json:"machine"`
	Session        string     `json:"session"`
	State          string     `json:"state"`
	ManifestSHA256 string     `json:"manifest_sha256"`
	Bytes          uint64     `json:"bytes"`
	PendingAt      time.Time  `json:"pending_at"`
	LastProgressAt *time.Time `json:"last_progress_at,omitempty"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	NextAttemptAt  time.Time  `json:"next_attempt_at,omitempty"`
	VerifiedAt     *time.Time `json:"verified_at,omitempty"`
	Attempts       int        `json:"attempts"`
	Cause          string     `json:"cause,omitempty"`
}

type deliveryReceipt struct {
	DestinationID string `json:"destination_id,omitempty"`
	Execution
	Machine        string       `json:"machine"`
	Session        string       `json:"session"`
	Policy         string       `json:"policy"`
	ManifestSHA256 string       `json:"manifest_sha256"`
	Files          FileManifest `json:"files"`
}

type deliveryOperation struct {
	job      Job
	done     chan error
	progress atomic.Uint64
	observed uint64
}

func deliveryPending(j *Job) bool {
	return j != nil && j.Delivery != nil && j.Delivery.State != "verified"
}

func validateOutputDelivery(s Spec) error {
	if s.OutputDelivery == "" {
		return nil
	}
	if s.OutputDelivery != asyncNAS {
		return errors.New("output_delivery must be async_nas")
	}
	if len(s.Outputs) == 0 || s.Disk == nil || s.Disk.OutputBytes == 0 {
		return errors.New("async_nas requires outputs and positive disk.output_bytes")
	}
	// Fan-out aggregation has its own result ownership, not a single execution.
	if s.FanOut != nil || s.Run.Docker {
		return errors.New("async_nas requires individual tasks")
	}
	for name, prefix := range s.Outputs {
		if !safeRelative(name) || !safeRelative(prefix) || (strings.Split(name, "/")[0] == deliveryReceiptName || strings.Split(name, "/")[0] == ".myce-result-spool") {
			return errors.New("async_nas requires relative output names/prefixes without the receipt name")
		}
	}
	return nil
}

func deliveryDigest(j Job) string {
	if j.Spec.OutputDestination != nil {
		return identityHash(struct {
			Destination OutputDestination
			Files       FileManifest
			Prefixes    map[string]string
		}{*j.Spec.OutputDestination, taskOutputFiles(j), j.Spec.Outputs})
	}
	data, _ := json.Marshal(struct {
		Policy       string
		Files        FileManifest
		Destinations map[string]string
	}{asyncNAS + "/1", j.OutputRefs, j.Spec.Outputs})
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sameDelivery(current *Job, snapshot Job) bool {
	return current != nil && current.State == "succeeded" &&
		current.ID == snapshot.ID && current.Attempt == snapshot.Attempt &&
		current.Machine == snapshot.Machine && current.Session == snapshot.Session &&
		current.Delivery != nil && snapshot.Delivery != nil &&
		current.Delivery.Execution.ID == current.ID && current.Delivery.Execution.Attempt == current.Attempt &&
		current.Delivery.Execution == snapshot.Delivery.Execution &&
		current.Delivery.VerificationRound == snapshot.Delivery.VerificationRound &&
		current.Delivery.Machine == snapshot.Machine && current.Delivery.Session == snapshot.Session &&
		current.Delivery.ManifestSHA256 == snapshot.Delivery.ManifestSHA256 &&
		current.Delivery.DestinationID == snapshot.Delivery.DestinationID &&
		deliveryDigest(*current) == snapshot.Delivery.ManifestSHA256
}

// Tick only starts/observes background work. NAS I/O never owns a queue lock,
// nor the file-lifecycle lock: pending references already pin its source blobs.
func (q *Queue) advanceOutputDelivery(ctx context.Context) error {
	q.deliveryMu.Lock()
	defer q.deliveryMu.Unlock()
	q.mu.RLock()
	active := q.deliveryActive
	eligible := active != nil
	if !eligible {
		for _, j := range q.liveJobs() {
			if !parkedStorage(j) && deliveryPending(j) && !q.clock.Now().Before(j.Delivery.NextAttemptAt) {
				eligible = true
				break
			}
		}
	}
	q.mu.RUnlock()
	if !eligible {
		return nil
	}
	if active != nil {
		select {
		case publishErr := <-active.done:
			err := q.updateRequest(ctx, func() error {
				j := q.touchJob(active.job.ID)
				if !sameDelivery(j, active.job) {
					return nil
				}
				now := q.clock.Now()
				j.Delivery.LastProgressAt = &now
				if publishErr != nil {
					j.Delivery.State, j.Delivery.Cause = "failed", "NAS publication or receipt verification failed; source files retained"
					j.Delivery.NextAttemptAt = now.Add(deliveryRetryDelay)
				} else {
					j.Delivery.State, j.Delivery.Cause = "verified", ""
					j.Delivery.VerifiedAt = &now
					j.Delivery.NextAttemptAt = time.Time{}
				}
				return nil
			}, nil)
			if err != nil {
				// Preserve the observed answer for a transaction retry (including lost ack).
				active.done <- publishErr
				return err
			}
			q.mu.Lock()
			q.deliveryActive = nil
			q.mu.Unlock()
		default:
			progress := active.progress.Load()
			err := q.updateRequest(ctx, func() error {
				j := q.s.Jobs[active.job.ID]
				if !sameDelivery(j, active.job) {
					return nil
				}
				if progress != active.observed {
					j = q.touchJob(j.ID)
					now := q.clock.Now()
					j.Delivery.LastProgressAt, j.Delivery.Cause = &now, ""
				} else if j.Delivery.Cause == "" && j.Delivery.LastProgressAt != nil && q.clock.Now().Sub(*j.Delivery.LastProgressAt) >= checkpointAvailabilityStallBudget {
					j = q.touchJob(j.ID)
					j.Delivery.Cause = "NAS publication stalled; outstanding operation retained"
				}
				return nil
			}, nil)
			if err == nil {
				active.observed = progress
			}
			return err
		}
	}
	var next *deliveryOperation
	err := q.updateRequest(ctx, func() error {
		// Stable oldest-first ordering, without letting a failed task block others.
		jobs := []*Job{}
		for _, j := range q.liveJobs() {
			if !parkedStorage(j) && deliveryPending(j) && !q.clock.Now().Before(j.Delivery.NextAttemptAt) {
				jobs = append(jobs, j)
			}
		}
		slices.SortFunc(jobs, func(a, b *Job) int {
			if c := a.Delivery.PendingAt.Compare(b.Delivery.PendingAt); c != 0 {
				return c
			}
			return strings.Compare(a.ID, b.ID)
		})
		for _, candidate := range jobs {
			if q.deliveryRoot == "" || candidate.Delivery.DestinationID != deliveryRootID(q.deliveryRoot) {
				j := q.touchJob(candidate.ID)
				j.Delivery.State, j.Delivery.Cause = "failed", "NAS destination configuration missing or changed; restore the original output-delivery-root"
				continue
			}
			if candidate.State != "succeeded" || candidate.Attempt != candidate.Delivery.Attempt || candidate.ID != candidate.Delivery.ID || candidate.Machine != candidate.Delivery.Machine || candidate.Session != candidate.Delivery.Session || deliveryDigest(*candidate) != candidate.Delivery.ManifestSHA256 {
				continue
			}
			j := q.touchJob(candidate.ID)
			now := q.clock.Now()
			j.Delivery.State, j.Delivery.Cause = "publishing", ""
			j.Delivery.LastAttemptAt, j.Delivery.LastProgressAt = &now, &now
			j.Delivery.Attempts++
			next = &deliveryOperation{job: cloneJob(j), done: make(chan error, 1)}
			break
		}
		return nil
	}, nil)
	if err != nil || next == nil {
		return err
	}
	q.mu.Lock()
	q.deliveryActive = next
	store, root, publish := q.files, q.deliveryRoot, q.deliveryPublish
	q.mu.Unlock()
	if publish == nil {
		publish = func(j Job, s *FileStore, root string, progress func()) error {
			return publishOutputDelivery(j, s, root, progress)
		}
	}
	go func() { next.done <- publish(next.job, store, root, func() { next.progress.Add(1) }) }()
	return nil
}

// os.Root supplies confined filesystem operations even if ancestors change.
// Complete directories include their receipt before rename; replay verifies both
// receipt identity AND destination content, never just the existence of a file.
func publishOutputDelivery(j Job, store *FileStore, path string, callbacks ...func()) error {
	progress := outputDeliveryProgress(callbacks)
	if store == nil || j.Delivery == nil || deliveryDigest(j) != j.Delivery.ManifestSHA256 || deliveryRootID(path) != j.Delivery.DestinationID {
		return ErrStale
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	progress()
	groups := map[string]FileManifest{}
	for name, prefix := range j.Spec.Outputs {
		if groups[prefix] == nil {
			groups[prefix] = FileManifest{}
		}
		groups[prefix][name] = j.OutputRefs[name]
	}
	for _, prefix := range slices.Sorted(maps.Keys(groups)) {
		files := groups[prefix]
		dest := filepath.Join(prefix, j.ID, strconv.Itoa(j.Attempt))
		receipt := deliveryReceipt{DestinationID: j.Delivery.DestinationID, Execution: j.Delivery.Execution, Machine: j.Machine, Session: j.Session, Policy: asyncNAS + "/1", ManifestSHA256: j.Delivery.ManifestSHA256, Files: files}
		if _, e := root.Lstat(dest); e == nil {
			if err = verifyDeliveryDirectory(root, dest, receipt, progress); err != nil {
				return err
			}
			if err = syncDeliveryAncestors(root, filepath.Dir(dest), progress); err != nil {
				return err
			}
			continue
		} else if !os.IsNotExist(e) {
			return e
		}
		progress()
		parent := filepath.Dir(dest)
		if err = root.MkdirAll(parent, 0700); err != nil {
			return err
		}
		progress()
		// Deterministic stage is queue-owned and reusable after a crash. Nothing is
		// deleted until a fresh complete directory has been durably exposed.
		stage := filepath.Join(parent, ".delivery-"+strconv.Itoa(j.Attempt))
		if err = root.RemoveAll(stage); err != nil {
			return err
		}
		progress()
		if err = root.Mkdir(stage, 0700); err != nil {
			return err
		}
		progress()
		defer root.RemoveAll(stage) // Copies have joined before return; crash leftovers are retried.
		names := map[string]string{}
		for name := range files {
			names[name] = name
		}
		err = stageJobFiles(names, func(name, _ string) error {
			if err := root.MkdirAll(filepath.Join(stage, filepath.Dir(name)), 0700); err != nil {
				return err
			}
			progress()
			src, err := os.Open(store.blob(files[name]))
			if err != nil {
				return err
			}
			defer src.Close()
			return writeDeliveryFile(root, filepath.Join(stage, name), io.LimitReader(src, files[name].SizeBytes+1), progress)
		})
		if err != nil {
			return err
		}
		data, _ := json.Marshal(receipt)
		if err = writeDeliveryFile(root, filepath.Join(stage, deliveryReceiptName), strings.NewReader(string(data)), progress); err != nil {
			return err
		}
		if err = verifyDeliveryDirectory(root, stage, receipt, progress); err != nil {
			return err
		}
		if err = root.Rename(stage, dest); err != nil {
			return err
		}
		// Persist the rename and newly created ancestors as well as the tree itself.
		if err = syncDeliveryAncestors(root, parent, progress); err != nil {
			return err
		}
	}
	return nil
}

func syncDeliveryAncestors(root *os.Root, path string, callbacks ...func()) error {
	progress := outputDeliveryProgress(callbacks)
	for p := path; ; p = filepath.Dir(p) {
		if err := syncDeliveryDirectory(root, p, progress); err != nil {
			return err
		}
		if p == "." {
			return nil
		}
	}
}

func writeDeliveryFile(root *os.Root, path string, src io.Reader, callbacks ...func()) error {
	progress := outputDeliveryProgress(callbacks)
	defer progress()
	f, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, e := io.Copy(deliveryProgressWriter{f, progress}, src)
	return errors.Join(e, f.Sync(), f.Close())
}

func syncDeliveryDirectory(root *os.Root, path string, callbacks ...func()) error {
	progress := outputDeliveryProgress(callbacks)
	defer progress()
	f, err := root.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if cifs, e := checkpointCIFS(f); e == nil && cifs {
		return nil
	}
	return f.Sync()
}

func verifyDeliveryDirectory(root *os.Root, path string, receipt deliveryReceipt, callbacks ...func()) error {
	progress := outputDeliveryProgress(callbacks)
	f, err := root.Open(filepath.Join(path, deliveryReceiptName))
	if err != nil {
		return err
	}
	expected, _ := json.Marshal(receipt)
	actual, e := io.ReadAll(io.LimitReader(f, int64(len(expected))+1))
	err = errors.Join(e, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	var decoded deliveryReceipt
	if json.Unmarshal(actual, &decoded) != nil || identityHash(decoded) != identityHash(receipt) {
		return errors.New("conflicting delivery receipt")
	}
	seen := map[string]bool{}
	err = fs.WalkDir(root.FS(), path, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("delivery symlink refused")
		}
		if d.IsDir() {
			return syncDeliveryDirectory(root, p, progress)
		}
		name, err := filepath.Rel(path, p)
		if err != nil {
			return err
		}
		if name == deliveryReceiptName {
			return nil
		}
		ref, ok := receipt.Files[name]
		if !ok {
			return errors.New("unexpected delivery file")
		}
		f, err := root.Open(p)
		if err != nil {
			return err
		}
		st, e := f.Stat()
		if e != nil {
			f.Close()
			return e
		}
		if !st.Mode().IsRegular() {
			f.Close()
			return errors.New("delivery file is not regular")
		}
		h := sha256.New()
		n, e := io.Copy(deliveryProgressWriter{h, progress}, f)
		err = errors.Join(e, f.Sync(), f.Close())
		if err != nil {
			return err
		}
		if n != ref.SizeBytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
			return errors.New("delivery content mismatch")
		}
		seen[name] = true
		return nil
	})
	if err != nil {
		return err
	}
	if len(seen) != len(receipt.Files) {
		return errors.New("incomplete delivery")
	}
	return nil
}

// I/O goroutines report progress as a counter; only Tick reads the injected
// clock. Chunked writes/verification keep a large healthy copy from stalling.
func outputDeliveryProgress(callbacks []func()) func() {
	if len(callbacks) > 0 && callbacks[0] != nil {
		return callbacks[0]
	}
	return func() {}
}

type deliveryProgressWriter struct {
	io.Writer
	progress func()
}

func (w deliveryProgressWriter) Write(data []byte) (int, error) {
	n, err := w.Writer.Write(data)
	if n > 0 {
		w.progress()
	}
	return n, err
}
