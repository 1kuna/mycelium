package myce

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const managedStorageSource = "managed allocated-block samples; unenforced"

// The observer reads metadata only on attempt-local roots, never NAS or payload
// contents. It does not enforce a limit or claim to see deleted-between-scan data.
type storageObserver struct {
	mu                                             sync.Mutex
	root, checkpoint, publication, state, identity string
	peak                                           AttemptStoragePeak
	baselineBytes                                  uint64
	metadata                                       []string
	stop                                           chan struct{}
	done                                           chan struct{}
	once                                           sync.Once
	walkDir                                        func(string, fs.WalkDirFunc) error // Defaults to filepath.WalkDir.
}

type storageScan struct{ allocated, scratch, spool, checkpoint, metadata, files uint64 }
type storageInode struct{ dev, ino uint64 }

func (o *storageObserver) scan() (storageScan, error) {
	var out storageScan
	seen := map[storageInode]bool{}
	walkDir := o.walkDir
	if walkDir == nil {
		walkDir = filepath.WalkDir
	}
	walk := func(root string, kind string) error {
		return walkDir(root, func(path string, d fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			info, err := d.Info()
			if os.IsNotExist(err) {
				return nil // Publication cleanup can remove an entry after discovery.
			}
			if err != nil {
				return err
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fs.ErrInvalid
			}
			inode := storageInode{uint64(st.Dev), uint64(st.Ino)}
			if seen[inode] {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			seen[inode] = true
			bytes := uint64(max(0, st.Blocks)) * 512 // POSIX st_blocks is in 512-byte units.
			// The physical total comes from every inode in the managed roots.
			// Categories explain that total; they never decide what is charged.
			out.allocated = addDiskBytes(out.allocated, bytes)
			bucket := kind
			if kind == "scratch" && (strings.Contains(path, string(filepath.Separator)+".myce-result-spool") || strings.Contains(path, string(filepath.Separator)+".result-spool-")) {
				bucket = "spool"
			} else if strings.Contains(path, string(filepath.Separator)+".file-checkpoint-") {
				bucket = "checkpoint"
			}
			switch bucket {
			case "spool":
				out.spool = addDiskBytes(out.spool, bytes)
			case "checkpoint":
				out.checkpoint = addDiskBytes(out.checkpoint, bytes)
			case "metadata":
				out.metadata = addDiskBytes(out.metadata, bytes)
			default:
				out.scratch = addDiskBytes(out.scratch, bytes)
			}
			// Inode pressure includes directories and symlinks, with hardlinks counted once.
			out.files = addDiskBytes(out.files, 1)
			return nil // WalkDir never follows a symlink.
		})
	}
	if err := walk(o.root, "scratch"); err != nil {
		return out, err
	}
	if o.checkpoint != "" {
		if err := walk(o.checkpoint, "checkpoint"); err != nil {
			return out, err
		}
	}
	if o.publication != "" {
		if err := walk(o.publication, "metadata"); err != nil {
			return out, err
		}
	}
	paths := []string{}
	if o.identity != "" {
		var err error
		paths, err = filepath.Glob(filepath.Join(o.state, "*"+o.identity+"*"))
		if err != nil {
			return out, err
		}
	}
	for _, name := range o.metadata {
		if name != "" && filepath.Base(name) == name {
			paths = append(paths, filepath.Join(o.state, name), filepath.Join(o.state, name+".tmp"))
		}
	}

	for _, path := range paths {
		if err := walk(path, "metadata"); err != nil {
			return out, err
		}
	}
	return out, nil
}
func (o *storageObserver) sample() {
	if o == nil {
		return
	}
	scan, err := o.scan()
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		o.peak.Complete = false
		return
	}
	staged := min(scan.scratch, o.baselineBytes)
	scratch := scan.scratch - staged
	files := scan.files // Include staged-input inodes; only their bytes are separate.
	p := &o.peak
	p.ScratchBytes = max(p.ScratchBytes, scratch)
	p.SpoolBytes = max(p.SpoolBytes, scan.spool)
	p.CheckpointBytes = max(p.CheckpointBytes, scan.checkpoint)
	p.MetadataBytes = max(p.MetadataBytes, scan.metadata)
	p.ManagedBytes = max(p.ManagedBytes, scan.allocated-staged)
	p.Files = max(p.Files, files)
	p.Samples = addDiskBytes(p.Samples, 1)
}
func (o *storageObserver) finish() *AttemptStoragePeak {
	if o == nil {
		return nil
	}
	o.once.Do(func() { close(o.stop); <-o.done; o.sample() })
	return o.snapshot()
}
func (o *storageObserver) snapshot() *AttemptStoragePeak {
	o.mu.Lock()
	defer o.mu.Unlock()
	p := o.peak
	return &p
}

func attemptStorageRoot(j Job, state string) string {
	return filepath.Join(state, "job-files", j.ID, strconv.Itoa(j.Attempt))
}

// Keep live checkpoint snapshots outside the payload's writable workspace and
// checkpoint grants, but inside one exact-attempt root for all publication I/O.
func attemptPublicationRoot(j Job, state string) string {
	return filepath.Join(state, "attempt-publication", j.ID, strconv.Itoa(j.Attempt))
}

func (w Worker) observeAttemptStorage(j Job, state, checkpoint, identity string, metadata ...string) *storageObserver {
	root := attemptStorageRoot(j, state)
	o := &storageObserver{root: root, publication: attemptPublicationRoot(j, state), state: state, identity: identity, metadata: metadata, peak: AttemptStoragePeak{Complete: true, Source: managedStorageSource}, stop: make(chan struct{}), done: make(chan struct{})}
	// Only known managed roots can claim complete sampling. Docker overlay writes
	// and external checkpoint/work paths need #506 accounting before they can learn.
	if j.Spec.Run.Docker || filepath.Clean(j.Spec.Run.Workdir) != filepath.Clean(root) {
		o.peak.Complete = false
	}
	if checkpoint != "" {
		rel, err := filepath.Rel(state, checkpoint)
		if err == nil && safeRelative(rel) {
			o.checkpoint = checkpoint
		} else if needsCheckpoint(&j) {
			o.peak.Complete = false
		}
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err == nil {
		o.peak.AllocationUnit = diskBlockSize(&st)
	} else {
		o.peak.Complete = false
	}
	scan, err := o.scan()
	if err != nil {
		o.peak.Complete = false
	}
	o.baselineBytes = scan.scratch
	o.peak.StagedInputBytes = o.baselineBytes
	o.peak.StagedInputFiles = scan.files
	o.sample()
	go func() {
		defer close(o.done)
		tick := time.NewTicker(attemptMemorySamplePeriod)
		defer tick.Stop()
		// This ticker schedules metadata I/O only. Learning/age decisions use
		// the queue's injected Clock; never read a mutable queue clock here.
		for {
			select {
			case <-o.stop:
				return
			case <-tick.C:
				o.sample()
			}
		}

	}()
	return o
}

// Publication can allocate a second immutable copy after compute exits. Keep
// that simultaneous workspace+spool peak in the same exact result, on replay too.
func samplePublishedStorage(j Job, r Result, state string, metadata ...string) Result {
	o := publicationStorageObserver(j, r, state, metadata...)
	if o != nil {
		o.sample()
		r.StoragePeak = o.snapshot()
	}
	return r
}

func publicationStorageObserver(j Job, r Result, state string, metadata ...string) *storageObserver {
	if r.StoragePeak == nil {
		return nil
	} // Recovery cannot invent missing lifetime coverage.
	p := *r.StoragePeak
	o := &storageObserver{root: attemptStorageRoot(j, state), publication: attemptPublicationRoot(j, state), state: state, metadata: metadata, peak: p, baselineBytes: p.StagedInputBytes}
	for _, name := range metadata {
		if strings.HasSuffix(name, ".result.json") {
			o.identity = strings.TrimSuffix(name, ".result.json")
		}
	}
	path := attemptCheckpoint(j)
	if !needsCheckpoint(&j) {
		path = filepath.Join(state, "checkpoints", j.ID, "attempt-"+strconv.Itoa(j.Attempt))
	}
	rel, err := filepath.Rel(state, path)
	if err == nil && safeRelative(rel) {
		o.checkpoint = path
	} else {
		o.peak.Complete = false
	}

	return o
}

func workerStorageMetadata(s *workerStore) string {
	if s.stateName != "" {
		return s.stateName
	}
	return "state.json"
}
