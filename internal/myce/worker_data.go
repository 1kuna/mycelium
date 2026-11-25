package myce

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// v2 also confines native groups and checks frozen extension executions. A v1
// worker must never receive a newly supported job through a v2 queue.
const (
	ephemeralCapability       = "worker_data_ephemeral_v2"
	ephemeralLegacyCapability = "worker_data_ephemeral_v1"
	// Older v2 workers bypassed all shared staging for ephemeral attempts,
	// including the declared model snapshot bindings.
	ephemeralSnapshotsCapability = "worker_data_ephemeral_model_snapshots_v1"
)

// Only bounded codes cross the inventory boundary. Launch errors stay local
// to the attempt's existing diagnostic path.
type ephemeralRefusal string

const ephemeralRefusalHeader = "X-Myce-Ephemeral-Refusal"

const (
	ephemeralLinuxRequired        ephemeralRefusal = "linux_required"
	ephemeralNamespaceRequired    ephemeralRefusal = "namespaces_unqualified"
	ephemeralNamespacePolicy      ephemeralRefusal = "namespace_policy_denied"
	ephemeralCoreRouteUnqualified ephemeralRefusal = "core_route_unqualified"
	ephemeralDetachedUnqualified  ephemeralRefusal = "detached_core_route_unqualified"
	ephemeralCoreLimitUnqualified ephemeralRefusal = "core_limit_unqualified"
)

func safeEphemeralRefusal(r ephemeralRefusal) ephemeralRefusal {
	switch r {
	case ephemeralLinuxRequired, ephemeralNamespaceRequired, ephemeralNamespacePolicy, ephemeralCoreRouteUnqualified, ephemeralDetachedUnqualified, ephemeralCoreLimitUnqualified:
		return r
	}
	return ""
}

func (s Spec) ephemeral() bool { return s.WorkerData == "ephemeral" }

func (s Spec) ephemeralRequiredCapability() string {
	if len(s.ModelSnapshots) != 0 {
		return ephemeralSnapshotsCapability
	}
	return ephemeralCapability
}

func ephemeralWorkerRefusal(s Spec, m *Machine) string {
	if !contains(m.Capabilities, ephemeralCapability) {
		return ephemeralHostReason(m)
	}
	if len(s.ModelSnapshots) != 0 && !contains(m.Capabilities, ephemeralSnapshotsCapability) {
		return "the worker release must support declared model snapshots with ephemeral worker data"
	}
	return ""
}

func workerDataFits(s Spec, m *Machine) bool {
	return !s.ephemeral() || ephemeralWorkerRefusal(s, m) == ""
}

// The policy opts into the ci boundary, including extension-owned executions.
func validateWorkerData(s Spec) error {
	if s.WorkerData == "" {
		return nil
	}
	if !s.ephemeral() {
		return errors.New("advanced.worker_data must be ephemeral or omitted")
	}
	if !s.extensionJob() && s.SizeHint != (SizeHint{}) {
		return errors.New("ephemeral plain commands use needs, not extension size_hint")
	}
	if s.Resumable || s.ResumeCompatibility != nil {
		return errors.New("ephemeral worker data does not support resumable jobs or resume compatibility")
	}
	if s.Program != nil || s.ExtensionCheck != nil {
		return errors.New("ephemeral worker data does not cover program usage files or extension-check publication")
	}
	if s.OutputDelivery != "" || s.Diagnostics != nil || len(s.KeepOnFailure) != 0 {
		return errors.New("ephemeral worker data does not cover alternate output delivery, diagnostics or keep_on_failure")
	}
	if !s.extensionJob() && len(s.Args) != 0 {
		return errors.New("ephemeral plain commands put arguments in run.command")
	}
	if s.Isolation != "" && s.Isolation != nativeIsolationCI {
		return errors.New("ephemeral worker data requires ci isolation (selected automatically when omitted)")
	}
	if err := validateEphemeralExecution(s); err != nil {
		return err
	}
	if len(s.Outputs) == 0 {
		return errors.New("ephemeral commands require declared outputs")
	}
	// Plain commands historically rejected NAS-path inputs before reaching
	// staging. Apply the same containment and collision checks here explicitly.
	paths := maps.Clone(s.Files)
	if paths == nil {
		paths = FileManifest{}
	}
	for name, source := range s.Inputs {
		if !safeRelative(name) || !safeRelative(source) {
			return errors.New("ephemeral inputs require clean relative local names and NAS paths")
		}
		if _, exists := paths[name]; exists {
			return errors.New("ephemeral input names overlap")
		}
		paths[name] = FileRef{SHA256: strings.Repeat("0", 64)}
	}
	for name := range s.Outputs {
		if kind, _ := outputKind(name); kind != outputFile {
			continue // Pattern collection validates its actual matches separately.
		}
		if _, exists := paths[name]; exists {
			return errors.New("ephemeral input and output names overlap")
		}
		paths[name] = FileRef{SHA256: strings.Repeat("0", 64)}
	}
	if err := paths.validate(); err != nil {
		return err
	}
	return nil
}

// Check both authored commands and frozen extension lowering before data staging.
// Environment builders receive only software recipes, never attempt work/data;
// payloads can read their caches but cannot write them under the ci boundary.
func validateEphemeralExecution(s Spec) error {
	if !s.ephemeral() {
		return nil
	}
	if s.Needs.Network {
		return errors.New("ephemeral commands currently require offline execution")
	}
	if s.Needs.Model != "" {
		return errors.New("ephemeral worker data does not cover model servers: shared server memory and request spools are outside attempt cleanup")
	}
	if s.Run.Workdir != "" && s.Run.Workdir != "{work}" || s.Execution != nil && s.Execution.Main.Workdir != "" && s.Execution.Main.Workdir != "{work}" {
		return errors.New("ephemeral worker data requires the managed {work} directory, including extension main and sidecars")
	}
	return nil
}

func validateEphemeralExtensions(s Spec, extensions []Extension) error {
	if !s.ephemeral() {
		return nil
	}
	for _, e := range extensions {
		for _, v := range e.Variants {
			candidate := s
			candidate.Needs = v.Needs
			candidate.Run = RunSpec{}
			candidate.Execution = &v.Execution
			if err := validateEphemeralExecution(candidate); err != nil {
				return fmt.Errorf("extension %s: %w", extensionKey(e), err)
			}
		}
	}
	return nil
}

func ephemeralRoot(spool string, j Job) string {
	return filepath.Join(spool, "ephemeral", j.ID, strconv.Itoa(j.Attempt)+"-"+identityHash(j.Session))
}
func ephemeralLogName(j Job) string {
	return filepath.Join("ephemeral-logs", j.ID+"-"+strconv.Itoa(j.Attempt)+"-"+identityHash(j.Session)+".log")
}
func (w Worker) checkEphemeralSupport() (ephemeralRefusal, error) {
	if w.ephemeralProbe != nil {
		return "", w.ephemeralProbe()
	}
	return w.ephemeralWorkerSupport()
}

// No lookup, pin or insertion in either persistent payload cache, even on a
// warm hit. Interrupted copies and extraction stages live under this attempt.
func (w Worker) copyAttemptInput(ctx context.Context, j Job, ref FileRef, dest string) error {
	if !j.Spec.ephemeral() {
		return w.fileCache.copyTo(ctx, ref, dest)
	}
	release, err := w.diskBudget.reserve(ctx, filepath.Dir(dest), uint64(ref.SizeBytes), 0)
	if err != nil {
		return err
	}
	defer release()
	return w.Client.recoverInputTransfer(ctx, ref, "private_input", func(one Client) error { return one.DownloadFile(ctx, ref, dest) })
}
func (w Worker) acquireEphemeralInputs(ctx context.Context, j Job) (map[string]string, error) {
	env := map[string]string{}
	owner, ok := ctx.Value(sharedInputOwnerKey{}).(*workerStore)
	if !ok {
		return nil, errors.New("private inputs require exact attempt custody")
	}
	root := ephemeralRoot(owner.dir, j)
	// StateDir is canonical in the running worker. No shared cache methods run.
	for name, input := range j.Spec.SharedInputs {
		stage := filepath.Join(root, "shared", name)
		ready, err := completedSharedStage(ctx, stage)
		if err != nil {
			return nil, err
		}
		if ready {
			env["MYCE_INPUT_"+strings.ToUpper(name)] = filepath.Join(stage, "data")
			continue
		}
		// No detached writer or payload owns this private stage yet. Reset an
		// interrupted extraction rather than copying over a read-only partial tree.
		if err := removeSharedEntry(stage); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(stage, 0700); err != nil {
			return nil, err
		}
		c := SharedInputCache{Client: w.Client}
		bytes := uint64(input.bytes())
		if input.Archive != "" {
			bytes = addDiskBytes(bytes, uint64(input.SizeBytes))
		}
		release, err := w.diskBudget.reserve(ctx, stage, bytes, 0)
		if err != nil {
			return nil, err
		}
		err = c.populateInput(ctx, input, w.CheckpointRoot, stage)
		release()
		if err != nil {
			return nil, err
		}
		env["MYCE_INPUT_"+strings.ToUpper(name)] = filepath.Join(stage, "data")
	}
	return env, nil
}

// Paths are relative to the worker spool, never private host paths. Bytes are
// logical file lengths unlinked, not a claim about physical disk reclamation.
type WorkerDataRemoval struct {
	Path    string `json:"path"`
	Bytes   int64  `json:"bytes"`
	Removed bool   `json:"removed"`
	Error   string `json:"error,omitempty"`
}
type WorkerDataCleanup struct {
	Execution
	Machine        string              `json:"machine"`
	Session        string              `json:"session"`
	State          string              `json:"state"` // transfer_pending, cleanup_pending, failed, complete
	ResultAccepted bool                `json:"result_accepted"`
	CheckedAt      time.Time           `json:"checked_at"`
	Paths          []WorkerDataRemoval `json:"paths,omitempty"`
}

// The filesystem seam exercises failed unlink, missing-path and sync behavior
// with the same state machine as the real filesystem. No I/O runs under q.mu.
type workerDataFS interface {
	Lstat(string) (fs.FileInfo, error)
	WalkDir(string, fs.WalkDirFunc) error
	RemoveAll(string) error
	SyncDir(string) error
}
type osWorkerDataFS struct{}

func (osWorkerDataFS) Lstat(p string) (fs.FileInfo, error)      { return os.Lstat(p) }
func (osWorkerDataFS) WalkDir(p string, f fs.WalkDirFunc) error { return filepath.WalkDir(p, f) }
func (osWorkerDataFS) RemoveAll(p string) error                 { return removeSharedEntry(p) }
func (osWorkerDataFS) SyncDir(p string) error                   { return syncWorkerDir(p) }

func (w Worker) removeEphemeralPaths(s *workerStore, logs bool) error {
	j := *s.state.Job
	receipt := s.state.WorkerDataCleanup
	if receipt == nil {
		receipt = &WorkerDataCleanup{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session}
	}
	receipt = cloneCleanup(receipt)
	receipt.ResultAccepted = s.state.WorkerDataAccepted
	receipt.CheckedAt = w.Clock.Now()
	receipt.State = "cleanup_pending"
	paths := []string{ephemeralRoot(s.dir, j)}
	if logs {
		paths = append(paths, filepath.Join(s.dir, ephemeralLogName(j)))
	}
	fsys := w.cleanupFS
	if fsys == nil {
		fsys = osWorkerDataFS{}
	}
	var failure error
	for _, path := range paths {
		rel, err := filepath.Rel(s.dir, path)
		if err != nil || !filepath.IsLocal(rel) {
			return errors.New("invalid ephemeral cleanup root")
		}
		index := slices.IndexFunc(receipt.Paths, func(p WorkerDataRemoval) bool { return p.Path == filepath.ToSlash(rel) })
		if index < 0 {
			receipt.Paths = append(receipt.Paths, WorkerDataRemoval{Path: filepath.ToSlash(rel)})
			index = len(receipt.Paths) - 1
		}
		item := &receipt.Paths[index]
		// Preserve the original byte census over retries and crashes before unlink.
		var bytes int64
		err = fsys.WalkDir(path, func(_ string, d fs.DirEntry, e error) error {
			if errors.Is(e, os.ErrNotExist) {
				return nil
			}
			if e != nil {
				return e
			}
			if !d.IsDir() {
				info, e := d.Info()
				if e != nil {
					return e
				}
				bytes += info.Size()
			}
			return nil
		})
		item.Bytes = max(item.Bytes, bytes)
		item.Removed, item.Error = false, ""
		if err != nil {
			item.Error = "inventory_failed"
			failure = errors.Join(failure, err)
			continue
		}
		// Persist the inventory first. A restart after unlink still records its bytes.
		s.state.WorkerDataCleanup = receipt
		if err = s.save(); err != nil {
			return err
		}
		err = fsys.RemoveAll(path)
		if err == nil {
			_, e := fsys.Lstat(path)
			if !errors.Is(e, os.ErrNotExist) {
				err = errors.New("path still present or absence unverifiable")
			}
		}
		if err == nil {
			// Sync the closest existing parent (also handles an unstarted attempt).
			parent := filepath.Dir(path)
			for {
				_, e := fsys.Lstat(parent)
				if !errors.Is(e, os.ErrNotExist) {
					break
				}
				next := filepath.Dir(parent)
				if next == parent {
					break
				}
				parent = next
			}
			err = fsys.SyncDir(parent)
		}
		if err != nil {
			item.Error = "removal_or_sync_failed"
			failure = errors.Join(failure, err)
		} else {
			item.Removed = true
		}
	}
	if failure != nil {
		receipt.State = "failed"
	} else if logs && receipt.ResultAccepted {
		receipt.State = "complete"
	}
	s.state.WorkerDataCleanup = receipt
	if s.state.Result != nil {
		s.state.Result.WorkerDataCleanup = receipt
	}
	return errors.Join(failure, s.save())
}
func cloneCleanup(c *WorkerDataCleanup) *WorkerDataCleanup {
	if c == nil {
		return nil
	}
	v := *c
	v.Paths = slices.Clone(c.Paths)
	return &v
}

// Result acceptance and physical release are separate durable facts. Successful
// output bytes survive queue/transfer failure, including lost acceptance replies.
// On failure/restart, inputs are disposable after process/writer custody ends;
// logs stay until delivery. Recovery calls this same idempotent state machine.
func (w Worker) finishEphemeralAttempt(ctx context.Context, s *workerStore, connected bool) (bool, error) {
	j, r := *s.state.Job, *s.state.Result
	if !attemptRecordFor(&j, w.Machine.ID).ownsResult(&r) {
		return false, errors.New("mismatched result; ephemeral data retained")
	}
	if r.ReleasePending || s.extensionFiles.inFlight() {
		return false, errors.New("attempt writers still held")
	}
	if !r.Success || r.Interrupted {
		if err := w.removeEphemeralPaths(s, false); err != nil {
			if connected {
				pending := r
				pending.ReleasePending = true
				pending.WorkerDataCleanup = s.state.WorkerDataCleanup
				_ = w.Client.Call(ctx, "POST", "/api/workers/complete", pending, nil)
			}
			return false, err
		}
	}
	if !connected {
		return false, nil
	}
	if !s.state.WorkerDataAccepted {
		if err := w.replay(ctx, s); err != nil {
			return false, err
		}
		var err error
		if r.Success && !r.Interrupted {
			value := j
			value.Spec.Run.Workdir = attemptStorageRoot(j, s.dir)
			r, err = w.finishFileResultContext(ctx, value, r, s.dir)
			if err != nil && !stale(err) {
				return false, err
			}
		}
		pending := r
		pending.ReleasePending = true
		pending.WorkerDataCleanup = &WorkerDataCleanup{Execution: r.Execution, Machine: r.Machine, Session: r.Session, State: "transfer_pending", CheckedAt: w.Clock.Now()}
		if err = w.Client.Call(ctx, "POST", "/api/workers/complete", pending, nil); err != nil && !stale(err) {
			return false, err
		}
		// A generic completion acknowledgement can settle a stale copy without
		// accepting its outputs. Inspect the exact outcome before destroying bytes.
		var accepted Job
		if err = w.Client.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &accepted); err != nil {
			return false, err
		}
		confirmed := false
		for _, h := range accepted.AttemptHistory {
			if h.Attempt == j.Attempt && h.Machine == r.Machine && h.Session == r.Session {
				confirmed = ephemeralOutcomeAccepted(h)
				if h.State == "succeeded" {
					confirmed = accepted.Attempt == j.Attempt && accepted.Machine == r.Machine && accepted.Session == r.Session && maps.Equal(accepted.OutputRefs, r.OutputRefs)
				}
			}
		}
		if !confirmed {
			return false, errors.New("exact result acceptance is unconfirmed; local bytes retained")
		}
		prior := s.state.Result
		s.state.Result = &r
		s.state.WorkerDataAccepted = true
		if err = s.save(); err != nil {
			s.state.Result = prior
			s.state.WorkerDataAccepted = false
			return false, err
		}
	}
	if err := w.removeEphemeralPaths(s, true); err != nil {
		pending := *s.state.Result
		pending.ReleasePending = true
		pending.WorkerDataCleanup = s.state.WorkerDataCleanup
		_ = w.Client.Call(ctx, "POST", "/api/workers/complete", pending, nil)
		return false, err
	}
	if err := s.releaseAttemptCachePins(); err != nil {
		return false, err
	}
	released := *s.state.Result
	released.ReleaseOnly = true
	released.WorkerDataCleanup = s.state.WorkerDataCleanup
	released.OutputRefs = nil
	if err := w.Client.Call(ctx, "POST", "/api/workers/complete", released, nil); err != nil {
		return false, err
	}
	s.state.Job, s.state.Result = nil, nil
	s.state.PID, s.state.LaunchPending = 0, false
	s.state.Log = ""
	return true, s.save()
}

func cleanupComplete(c *WorkerDataCleanup) bool {
	if c == nil || c.State != "complete" || !c.ResultAccepted || len(c.Paths) != 2 {
		return false
	}
	for _, p := range c.Paths {
		if !p.Removed || p.Error != "" || p.Bytes < 0 {
			return false
		}
	}
	return true
}

func ephemeralOutcomeAccepted(h AttemptOutcome) bool {
	// A pressure stop retains the stopping state through exact cleanup.
	return h.State == "queued" || terminal(h.State) ||
		h.State == "stopping" && h.PriorityStop != nil && h.PriorityStop.Cause == memoryPressureCause
}

// Receipts bind the exact copy, including superseded executions. Never let a
// late cleanup receipt change a newer attempt's outcome or certify its data.
func (q *Queue) retainWorkerDataCleanup(r Result) error {
	j := q.retainedJob(r.ID)
	if j == nil || !j.Spec.ephemeral() {
		return nil
	}
	c := r.WorkerDataCleanup
	if !r.ReleasePending && !cleanupComplete(c) {
		return reject("worker_data_cleanup_pending", nil)
	}
	if c == nil {
		return nil
	}
	if c.Execution != r.Execution || c.Machine != r.Machine || c.Session != r.Session {
		return ErrStale
	}
	known := j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session
	for _, copy := range j.Copies {
		known = known || copy.Attempt == r.Attempt && copy.Machine == r.Machine && copy.Session == r.Session
	}
	if !known {
		return ErrStale
	}
	// Validate the tracked roots rather than accepting an empty/partial receipt.
	if cleanupComplete(c) {
		accepted := false
		for _, h := range j.AttemptHistory {
			accepted = accepted || h.Attempt == r.Attempt && h.Machine == r.Machine && h.Session == r.Session && ephemeralOutcomeAccepted(h)
		}
		if !r.ReleaseOnly || !accepted {
			return reject("worker_data_cleanup_pending", errors.New("exact outcome must be durably accepted before cleanup release"))
		}
		expected := []string{filepath.ToSlash(strings.TrimPrefix(ephemeralRoot("", Job{ID: r.ID, Attempt: r.Attempt, Session: r.Session}), "./")), filepath.ToSlash(ephemeralLogName(Job{ID: r.ID, Attempt: r.Attempt, Session: r.Session}))}
		actual := []string{c.Paths[0].Path, c.Paths[1].Path}
		slices.Sort(actual)
		slices.Sort(expected)
		if !slices.Equal(actual, expected) {
			return fmt.Errorf("invalid ephemeral removal receipt roots")
		}
	}
	c = cloneCleanup(c)
	if j.WorkerDataCleanup == nil || j.WorkerDataCleanup.Attempt <= r.Attempt {
		if j.WorkerDataCleanup == nil || !cleanupComplete(j.WorkerDataCleanup) || j.WorkerDataCleanup.Attempt < r.Attempt {
			j.WorkerDataCleanup = c
		}
	}
	for i := range j.AttemptHistory {
		h := &j.AttemptHistory[i]
		if h.Attempt == r.Attempt && h.Machine == r.Machine && h.Session == r.Session && !cleanupComplete(h.WorkerDataCleanup) {
			h.WorkerDataCleanup = c
		}
	}
	return nil
}
