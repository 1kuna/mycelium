package myce

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Anonymous, kernel and shmem pages charged to one cgroup subtree.
func cgroupResidentGB(fs memoryFS, path string) (float64, bool) {
	b, err := fs.ReadFile(filepath.Join(path, "memory.stat"))
	if err != nil {
		return 0, false
	}
	stats := memoryCounters(b)
	anon, aok := stats["anon"]
	kernel, kok := stats["kernel"]
	shmem, sok := stats["shmem"]
	return (float64(anon) + float64(kernel) + float64(shmem)) / (1 << 30), aok && kok && sok
}

func (m *availabilityMonitor) sample(s OwnerSignals) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = m.policy.evaluate(s)
	if !m.current.Available {
		m.cancel()
	} else if m.work.Err() != nil {
		m.work, m.cancel = context.WithCancel(context.Background())
	}
}

func (n *checkpointAvailability) run(key string, fn func() (bool, error)) (bool, error) {
	return n.runProgress(key, func(func()) (bool, error) { return fn() })
}

func deviceTelemetryReady(s *DeviceTelemetry, now time.Time) bool {
	return deviceTelemetryFresh(s, now) && s.FreeGB != nil && s.ActivePercent != nil
}

// Test/explicit maintenance entry point; ordinary Tick shares its existing commit.
func (q *Queue) updateEfficiencyDiagnostics(ctx context.Context) error {
	d, err := q.prepareEfficiencyDiagnostics(ctx)
	if err != nil {
		return err
	}
	err = q.updateRequest(ctx, func() error { q.applyEfficiencyDiagnostics(d); return nil }, nil)
	if err == nil {
		q.publishEfficiencyDiagnostics(d)
	}
	return err
}

func EvaluateEnrollment(receipt EnrollmentReceipt, o EnrollmentObservation) (string, error) {
	return evaluateEnrollmentVersion(receipt, o, BuildVersion)
}

func snapshotEnvironment(r EnvironmentRecipe, platform string) (EnvironmentRecipe, error) {
	return snapshotEnvironmentFiles(r, platform, "queue-working-directory", "", nil)
}

func (q *Queue) resolveEnvironment(e Environment) (map[string]EnvironmentRecipe, error) {
	return q.environments.resolveEnvironment(e)
}

// SetQueueHostMemorySource supplies the queue process's own host-memory probe
// during embedded process setup. It must not run concurrently with queue reads.
// Worker inventory overrides do not change this source.
func SetQueueHostMemorySource(source func(float64) (MemorySource, error)) {
	queueHostMemorySource = source
}

func (w Worker) prepareExtensionFiles(j *Job, stateDir string) error {
	return w.prepareExtensionFilesContext(context.Background(), j, stateDir)
}

func (q *Queue) pick(m *Machine, scans ...*placementScan) *Job {
	return q.immediateCandidate(q.candidate(m, nil, scans...), m)
}

func (q *Queue) fanOutCandidate(j *Job, m *Machine) *Job {
	return q.fanOutCandidateWithResources(j, m, nil)
}

func (q *Queue) fileStoreStatus(ctx context.Context) *FileStoreStatus {
	if err := lockUploadContext(ctx, q.mu.RLock, q.mu.RUnlock); err != nil {
		return &FileStoreStatus{Error: "file store capacity unavailable"}
	}
	view := q.captureFileStoreStatus()
	q.mu.RUnlock()
	return view.status(ctx)
}

func (w Worker) finishFileResult(j Job, result Result, parent string, metadata ...string) (finished Result, err error) {
	return w.finishFileResultContext(context.Background(), j, result, parent, metadata...)
}

func (q *Queue) reclaimJobFiles() error { return q.reclaimJobFilesContext(context.Background()) }

func (q *Queue) reclaimPublishedFiles() error {
	return q.reclaimPublishedFilesContext(context.Background())
}

func setUploadHeaders(req *http.Request, ref FileRef) {
	req.Header.Set("Upload-Length", strconv.FormatInt(ref.SizeBytes, 10))
	req.Header.Set("Upload-Metadata", "sha256 "+base64.StdEncoding.EncodeToString([]byte(ref.SHA256)))
}

func pollFleetRuntime(ctx context.Context, config ServiceConfig, client Client, collect func(context.Context, ServiceConfig, string) (*FleetRuntimeProof, error), launch func(context.Context, ServiceConfig, FleetUpdateInstruction) error, host, machine, session string, eligible bool, attempted string, failed *fleetLaunchFailure, handled ...string) fleetPollResult {
	prior := fleetPollResult{attempted: attempted, failed: failed}
	if len(handled) > 0 {
		prior.failureReceipt = handled[0]
	}
	return pollFleetRuntimeAt(ctx, config, client, collect, launch, host, machine, session, eligible, prior, RealClock{})
}

func readGPUTelemetryHelper(ctx context.Context, clock Clock, path string) (map[string]string, error) {
	s, err := readGPUTelemetrySnapshot(ctx, clock, path)
	if err != nil {
		return nil, err
	}
	return s.GTTMM, nil
}

func (h Hold) operatorAction() bool { return h.Owner == ownerOperator }

func (m *hostHealthMonitor) sample(ctx context.Context) error {
	at := m.clock.Now()
	r, err := m.source(ctx)
	return m.record(ctx, at, r, err, nil)
}

// Inventory is an observation lane, not an attempt sampler. No per-host driver
// latency default kills a progressing scan; cancellation owns its lifetime.
// A pending scan retains identity but never indefinitely retains live capacity.
func sampleHostInventory(ctx context.Context, lane *hostProbeLane[[]MachineDevice], source func(context.Context) ([]MachineDevice, error)) ([]MachineDevice, error) {
	lane.equal = sameInventoryObservation
	lane.step(ctx, source)
	return hostInventorySnapshot(lane)
}

func (m *hostInventoryObserver) snapshot() ([]MachineDevice, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func currentHostMemorySamples(reports []AttemptReport, now time.Time) map[attemptKey]hostMemorySample {
	return updateHostMemorySamples(reports, now, nil)
}

func (m *hostInventoryObserver) observation() *InventoryObservation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observationLocked()
}

// workerRetention is one pass of the worker's periodic local retention.
func workerRetention(dir string, now time.Time, logAge time.Duration) error {
	return errors.Join(pruneWorkerLogs(dir, "", now, logAge), pruneAttemptFiles(dir, now), pruneAttemptState(dir, now, logAge))
}

// Limits remain per execution. Attribution includes detached siblings and the
// worker's engines; subtracting only one scope would blame it for a neighbor.
func (a *memoryAttempt) accountingPaths() []string {
	if len(a.accountingRoots) > 0 {
		return a.accountingRoots
	}
	return []string{filepath.Dir(a.path)}
}

func (a *memoryAttempt) accountingResidentGB() (float64, bool) {
	total := 0.0
	for _, path := range a.accountingPaths() {
		v, ok := cgroupResidentGB(a.fs, path)
		if !ok {
			return 0, false
		}
		total += v
	}
	return total, true
}

func (a *memoryAttempt) accountingProcessIDs(ids map[int]bool) error {
	for _, path := range a.accountingPaths() {
		if err := (&memoryAttempt{fs: a.fs, path: path}).processIDs(ids); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) finishCall(id string, requestContexts ...context.Context) error {
	return q.finishCallWithMemory(id, nil, requestContext(requestContexts))
}

func (p *queuePressure) pressure() (bool, time.Duration) {
	pressure, delay, _ := p.measurement()
	return pressure, delay
}

func probeNASDirectory(dir string) error {
	ctx, abort := context.WithCancelCause(context.Background())
	defer abort(nil)
	return probeNASDirectoryContext(ctx, abort, dir)
}

// BootstrapEnvironmentTools installs only into this user's Mycelium tool tree.
// Existing suitable service tools and verified pinned executables are reused.
// User-managed uv/pixi are never overwritten; acquired paths are appended.
func BootstrapEnvironmentTools(ctx context.Context, c ServiceConfig, p ToolBootstrapProbe) (ServiceConfig, error) {
	releases, err := PinnedEnvironmentTools(c.Detection.OS, c.Detection.Arch)
	if err != nil {
		return c, err
	}
	if !filepath.IsAbs(c.WorkerHome) || p.Command == nil || p.Fetch == nil {
		return c, preflightError("tool_bootstrap_unavailable", "tools", "user home and tool probes are required")
	}
	return bootstrapEnvironmentTools(ctx, c, p, releases)
}

func (q *Queue) touchCurveLabel(id string) {
	if q.records != nil && q.records.updating {
		if _, touched := q.records.staleness[id]; !touched {
			q.records.staleness[id] = q.s.WorkObservations[id].StaleReason
		}
	}
}

// beginCheckin records a check-in before it waits for q.mu. The returned
// settle is idempotent. Only the last accepted current session gets pending
// credit. Rejection removes its credit; cancellation grants only a measured
// reconnect opportunity, never refreshing accepted LastSeen. An abandoned arrival
// cannot hide silence after the queue has had enough service opportunities.
func (q *Queue) beginWorkerCheckin(machine, session string, period time.Duration, active bool, contexts ...context.Context) func(accepted bool) {
	_, settle := q.trackWorkerCheckin(machine, session, period, active, contexts...)
	return settle
}

func readQueueState(db queueRecordReader, data []byte) ([]byte, error) {
	if !bytes.Equal(data, normalizedQueueMarker) {
		return nil, fmt.Errorf("unsupported queue persistence format; current and previous releases require normalized-state-v3")
	}
	records, err := readQueueRecords(db)
	if err != nil {
		return nil, err
	}
	var base map[string]json.RawMessage
	groups := map[string]map[string]json.RawMessage{"jobs": {}, "submissions": {}, "extensions": {}, "work_observations": {}, "storage_observations": {}, "external_memory": {}, "health_events": {}, "machines": {}}
	for key, value := range records {
		if key == "state" {
			if err = json.Unmarshal(value, &base); err != nil {
				return nil, err
			}
			continue
		}
		group, id, ok := strings.Cut(key, "/")
		if !ok || groups[group] == nil {
			return nil, fmt.Errorf("unknown queue state fragment")
		}
		groups[group][id] = value
	}
	if base == nil {
		return nil, fmt.Errorf("queue state fragment missing")
	}
	if events := groups["health_events"]; len(events) > 0 {
		var health map[string]json.RawMessage
		if err := json.Unmarshal(base["health_alerts"], &health); err != nil {
			return nil, err
		}
		ordered := make([]json.RawMessage, len(events))
		for key, value := range events {
			index, e := strconv.Atoi(key)
			if e != nil || index < 0 || index >= len(ordered) {
				return nil, fmt.Errorf("invalid health evidence index")
			}
			ordered[index] = value
		}
		health["events"], err = json.Marshal(ordered)
		if err != nil {
			return nil, err
		}
		base["health_alerts"], err = json.Marshal(health)
		if err != nil {
			return nil, err
		}
	}
	for id, data := range groups["jobs"] {
		if groups["jobs"][id], err = expandJobRecord(data); err != nil {
			return nil, err
		}
	}
	// Live keyed answers reference the same committed job instead of duplicating
	// its payload on every check-in. Removed jobs keep their full retained answer.
	for id, data := range groups["submissions"] {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, err
		}
		if record == nil || len(record["job"]) > 0 {
			continue
		}
		var jobID string
		if err := json.Unmarshal(record["job_id"], &jobID); err != nil {
			continue
		}
		if job := groups["jobs"][jobID]; job != nil {
			record["job"] = job
			encoded, err := json.Marshal(record)
			if err != nil {
				return nil, err
			}
			groups["submissions"][id] = encoded
		}
	}
	delete(groups, "health_events")
	for key, group := range groups {
		if len(group) == 0 && key != "jobs" && key != "machines" {
			continue
		}
		base[key], err = json.Marshal(group)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(base)
}

// Reassemble logical records for startup/recovery, outside ordinary writes.
func readQueueRecords(db queueRecordReader, only ...string) (map[string][]byte, error) {
	return readQueueRecordsVisit(db, nil, only...)
}

// Active independent inputs retain s055's concurrency. Once a source is
// withdrawn, refuse new source operations until its private writer joins. This
// bounds detached work by the attempts already admitted, without serializing I/O.
// Publication belongs only to the caller; no process runs in a detached writer.
func (c *SharedInputCache) stageInput(parent context.Context, populate func(context.Context, string) error, publish func(string) error) error {
	return c.stageInputNamed(parent, "", populate, publish)
}

func (c Client) inspectionSnapshot(ctx context.Context) (Snapshot, error) {
	var out Snapshot
	err := c.Call(ctx, "GET", "/api/inspection", nil, &out)
	return out, err
}

func (q *Queue) jobSnapshotGeneration(id string, contexts ...context.Context) (*Job, error) {
	j, _, err := q.jobSnapshotCapture(id, contexts...)
	return j, err
}

func (s *waitStateIndex) read(request WaitRequest) ([]WaitState, <-chan struct{}, bool, error) {
	return s.readContext(context.Background(), request)
}

func (s *waitStateIndex) wait(ctx context.Context, request WaitRequest) (WaitReply, error) {
	return s.waitFor(ctx, request, time.Duration(request.Seconds)*time.Second)
}
