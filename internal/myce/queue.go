package myce

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const LossAfter = 60 * time.Second
const ProductionLossAfter = 5 * time.Second
const DeadlineLossAfter = 20 * time.Second
const RestartGrace = 3 * time.Minute
const StopGrace = 2 * time.Minute

var ErrStale = errors.New("stale execution: stop this copy")

// A stop acknowledgement can precede the worker's free-memory report. This
// credit is only for release planning, never for assigning work.
type batchRelease struct {
	Session  string    `json:"session"`
	MemoryGB float64   `json:"memory_gb"`
	Until    time.Time `json:"until"`
}

type state struct {
	WorkObservations  map[string]WorkObservation `json:"work_observations,omitempty"`
	EnrollmentRelease *EnrollmentRelease         `json:"enrollment_release,omitempty"`
	MachineConversion *MachineConversion         `json:"machine_conversion,omitempty"`
	ProductionDrains  map[string]productionDrain `json:"production_drains,omitempty"`

	PriorityPolicy *PriorityPolicyRecord `json:"priority_policy,omitempty"`
	// Actual fleet contact and the first reconnect after silence are separate
	// from each worker's LastSeen. Reconnects never renew an active outage epoch.
	LastWorkerContact time.Time `json:"last_worker_contact,omitempty"`
	NetworkRecoveryAt time.Time `json:"network_recovery_at,omitempty"`

	Schema           int                        `json:"schema_version"`
	PreparationPlans map[string]HostLaunch      `json:"preparation_plans,omitempty"`
	HostMaintenance  map[string]HostMaintenance `json:"host_maintenance,omitempty"`

	Submissions      map[string]*submissionRecord `json:"submissions,omitempty"`
	ProgramNeeds     map[string]SizeHint          `json:"program_needs,omitempty"`
	Extensions       map[string]Extension         `json:"extensions,omitempty"`
	Hosts            map[string]*HostState        `json:"hosts,omitempty"`
	HostReleases     map[string]HostRelease       `json:"host_releases,omitempty"`
	BatchReleases    map[string]batchRelease      `json:"batch_releases,omitempty"`
	UsageMaintenance time.Time                    `json:"usage_maintenance,omitempty"`
	ProductionStarts []ProductionStart            `json:"production_starts,omitempty"`
	Policies         map[string]ProjectPolicy     `json:"project_policies,omitempty"`
	FairUsageVersion int                          `json:"fair_usage_version,omitempty"`
	FairUsage        map[string]FairUsage         `json:"fair_usage,omitempty"`
	UsageAt          time.Time                    `json:"usage_at,omitempty"`
	Preemptions      []Preemption                 `json:"preemptions,omitempty"`

	SyncRequests []SyncRequest         `json:"sync_requests,omitempty"`
	ModelUses    map[string]ModelUse   `json:"model_uses,omitempty"`
	Pins         []ModelPin            `json:"pins,omitempty"`
	ModelSlots   map[string]*ModelSlot `json:"model_slots,omitempty"`
	Jobs         map[string]*Job       `json:"jobs"`
	Machines     map[string]*Machine   `json:"machines"`
	Front        int64                 `json:"front"`
}
type Queue struct {
	onboardingNAS                   NASIdentity
	productionInfrastructureRetries int

	files                  *FileStore
	fileCheckpoints        bool
	submitVerificationSlot chan struct{}
	nasProbe               checkpointIO[bool]
	nasProgress            checkpointIO[int64]
	nasAvailable           bool

	nextApprovalWarning time.Time

	// simulation bypasses durable storage only; all scheduling/accounting paths remain shared.
	checkpointProbe  func() bool // Availability seam; bounded real I/O is the default.
	simulation       bool
	simID            int64
	uploadSpool      uploadSpool
	logRetention     time.Duration
	nextLogCleanup   time.Time
	environments     EnvironmentCatalog
	productionGrace  time.Duration
	wake             chan struct{}
	models           ModelConfig
	calls            map[string]*ModelCall
	lock             *os.File
	mu               sync.RWMutex
	db               *sql.DB
	persisted        map[string][]byte
	clock            Clock
	root             string
	restartUntil     time.Time
	unreported       map[string]bool
	networkQuiet     bool
	lossTimeouts     [3]time.Duration
	usageWrites      []usageSample
	usageMaintenance bool
	s                state
}

func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// QueueOption configures a queue before its startup transactions run.
type QueueOption func(*Queue)

// WithCheckpointAvailability substitutes an availability probe for synthetic
// queues. A nil probe retains bounded real checkpoint I/O. The probe must be
// safe for concurrent calls.
func WithCheckpointAvailability(probe func() bool) QueueOption {
	return func(q *Queue) { q.checkpointProbe = probe }
}

func Open(path, root string, clock Clock, options ...QueueOption) (*Queue, error) {
	if clock == nil {
		return nil, errors.New("clock is required")
	}
	if err := PrepareStateRoots(filepath.Dir(path), path+".uploads"); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("queue database already in use: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
			_ = lock.Close()
		}
	}()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	q := &Queue{logRetention: DefaultLogRetention, uploadSpool: uploadSpool{limit: DefaultUploadSpoolBytes, defaultDir: path + ".uploads"}, productionGrace: 10 * time.Second, productionInfrastructureRetries: DefaultProductionInfrastructureRetries, wake: make(chan struct{}), lossTimeouts: [3]time.Duration{ProductionLossAfter, DeadlineLossAfter, LossAfter}, lock: lock, db: db, clock: clock, root: abs, s: state{Schema: persistedSchema, Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	q.nasProbe.timeout = heartbeatFilesystemBudget
	q.nasProgress.timeout = heartbeatFilesystemBudget
	for _, option := range options {
		option(q)
	}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS enrollment_tickets (digest TEXT PRIMARY KEY, pin TEXT NOT NULL, expires INTEGER NOT NULL, used INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS logs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT NOT NULL, attempt INTEGER NOT NULL, text TEXT NOT NULL); CREATE TABLE IF NOT EXISTS log_offsets (job TEXT, attempt INTEGER, offset INTEGER, PRIMARY KEY(job,attempt,offset)); CREATE TABLE IF NOT EXISTS environment_log_offsets (job TEXT, machine TEXT, session TEXT, key TEXT, offset INTEGER, PRIMARY KEY(job,machine,session,key,offset)); CREATE TABLE IF NOT EXISTS environment_build_logs (machine TEXT, session TEXT, key TEXT, offset INTEGER, text TEXT, PRIMARY KEY(machine,session,key,offset)); CREATE INDEX IF NOT EXISTS logs_job ON logs(job,seq); CREATE TABLE IF NOT EXISTS usage_samples (at INTEGER NOT NULL, span INTEGER NOT NULL, machine TEXT NOT NULL, device TEXT NOT NULL, project TEXT NOT NULL, job TEXT NOT NULL, model TEXT NOT NULL, busy REAL NOT NULL, idle REAL NOT NULL, pinned REAL NOT NULL, memory_seconds REAL NOT NULL, observed REAL NOT NULL, PRIMARY KEY(at,span,machine,device,project,job,model));`); err != nil {
		db.Close()
		return nil, err
	}
	var data []byte
	err = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data)
	if err == nil {
		data, err = readQueueState(db, data)
		if err == nil {
			err = json.Unmarshal(data, &q.s)
		}
	}
	if err != nil && err != sql.ErrNoRows {
		db.Close()
		return nil, err
	}
	if err = q.loadPersistedState(); err != nil {
		db.Close()
		return nil, err
	}
	if err = q.importProductionDrain(data); err != nil {
		db.Close()
		return nil, err
	}
	if q.s.MachineConversion != nil && q.s.MachineConversion.PublicationPending {
		db.Close()
		return nil, errors.New("machine conversion publication incomplete; keep queue stopped and rerun install queue conversion or rollback")
	}
	q.unreported = map[string]bool{}
	if len(data) > 0 {
		q.restartUntil = clock.Now().Add(RestartGrace)
		for id, m := range q.s.Machines {
			if !m.Gone {
				q.unreported[id] = true
			}
		}
	}
	if err = q.ConfigureModels(ModelConfig{Idle: 10 * time.Minute, Wait: 5 * time.Minute}); err != nil {
		db.Close()
		return nil, err
	}
	if err = q.ConfigureUploadSpool("", DefaultUploadSpoolBytes); err != nil {
		db.Close()
		return nil, err
	}
	if q.importCancellationEvidence() || q.s.FairUsageVersion == 0 {
		if err = q.update(func() error { return nil }); err != nil {
			db.Close()
			return nil, err
		}
	}
	for _, j := range q.s.Jobs {
		if j.Timing != nil && !terminal(j.State) {
			j.Timing.Reason = "queue restarted; occupancy unknown"
		}
	}
	success = true
	return q, nil
}

// SetRestartGrace configures the recovery window from this queue's startup.
func (q *Queue) SetRestartGrace(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.unreported) > 0 {
		q.restartUntil = q.clock.Now().Add(d)
	}
}

const DefaultProductionInfrastructureRetries = 2

// SetProductionInfrastructureRetries bounds infrastructure requeues per production job.
// Zero disables retries; values above two are clamped to the agreed maximum.
// Stage failures never use this budget.
func (q *Queue) SetProductionInfrastructureRetries(n int) error {
	if n < 0 {
		return errors.New("production infrastructure retries must be nonnegative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.productionInfrastructureRetries = min(n, 2)
	return nil
}

// SetLossTimeouts sets the quiet-worker thresholds by workload priority.
func (q *Queue) SetLossTimeouts(production, deadline, experiment time.Duration) error {
	if production <= 0 || deadline <= 0 || experiment <= 0 {
		return errors.New("loss timeouts must be positive")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lossTimeouts = [3]time.Duration{production, deadline, experiment}
	return nil
}
func (q *Queue) machineLossTimeout(id string) time.Duration {
	timeout := time.Duration(0)
	add := func(priority string) {
		d := q.lossTimeouts[min(2, legacyRank(priority))]
		if timeout == 0 || d < timeout {
			timeout = d
		}
	}
	for _, j := range q.s.Jobs {
		if j.Machine == id && (j.State == "running" || j.State == "stopping") {
			add(j.Spec.Priority)
		}
	}
	for _, c := range q.calls {
		if c.Machine == id && c.Error == "" {
			add(c.Priority)
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.Machine == id && (slot.State == "loading" || slot.InFlight > 0) {
			add(slot.Priority)
		}
	}
	if timeout == 0 {
		return q.lossTimeouts[2]
	}
	return timeout
}

// Simulated workers have an always-available pretend NAS; production still probes it.
func (q *Queue) checkpointsAvailable() bool { return q.simulation || q.nasAvailable }
func (q *Queue) checkpointReady(j *Job, m *Machine) bool {
	return !needsCheckpoint(j) || (!m.CheckpointUnavailable && q.checkpointsAvailable())
}
func checkpointAvailable(root string) bool {
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return false
	}
	f, err := os.CreateTemp(root, ".myce-probe-")
	if err != nil {
		return false
	}
	name := f.Name()
	err = f.Sync()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	return err == nil && closeErr == nil && removeErr == nil
}
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	err := q.db.Close()
	if q.uploadSpool.lock != nil {
		err = errors.Join(err, q.uploadSpool.lock.Close())
		q.uploadSpool.lock = nil
	}
	if q.lock != nil {
		err = errors.Join(err, syscall.Flock(int(q.lock.Fd()), syscall.LOCK_UN), q.lock.Close())
		q.lock = nil
	}
	return err
}

// Every mutation is atomic and durable; failed writes restore the previous state.
func (q *Queue) update(fn func() error) error {
	available := true
	if !q.simulation {
		if q.checkpointProbe != nil {
			available = q.checkpointProbe()
		} else {
			available, _ = q.nasProbe.run(q.root, func() (bool, error) {
				return checkpointAvailable(q.root), nil
			})
		}
	}
	q.mu.Lock()
	q.nasAvailable = available
	defer q.mu.Unlock()
	if q.simulation {
		q.usageWrites = nil
		q.accountUsage(q.clock.Now())
		return fn() // Simulation aborts on any error; it never resumes a failed mutation.
	}
	old, _ := json.Marshal(q.s)
	oldCalls := map[string]*ModelCall{}
	for id, c := range q.calls {
		copy := *c
		oldCalls[id] = &copy
	}
	oldUnreported := map[string]bool{}
	for id, v := range q.unreported {
		oldUnreported[id] = v
	}
	oldQuiet := q.networkQuiet
	q.usageWrites = nil
	q.accountUsage(q.clock.Now())
	err := fn()
	if err == nil {
		q.invalidateWorkTimings()
		q.invalidateWorkCurves()
	}
	if err == nil {
		q.maintainSubmissions()
		err = q.persistUsage()
	}
	if err == nil && q.wake != nil {
		close(q.wake)
		q.wake = make(chan struct{})
	}
	if err != nil {
		q.calls = oldCalls
		q.unreported = oldUnreported
		q.networkQuiet = oldQuiet
		q.s = state{}
		_ = json.Unmarshal(old, &q.s)
	}

	return err
}
func (q *Queue) Submit(s Spec) (*Job, error) {
	return q.SubmitContext(context.Background(), s)
}

// SubmitContext snapshots input custody before bulk verification. Key replay,
// cancellation and job creation still serialize in the final update transaction.
func (q *Queue) SubmitContext(ctx context.Context, s Spec) (_ *Job, err error) {
	defer func() {
		if err == nil {
			return
		}
		// Only explicitly marked field rules may publish their reason. A coded
		// rejection alone does not make a wrapped filesystem cause public.
		var rule *rejectionError
		if errors.As(err, &rule) {
			message := rejectionMessages[rule.code]
			var field *fieldRuleError
			if errors.As(err, &field) {
				message = field.Error()
			}
			err = &HTTPError{Status: 400, Code: rule.code, Message: message}
		}
		code, reason := "request_failed", rejectionMessages["request_failed"]
		var apiError *HTTPError
		if errors.As(err, &apiError) {
			code, reason = apiError.Code, apiError.Message
		} else if errors.Is(err, ErrStale) {
			code, reason = "request_stale", rejectionMessages["request_stale"]
		}
		log.Printf("submission rejected code=%s reason=%q", code, reason)
	}()
	ctx, cancel := context.WithTimeout(ctx, submitVerificationTimeout)
	defer cancel()
	if s.SubmissionKey != "" {
		if err := validateSubmissionKey(s.SubmissionKey); err != nil {
			return nil, err
		}
	}
	fingerprint, err := submissionFingerprint(s)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	// Own all client maps/slices across the unlocked verification interval.
	s = cloneJob(&Job{Spec: s}).Spec
	store, fileCheckpoints := q.files, q.fileCheckpoints
	replay := s.SubmissionKey != "" && q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)] != nil
	if len(s.Files) > 0 && !replay && q.submitVerificationSlot == nil {
		q.submitVerificationSlot = make(chan struct{}, 1)
	}
	slot := q.submitVerificationSlot
	q.mu.Unlock()
	var verificationErr error
	if len(s.Files) > 0 && !replay {
		if !fileCheckpoints {
			verificationErr = errors.New("stored job files require file-store checkpoints; omit queue --checkpoints")
		} else {
			verificationErr = verifySubmissionInputs(ctx, store, s.Files, slot)
		}
	}
	var out Job
	err = q.update(func() error {
		q.maintainSubmissions()
		existing, err := q.existingSubmission(s, fingerprint)
		if err != nil {
			return err
		}
		if existing != nil {
			out = *existing
			return nil
		}
		if err := q.validateMachineNames(s.AllowedMachines); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// A concurrent key binding/reservation wins even if verification failed.
		if verificationErr != nil {
			return reject("job_files_invalid", verificationErr)
		}
		if len(s.Files) > 0 && (q.files != store || q.fileCheckpoints != fileCheckpoints) {
			return ErrStale
		}
		submitted := s
		extensions, e := q.resolveExtensions(&s)
		if e != nil {
			return rejectFieldRule("job_program_invalid", e)
		}
		// Validate the original execution/needs, with the resolved class default.
		// Resolution must not hide forbidden overrides or bypass deadline rules.
		submitted.Class = s.Class
		if err := submitted.Validate(); err != nil {
			return specRejection(err)
		}
		if e := q.validateProjectPlacement(s.Project, s.AllowedMachines); e != nil {
			return rejectFieldRule("job_placement_invalid", e)
		}
		if err := normalizeJobClass(&s); err != nil {
			return specRejection(err)
		}
		if err := normalizeResources(&s); err != nil {
			return err
		}
		if !s.extensionJob() {
			if err := lowerExecution(&s); err != nil {
				return specRejection(err)
			}
		}
		recipes, e := q.resolveEnvironment(s.Environment)
		if e != nil {
			return reject("job_environment_invalid", e)
		}
		sidecarRecipes, e := q.resolveSidecarEnvironments(s)
		if e != nil {
			return reject("job_environment_invalid", e)
		}
		var id string
		if q.simulation {
			q.simID++
			id = fmt.Sprintf("sim-%012d", q.simID)
		} else {
			id = ID()
		}
		if s.Priority == "production" && s.StartBy == nil {
			deadline := q.clock.Now().Add(ProductionStartTimeout)
			s.StartBy = &deadline
		}
		j := &Job{PriorityPolicy: q.priorityMatch(s), FileTransport: q.fileCheckpoints, Extensions: extensions, SidecarEnvironmentRecipes: sidecarRecipes, EnvironmentRecipes: recipes, ID: id, Spec: s, State: "queued", Queued: q.clock.Now(), WaitingSince: q.clock.Now(), Checkpoint: filepath.Join(q.root, id)}
		q.applyLearnedNeeds(j)
		if e := q.validateProjectCapacity(j); e != nil {
			return rejectFieldRule("job_capacity_invalid", e)
		}
		copy := cloneJob(j)
		j = &copy
		q.s.Jobs[id] = j
		if s.SubmissionKey != "" {
			if q.s.Submissions == nil {
				q.s.Submissions = map[string]*submissionRecord{}
			}
			created := q.clock.Now()
			q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)] = &submissionRecord{Kind: submissionBinding, Project: s.Project, Key: s.SubmissionKey, Fingerprint: fingerprint, JobID: id, Created: &created, Job: j}
		}
		q.maintain()
		out = cloneJob(j)
		return nil
	})
	return &out, err
}
func (q *Queue) retry(j *Job, why string) {
	q.requeue(j, why, !plannedStop(j))
}

// Admission refusal and planned stops do not spend infrastructure retry budgets.
func (q *Queue) requeue(j *Job, why string, chargeInfrastructure bool) {
	if cancelled(j) {
		if !terminal(j.State) {
			q.finishCancellation(j)
		}
		return
	}
	if j.Spec.Priority == "production" {
		// Fence every old copy, including a lost copy that later reports newer progress.
		for i := range j.Copies {
			j.Copies[i].Recoverable = false
		}
		if chargeInfrastructure && j.InfrastructureRetries >= q.productionInfrastructureRetries {
			j.State, j.Reason = "failed", "infrastructure_retries_exhausted"
			j.StopAt = nil
			now := q.clock.Now()
			j.FinishedAt = &now
			return
		}
		q.renewRecoveryStartWindow(j)
		if chargeInfrastructure {
			j.InfrastructureRetries++
		}
	}
	j.Machine = ""
	j.Session = ""
	j.StopAt = nil
	j.StartedAt = nil
	j.FinishedAt = nil
	j.Reason = why
	if chargeInfrastructure && j.Spec.Priority != "production" && j.Losses >= 3 {
		j.State = "failed"
		j.Reason = "failed after 3 infrastructure losses: " + why
		return
	}
	j.WaitingSince = q.clock.Now()
	q.s.Front++
	j.Front = q.s.Front
	j.State = "queued"
}
func (q *Queue) stop(j *Job, why string) {
	if j.State != "running" {
		return
	}
	t := q.clock.Now().Add(StopGrace)
	j.StopAt = &t
	j.State = "stopping"
	j.Reason = why
}
func (q *Queue) fleetQuiet(now time.Time) bool {
	active, healthy := 0, 0
	for _, m := range q.s.Machines {
		if !m.Gone && !m.Leaving {
			active++
			if now.Sub(m.LastSeen) < min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2])) {
				healthy++
			}
		}
	}
	// Cached contact can identify a continuously reporting minority, but cannot
	// replace a currently healthy worker when the entire active fleet is quiet.
	return q.s.NetworkRecoveryAt.IsZero() && active > 0 && (healthy == 0 || healthy*2 < active && now.Sub(q.s.LastWorkerContact) >= min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2])))
}

func (q *Queue) maintain() {
	q.maintainHostMaintenance()
	q.maintainSubmissions()
	now := q.clock.Now()
	for _, j := range q.s.Jobs {
		if j.State == "queued" {
			q.applyLearnedNeeds(j)
		}
		if j.State == "running" || j.State == "stopping" {
			q.retainRelease(j)
		}
	}
	for _, m := range q.s.Machines {
		if a := machineAvailability(m); a != nil && a.Available && (a.Mode == "daily" || a.Signals.Laptop) && now.Sub(m.LastSeen) > signalMaxAge {
			a.Available = false
			a.Reason = "owner signals stale"
		}
		q.withdrawUnavailable(m)
	}
	q.maintainApprovalGrace()
	q.maintainStartDeadlines()
	q.maintainHolds()
	for machine, release := range q.s.BatchReleases {
		if !now.Before(release.Until) {
			delete(q.s.BatchReleases, machine)
		}
	}
	q.networkQuiet = q.fleetQuiet(now)
	for _, m := range q.s.Machines {
		unreported := q.unreported[m.ID]
		outageGrace := !q.s.NetworkRecoveryAt.IsZero() && m.LastSeen.Before(q.s.NetworkRecoveryAt) && now.Before(q.s.NetworkRecoveryAt.Add(q.machineLossTimeout(m.ID)))
		grace := !unreported && outageGrace || unreported && now.Before(q.restartUntil)
		restartExpired := unreported && !grace
		if !m.Gone && !grace && (restartExpired || (!q.networkQuiet && now.Sub(m.LastSeen) >= q.machineLossTimeout(m.ID))) {
			m.Gone = true
			delete(q.unreported, m.ID)
			m.Models = nil
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
					if cancelled(j) {
						j.State = "cancelled"
						j.StopAt = nil
					} else {
						for i := range j.Copies {
							if j.Copies[i].Attempt == j.Attempt {
								j.Copies[i].Recoverable = j.State == "running"
							}
						}
						if j.State == "running" {
							j.Losses++
							q.chargeLoss(j)
						}
						q.retry(j, fmt.Sprintf("machine quiet for %s", q.machineLossTimeout(m.ID)))
					}
				}
			}
		}
	}
	if !q.s.NetworkRecoveryAt.IsZero() {
		pending := false
		for _, m := range q.s.Machines {
			if !m.Gone && !m.Leaving && m.LastSeen.Before(q.s.NetworkRecoveryAt) {
				pending = true
				break
			}
		}
		if !pending {
			q.s.NetworkRecoveryAt = time.Time{}
		}
	}

	for _, j := range q.s.Jobs {
		// Machine attempts retain their exact device and host charge through kill;
		// only their release acknowledgement can retire the stopping attempt.
		m := q.s.Machines[j.Machine]
		if (m == nil || m.Devices == nil) && !q.networkQuiet && !q.unreported[j.Machine] && j.State == "stopping" && !plannedPriorityStop(j) && !j.Spec.Run.Docker && expired(j.StopAt, now) {
			if cancelled(j) {
				j.State = "cancelled"
				j.StopAt = nil
			} else {
				q.retry(j, j.Reason)
			}
		}
		if j.State == "queued" && expired(j.Spec.GiveUpAt, now) {
			j.State = "failed"
			j.Reason = "give-up time reached"
		}
	}
	q.maintainStartDeadlines()
	q.maintainHolds()
	q.refreshHostMemory()
	q.maintainModels()
	q.escalateDockerCleanup()
	q.refreshHostMemory()
	q.maintainHolds()
	for _, j := range q.s.Jobs {
		if startupPending(j) {
			j.Reason = q.startStatusReason(j)
		}
	}
}
func (q *Queue) Tick() error {
	if err := q.update(func() error { q.maintain(); return nil }); err != nil {
		return err
	}
	return q.pruneBatchLogs()
}
func (q *Queue) Checkin(c Checkin) (Reply, error) {
	var err error
	c, err = normalizeAttemptCheckin(c)
	if err != nil {
		return Reply{}, err
	}
	if err := validateCurrentCheckin(c); err != nil {
		return Reply{}, err
	}
	reply := Reply{FileCheckpoints: q.fileCheckpoints, CheckpointRoot: q.root}
	reports := c.Attempts
	if c.AttemptProtocol == "" && c.Running != nil {
		reports = []AttemptReport{{Execution: *c.Running, Session: c.Machine.Session, Started: c.Started}}
	}
	progresses := make([]attemptProgress, len(reports))
	for i := range reports {
		report, progress := &reports[i], &progresses[i]
		q.mu.Lock()
		if j := q.s.Jobs[report.ID]; j != nil && j.FileTransport {
			report.Progress = 0
			if save, ok := j.FileCheckpoints[report.Attempt]; ok {
				report.Progress = save.Generation
			}
			if save, ok := j.FileCheckpoints[j.Attempt]; ok {
				progress.durable = save.Generation
			}
		} else if j != nil && j.Spec.Resumable && j.Attempt != report.Attempt {
			progress.path = attemptCheckpoint(*j)
		}
		q.mu.Unlock()
		if progress.path != "" {
			progress.durable, progress.err = q.nasProgress.run(progress.path, func() (int64, error) {
				generation, err := checkpointProgress(progress.path)
				if errors.Is(err, os.ErrNotExist) {
					return 0, nil
				}
				return generation, err
			})
		}
	}
	err = q.update(func() error {
		m := cloneMachine(&c.Machine)
		if q.migrationInventoryBlocked(&m) {
			return migrationError("machine_worker_conflict", "machine installation is rolled back; restore the saved legacy workers or convert forward under maintenance")
		}
		if err := inventoryTarget(&m); err != nil {
			return err
		}
		// Dev's existing fake workers intentionally share a synthetic host ledger.
		if _, simulated := q.clock.(*devClock); !simulated {
			for _, owner := range q.s.Machines {
				if owner.ID != m.ID && (m.Devices != nil || owner.Devices != nil) && owner.Replacement == nil && !owner.Gone && hostID(owner) == hostID(&m) {
					return &HTTPError{Status: 409, Code: "machine_worker_conflict", Message: rejectionMessages["machine_worker_conflict"]}
				}
			}
		}
		m.Disabled = ""
		for _, progress := range progresses {
			if progress.err != nil {
				m.CheckpointUnavailable = true
			}
		}
		if _, err := MemoryCeilingConfig(m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB, 0); err != nil {
			return err
		}
		if !validMemoryValue(m.HostAvailableMemoryGB) || !validMemoryValue(m.HostMemoryBudgetGB) || !validMemoryValue(m.MemoryCeilingSlackGB) {
			return errors.New("invalid host memory report")
		}
		if m.CPUReserveCores < 0 || m.CPUBudgetCores < 0 {
			return errors.New("CPU reserve and allowance must be nonnegative")
		}
		if m.MaxJobs < 0 {
			return errors.New("max-jobs must be nonnegative")
		}
		if m.ID == "" || m.Session == "" || !validGPU(m.GPU) || !validMemoryValue(m.TotalMemoryGB) || m.FreeMemoryGB < 0 || math.IsNaN(m.FreeMemoryGB) || math.IsInf(m.FreeMemoryGB, 0) {
			return errors.New("invalid machine report")
		}
		if m.ModelURL != "" && len(q.models.Models) > 0 && m.ModelConfigHash != modelConfigHash(q.models) && !q.migrationLegacyCatalog(&m) {
			return errors.New("worker model config does not match queue")
		}
		old := q.s.Machines[m.ID]
		if old != nil && old.Replacement != nil {
			return q.validateMachineNames([]string{m.ID})
		}
		if old != nil && m.Devices != nil {
			for _, d := range old.Devices {
				if !slices.ContainsFunc(m.Devices, func(current MachineDevice) bool { return current.ID == d.ID }) {
					d.Reason, d.FreeMemoryGB = "device_probe_unavailable", 0
					m.Devices = append(m.Devices, d)
				}
			}
		}
		if old != nil && hostID(old) != hostID(&m) {
			return errors.New("machine physical host identity changed")
		}
		if err := q.registerMaintenanceHost(&m); err != nil {
			return err
		}
		budget := *q.s.HostMaintenance[hostID(&m)].MemoryBudget
		reply.HostMemoryBudget = &budget
		if cores := q.s.HostMaintenance[hostID(&m)].CPUBudgetCores; cores != nil {
			value := *cores
			reply.CPUBudgetCores = &value
		}
		reply.HostMaintenance = q.s.HostMaintenance[hostID(&m)].Mode
		if old != nil && old.Session != m.Session && !(c.Fresh && c.PreviousSession == old.Session) {
			if h := q.s.Hosts[hostID(old)]; q.maintenanceKnown(hostID(old)) || old.HostMemoryBudgetGB > 0 || old.hostBudgetGB > 0 || h != nil && h.Policy.ProductionHold {
				for _, slot := range q.s.ModelSlots {
					if slot.Machine == m.ID && slot.Session == old.Session {
						return errors.New("previous model execution release unknown; prior-session cleanup required")
					}
				}
			}
		}
		if c.AttemptProtocol == "" && c.Running == nil && old != nil && old.Session == m.Session {
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && j.State == "stopping" && !j.Spec.Run.Docker && q.held(hostID(old)) {
					if cancelled(j) {
						q.settleRelease(Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session})
						j.State = "cancelled"
						j.StopAt = nil
					} else {
						q.retry(j, j.Reason)
					}
				}
			}
		}
		q.reconcileHostReleases(c)
		if m.TotalMemoryGB == 0 {
			m.TotalMemoryGB = m.FreeMemoryGB
			if old != nil {
				m.TotalMemoryGB = max(m.TotalMemoryGB, old.TotalMemoryGB)
			}
		}
		if m.CapabilityProfile != nil {
			m.UnifiedMemory = m.CapabilityProfile.MemoryLayout == "unified"
		}
		if old != nil && old.Session != m.Session && !old.Gone && !(c.Fresh && c.PreviousSession == old.Session) {
			return errors.New("machine name already joined; leave the other worker first")
		}
		if old != nil && old.Session == m.Session {
			m.Leaving = old.Leaving || m.Leaving
		}
		if old != nil && old.Session != m.Session && c.Fresh {
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == old.Session && (j.State == "running" || j.State == "stopping") {
					if cancelled(j) {
						j.State = "cancelled"
						j.StopAt = nil
						continue
					}
					if j.State == "running" {
						j.Losses++
						q.chargeLoss(j)
					}
					q.retry(j, "worker restarted after cleaning old processes")
				}
			}
		}
		delete(q.unreported, m.ID)
		m.Gone = false
		m.LastSeen = q.clock.Now()
		// Recovery is queue-observed admission state, not worker-supplied status.
		m.Recovering, m.RecoveringSince, m.RecoveryNeedsAttention = c.Recovering, nil, false
		if m.Recovering {
			since := m.LastSeen
			if old != nil && old.Recovering && old.RecoveringSince != nil {
				since = *old.RecoveringSince
			}
			m.RecoveringSince = &since
		}
		q.s.Machines[m.ID] = &m
		q.refreshHostMemory()
		q.acceptCurveReports(c, &m)
		q.reconcilePreparationPlans(c)
		// Count this worker as healthy, but detect majority silence before
		// replacing the previous fleet contact. Otherwise the first reconnect
		// looks like a continuously reporting survivor and expires its peers.
		// Keep actual peer heartbeats and the active recovery deadline unchanged.
		if (q.networkQuiet || q.fleetQuiet(m.LastSeen)) && old != nil && !old.Gone && !m.LastSeen.Before(q.restartUntil) && q.s.NetworkRecoveryAt.IsZero() {
			q.s.NetworkRecoveryAt = m.LastSeen
		}
		q.s.LastWorkerContact = m.LastSeen
		delete(q.s.BatchReleases, m.ID) // This report supersedes acknowledgement credit.
		for _, d := range m.Devices {
			delete(q.s.BatchReleases, m.ID+"/"+d.ID)
		}
		q.reconcileModels(&m, c.ModelReports)
		if e := q.reconcileEnvironments(&m); e != nil {
			return e
		}
		reservation := q.modelReservation(&m, c.ModelReports)
		m.modelReservationDebtGB = max(0, reservation-m.FreeMemoryGB)
		m.FreeMemoryGB = max(0, m.FreeMemoryGB-reservation)
		q.withdrawUnavailable(&m)
		q.quarantineDeviceAttempts(&m)
		q.maintain()
		defer func() {
			m.AdmissionReason = q.machineLimitReason(&m)
			if a := machineAvailability(&m); a != nil && !a.Available {
				m.AdmissionReason = a.Reason
			}

			// AdmissionBlocked also holds existing supervisors' result replay.
			// Recovery gates new offers/planning separately so saved results drain.
			reply.AdmissionBlocked = availabilityBlocked(&m) || m.HostMemoryBudgetStatus() != "" || q.maintenanceBlocked(hostID(&m))
			for _, p := range q.hostObligations(hostID(&m)) {
				if p.Preparation && p.Result.Machine == m.ID && p.Result.Session == m.Session {
					if r, ok := q.s.HostReleases[releaseKey(HostRelease{Job: p.Result.ID, Machine: m.ID, Session: m.Session})]; ok && !r.Until.IsZero() {
						boundStop(&reply.PreparationStopAt, r.Until)
					}
				}
			}
			// Onboarding reports health and exact custody before any ordinary
			// admission. Do not return offers that this startup phase cannot run.
			if c.Recovering {
				q.cancelUnofferedPreparations(&m, reply)
				return
			}
			if m.BenchmarkPending || c.BenchmarkDone != "" {
				if m.BenchmarkPending && q.hostLaunchAllowed(&m, m.Session) && q.benchmarkIdle(&m) {
					l := q.issuePreparation(&m, "benchmark", "onboarding")
					reply.BenchmarkPlan = &l
					copy := cloneMachine(&m)
					reply.BenchmarkMachine = &copy
					for _, point := range q.s.WorkObservations {
						if point.Key.Host == hostID(&m) {
							reply.BenchmarkCurves = append(reply.BenchmarkCurves, point)
						}
					}
				}
				q.cancelUnofferedPreparations(&m, reply)
				return
			}
			for _, l := range q.s.PreparationPlans {
				if l.Machine == m.ID && l.Session == m.Session && l.StopAt != nil {
					boundStop(&reply.PreparationStopAt, *l.StopAt)
				}
			}
			if m.Devices == nil {
				reply.EnvironmentPlan = q.environmentPlan(&m)
			} else {
				for _, target := range q.machineTargets(&m) {
					if plans := q.environmentPlan(target); len(plans) > 0 {
						reply.EnvironmentPlan = plans
						break
					}
				}
			}
			reply.ModelPlan = q.modelPlan(m.ID)
			reply.CopyPlan, reply.CacheUsage, reply.CachePins = q.copyPlan(&m)
			q.cancelUnofferedPreparations(&m, reply)
		}()
		reply.Leave = m.Leaving
		if m.BenchmarkPending || c.BenchmarkDone != "" {
			return nil
		}
		if m.Leaving {
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && j.Spec.Resumable {
					q.stop(j, "machine leaving")
				}
			}
		}
		if c.AttemptProtocol != "" && m.Devices != nil {
			q.checkinMachineAttempts(&m, reports, progresses, c.Recovering, &reply)
			q.maintain()
			return nil
		}

		if len(reports) != 0 {
			seen := map[attemptKey]bool{}
			for i := range reports {
				a := q.reconcileAttemptReport(&m, &reports[i], progresses[i])
				if c.AttemptProtocol == "" {
					reply.Stop, reply.Kill, reply.Hold = a.Stop, a.Kill, a.Hold
					reply.GraceSeconds, reply.PriorityStop = a.GraceSeconds, a.PriorityStop
				} else {
					reply.Attempts = append(reply.Attempts, a)
					seen[attemptKey{a.ID, a.Attempt, a.Session}] = true
				}
			}
			// A report for another key neither acknowledges an omitted stop nor
			// hides it. Keep every outstanding control individually addressed.
			if c.AttemptProtocol != "" {
				for _, j := range q.s.Jobs {
					if j.Machine == m.ID && j.Session == m.Session && j.State == "stopping" && !seen[attemptKey{j.ID, j.Attempt, j.Session}] {
						if j.ReleaseConfirmed {
							q.retry(j, j.Reason)
							continue
						}
						reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: j.StopAt.Sub(q.clock.Now()).Seconds(), PriorityStop: priorityStop(j)})
					}
				}
			}
			return nil
		}
		// Re-deliver an assignment if its HTTP response was lost before startup.
		for _, j := range q.s.Jobs {
			if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
				if j.State == "stopping" {
					// Omission only finishes a keyed stop after an exact release ack.
					if c.AttemptProtocol != "" && !j.ReleaseConfirmed {
						reply.Attempts = []AttemptDirective{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: j.StopAt.Sub(q.clock.Now()).Seconds(), PriorityStop: priorityStop(j)}}
						return nil
					}
					if j.Spec.Run.Docker && !j.ReleaseConfirmed {
						return nil
					}
					if cancelled(j) {
						q.settleRelease(Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session})
						j.State = "cancelled"
						j.StopAt = nil
					} else {
						q.retry(j, j.Reason)
					}
					break
				}
				if c.Recovering || q.maintenanceBlocked(hostID(&m)) || !logPolicyFits(j.Spec.Logs, &m) || !q.checkpointReady(j, &m) {
					return nil
				}
				out := cloneJob(j)
				reply.Job = &out
				return nil
			}
		}
		if !m.Leaving && !c.Recovering {
			j := q.pick(&m)
			if j != nil && q.betterPlacement(j, &m) {
				j = nil
			}
			if j != nil {
				out := q.assign(j, &m)
				reply.Job = &out
			}
		}
		if m.Leaving && reply.Job == nil && len(q.modelPlan(m.ID)) == 0 {
			m.Gone = true
			m.Models = nil
		}
		q.maintain()
		return nil
	})
	if reply.Job != nil {
		relative, e := filepath.Rel(q.root, reply.Job.Checkpoint)
		if e != nil {
			return Reply{}, e
		}
		reply.Job.Checkpoint = relative
	}
	for i := range reply.Attempts {
		if j := reply.Attempts[i].Job; j != nil {
			relative, e := filepath.Rel(q.root, j.Checkpoint)
			if e != nil {
				return Reply{}, e
			}
			j.Checkpoint = relative
		}
	}
	return attemptReply(c, reply), err
}
func validGPU(s string) bool {
	return s == "cuda" || s == "xpu" || s == "metal" || s == "fake" || s == "cpu"
}
func (q *Queue) current(r Result) (*Job, error) {
	j := q.s.Jobs[r.ID]
	if j == nil || j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || (j.State != "running" && j.State != "stopping") {
		return nil, ErrStale
	}
	return j, nil
}
func (q *Queue) Complete(r Result) error {
	return q.CompleteContext(context.Background(), r)
}

// CompleteContext keeps bulk output verification outside the queue transaction.
// Only a current, publishable attempt needs verification; release acknowledgements
// and interrupted or retransmitted results retain their normal handling.
func (q *Queue) CompleteContext(ctx context.Context, r Result) error {
	q.mu.Lock()
	j, err := q.current(r)
	verify := err == nil && j.FileTransport && j.State == "running" && !cancelled(j) && r.Success && !r.Interrupted && !r.ReleaseOnly
	var outputs map[string]string
	var host string
	if verify {
		host = q.executionHost(r.Machine)
		verify = q.s.Machines[r.Machine] != nil && q.s.Machines[r.Machine].Session == r.Session
		if verify {
			outputs = maps.Clone(j.Spec.Outputs)
			r.OutputRefs = maps.Clone(r.OutputRefs)
		}
	}
	q.mu.Unlock()
	if verify {
		if len(outputs) != len(r.OutputRefs) {
			return errors.New("complete output manifest required")
		}
		for name := range outputs {
			if _, ok := r.OutputRefs[name]; !ok {
				return errors.New("missing output")
			}
		}
		verificationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := q.verifyManifest(verificationCtx, r.OutputRefs); err != nil {
			return err
		}
		ctx = verificationCtx
	}
	var rejected error
	err = q.update(func() error {
		if verify {
			q.maintainStartDeadlines()
			j, err := q.current(r)
			m := q.s.Machines[r.Machine]
			if err != nil || j.State != "running" || cancelled(j) || m == nil || m.Session != r.Session ||
				q.executionHost(r.Machine) != host || !maps.Equal(j.Spec.Outputs, outputs) {
				return ErrStale
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		// Release evidence still settles custody. Every superseded result is
		// rejected after committing that settlement, without publishing its payload.
		if j := q.s.Jobs[r.ID]; j != nil && !r.ReleaseOnly &&
			(j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || j.State == "queued") {
			rejected = ErrStale
		}
		return q.complete(r, verify)
	})
	if err != nil {
		return err
	}
	return rejected
}
func (q *Queue) complete(r Result, outputsVerified bool) error {
	q.maintainStartDeadlines()
	operatorMatched := q.refundOperatorStop(r)
	if r.ReleaseOnly {
		if r.ReleasePending {
			return errors.New("release-only acknowledgement cannot be pending")
		}
		q.settleRelease(r)
		if j := q.s.Jobs[r.ID]; j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			j.ReleaseConfirmed = true
			if r.Forced {
				j.LastStopOutcome = "forced"
			} else if j.LastStopOutcome == "release pending" {
				j.LastStopOutcome = "released"
			}
			// Native kill-time reports can carry cleanup only. Advance the
			// recorded stop on this exact acknowledgement, rather than using
			// an omitted attempt in the next heartbeat as release evidence.
			if j.State == "stopping" && !j.Spec.Run.Docker {
				if cancelled(j) {
					j.State, j.StopAt = "cancelled", nil
				} else {
					q.retry(j, j.Reason)
				}
			}
		}
		q.maintain()
		return nil
	}
	if operatorMatched {
		if _, e := q.current(r); e != nil {
			if !r.ReleasePending {
				q.settleRelease(r)
			}
			q.maintain()
			return nil
		}
	}
	settled := false
	pendingTracked := false
	if !r.ReleasePending {
		settled = q.settleRelease(r)
		if j := q.s.Jobs[r.ID]; j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			j.ReleaseConfirmed = true
			if j.LastStopOutcome == "release pending" {
				j.LastStopOutcome = "released"
			}
		}
	}
	if r.ReleasePending {
		if j := q.s.Jobs[r.ID]; j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			q.retainRelease(j)
		}
		key := releaseKey(HostRelease{Job: r.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt})
		if hold, ok := q.s.HostReleases[key]; ok {
			pendingTracked = true
			hold.CleanupFailed = true
			q.s.HostReleases[key] = hold
		}
	}
	if j := q.s.Jobs[r.ID]; j != nil && r.Forced && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		j.LastStopOutcome = "forced"
		if r.ReleasePending {
			j.LastStopOutcome += "; release pending"
		}
	}
	if r.ReleasePending && pendingTracked && q.s.HostReleases[releaseKey(HostRelease{Job: r.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt})].Docker {
		q.maintain()
		return nil
	}
	if settled || pendingTracked {
		if _, err := q.current(r); err != nil {
			q.maintain()
			return nil
		}
	}
	// A retransmitted acknowledgement must not turn a completed job into stale work.
	if j := q.s.Jobs[r.ID]; j != nil && terminal(j.State) && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		return nil
	}
	j, err := q.current(r)
	if err != nil {
		return err
	}
	if m := q.s.Machines[r.Machine]; m != nil {
		m.LastSeen = q.clock.Now()
		delete(q.unreported, m.ID)
	}
	r = withStopProvenance(r, priorityStop(j))
	j.ExecutionCause = safeRuntimeCause(r.ExecutionCause)
	j.CleanupCause = safeRuntimeCause(r.CleanupCause)
	// A kernel OOM is the payload's failure even if preemption raced it.
	ceilingFailure := !r.Success && strings.HasPrefix(ceilingDiagnostic(r.Error, ""), "exceeded its ")
	oomFailure := !r.Success && (r.OutOfMemory || ceilingFailure)
	if oomFailure {
		q.recordWorkOOM(j, r)
	}
	stageFailure := (j.Spec.Priority == "production" || plannedPriorityStop(j)) && !r.Success && !r.Interrupted
	// Bridge cleanup-to-inventory telemetry with the same absolute bound as
	// pending cleanup. Completion and heartbeats cannot renew planning credit.
	if j.State == "stopping" && !r.ReleasePending && j.StopAt != nil {
		owner := allocationMachine(q.s.Machines[j.Machine], j.AllocatedDevices)
		until := q.batchCreditUntil(j)
		if owner != nil && q.clock.Now().Before(until) {
			if q.s.BatchReleases == nil {
				q.s.BatchReleases = map[string]batchRelease{}
			}
			q.s.BatchReleases[batchReleaseKey(owner)] = batchRelease{Session: j.Session, MemoryGB: j.Spec.Needs.MemoryGB, Until: until}
		}
	}
	if cancelled(j) || j.State == "stopping" && (!r.Success || !plannedPriorityStop(j)) && !oomFailure && !stageFailure {
		if cancelled(j) {
			j.State = "cancelled"
			j.StopAt = nil
		} else {
			q.retry(j, j.Reason)
		}
	} else if (j.Spec.Priority == "production" || plannedPriorityStop(j)) && (stageFailure || oomFailure) {
		j.State, j.Reason = "failed", "command failed: "+r.Error
	} else if oomFailure && j.Spec.extensionJob() {
		q.retryOOM(j)
	} else if r.ExtensionUnavailable && j.Spec.extensionJob() && j.StartedAt == nil && j.Extension != nil {
		if m := q.s.Machines[r.Machine]; m != nil {
			for i := range m.ExtensionReports {
				report := &m.ExtensionReports[i]
				if report.Digest == j.Extension.Digest && report.Work == j.Spec.workType() {
					report.Qualified = false
					report.Reason = "qualification unavailable; setup/check required"
				}
			}
		}
		q.retry(j, "waiting for a qualified extension")
	} else if !r.Success && j.StartedAt == nil && j.ExecutionCause != nil && j.ExecutionCause.Operation == "launch" && j.ExecutionCause.Class == cpuIsolationUnavailable {
		// Qualification may change after admission. Withdraw CPU readiness and
		// wait for fresh evidence rather than exhausting infrastructure retries.
		if m := q.s.Machines[r.Machine]; m != nil {
			m.CPUIsolation = cpuIsolationUnavailable
		}
		q.requeue(j, cpuIsolationUnavailable+": "+rejectionMessages[cpuIsolationUnavailable], false)
	} else if !r.Success && j.StartedAt == nil && j.ExecutionCause != nil && j.ExecutionCause.Operation == "launch" && j.ExecutionCause.Class == "resource_limit" {
		q.requeue(j, "waiting for memory after prelaunch admission changed", false)
	} else if r.Interrupted && r.PoliteStop {
		q.retry(j, "requeued: hypha availability withdrawn")
	} else if r.Interrupted && r.OperatorStop {
		q.retry(j, "requeued: stopped by operator")
		j.RequeuedByOperator = true
	} else if r.Interrupted {
		j.Losses++
		q.chargeLoss(j)
		q.retry(j, "worker restarted after cleaning old processes")
	} else if r.Success {
		if j.FileTransport {
			if !outputsVerified {
				return ErrStale
			}
			j.OutputRefs = r.OutputRefs
			j.OutputFiles = nil
		}
		q.recordWorkObservation(j, r)
		q.recordProgramNeeds(j, r)
		q.recordModelUse(j.Spec.Needs.Model)
		j.State = "succeeded"
	} else {
		j.State = "failed"
		j.Reason = "command failed: " + r.Error
	}
	if terminal(j.State) {
		now := q.clock.Now()
		j.FinishedAt = &now
	}
	q.maintain()
	return nil
}
func (q *Queue) Leave(id string, now bool) error {
	return q.update(func() error {
		q.maintain()
		m := q.s.Machines[id]
		if m == nil || m.Gone {
			return errors.New("machine is not in the fleet")
		}
		m.Leaving = true
		for _, j := range q.s.Jobs {
			if j.Machine == id && (j.Spec.Resumable || now) {
				q.stop(j, "machine leaving")
			}
		}
		return nil
	})
}
func (q *Queue) Snapshot() (Snapshot, error) { return q.snapshot(false) }

func (q *Queue) snapshot(forStatus bool) (Snapshot, error) {
	var out Snapshot
	project := func() error {
		out = Snapshot{HoldsDetail: []HoldDetail{}, SyncRequests: append([]SyncRequest(nil), q.s.SyncRequests...), Pins: append([]ModelPin(nil), q.s.Pins...), Jobs: []*Job{}, Machines: []*Machine{}, LoadedModels: []ModelSlot{}}
		for _, s := range q.s.ModelSlots {
			copy := *s
			model, _ := q.model(s.Model)
			copy.Placement = q.modelPlacement(model)
			if s.State == "ready" && s.InFlight == 0 {
				copy.IdleSeconds = max(0, q.clock.Now().Sub(s.LastUsed).Seconds())
			}
			out.LoadedModels = append(out.LoadedModels, copy)
		}
		sort.Slice(out.LoadedModels, func(i, j int) bool {
			return slotKey(out.LoadedModels[i].Machine, out.LoadedModels[i].Model) < slotKey(out.LoadedModels[j].Machine, out.LoadedModels[j].Model)
		})
		// Concurrent outages must remain visible; one probe cannot hide another.
		warnings := []string{}
		if q.networkQuiet {
			warnings = append(warnings, "most workers quiet: waiting for the network to return")
		}
		if q.clock.Now().Before(q.restartUntil) && len(q.unreported) > 0 {
			warnings = append(warnings, "queue restarted: waiting for workers to report")
		}
		if !q.checkpointsAvailable() {
			warnings = append(warnings, "NAS checkpoint root is unreachable; checkpoint-dependent jobs will not start")
		}
		for _, id := range slices.Sorted(maps.Keys(q.s.Machines)) {
			m := q.s.Machines[id]
			if m.Replacement == nil && !m.Gone && m.CheckpointUnavailable {
				warnings = append(warnings, "NAS checkpoint root is unreachable on worker "+m.ID)
			}
		}
		out.Warning = strings.Join(warnings, "; ")
		for host := range q.s.Hosts {
			if q.held(host) {
				detail := q.productionHoldDetail(host)
				out.HostHolds = append(out.HostHolds, detail.Text)
				out.HoldsDetail = append(out.HoldsDetail, detail)
			}
		}
		hosts := map[string]bool{}
		for _, m := range q.s.Machines {
			hosts[hostID(m)] = true
		}
		for host := range hosts {
			if detail := q.dockerHoldDetail(host); detail.Text != "" {
				if !slices.Contains(out.HostHolds, detail.Text) {
					out.HostHolds = append(out.HostHolds, detail.Text)
				}
				out.HoldsDetail = append(out.HoldsDetail, detail)
			}
		}
		// Also report fences without a configured production policy or live worker.
		for _, r := range q.s.HostReleases {
			if _, ok := hosts[r.Host]; !ok {
				hosts[r.Host] = false
			}
		}

		for host := range q.s.HostMaintenance {
			if _, ok := hosts[host]; !ok {
				hosts[host] = false
			}
		}

		for host, hasMachine := range hosts {
			if !hasMachine {
				if detail := q.dockerHoldDetail(host); detail.Text != "" {
					out.HoldsDetail = append(out.HoldsDetail, detail)
				}
			}
			if detail := q.startupReleaseDetail(host); detail.Text != "" {
				out.HoldsDetail = append(out.HoldsDetail, detail)
			}
			if q.maintenanceBlocked(host) {
				out.HoldsDetail = append(out.HoldsDetail, HoldDetail{Host: host, Kind: "host_disabled", Text: "host " + host + " disabled by operator"})
			}
		}
		sort.Strings(out.HostHolds)
		sort.Slice(out.HoldsDetail, func(i, j int) bool {
			a, b := out.HoldsDetail[i], out.HoldsDetail[j]
			if a.Host != b.Host {
				return a.Host < b.Host
			}
			return a.Kind < b.Kind
		})
		for _, key := range slices.Sorted(maps.Keys(q.s.WorkObservations)) {
			out.WorkCurves = append(out.WorkCurves, q.s.WorkObservations[key])
		}
		out.PriorityPolicy = q.priorityPolicyRecord()
		out.Now = q.clock.Now()
		order, reasons := q.queueProjection()
		for _, stored := range q.s.Jobs {
			value := *stored
			j := &value
			if j.State == "queued" {
				j.Reason = reasons[j.ID]
				if j.RequeuedByOperator {
					j.Reason = "requeued: disabled by operator; " + j.Reason
				}
				if needsCheckpoint(j) && !q.checkpointsAvailable() {
					j.Reason = "NAS checkpoint root is unreachable"
				}
			}
			if startupPending(j) {
				j.Reason = q.startStatusReason(j)
			}
			// Status reasons/eligibility need the original spec under the lock;
			// detach the public summary only after those calculations.
			copy := *j
			if !forStatus {
				copy = cloneJob(j)
			}
			copy.Spec.Class = jobClass(copy.Spec)
			copy.PriorityPolicy = q.priorityMatch(copy.Spec)
			copy.QueueOrder = order[j.ID]
			if terminal(j.State) && j.LastStopOutcome == "release pending" {
				copy.Reason += "; release pending"
			}
			out.Jobs = append(out.Jobs, &copy)
		}
		for _, c := range q.calls {
			call := callJob(c)
			call.Spec.Name = "model call: " + c.Model
			model, _ := q.model(c.Model)
			call.Spec.Logs = model.Logs
			call.Spec.Needs.MemoryGB = model.MemoryGB
			call.Spec.Needs.HostMemoryGB = model.HostMemoryGB
			call.Machine = c.Machine
			call.State = "queued"
			call.Reason = "waiting for model " + c.Model + " on an eligible worker"
			if c.Machine == "" && (model.Project != "" || len(model.AllowedMachines) > 0 || model.LocalSource != "") {
				eligible := false
				for _, worker := range q.s.Machines {
					if q.projectAllows(c.Project, worker.ID) && q.modelFitsMachine(model, worker) && !worker.Gone && !worker.Leaving {
						eligible = true
						break
					}
				}
				if !eligible {
					call.Reason = "waiting for model placement: entry/owner/caller allowed_machines or local_source approval excludes available workers"
				}
			}
			call.QueueOrder = order[c.ID]
			if reason := reasons[c.ID]; reason != "" {
				call.Reason = reason
			}
			if reason := q.capReason(call); reason != "" {
				call.Reason = reason
			}
			if reason := q.holdWaitReason(call); reason != "" {
				call.Reason = reason
			}
			if c.Machine == "" && model.HostMemoryGB > 0 {
				eligible, room := false, false
				for _, m := range q.s.Machines {
					if m.Gone || m.Leaving || q.callExcludes(c, m.ID) || m.ModelURL == "" || !q.modelFitsMachine(model, m) {
						continue
					}
					eligible = true
					slot := q.s.ModelSlots[targetSlotKey(m, c.Model)]
					if slot != nil && (slot.State == "ready" || slot.State == "loading") || hostFits(m, model.MemoryGB, model.HostMemoryGB) {
						room = true
					}
				}
				if eligible && !room {
					call.Reason = fmt.Sprintf("waiting for RAM: model %s needs %.1f GiB host memory (or telemetry unavailable)", c.Model, model.HostMemoryGB)
				}
			}
			if reason := q.logsWaitingReason(call); reason != "" {
				call.Reason = reason
			}
			if reason := q.engineWaitReason(c); reason != "" && call.Reason == "waiting for model "+c.Model+" on an eligible worker" {
				call.Reason = reason
			}
			if c.Machine != "" {
				call.State = "running"
				call.Reason = "model call in flight"
			}
			if c.Error != "" {
				call.State = "failed"
				call.Reason = c.Error
			}
			call.Spec.Class = jobClass(call.Spec)
			call.PriorityPolicy = q.priorityMatch(call.Spec)
			out.Jobs = append(out.Jobs, call)
		}
		for _, m := range q.s.Machines {
			copy := cloneMachine(m)
			copy.RecoveryNeedsAttention = m.Recovering && m.RecoveringSince != nil && q.clock.Now().Sub(*m.RecoveringSince) > recoveryAttentionDelay
			q.machineDeviceStatus(&copy, m)
			copy.AdmissionReason = q.machineLimitReason(m)
			copy.Disabled = ""
			if q.maintenanceBlocked(hostID(m)) {
				copy.Disabled = "planned host maintenance"
			}
			if a := machineAvailability(m); a != nil && !a.Available {
				copy.AdmissionReason = a.Reason
			}
			copy.ReservedMemoryGB = 0
			copy.MemoryUsedGB = max(0, copy.TotalMemoryGB-copy.FreeMemoryGB)
			for _, slot := range q.s.ModelSlots {
				if slot.Machine == m.ID && slotReservesMemory(slot) && q.slotPinned(slot) {
					model, _ := q.model(slot.Model)
					copy.ReservedMemoryGB += model.MemoryGB
				}
			}
			copy.ModelCopySeconds = map[string]float64{}
			for _, model := range q.models.Models {
				if q.modelFitsMachine(model, m) {
					copy.ModelCopySeconds[model.Name] = q.loadSeconds(model.Name, m)
				}
			}
			out.Machines = append(out.Machines, &copy)
		}
		for _, j := range out.Jobs {
			if strings.Contains(j.Reason, cpuIsolationUnavailable) {
				j.DiagnosticCode = cpuIsolationUnavailable
			}
			if strings.HasPrefix(j.Reason, "no_matching_device:") {
				j.DiagnosticCode = "no_matching_device"
			}
		}
		if forStatus {
			for _, j := range out.Jobs {
				q.statusEligibility(j)
				if j.State == "queued" {
					j.Reason = q.statusWaitingReason(j)
					if strings.Contains(j.Reason, cpuIsolationUnavailable) {
						j.DiagnosticCode = cpuIsolationUnavailable
					}
					if strings.HasPrefix(j.Reason, "no_matching_device:") {
						j.DiagnosticCode = "no_matching_device"
					}
				}
				*j = statusJobCopy(j)
			}
		}
		sort.SliceStable(out.Jobs, func(i, k int) bool { return statusBefore(out.Jobs[i], out.Jobs[k]) })
		sort.Slice(out.Machines, func(i, k int) bool { return out.Machines[i].ID < out.Machines[k].ID })
		return nil
	}
	var err error
	if forStatus {
		// Observation never performs maintenance, usage accounting, rollback
		// snapshots or persistence. Tick/check-ins own those durable mutations.
		q.mu.RLock()
		defer q.mu.RUnlock()
		err = project()
		if err == nil {
			out.Usage, err = q.usageLocked(q.clock.Now().Add(-7 * 24 * time.Hour))
		}
	} else {
		err = q.update(func() error { q.maintain(); return project() })
		if err == nil {
			q.mu.Lock()
			out.Usage, err = q.usageLocked(q.clock.Now().Add(-7 * 24 * time.Hour))
			q.mu.Unlock()
		}
	}
	return out, err
}
func (q *Queue) AppendLog(l LogChunk) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	j, err := q.current(l.Result)
	if err != nil {
		return err
	}
	if j.Spec.Logs == "none" {
		return nil
	}
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if l.Offset > 0 {
		res, err := tx.Exec("INSERT OR IGNORE INTO log_offsets(job,attempt,offset) VALUES(?,?,?)", l.ID, l.Attempt, l.Offset)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return tx.Commit()
		}
	}
	if _, err = tx.Exec("INSERT INTO logs(job,attempt,text) VALUES(?,?,?)", l.ID, l.Attempt, l.Text); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if m := q.s.Machines[l.Machine]; m != nil && m.Session == l.Session {
		m.LastSeen = q.clock.Now()
		delete(q.unreported, m.ID)
	}
	return nil
}

type Log struct {
	Seq     int64  `json:"seq"`
	Attempt int    `json:"attempt"`
	Text    string `json:"text"`
}

func (q *Queue) Logs(id string, after int64) ([]Log, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.s.Jobs[id] == nil {
		jobs := []*Job{}
		for _, j := range q.s.Jobs {
			jobs = append(jobs, j)
		}
		j, err := FindJob(jobs, id)
		if err != nil {
			return nil, err
		}
		id = j.ID
	}
	if q.s.Jobs[id].Spec.Logs == "none" {
		return []Log{}, nil
	}
	rows, err := q.db.Query("SELECT seq,attempt,text FROM logs WHERE job=? AND seq>? ORDER BY seq LIMIT 256", id, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Log{}
	for rows.Next() {
		var l Log
		if err := rows.Scan(&l.Seq, &l.Attempt, &l.Text); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if text := q.s.Jobs[id].EnvironmentFailureLog; text != "" && len(out) < 256 {
		var seq int64
		if err := q.db.QueryRow("SELECT COALESCE(MAX(seq),0)+1 FROM logs WHERE job=?", id).Scan(&seq); err != nil {
			return nil, err
		}
		if seq > after {
			out = append(out, Log{Seq: seq, Text: text})
		}
	}
	return out, nil
}

func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}

func cloneJob(j *Job) Job {
	data, _ := json.Marshal(j)
	var out Job
	_ = json.Unmarshal(data, &out)
	return out
}
func cloneMachine(m *Machine) Machine {
	data, _ := json.Marshal(m)
	var out Machine
	_ = json.Unmarshal(data, &out)
	return out
}

// Startup acknowledgements use queue time, avoiding cross-machine clock skew.
func (q *Queue) markStarted(j *Job) {
	if j.StartedAt == nil {
		now := q.clock.Now()
		j.StartedAt = &now
		q.beginWorkTiming(j)
		if j.Spec.StartBy != nil && j.StartWindow == 0 {
			j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
		}
		j.Reason = ""
		j.StartLatencySeconds = now.Sub(j.Queued).Seconds()
		if j.Spec.Priority == "production" {
			q.s.ProductionStarts = append(q.s.ProductionStarts, ProductionStart{At: now, Job: j.ID, Seconds: j.StartLatencySeconds})
		}
	}
}
func (q *Queue) Started(r Result) error {
	var rejected error
	err := q.update(func() error {
		q.maintain()
		j, e := q.current(r)
		if e != nil {
			rejected = e
			return nil
		}
		if j.State != "running" || cancelled(j) || (j.Spec.StartBy != nil && j.StartedAt != nil) {
			rejected = ErrStale
			return nil
		}
		m := q.s.Machines[r.Machine]
		q.refreshHostMemory()
		if !q.hostLaunchAllowed(m, r.Session) || !q.startPlacementAllowed(j) {
			rejected = ErrStale
			return nil
		}
		q.markStarted(j)
		return nil
	})
	if err != nil {
		return err
	}
	return rejected
}

func (q *Queue) renewRecoveryStartWindow(j *Job) {
	// Planned leave keeps its original deadline; queue maintenance stops retain
	// the recovery window for already-started attempts.
	recovering := j.State == "running" || j.State == "stopping" && plannedStop(j)
	if j.Spec.Priority == "production" && recovering && j.StartedAt != nil && j.Spec.StartBy != nil {
		// Recovery is bounded from loss detection, using the original first-start window.
		if j.StartWindow == 0 {
			j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
		}
		deadline := q.clock.Now().Add(j.StartWindow)
		j.Spec.StartBy = &deadline
	}
}

type attemptProgress struct {
	durable int64
	path    string
	err     error
}

// Reuse the same recovery/stop rules for each legacy or keyed report.
func (q *Queue) reconcileAttemptReport(m *Machine, report *AttemptReport, progress attemptProgress) AttemptDirective {
	reply := AttemptDirective{Execution: Execution{ID: report.ID, Attempt: report.Attempt}, Session: report.Session}
	j := q.s.Jobs[report.ID]
	if j != nil && !terminal(j.State) && !cancelled(j) && (j.State == "queued" || j.Attempt != report.Attempt || j.Machine != m.ID || j.Session != m.Session) {
		for _, copy := range j.Copies {
			canRecover := q.machineRecoveryFits(m, j, copy)
			if canRecover && q.placementAllowed(j, m) && copy.Recoverable && copy.Attempt == report.Attempt && copy.Machine == m.ID && copy.Session == m.Session && j.State != "queued" && (m.CheckpointUnavailable || !q.checkpointsAvailable() || progress.err != nil || (progress.path != "" && progress.path != attemptCheckpoint(*j))) {
				reply.Hold = true
				return reply
			}
			if canRecover && q.placementAllowed(j, m) && copy.Recoverable && copy.Attempt == report.Attempt && copy.Machine == m.ID && copy.Session == m.Session && (j.State == "queued" || (j.Spec.Resumable && report.Progress > max(0, progress.durable))) {
				for i := range j.Copies {
					j.Copies[i].Recoverable = false
				}
				j.Attempt = copy.Attempt
				j.Machine = m.ID
				j.Session = m.Session
				j.AllocatedDevices = slices.Clone(copy.AllocatedDevices)
				j.State = "running"
				j.ReleaseConfirmed = false
				j.StopAt = nil
				j.Reason = ""
				j.Progress = report.Progress
				break
			}
		}
	}
	if j == nil || j.Attempt != report.Attempt || j.Machine != m.ID || j.Session != m.Session || terminal(j.State) || j.State == "queued" {
		if j != nil {
			for i := range j.Copies {
				if j.Copies[i].Attempt == report.Attempt {
					j.Copies[i].Recoverable = false
				}
			}
		}
		reply.Stop = true
		reply.Kill = true
		return reply
	}
	if report.Started && j.Spec.StartBy == nil && j.StartedAt == nil && j.State == "running" && q.startPlacementAllowed(j) {
		q.markStarted(j)
	}
	j.Progress = report.Progress
	if j.State == "stopping" {
		reply.PriorityStop = priorityStop(j)
		reply.Stop = true
		reply.Kill = expired(j.StopAt, q.clock.Now())
		reply.GraceSeconds = j.StopAt.Sub(q.clock.Now()).Seconds()
	}
	return reply
}

// The bounded legacy adapter retains its original scalar recovery limit.
func (q *Queue) workerHasOtherAttempt(machine, session, job string) bool {
	for _, j := range q.s.Jobs {
		if j.Machine == machine && j.Session == session && j.ID != job && (j.State == "running" || j.State == "stopping") {
			return true
		}
	}
	return false
}

// assign persists the exact target before delivering it through either wire form.
func (q *Queue) assign(j *Job, m *Machine) Job {
	// The planner returns a target-specific copy. Persist its exact
	// reservation on the live attempt before charging any resources.
	selected := j
	j = q.s.Jobs[selected.ID]
	j.Spec.Needs = selected.Spec.Needs
	if e := qualifiedExtension(selected, m); e != nil {
		j.Extension = &ExtensionSelection{Name: e.Name, Version: e.Version, Digest: e.Digest}
	}
	j.State = "running"
	j.ReleaseConfirmed = false
	j.RequeuedByOperator = false
	j.StartedAt = nil
	j.Timing = nil
	now := q.clock.Now()
	j.PriorityPolicy = q.priorityMatch(j.Spec)
	j.AssignedAt = &now
	j.FinishedAt = nil
	j.StartLatencySeconds = 0
	m.FreeMemoryGB = max(0, m.FreeMemoryGB-j.Spec.Needs.MemoryGB)
	j.Starts = max(j.Starts, j.Attempt) + 1
	j.Attempt = j.Starts
	if j.FileTransport {
		j.OutputRefs = nil
		j.ResumeFiles = bestFileCheckpoint(j)
	}
	if j.Spec.extensionJob() && !j.FileTransport {
		j.OutputFiles = map[string]string{}
		for name, prefix := range j.Spec.Outputs {
			j.OutputFiles[name] = outputRelative(*j, name, prefix)
		}
	}
	j.Progress = 0
	j.Machine = m.ID
	j.AllocatedDevices = nil
	if m.Devices != nil && !j.Spec.Needs.cpuOnly() {
		j.AllocatedDevices = []string{m.Device}
	}
	j.Copies = append(j.Copies, Copy{AllocatedDevices: slices.Clone(j.AllocatedDevices), Machine: m.ID, Session: m.Session, Attempt: j.Attempt, Extension: j.Extension, ReleaseReason: "awaiting_release"})
	j.Session = m.Session
	j.Reason = ""
	q.refreshHostMemory()
	return cloneJob(j)
}
