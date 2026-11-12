package myce

import (
	"cmp"
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
	"sync/atomic"
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
	DeviceUse map[string]DeviceUse `json:"device_use,omitempty"`
	// Frozen accepted-contact validity is only for stopped-queue conversion.
	// A running/restarted queue always requires live session contact evidence.
	ContactEvidence        map[string]contactEvidence    `json:"contact_evidence,omitempty"`
	EfficiencyAt           time.Time                     `json:"efficiency_at,omitempty"`
	EfficiencyHosts        map[string]EfficiencyHost     `json:"efficiency_hosts,omitempty"`
	EfficiencyHealth       map[string]efficiencyBaseline `json:"efficiency_health,omitempty"`
	StrandedDemand         []StrandedDemand              `json:"stranded_demand,omitempty"`
	PublicationHorizon     time.Duration                 `json:"publication_horizon,omitempty"`
	ToolInventories        map[string]toolInventoryFence `json:"tool_inventories,omitempty"`
	FileInputClaims        map[string]FileInputClaim     `json:"file_input_claims,omitempty"`
	ModelIntervalSince     *time.Time                    `json:"model_interval_since,omitempty"`
	ModelIntervalHighWater int                           `json:"model_interval_high_water,omitempty"`
	// Read-only parked policy. Preserve retained DB records; these fields no
	// longer activate admission or replace the configured file-store guards.
	StorageMigration       json.RawMessage                `json:"storage_migration,omitempty"`
	StorageProtection      json.RawMessage                `json:"storage_protection,omitempty"`
	StorageAdmissionActive bool                           `json:"storage_admission_active,omitempty"`
	MemoryExplorations     map[string]memoryExploration   `json:"memory_explorations,omitempty"`
	StorageObservations    map[string]StorageObservation  `json:"storage_observations,omitempty"`
	ExternalMemory         map[string]*externalMemoryRing `json:"external_memory,omitempty"`
	HostLaunchMemory       map[string]*hostLaunchMemory   `json:"host_launch_memory,omitempty"`
	AttemptOutputUploads   map[string]time.Time           `json:"attempt_output_uploads,omitempty"`
	Health                 healthAlerts                   `json:"health_alerts,omitempty"`
	FleetUpgrade           *fleetUpgrade                  `json:"fleet_upgrade,omitempty"`
	FleetSuperseded        []fleetAbandoned               `json:"fleet_superseded,omitempty"`
	ProductionLeases       map[string]ProductionLease     `json:"production_leases,omitempty"`

	// Positive orphan eligibility, persisted with the existing queue state.
	// Zero time means an input upload has not finished publication yet.
	JobInputUploads map[string]time.Time `json:"job_input_uploads,omitempty"`
	// Internally created bundles remain eligible after their last reference goes.
	BundleUploads map[string]time.Time `json:"bundle_uploads,omitempty"`
	// Expired blob cleanup survives removal of its job/key answer, deduplicated by hash.
	RetiredJobFiles map[string]bool `json:"retired_job_files,omitempty"`

	WorkObservations     map[string]WorkObservation `json:"work_observations,omitempty"`
	EnrollmentRelease    *EnrollmentRelease         `json:"enrollment_release,omitempty"`
	EnrollmentReleaseKey []byte                     `json:"enrollment_release_key,omitempty"`

	PriorityPolicy *PriorityPolicyRecord `json:"priority_policy,omitempty"`
	// Actual fleet contact and the first reconnect after silence are separate
	// from each worker's LastSeen. Reconnects never renew an active outage epoch.
	LastWorkerContact time.Time `json:"last_worker_contact,omitempty"`
	NetworkRecoveryAt time.Time `json:"network_recovery_at,omitempty"`

	Reservations        map[string]Reservation                  `json:"reservations,omitempty"`
	ComponentExclusions map[string]ComponentExclusion           `json:"component_exclusions,omitempty"`
	DeviceMaintenance   map[string]map[string]DeviceMaintenance `json:"device_maintenance,omitempty"`

	Schema           int                        `json:"schema_version"`
	PreparationPlans map[string]HostLaunch      `json:"preparation_plans,omitempty"`
	HostMaintenance  map[string]HostMaintenance `json:"host_maintenance,omitempty"`

	// The last accepted startup host file; live policy may differ after
	// `myce policy set`.
	HostConfig map[string]HostPolicy `json:"host_config,omitempty"`

	Submissions      map[string]*submissionRecord `json:"submissions,omitempty"`
	ProgramNeeds     map[string]SizeHint          `json:"program_needs,omitempty"`
	Extensions       map[string]Extension         `json:"extensions,omitempty"`
	Hosts            map[string]*HostState        `json:"hosts,omitempty"`
	HostReleases     map[string]HostRelease       `json:"host_releases,omitempty"`
	BatchReleases    map[string]batchRelease      `json:"batch_releases,omitempty"`
	UsageMaintenance time.Time                    `json:"usage_maintenance,omitempty"`
	ProductionStarts []ProductionStart            `json:"production_starts,omitempty"`
	Policies         map[string]ProjectPolicy     `json:"project_policies,omitempty"`
	// Project-owned production approvals by machine kind (MY-51, #1503); nil
	// until converted from the host form below.
	ParityKinds *parityKindStore `json:"parity_kinds,omitempty"`
	// Host-form mirror of ParityKinds for a rollback to the previous release
	// (#1341, MY-42); removed next release.
	ProjectParity    *projectParityStore  `json:"project_parity,omitempty"`
	FairUsageVersion int                  `json:"fair_usage_version,omitempty"`
	FairUsage        map[string]FairUsage `json:"fair_usage,omitempty"`
	UsageAt          time.Time            `json:"usage_at,omitempty"`
	Preemptions      []Preemption         `json:"preemptions,omitempty"`

	// Per adapter/profile entry without targets (#853): recipe, proven and skipped types.
	EntryCoverage map[string]entryCoverageRecord `json:"entry_coverage,omitempty"`

	SyncRequests []SyncRequest         `json:"sync_requests,omitempty"`
	ModelUses    map[string]ModelUse   `json:"model_uses,omitempty"`
	Pins         []ModelPin            `json:"pins,omitempty"`
	ModelSlots   map[string]*ModelSlot `json:"model_slots,omitempty"`
	Jobs         map[string]*Job       `json:"jobs"`
	Machines     map[string]*Machine   `json:"machines"`
	Front        int64                 `json:"front"`
}
type Queue struct {
	readInvalidations       map[string]bool // q.mu; applied atomically with capture publication.
	readDependenciesInvalid bool

	readGeneration       *readGeneration
	publishedReads       atomic.Pointer[publishedReadIndex]
	readWarm             readWarmer // Decodes new published records off q.mu.
	readRoot             *readNode  // q.mu; immutable committed input generations.
	readModels           *ModelConfig
	readEnvironments     *EnvironmentCatalog
	readRuntime          *observationState
	readSamples          map[string]map[attemptKey]hostMemorySample
	detachedRead         bool          // Private immutable source: never owns the live writer mutex.
	hostHealthReports    atomic.Uint64 // Queue receipt order; never worker wall time.
	requestBudgets       requestBudgets
	evidencePending      map[string]bool
	evidenceRows         []string
	evidenceBytes        uint64
	evidenceDeferred     map[string]bool
	evidenceLearned      map[string]bool
	evidenceLedgerCounts map[string]int

	observationCacheBudget observationCacheBudget
	evidenceGeneration     uint64
	evidenceMaintenance    evidenceMaintenance
	evidenceRetention      *EvidenceRetention
	waitStates             *waitStateIndex // scalar committed wait observations; separate reader mutex.
	modelIntervalWrites    []ModelIntervalEvent
	efficiencyView         *EfficiencyReport // immutable maintenance projection; published only after commit.
	nasInspections         *resolveNASCache  // ephemeral evidence shared by every listener for this queue.
	// Qualification removes occupancy, but retains diagnostic history/inventory.
	memoryOrigin *Queue
	// Only detached observation views set this page selection.
	observationSelection *statusSelection
	deliveryRoot         string
	deliveryMu           sync.Mutex
	deliveryActive       *deliveryOperation                          // q.mu; one outstanding NAS publication per root.
	deliveryPublish      func(Job, *FileStore, string, func()) error // Test seam; set before use.
	// Ephemeral exact-attempt samples: restart cannot renew a cached credit.
	admissionWaitDecisions map[string]map[string]admissionWaitDecision // transient actual planner decisions
	admissionTrace         *admissionTrace
	admissionWaitPending   bool                                 // committed maintenance requests an off-lock observation
	admissionWaitLast      atomic.Pointer[admissionWaitCapture] // completed observation inputs; one retained generation
	admissionWaitBuild     admissionWaitBuilder                 // post-commit background observation, independent of q.mu
	hostMemorySamples      map[string]map[attemptKey]hostMemorySample
	hostMemoryLedgers      map[string]*HostMemoryLedger // Immutable after each refresh; shared by diagnostics.
	memoryLedgerGeneration uint64
	memoryAdmissionCapture *memoryAdmissionCapture // Only a writer's existing planning pass records comparisons.

	// Built once on a read-only cancellation projection, never on the live queue.
	cancellationMembersByParent map[string][]*Job

	requestLatencies                requestLatencies
	requestHealth                   requestHealth
	requestHealthAt                 time.Time
	healthStoreProbe                checkpointAvailability
	healthNotifier                  string
	healthStorageDelay              time.Duration
	fleetQueue                      *fleetQueueRuntime
	fleetCadence                    map[string]*fleetProofCadence // proof session -> measured proof cadence
	fleetService                    *ServiceConfig
	onboardingNAS                   NASIdentity
	productionInfrastructureRetries int

	fileReferenceRevision      uint64
	environmentRevision        uint64
	fileLifecycle              sync.RWMutex // Reference publication versus unlink; acquire before q.mu.
	fileRetention              time.Duration
	fileCollectionDeferred     bool // q.mu; warn once per contiguous deferred streak.
	files                      *FileStore
	startupFiles               *fileStoreConfiguration
	fileStoreProductionReserve int64
	fileCheckpoints            bool
	submitVerificationSlot     chan struct{}
	nasProbe                   checkpointAvailability
	nasProgress                checkpointIO[int64]
	shards                     checkpointShards
	checkpointClaims           map[string]checkpointExpiryClaim // Under q.mu; in memory, reissued after restart.
	checkpointGrace            time.Duration
	checkpointExpiryStatus     *atomic.Pointer[CheckpointExpiryStatus] // Shared by detached views; counts are published at commit, after read capture.
	nasAvailable               bool

	nextApprovalWarning time.Time

	// Status views carry the admissions summary counted under q.mu, so
	// dependency jobs need no envelope copy (#585).
	statusAdmissions *[]HostAdmissions
	observationScan  *placementScan // Only an immutable, detached wide-read generation.
	observationLive  map[string]*Job

	// simulation bypasses durable storage only; all scheduling/accounting paths remain shared.
	checkpointProbe func() bool // Availability seam; bounded real I/O is the default.
	simulation      bool
	simID           int64
	uploadSpool     uploadSpool
	logRetention    time.Duration
	environments    EnvironmentCatalog
	productionGrace time.Duration
	wake            chan struct{}
	models          ModelConfig
	calls           map[string]*ModelCall
	lock            *os.File
	mu              queueMutex
	stopping        atomic.Bool // Fence before waiting for a queue transaction or either listener.
	shutdownOnce    sync.Once
	shutdownContext context.Context
	shutdownCancel  context.CancelFunc
	contactMu       sync.Mutex
	contacts        map[string]*workerContact
	workerPolls     map[string]map[string]*workerPoll              // contactMu; machine -> session; request lifecycle, not transactional.
	healthPreparing map[string]map[*hostHealthPreparation]struct{} // contactMu; request lifecycle, not transactional.
	holdClock       Clock                                          // Optional observational clock; never used for queue decisions.
	updateHolds     queueHoldSampler
	statusHolds     queueHoldSampler
	statusCache     statusCache
	jobSnapshotsMu  sync.Mutex // Only cache metadata; never acquired across projection or q.mu acquisition.
	jobSnapshots    map[string]*jobSnapshotCache
	observeRead     func(string, time.Duration, time.Duration)
	observeRequest  func(string, time.Duration, time.Duration, time.Duration, time.Duration)
	usageReader     *sql.DB
	db              *sql.DB
	persisted       map[string][]byte
	slimJobRecords  bool // Write settled job rows with shared values; see job_record_codec.go.
	records         *queueRecords
	observeUpdate   func(time.Duration, time.Duration)
	clock           Clock
	root            string
	restartUntil    time.Time
	unreported      map[string]bool
	networkQuiet    bool
	lossTimeouts    [3]time.Duration
	usageWrites     []usageSample
	s               state

	restartControlUntil time.Duration       // Available startup service, not blocked wall time.
	contactTransaction  *contactTransaction // Lazy, per-worker undo while q.mu owns a transaction.

	persistencePageBytes    int
	histories               *historySnapshot // Measurement histories as SQLite holds them (history_persistence.go).
	historiesPending        *historySnapshot
	flushHistoriesNow       bool
	observePersistence      func(queuePersistenceTiming)
	observePersistenceHold  func(queueHoldPhases)
	maintenance             *queueMaintenance
	observeBodyPhase        func(string, time.Duration) // Optional diagnostic observer; never changes decisions.
	observeUpdatePhases     func([5]time.Duration)      // Optional transaction phase observer.
	observeSubmissionAdvice func(submissionAdviceTiming)
	observeMemoryHistory    func(int, bool) // Test observer: jobs visited by a full evidence scan.

	memoryPlanning          *memoryPlanningCache // Only while a read-only placement scan is active.
	memoryObservationIndex  *memoryObservationCatalog
	retainedMemoryIndex     retainedMemoryIndex // Immutable cold facts; record mutations invalidate.
	retainedMemoryRevision  uint64
	expiredMemory           []learnedMemoryFact // Memory evidence of expired job records; append-only.
	checkinMemoryIdentities map[*Job]string     // Fenced, transaction-local planner input.
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

// WithFileStore selects storage before startup verifies frozen bundle references.
// An empty root uses the queue database's .files directory.
func WithFileStore(root string, limit int64, checkpoints bool) QueueOption {
	return func(q *Queue) {
		q.startupFiles = &fileStoreConfiguration{root, limit, checkpoints}
	}
}

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
	q := &Queue{slimJobRecords: slimJobRecordsDefault, fileRetention: DefaultFileRetention, logRetention: DefaultLogRetention, uploadSpool: uploadSpool{limit: DefaultUploadSpoolBytes, defaultDir: path + ".uploads"}, productionGrace: 10 * time.Second, productionInfrastructureRetries: DefaultProductionInfrastructureRetries, wake: make(chan struct{}), lock: lock, db: db, clock: clock, root: abs, s: state{Schema: persistedSchema, FairUsageVersion: 1, Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	q.mu.control = &controlClock{clock: func() time.Time { return q.clock.Now() }, at: clock.Now()}
	q.admissionWaitBuild.synchronous = synchronousAdmissionWaitDefault
	q.healthStoreProbe.timeout = heartbeatFilesystemBudget
	q.nasProbe.timeout = heartbeatFilesystemBudget
	q.nasProgress.timeout = heartbeatFilesystemBudget
	for _, option := range options {
		option(q)
	}
	if q.deliveryRoot != "" {
		q.deliveryRoot, err = filepath.Abs(q.deliveryRoot)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS model_intervals (seq INTEGER PRIMARY KEY, data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS enrollment_tickets (digest TEXT PRIMARY KEY, pin TEXT NOT NULL, expires INTEGER NOT NULL, used INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS queue_state (key TEXT PRIMARY KEY, data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS logs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT NOT NULL, attempt INTEGER NOT NULL, text TEXT NOT NULL); CREATE TABLE IF NOT EXISTS log_offsets (job TEXT, attempt INTEGER, offset INTEGER, PRIMARY KEY(job,attempt,offset)); CREATE TABLE IF NOT EXISTS environment_log_offsets (job TEXT, machine TEXT, session TEXT, key TEXT, offset INTEGER, PRIMARY KEY(job,machine,session,key,offset)); CREATE TABLE IF NOT EXISTS environment_build_logs (machine TEXT, session TEXT, key TEXT, offset INTEGER, text TEXT, PRIMARY KEY(machine,session,key,offset)); CREATE INDEX IF NOT EXISTS logs_job ON logs(job,seq); CREATE TABLE IF NOT EXISTS job_summaries (id TEXT PRIMARY KEY, finished INTEGER NOT NULL, summary BLOB NOT NULL, learned BLOB); CREATE TABLE IF NOT EXISTS usage_samples (at INTEGER NOT NULL, span INTEGER NOT NULL, machine TEXT NOT NULL, device TEXT NOT NULL, project TEXT NOT NULL, job TEXT NOT NULL, model TEXT NOT NULL, busy REAL NOT NULL, idle REAL NOT NULL, pinned REAL NOT NULL, memory_seconds REAL NOT NULL, observed REAL NOT NULL, PRIMARY KEY(at,span,machine,device,project,job,model));`); err != nil {
		db.Close()
		return nil, err
	}
	if err = db.QueryRow("PRAGMA page_size").Scan(&q.persistencePageBytes); err != nil {
		db.Close()
		return nil, err
	}
	var data []byte
	err = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data)
	if err == nil {
		q.s.FairUsageVersion = 0
		data, err = readQueueState(db, data)
		if err == nil {
			err = json.Unmarshal(data, &q.s)
		}
	}
	if err != nil && err != sql.ErrNoRows {
		db.Close()
		return nil, err
	}
	if q.s.FairUsageVersion != 1 {
		db.Close()
		return nil, errors.New("unsupported fair-usage format; current and previous releases require version 1")
	}
	// Inventories are refreshed by workers after restart.
	for _, m := range q.s.Machines {
		m.ToolInventory = nil
	}
	// A previous binary can rewrite away new state fields while retaining this
	// additive table. Recover allocation before any startup transaction emits an
	// event; the retained SQLite sequence is authoritative across re-upgrades.
	if err = db.QueryRow("SELECT COALESCE(MAX(seq),0) FROM model_intervals").Scan(&q.s.ModelIntervalHighWater); err != nil {
		db.Close()
		return nil, err
	}
	if err = q.loadPersistedState(); err != nil {
		db.Close()
		return nil, err
	}
	q.retireWholeMachineHolds()
	fileRoot, fileLimit, fileCheckpoints := "", int64(100<<30), false
	if q.startupFiles != nil {
		fileRoot, fileLimit, fileCheckpoints = q.startupFiles.root, q.startupFiles.limit, q.startupFiles.checkpoints
	}
	if fileRoot == "" {
		fileRoot = path + ".files"
	}
	if err = q.ConfigureFileStore(fileRoot, fileLimit, fileCheckpoints); err == nil {
		err = q.verifyBundleFiles()
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	q.unreported = map[string]bool{}
	q.contacts = map[string]*workerContact{}
	for id, m := range q.s.Machines {
		q.contacts[id] = &workerContact{session: m.Session}
	}
	if len(data) > 0 {
		q.restartUntil = clock.Now().Add(RestartGrace)
		q.restartControlUntil = q.controlNow() + RestartGrace
		for id, m := range q.s.Machines {
			if !m.Gone {
				q.unreported[id] = true
			}
		}
	}
	if err = q.update(func() error {
		q.migrateParityKinds()
		q.mirrorProjectParity()
		q.convertJobRecords()
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	if err = q.ConfigureModels(ModelConfig{Idle: 10 * time.Minute, Wait: 5 * time.Minute}); err != nil {
		db.Close()
		return nil, err
	}
	if err = q.ConfigureUploadSpool("", DefaultUploadSpoolBytes); err != nil {
		db.Close()
		return nil, err
	}
	q.usageReader, err = openUsageReader(path)
	if err != nil {
		db.Close()
		return nil, err
	}
	success = true
	if q.checkpointExpiryStatus == nil {
		q.checkpointExpiryStatus = &atomic.Pointer[CheckpointExpiryStatus]{}
	}
	q.kickCheckpointShards()
	return q, nil
}

// SetRestartGrace configures the recovery window from this queue's startup.
func (q *Queue) SetRestartGrace(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.unreported) > 0 {
		q.restartUntil = q.clock.Now().Add(d)
		q.restartControlUntil = q.controlNow() + d
		q.republishReads()
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
	q.republishReads()
	return nil
}

// SetLossTimeouts sets optional operator minimums. Zero uses measured timing.
// A minimum can delay detection; it can never override queue-delay protection.
func (q *Queue) SetLossTimeouts(production, deadline, experiment time.Duration) error {
	if production < 0 || deadline < 0 || experiment < 0 {
		return errors.New("loss timeout minimums must be nonnegative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lossTimeouts = [3]time.Duration{production, deadline, experiment}
	q.republishReads()
	return nil
}
func (q *Queue) machineLossTimeout(id string) time.Duration {
	timeout := time.Duration(-1)
	add := func(priority string) {
		d := q.lossTimeouts[min(2, legacyRank(priority))]
		if timeout < 0 || d < timeout {
			timeout = d
		}
	}
	for _, j := range q.liveJobs() {
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
	if timeout < 0 {
		timeout = q.lossTimeouts[2]
	}
	if m := q.s.Machines[id]; m != nil {
		measured := q.workerLossAllowance(m)
		if measured == 0 {
			return time.Duration(1<<63 - 1)
		}
		timeout = max(timeout, measured)
	}
	if timeout == 0 {
		// No advertised/observed cadence and no operator minimum: unknown.
		return time.Duration(1<<63 - 1)
	}
	return timeout
}

// Simulated workers have an always-available pretend NAS; production still probes it.
func (q *Queue) checkpointsAvailable() bool { return q.simulation || q.nasAvailable }
func (q *Queue) checkpointReady(j *Job, m *Machine) bool {
	return !needsCheckpoint(j) || (!m.CheckpointUnavailable && q.checkpointsAvailable())
}
func checkpointAvailable(root string, progress func()) bool {
	info, err := os.Stat(root)
	progress()
	if err != nil || !info.IsDir() {
		return false
	}
	f, err := os.CreateTemp(root, ".myce-probe-")
	progress()
	if err != nil {
		return false
	}
	name := f.Name()
	err = f.Sync()
	progress()
	closeErr := f.Close()
	progress()
	removeErr := os.Remove(name)
	progress()
	return err == nil && closeErr == nil && removeErr == nil
}

// BeginShutdown fences maintenance and in-flight mutations before API shutdown.
// Workers retry rejected reports against the replacement queue.
func (q *Queue) BeginShutdown() {
	first := !q.stopping.Swap(true)
	q.initShutdown()
	q.shutdownCancel()
	if first {
		log.Printf("event=queue_shutdown phase=fenced")
	}
}

func (q *Queue) Close() error {
	err := q.closeCheckpointShards()
	if q.stopping.Load() {
		log.Printf("event=queue_shutdown phase=storage_wait")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if flushErr := q.flushHistories(); flushErr != nil {
		err = errors.Join(err, fmt.Errorf("write measurement histories at close: %w", flushErr))
	}
	if q.usageReader != nil {
		err = errors.Join(err, q.usageReader.Close())
	}
	err = errors.Join(err, q.db.Close())
	if q.uploadSpool.lock != nil {
		err = errors.Join(err, q.uploadSpool.lock.Close())
		q.uploadSpool.lock = nil
	}
	if q.lock != nil {
		err = errors.Join(err, syscall.Flock(int(q.lock.Fd()), syscall.LOCK_UN), q.lock.Close())
		q.lock = nil
	}
	if q.stopping.Load() {
		log.Printf("event=queue_shutdown phase=storage_closed clean=%t", err == nil)
	}
	return err
}

// Every mutation is atomic and durable; failed writes restore the previous state.
func (q *Queue) update(fn func() error) error {
	return q.updateTimed(fn, nil)
}

func (q *Queue) updateTimed(fn func() error, timing *queueUpdateTiming) error {
	return q.updateRequest(context.Background(), fn, timing)
}

func (q *Queue) updateRequest(ctx context.Context, fn func() error, timing *queueUpdateTiming) error {
	return q.updateTransaction(ctx, fn, timing, false, nil)
}

// Upload persistence may abandon a pending transaction before it owns q.mu.
// Ordinary transactions must still run cleanup when their request is cancelled.
func (q *Queue) updateUploadRequest(ctx context.Context, fn func() error, timing *queueUpdateTiming) error {
	return q.updateTransaction(ctx, fn, timing, true, nil)
}

func (q *Queue) updateTransaction(ctx context.Context, fn func() error, timing *queueUpdateTiming, cancelWait bool, committed func()) error {
	return q.updateTransactionMode(ctx, fn, timing, cancelWait, committed, true)
}

func (q *Queue) updateTransactionMode(ctx context.Context, fn func() error, timing *queueUpdateTiming, cancelWait bool, committed func(), captureWait bool) error {
	if q.stopping.Load() {
		return queueStopping()
	}
	var waitCapture *admissionWaitCapture
	available := true
	if !q.simulation {
		if q.checkpointProbe != nil {
			available = q.checkpointProbe()
		} else {
			available, _ = q.nasProbe.runProgress(q.root, func(progress func()) (bool, error) {
				return checkpointAvailable(q.root, progress), nil
			})
		}
	}
	if timing == nil {
		timing = &queueUpdateTiming{}
	}
	var writer sync.Locker = &q.mu
	if ctx.Value(submissionTurnKey{}) != nil {
		writer = submissionMutex{&q.mu}
	}
	if cancelWait {
		if err := timing.lockContext(ctx, writer); err != nil {
			return err
		}
	} else {
		timing.lock(writer)
	}
	hold := newQueueHoldPhases(q.updateHolds.startClock(q.holdClock), q.holdClock)
	q.nasAvailable = available
	defer func() {
		duration := hold.finish()
		if q.observePersistenceHold != nil {
			q.observePersistenceHold(hold)
		}
		if q.observeUpdatePhases != nil {
			q.observeUpdatePhases(hold.phases)
		}
		hold.persistence = timing.persistence
		timing.observe(q.observeUpdate)
		unlockTimedRequest(ctx, &q.mu, timing)
		logQueueHold(duration, "update", fn, &hold)
		if waitCapture != nil {
			q.enqueueAdmissionWait(waitCapture)
		}
	}()
	if q.stopping.Load() {
		return queueStopping()
	}
	previousMaintenance := q.maintenance
	q.maintenance = &queueMaintenance{}
	defer func() { q.maintenance = previousMaintenance }()
	if q.simulation {
		hold.next(holdUsage)
		q.usageWrites = nil
		q.accountUsage(q.clock.Now())
		hold.next(holdBody)
		err := fn() // Simulation aborts on any error; it never resumes a failed mutation.
		if err == nil {
			q.settleMaintenance()
		}
		if err == nil && committed != nil {
			committed()
		}
		return err
	}
	q.admissionWaitPending = false
	q.admissionWaitDecisions = nil
	q.admissionTrace = nil
	q.memoryObservationIndex = nil
	defer func() { q.memoryObservationIndex = nil }()
	old, err := q.beginRecords()
	if err != nil {
		return err
	}
	var audit func() error
	if auditQueueRecords != nil {
		audit = auditQueueRecords(q)
	}
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
	contacts := &contactTransaction{}
	q.contactTransaction = contacts
	defer func() { q.contactTransaction = nil }()
	oldModels, oldEnvironments := q.models, q.environments
	oldEnvironmentRevision := q.environmentRevision
	hold.next(holdUsage)
	q.usageWrites = nil
	q.modelIntervalWrites = nil
	q.accountUsage(q.clock.Now())
	hold.next(holdBody)
	err = fn()
	// A request that loses the shutdown race retries from durable worker state.
	// Do not publish its speculative loss/maintenance decisions after the fence.
	if err == nil && q.stopping.Load() {
		err = queueStopping()
	}
	if err == nil {
		q.settleMaintenance()
		q.refreshEfficiency()
	}
	if err == nil {
		q.observeModelIntervals()
	}
	if err == nil {
		q.detectHealth()
		q.invalidateWorkCurves()
	}
	if err == nil {
		q.closeChangedAdmissionWaits()
		q.maintainSubmissions()
		q.releaseFileUploadMarkers()
		hold.next(holdPersist)
		if audit != nil {
			err = audit()
		}
		if err == nil {
			err = q.persistUsage(timing)
		}
	}
	if err == nil {
		q.fileReferenceRevision++
		q.observationCacheBudget.mutation(q.clock.Now())
	}
	hold.next(holdNotify)
	if err == nil && q.wake != nil {
		close(q.wake)
		q.wake = make(chan struct{})
	}
	if err != nil {
		q.models, q.environments = oldModels, oldEnvironments
		q.environmentRevision = oldEnvironmentRevision
		q.calls = oldCalls
		q.unreported = oldUnreported
		q.networkQuiet = oldQuiet
		q.rollbackContacts(contacts)
		q.rollbackRecords(old)
		q.hostMemoryLedgers = nil // Never retain diagnostic inputs from an aborted writer.
	}

	if err == nil {
		q.publishWaitStates(oldCalls)
		q.invalidateCallSnapshots(oldCalls)
		q.invalidateTerminalStatus()
		// Freeze contact while the transaction's active-job index is still
		// selected. After commitRecords ends the journal, liveJobs includes
		// retained history for callers that may mutate records directly.
		q.publishReadRuntime()
		q.commitRecords()
		if captureWait && q.admissionWaitPending {
			waitCapture = q.freezeAdmissionWait()
		}
		if committed != nil {
			committed()
		}
	}
	return err
}
func (q *Queue) Submit(s Spec) (*Job, error) {
	return q.SubmitContext(context.Background(), s)
}

// SubmitContext snapshots input custody before bulk verification. Key replay,
// cancellation and job creation still serialize in the final update transaction.
func (q *Queue) SubmitContext(ctx context.Context, s Spec) (*Job, error) {
	return q.submitContext(ctx, s, nil)
}

// committed pins immutable advice inputs before releasing the commit lock.
func (q *Queue) submitContext(ctx context.Context, s Spec, committed func()) (_ *Job, err error) {
	ctx = context.WithValue(ctx, submissionTurnKey{}, true)
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	defer func() {
		if err == nil {
			return
		}
		// Only marked field rules and sanitized read diagnostics may publish their
		// reason. A code alone does not make a wrapped filesystem cause public.
		var rule *rejectionError
		if errors.As(err, &rule) {
			message := rejectionMessages[rule.code]
			var field *fieldRuleError
			if errors.As(err, &field) {
				message = field.Error()
			}
			detail := pathDiagnostic(err)
			if detail != nil {
				message = detail.String()
			}
			err = &HTTPError{Status: 400, Code: rule.code, Message: message, PathDiagnostic: detail}
		}
		code, reason := "request_failed", rejectionMessages["request_failed"]
		var apiError *HTTPError
		if errors.As(err, &apiError) {
			code, reason = apiError.Code, apiError.Message
			if apiError.PathDiagnostic != nil {
				reason = rejectionMessages[code]
			}
		} else if errors.Is(err, ErrStale) {
			code, reason = "request_stale", rejectionMessages["request_stale"]
		}
		log.Printf("submission rejected code=%s reason=%q", code, reason)
	}()
	// Direct API callers may send the MY-54 authoring form; store it flat.
	if _, err := s.FoldAdvanced(); err != nil {
		return nil, err
	}
	if s.SubmissionKey != "" {
		if err := validateSubmissionKey(s.SubmissionKey); err != nil {
			return nil, err
		}
	}
	fingerprint, err := submissionFingerprint(s)
	if err != nil {
		return nil, err
	}
	// Own all client maps/slices before the lock and across verification.
	s = cloneJob(&Job{Spec: s}).Spec
	unlockRequest := requestLock(ctx, submissionMutex{&q.mu})
	store := q.files
	catalog := q.environments
	environmentRevision := q.environmentRevision
	replay := s.SubmissionKey != "" && q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)] != nil
	if len(submissionFiles(s)) > 0 && !replay && q.submitVerificationSlot == nil {
		q.submitVerificationSlot = make(chan struct{}, 1)
	}
	slot := q.submitVerificationSlot
	unlockRequest()
	var verificationErr error
	if len(submissionFiles(s)) > 0 && !replay {
		verificationErr = verifySubmissionInputs(ctx, store, submissionFiles(s), slot)
	}
	// Use the same execution adapter as the transaction, on an owned copy.
	// Validation errors are handled below in their existing order; key replay
	// and cancellation still win over all preparation failures.
	prepared := cloneJob(&Job{Spec: s}).Spec
	normalizationErr := normalizeResources(&prepared)
	var executionErr error
	if !prepared.extensionJob() {
		executionErr = lowerExecution(&prepared)
	}
	// Plain-command validation and payload normalization need no queue state.
	// Keep their original error ordering inside the commit: replay/cancellation
	// and live policy/custody fences still win over preparation failures.
	var plainValidationErr error
	if !s.extensionJob() {
		validation := s
		if validation.memoryOmitted() {
			validation.Needs.Memory = "auto"
		}
		plainValidationErr = validation.Validate()
	}
	var recipes map[string]EnvironmentRecipe
	var frozenLock *EnvironmentLock
	var sidecarRecipes map[string]map[string]EnvironmentRecipe
	var recipeErr error
	var discovery *CommandDiscovery
	var entryCoverage *EntryCoverage
	if !replay {
		recipes, recipeErr = catalog.resolveEnvironment(prepared.Environment)
		// Digest before freezeRecipes moves the bytes to the file store.
		frozenLock = environmentLock(recipes)
		if recipeErr == nil {
			sidecarRecipes, recipeErr = catalog.resolveSidecarEnvironments(prepared)
		}
		if recipeErr == nil {
			discovery, recipeErr = deriveCommandDiscovery(prepared, recipes, sidecarRecipes)
			if recipeErr != nil {
				recipeErr = rejectFieldRule("job_environment_invalid", recipeErr)
			}
		}
		if recipeErr == nil {
			entryCoverage = deriveEntryCoverage(prepared, recipes, discovery)
		}
		if recipeErr == nil && !q.simulation {
			verified := map[FileRef]bool{}
			recipeErr = store.freezeRecipes(recipes, verified, ctx)
			for _, side := range sidecarRecipes {
				if recipeErr != nil {
					break
				}
				recipeErr = store.freezeRecipes(side, verified, ctx)
			}
		}
	}
	var resultCapacity FileStoreStatus
	var resultPayloads map[string]storedPayload
	var resultView *fileCapacitySnapshot
	if s.OutputDelivery == asyncNAS && !replay && store != nil {
		resultView = store.snapshotCapacity(ctx)
		defer resultView.release()
	}

	var out Job
	var refusedHealthDemand *Job
	err = q.updateTransaction(ctx, func() error {
		if resultView != nil {
			store.mu.Lock()
			resultCapacity, resultPayloads = resultView.current()
			store.mu.Unlock()
		}
		q.maintainSubmissions()
		existing, err := q.existingSubmission(s, fingerprint)
		if err != nil {
			return err
		}
		if existing != nil {
			out = *existing
			out.SubmissionReused = true
			return nil
		}
		if s.StartBy != nil && s.StartBy.Before(q.clock.Now()) {
			return rejectFieldRule("job_deadline_invalid", errors.New("start_by is already past"))
		}
		if err := q.validateMachineNames(s.AllowedMachines); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// A concurrent key binding/reservation wins even if verification failed.
		if verificationErr != nil {
			var typed *HTTPError
			if errors.As(verificationErr, &typed) {
				return typed
			}
			return reject("job_files_invalid", verificationErr)
		}
		if q.files != store || q.environmentRevision != environmentRevision {
			return ErrStale
		}
		if err := q.defaultMemory(&s); err != nil {
			return specRejection(err)
		}
		submitted := s
		extensions, e := q.resolveExtensions(&s)
		if e != nil {
			return rejectFieldRule("job_program_invalid", e)
		}
		if err := q.validateAutomaticExtensionCheck(s, extensions); err != nil {
			return err
		}
		// Validate the original execution/needs, with the resolved class default.
		// Resolution must not hide forbidden overrides or bypass deadline rules.
		submitted.Class = s.Class
		q.maintainFileExpiry()
		if err := q.validatePieceSource(submitted); err != nil {
			if isResultExpired(err) {
				return err
			}
			return reject("job_files_invalid", err)
		}
		validationErr := plainValidationErr
		if s.extensionJob() {
			validationErr = submitted.Validate()
		}
		if err := validationErr; err != nil {
			return specRejection(err)
		}
		if s.OutputDelivery == asyncNAS {
			if q.deliveryRoot == "" {
				return &HTTPError{Status: 409, Code: "output_delivery_unavailable", Message: "async_nas requires queue --output-delivery-root"}
			}
			if store == nil {
				return &HTTPError{Status: 503, Code: "file_store_unavailable", Message: "queue-file-store is not configured"}
			}
			if err := q.fileStoreAdmissionRefusal(WithFileStoreSubmission(ctx, s), resultCapacity, resultPayloads, addDiskBytes(q.outputReservations(resultPayloads, ""), s.Disk.OutputBytes), s.Disk.OutputBytes, q.outputReservationsFor(resultPayloads, "", true), q.priorityMatch(s).Tier == "production"); err != nil {
				return err
			}
		}
		if e := q.validateJobExclusions(s); e != nil {
			return rejectFieldRule("job_placement_invalid", e)
		}
		if e := q.validateProjectPlacement(s.Project, s.AllowedMachines); e != nil {
			return rejectFieldRule("job_placement_invalid", e)
		}
		if err := normalizeJobClass(&s); err != nil {
			return specRejection(err)
		}
		var declared uint64
		if s.Disk != nil {
			declared = s.Disk.OutputBytes
		}
		if err := q.fileStoreQuotaRefusal(s, submissionFiles(s), declared); err != nil {
			return err
		}
		modelLimits, err := q.modelLimits(s)
		if err != nil {
			return err
		}
		if s.extensionJob() {
			if err := normalizeResources(&s); err != nil {
				return err
			}
		} else {
			if normalizationErr != nil {
				return normalizationErr
			}
			if executionErr != nil {
				return specRejection(executionErr)
			}
			// The live policy only supplies memory/class defaults to plain
			// commands. Keep those rechecked values on the prepared payload.
			prepared.Needs.Memory = s.Needs.Memory
			prepared.Class, prepared.Priority = s.Class, s.Priority
			s = prepared
		}
		if recipeErr != nil {
			var rule *rejectionError
			if errors.As(recipeErr, &rule) {
				return rule
			}
			return reject("job_environment_invalid", recipeErr)
		}
		if !environmentPlatformsMatch(s.Needs, recipes) {
			return reject("job_environment_platform_mismatch", nil)
		}
		s.AuthoringOrigin = submittedOrigin(s)
		var id string
		if q.simulation {
			q.simID++
			id = fmt.Sprintf("sim-%012d", q.simID)
		} else {
			id = ID()
		}
		j := &Job{EntryCoverage: entryCoverage, CommandDiscovery: discovery, PriorityPolicy: q.priorityMatch(s), FileTransport: q.fileCheckpoints, Extensions: extensions, SidecarEnvironmentRecipes: sidecarRecipes, EnvironmentRecipes: recipes, ID: id, Spec: s, State: "queued", Queued: q.clock.Now(), WaitingSince: q.clock.Now(), Checkpoint: q.checkpointPath(id)}
		// Uploaded lock bytes are frozen in EnvironmentRecipes (with their digest);
		// do not keep a second copy in every stored job spec.
		j.Spec.Environment.Files = nil
		j.EnvironmentLock = frozenLock
		j.ModelLimits = modelLimits
		j.PlacementIntent = submissionPlacementIntent(submitted, j)
		explicitStartBy := s.StartBy != nil
		j.StartByExplicit = &explicitStartBy
		if s.Priority == "production" {
			j.StartWindow = productionStartWindow(s)
			if s.StartBy != nil {
				j.StartWindow = s.StartBy.Sub(j.Queued)
			}
			if s.After == nil {
				initializeStartDeadline(j, j.Queued)
			}
		}
		if s.ExtensionCheck != nil && s.ExtensionCheck.Timeout != "" {
			initializeStartDeadline(j, j.Queued)
		}
		if submitted.Needs.Memory == "auto" {
			// Capture only submitter hints, before recipe defaults can mask an
			// undeclared first run. A non-nil empty hint explicitly means none.
			hintSpec := submitted
			_ = normalizeResources(&hintSpec) // submitted.Validate already checked it.
			j.MemoryFallback = &SizeHint{MemoryGB: max(hintSpec.Needs.MemoryGB, submitted.SizeHint.MemoryGB), HostMemoryGB: max(hintSpec.Needs.HostMemoryGB, submitted.SizeHint.HostMemoryGB)}
		}
		q.applyLearnedNeeds(j)
		if err := q.sharedInputCapacityRefusal(j); err != nil {
			return err
		}
		if reason := q.diskCapacityRefusal(j); reason != "" {
			return &HTTPError{Status: 409, Code: "disk_headroom", Message: reason}
		}
		if j.EntryCoverage != nil && q.entryCoverageProduction(j) {
			return rejectFieldRule("job_placement_invalid", errors.New("production entries declare explicit targets (MY-19 parity); only non-production entries may omit them"))
		}
		if s.RequireTarget || nativeNetworkRequired(j) {
			if ok, reason := q.qualifiedTarget(j); !ok {
				refusedHealthDemand = j
				return &HTTPError{Status: 409, Code: "no_qualified_target", Message: reason + "; intent retained locally"}
			}
		}
		// require_target already checked permanent capacity and project policy;
		// do not reclassify temporary holds through the legacy capacity diagnostic.
		if !s.RequireTarget {
			if e := q.validateProjectCapacity(j); e != nil {
				refusedHealthDemand = j
				return rejectFieldRule("job_capacity_invalid", e)
			}
		}
		copy := cloneJob(j)
		j = &copy
		if s.FanOut != nil {
			j.FanOut = &FanOutProgress{Results: map[string]PieceResult{}}
		}
		if s.After != nil {
			j.WaitingSince = time.Time{}
			j.Dependency = &PieceBinding{Job: s.After.Job, Piece: s.After.Piece, State: "blocked"}
		}
		q.putJob(id, j)
		q.materializePlainPieces(j)
		q.settleFileInputClaims(s)
		if s.SubmissionKey != "" {
			if q.s.Submissions == nil {
				q.s.Submissions = map[string]*submissionRecord{}
			}
			created := q.clock.Now()
			q.putSubmission(submissionDigest(s.Project, s.SubmissionKey), &submissionRecord{Kind: submissionBinding, Project: s.Project, Key: s.SubmissionKey, Fingerprint: fingerprint, JobID: id, Created: &created, Job: j})
		}
		if check := s.ExtensionCheck; check != nil && check.Reserve != nil {
			if _, err := q.acquireReservation(ReservationRequest{ReservationOptions: *check.Reserve, Job: j.ID}); err != nil {
				return err
			}
		}
		q.maintain()
		out = cloneJob(j)
		return nil
	}, nil, false, committed)
	if err != nil && refusedHealthDemand != nil {
		// The refused submission rolls back. Record only a rechecked health
		// observation in its own transaction, never create or admit the job.
		healthErr := q.update(func() error {
			q.observeEnvelopeHealth(refusedHealthDemand, true)
			return nil
		})
		if healthErr != nil {
			return &out, errors.Join(err, healthErr)
		}
	}
	if err == nil {
		err = store.hydrateJob(&out) // Submit holds the publication fence through delivery.
	}
	return &out, err
}
func (q *Queue) retry(j *Job, why string) {
	q.touchJob(j.ID)
	if wallTimeStopped(j) && !cancelled(j) {
		q.finishWallTime(j)
		return
	}
	if yieldMatches(j) && !j.ScarceHostYield.FinalReceived && !cancelled(j) {
		j.ScarceHostYield.Hold = "final checkpoint/transport pending; release alone cannot resume work"
		j.Reason = "scarce_host_yield: " + j.ScarceHostYield.Hold
		return
	}
	q.requeue(j, why, !plannedStop(j))
}

// Admission refusal and planned stops do not spend infrastructure retry budgets.
func (q *Queue) requeue(j *Job, why string, chargeInfrastructure bool) {
	q.touchJob(j.ID)
	// Read the stop cause while the attempt identity still names it.
	maintenance := plannedMaintenanceStop(j)
	j.PendingOutputRefs = nil
	j.OutputAdmissionPending = j.Spec.OutputDelivery == asyncNAS
	// Snapshot identity before requeue clears it, including loss without a result.
	outcome := attemptOutcome(j, Result{Interrupted: true, PoliteStop: historyPlannedStop(j), PriorityStop: priorityStop(j)})
	if chargeInfrastructure && strings.HasPrefix(why, "machine quiet for ") {
		q.alertAttemptLoss(j, outcome, true)
	}
	q.recordAttemptMemory(j, Result{Interrupted: true})
	defer func() { q.recordAttemptOutcome(j, outcome) }()
	if cancelled(j) {
		if !terminal(j.State) {
			q.finishCancellation(j)
		}
		return
	}
	if automaticExtensionCheck(j) != nil && why != launchCapacityWaitReason {
		now := q.clock.Now()
		j.State, j.Reason, j.FinishedAt = "failed", "native check interrupted: "+why, &now
		j.StopAt = nil
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
		if why != launchCapacityWaitReason {
			q.renewRecoveryStartWindow(j)
		}
		if chargeInfrastructure {
			j.InfrastructureRetries++
		}
	}
	j.Machine = ""
	j.StorageEnvelope = nil
	j.MemoryEnvelope = nil // Re-resolve variants and live sizing for every retry.
	j.Session = ""
	j.StopAt = nil
	j.StartedAt = nil
	j.InputsStagedAt = nil
	j.FinishedAt = nil
	j.Reason = why
	if chargeInfrastructure && j.Spec.Priority != "production" && j.Losses >= 3 {
		j.State = "failed"
		j.Reason = "failed after 3 infrastructure losses: " + why
		return
	}
	j.LastMemoryAdmission = nil // Only an actual new wait episode clears the previous decision.
	j.WaitingSince = q.clock.Now()
	q.s.Front++
	j.Front = q.s.Front
	restoreCPUFallback(j)
	j.State = "queued"
	if maintenance {
		now := q.clock.Now()
		j.MaintenanceResume = &now
	}
}
func (q *Queue) stop(j *Job, why string) {
	q.touchJob(j.ID)
	if j.State != "running" {
		return
	}
	now := q.clock.Now()
	for i := range j.Copies {
		c := &j.Copies[i]
		if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && c.StoppedAt == nil {
			c.StoppedAt = &now
		}
	}
	t := now.Add(StopGrace)
	j.StopAt = &t
	j.State = "stopping"
	j.Reason = why
}
func (q *Queue) fleetQuiet(now time.Time, includePending bool) bool {
	active, healthy := 0, 0
	quietFor := time.Duration(1<<63 - 1)
	for _, m := range q.s.Machines {
		if !m.Gone && !m.Leaving && q.workerLossAllowance(m) > 0 {
			active++
			quietFor = min(quietFor, q.machineLossTimeout(m.ID))
			if q.contactFresh(m, now, includePending) || !q.workerArrivalSuspect(m, now, includePending) {
				healthy++
			}
		}
	}
	// Cached contact can identify a continuously reporting minority, but cannot
	// replace a currently healthy worker when the entire active fleet is quiet.
	return q.s.NetworkRecoveryAt.IsZero() && active > 0 && (healthy == 0 || healthy*2 < active && now.Sub(q.s.LastWorkerContact) >= quietFor)
}

// Run time/custody fences before offering work. Later mutations close only
// affected relations; the transaction settles them again before persistence.
func (q *Queue) maintain() {
	if q.stopping.Load() {
		return
	}
	if q.maintenance != nil && q.maintenance.ran {
		q.settleMaintenance()
		return
	}
	if q.maintenance == nil {
		defer func() { q.maintenance = nil }()
	}
	defer q.bodyPhaseFinish("maintain", q.bodyPhaseStart())
	q.beginMaintenance()
	previousCapture := q.memoryAdmissionCapture
	q.memoryAdmissionCapture = &memoryAdmissionCapture{decisions: map[string]memoryAdmissionDecision{}, drift: map[string]float64{}}
	defer func() {
		q.retainMemoryAdmissions()
		q.memoryAdmissionCapture = previousCapture
	}()
	q.maintainWallTimeLimits()
	q.maintainFleetUpgrade()
	q.maintainFileExpiry()
	q.maintainHostMaintenance()
	q.maintainSharedHosts()
	q.yieldMemoryOvercommit()
	q.maintainDeviceMaintenance()
	q.maintainExtensionQualifications()
	now := q.clock.Now()
	for _, j := range q.liveJobs() {
		if j.State == "queued" {
			q.applyLearnedNeeds(j)
		}
		if j.State == "running" || j.State == "stopping" {
			q.retainRelease(j)
		}
	}
	for _, m := range q.s.Machines {
		if a := machineAvailability(m); a != nil && a.Available && (a.Mode == "daily" || a.Signals.Laptop) && !hostAvailabilityFresh(m, now) {
			q.touchMachine(m.ID)
			a.Available = false
			a.Reason = "owner signals stale"
		}
		q.withdrawUnavailable(m)
	}
	q.maintainApprovalGrace()
	for machine, release := range q.s.BatchReleases {
		if !now.Before(release.Until) {
			delete(q.s.BatchReleases, machine)
		}
	}
	q.networkQuiet = q.fleetQuiet(now, true)
	for _, m := range q.s.Machines {
		unreported := q.unreported[m.ID]
		grace := unreported && q.inRestartGrace(now)
		restartExpired := unreported && !grace
		if !m.Gone && !grace && (restartExpired || (!q.networkQuiet && q.workerSuspect(m, now) && now.Sub(q.lastContact(m, now)) >= q.machineLossTimeout(m.ID))) {
			q.touchMachine(m.ID)
			m.Gone = true
			q.discontinueContactCadence(m.ID)
			q.clearPreemptionPlacements(m.ID, m.Session)
			delete(q.unreported, m.ID)
			m.Models = nil
			for _, j := range q.liveJobs() {
				if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
					q.touchJob(j.ID)
					if cancelled(j) {
						q.finishCancellation(j)
					} else {
						if q.launchCapacityPending(j) {
							// A validated refusal may still be verifying diagnostics
							// outside the lock. Its stopped execution is known; quiet
							// contact adds no infrastructure loss or release proof.
							for i := range j.Copies {
								j.Copies[i].Recoverable = false
							}
							q.retainRelease(j)
							continue
						}
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

	for _, j := range q.liveJobs() {
		// Machine attempts retain their exact device and host charge through kill;
		// only their release acknowledgement can retire the stopping attempt.
		m := q.s.Machines[j.Machine]
		if (m == nil || m.Devices == nil) && !q.networkQuiet && !q.unreported[j.Machine] && j.State == "stopping" && !plannedPriorityStop(j) && expired(j.StopAt, now) {
			if cancelled(j) {
				q.finishCancellation(j)
			} else {
				q.retry(j, j.Reason)
			}
		}
	}
	q.settleMaintenance()
	q.maintainStartDeadlines()
	q.settleMaintenance()
	// Device recovery resolves competing absolute cutoffs before the generic
	// queued/paused expiry. Every terminal decision must carry its own timestamp;
	// later maintenance need not see a job after it leaves the live set.
	for _, j := range q.liveJobs() {
		if (j.State == "queued" || j.State == "paused") && !(j.Spec.FanOut != nil && j.FanOut != nil && j.FanOut.Next > 0) && expired(j.Spec.GiveUpAt, now) {
			q.touchJob(j.ID)
			j.State, j.Reason, j.FinishedAt = "failed", "give-up time reached", &now
		}
	}
	q.settleMaintenance()
	q.maintainReservations()
	q.settleMaintenance()
	q.refreshHostMemory()
	q.maintainModels()
	q.settleMaintenance()
	// Only display reasons change in this final read-only planning pass.
	// Share memory evidence across the backlog, then discard it before the
	// next mutation. Subsequent decisions settle only affected relations.
	previous := q.memoryPlanning
	q.memoryPlanning = newMemoryPlanningCache()
	defer func() { q.memoryPlanning = previous }()
	scan := q.placementScan()
	scan.memory = q.memoryPlanning
	q.admissionWaitPending = true
	for _, j := range q.liveJobs() {
		if q.startupPending(j) || q.startCapacityPaused(j) {
			reason := q.startStatusReason(j, scan)
			if reason != j.Reason {
				q.touchJobReason(j)
				j.Reason = reason
			}
		}
	}
}
func (q *Queue) Tick() error { return q.TickContext(context.Background()) }

func (q *Queue) TickContext(requestContexts ...context.Context) error {
	if q.stopping.Load() {
		return queueStopping()
	}
	requestCtx := requestContext(requestContexts)
	traffic := q.requestHealth.snapshot(q.clock.Now())
	diagnostics, err := q.prepareEfficiencyDiagnostics(requestCtx)
	if err != nil {
		return err
	}

	// File-store health belongs to existing maintenance, not every control
	// transaction. Snapshot under the lock, then perform bounded I/O outside it.
	requestUnlock1 := requestReadLock(requestCtx, &q.mu)
	files := q.files
	requestUnlock1()
	fileAvailable := true
	if !q.simulation && files != nil {
		fileAvailable, _ = q.healthStoreProbe.runProgress(files.root, func(progress func()) (bool, error) { return checkpointAvailable(files.root, progress), nil })
	}
	if err := q.updateRequest(requestCtx, func() error {
		delay := q.healthStorageDelay
		if delay <= 0 {
			delay = DefaultHealthStorageDelay
		}
		q.applyRequestHealth(traffic)
		q.applyEfficiencyDiagnostics(diagnostics)
		q.observeHealth("queue-files", !fileAvailable, delay, HealthAlert{Kind: "checkpoint_store_unavailable", Store: "queue-file-store"})
		q.maintain()
		q.maintainDeadEndWaits()
		q.settleMaintenance()
		return nil
	}, nil); err != nil {
		return err
	}
	q.publishEfficiencyDiagnostics(diagnostics)
	return errors.Join(q.maintainEvidence(requestCtx), q.advanceOutputDelivery(requestCtx), q.pruneBatchLogs(requestCtx), q.reclaimJobFilesContext(requestCtx), q.admitOutputRetries(requestCtx), q.pruneJobHistory(requestCtx))
}
func (q *Queue) CheckinKeyed(c Checkin) (Reply, error) {
	return q.CheckinKeyedContext(context.Background(), c)
}

func (q *Queue) CheckinKeyedContext(ctx context.Context, c Checkin) (Reply, error) {
	contact, settle := q.trackWorkerCheckin(c.Machine.ID, c.Machine.Session, c.period, true, ctx)
	c.contact = contact
	defer settle(false)
	reply, err := q.checkinContext(ctx, c)
	settle(err == nil)
	return reply, err
}

// Callers already own contact tracking, including HTTP and direct queue calls.
func (q *Queue) checkinContext(ctx context.Context, c Checkin) (Reply, error) {
	return q.checkinResponseContext(ctx, c, nil)
}

// The HTTP caller keeps the same request lifecycle through NAS instructions
// and response encoding. Direct queue callers complete it when their reply is
// ready. The synchronous response callback runs outside q.mu. Neither path
// can leave pending credit after error, panic or cancellation.
func (q *Queue) checkinResponseContext(ctx context.Context, c Checkin, respond func(Reply, error)) (Reply, error) {
	healthReportOrder := q.hostHealthReports.Add(1)
	receivedAt := q.clock.Now()
	healthPreparation := q.beginHostHealthPreparation(ctx, c.Machine, healthReportOrder)
	defer q.finishHostHealthPreparation(healthPreparation, false)
	if healthPreparation != nil {
		stopCancellation := context.AfterFunc(ctx, func() { q.finishHostHealthPreparation(healthPreparation, false) })
		defer stopCancellation()
	}
	reply, err := q.processCheckinContext(ctx, c, healthReportOrder, receivedAt, healthPreparation)
	if respond != nil {
		respond(reply, err)
	}
	q.finishHostHealthPreparation(healthPreparation, err == nil)
	return reply, err
}

func (q *Queue) processCheckinContext(ctx context.Context, c Checkin, healthReportOrder uint64, memoryReceivedAt time.Time, healthPreparation *hostHealthPreparation) (Reply, error) {
	var reportedHealthAge float64
	if c.Machine.HostHealth != nil {
		reportedHealthAge = c.Machine.HostHealth.AgeSeconds
	}
	if err := ctx.Err(); err != nil {
		return Reply{}, err
	}
	var err error
	c, err = normalizeAttemptCheckin(c)
	if err != nil {
		return Reply{}, err
	}
	if err := validateCurrentCheckin(c); err != nil {
		return Reply{}, err
	}
	preparedInventory, err := prepareCheckinToolInventory(c)
	if err != nil {
		return Reply{}, err
	}
	c.Machine.ToolInventory, c.Machine.toolInventory = nil, nil
	c = receivedDeviceTelemetry(c, q.clock.Now())
	preparedMachine := cloneMachine(&c.Machine)
	reply := Reply{WorkerDiskBudgetProtocol: workerDiskBudgetProtocol, WorkerDiskHeadroomProtocol: workerDiskHeadroomProtocol, WorkerDiskTrendProtocol: workerDiskTrendProtocol, DeviceHealthProtocol: deviceHealthProtocol, DeviceRecoveryProtocol: deviceRecoveryProtocol, HostMountProgressProtocol: hostMountProgressProtocol, DeviceTelemetryProtocol: deviceTelemetryProtocol, HostProbeProtocol: hostProbeProtocol, ProbeMemoryProtocol: probeMemoryProtocol, HostMemorySampleProtocol: hostMemorySampleProtocol, UnifiedMemoryProtocol: unifiedMemoryProtocol, HostCollectionProtocol: hostCollectionProtocol, HostHygieneProtocol: hostHygieneProtocol, HostForeignProtocol: hostForeignProtocol, ToolInventoryProtocol: toolInventoryProtocol, CheckpointExpiryProtocol: checkpointExpiryProtocol, FileCheckpoints: q.fileCheckpoints, CheckpointRoot: q.root}
	q.kickCheckpointShards()
	reports := c.Attempts
	progresses := make([]attemptProgress, len(reports))
	for i := range reports {
		report, progress := &reports[i], &progresses[i]
		unlockRequest := requestLock(ctx, &q.mu)
		if j := q.s.Jobs[report.ID]; j != nil && j.FileTransport {
			report.Progress = 0
			if save, ok := j.FileCheckpoints[report.Attempt]; ok {
				report.Progress = save.Generation
			}
			if save, ok := j.FileCheckpoints[j.Attempt]; ok {
				progress.durable = save.Generation
			}
		} else if j != nil && j.Spec.Resumable && (j.Attempt != report.Attempt || j.Spec.ResumeCompatibility != nil) {
			progress.path = attemptCheckpoint(*j)
		}
		unlockRequest()
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
	identities := q.snapshotCheckinMemoryIdentities(ctx)
	var releaseStops []AttemptDirective
	err = q.updateRequest(ctx, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		identities.installRetainedMemory(q)
		q.checkinMemoryIdentities = identities.current(q)
		defer func() { q.checkinMemoryIdentities = nil }()
		m := preparedMachine
		m.ExternalMemory = nil // Summary is queue-owned, never worker-supplied.
		m.HostMemoryDerivation = nil
		// Worker clocks are unrelated to queue time. Translate local proof age
		// into a queue-owned receipt timestamp; cached observations keep aging.
		// Freshness is judged later against the process's own cadence.
		if m.Fleet != nil {
			age := m.Fleet.ObservationAgeSeconds
			if math.IsNaN(age) || math.IsInf(age, 0) || age < 0 || age >= fleetProofMaxAge.Seconds() {
				m.Fleet = nil
			} else {
				m.Fleet.ObservedAt = q.clock.Now().Add(-time.Duration(age * float64(time.Second)))
				var prev *FleetRuntimeProof
				if old := q.s.Machines[m.ID]; old != nil {
					prev = old.Fleet
				}
				q.noteFleetProof(prev, m.Fleet, q.clock.Now())
			}
		}
		if err := inventoryTarget(&m); err != nil {
			return err
		}
		// Dev and the event simulator intentionally use synthetic per-component
		// workers on one shared host ledger; neither creates native workers.
		if _, simulated := q.clock.(*devClock); !simulated && !q.simulation {
			for _, owner := range q.s.Machines {
				if owner.ID != m.ID && (m.Devices != nil || owner.Devices != nil) && owner.Replacement == nil && !owner.Gone && hostID(owner) == hostID(&m) {
					return &HTTPError{Status: 409, Code: "machine_worker_conflict", Message: rejectionMessages["machine_worker_conflict"]}
				}
			}
		}
		m.Disabled = ""
		m.LastResort = false
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
		if m.ModelURL != "" && len(q.models.Models) > 0 && m.ModelConfigHash != modelConfigHash(q.models) {
			return errors.New("worker model config does not match queue")
		}
		old := q.s.Machines[m.ID]
		q.receiveDeviceRecovery(&m, old)
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
		reply.SharedHost = cloneSharedHost(q.s.HostMaintenance[hostID(&m)].SharedHost)
		if reply.SharedHost == nil {
			reply.SharedHost = &SharedHostProfile{} // Explicitly clear a removed profile.
		}
		if cores := q.s.HostMaintenance[hostID(&m)].CPUBudgetCores; cores != nil {
			value := *cores
			reply.CPUBudgetCores = &value
		}
		reply.HostMaintenance = q.s.HostMaintenance[hostID(&m)].Mode
		if old != nil && old.Session != m.Session && !(c.Fresh && c.PreviousSession == old.Session) {
			if h := q.s.Hosts[hostID(old)]; q.maintenanceKnown(hostID(old)) || old.HostMemoryBudgetGB > 0 || old.EffectiveHostMemoryBudgetGB > 0 || h != nil && h.Policy.ProductionHold {
				for _, slot := range q.s.ModelSlots {
					if slot.Machine == m.ID && slot.Session == old.Session {
						return errors.New("previous model execution release unknown; prior-session cleanup required")
					}
				}
			}
		}
		q.reconcileHostReleases(c)
		releaseStops = q.unheldCopyStops(c.Machine.ID, c.Machine.Session, reports)
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
			for _, j := range q.liveJobs() {
				if j.Machine == m.ID && j.Session == old.Session && (j.State == "running" || j.State == "stopping") {
					q.touchJob(j.ID)
					if cancelled(j) {
						q.finishCancellation(j)
						continue
					}
					if q.launchCapacityPending(j) {
						q.requeueLaunchCapacity(j)
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
		q.touchMachine(m.ID)
		m.LastSeen = q.clock.Now()
		m.DiskReceivedAt = m.LastSeen // Ignore worker-supplied queue timestamps.
		// Recovery is queue-observed admission state, not worker-supplied status.
		m.Recovering, m.RecoveringSince, m.RecoveryNeedsAttention = c.Recovering, nil, false
		if m.Recovering {
			since := m.LastSeen
			if old != nil && old.Recovering && old.RecoveringSince != nil {
				since = *old.RecoveringSince
			}
			m.RecoveringSince = &since
		}
		if err := q.acceptToolInventory(c, &m, old, preparedInventory, &reply); err != nil {
			return err
		}
		q.acceptExtensionQualifications(&m)
		ageHostObservations(&m, memoryReceivedAt, q.clock.Now())
		q.acceptHostHealthDelivery(&m, old, healthReportOrder, reportedHealthAge, healthPreparation)
		acceptHostAvailability(&m, q.clock.Now())
		q.acceptSharedPressure(&m, old)
		m.IOPressure = safeIOPressure(m.IOPressure)
		if m.IOPressure != nil {
			m.IOPressure.ReceivedAt = q.clock.Now()
		}
		acceptHostHygiene(&m)
		if old != nil && (old.Gone || old.Session != m.Session) {
			q.clearPreemptionPlacements(old.ID, old.Session)
		}
		// Fresh workers attest cleanup only for their exact previous session.
		// Record that accepted evidence before reconciliation discards old slots
		// and before replacing the old inventory used to identify their cards.
		if c.Fresh && c.PreviousSession != "" && c.PreviousSession != m.Session {
			for _, slot := range q.s.ModelSlots {
				if slot.Machine == m.ID && slot.Session == c.PreviousSession {
					q.recordModelInterval(slot, "released")
				}
			}
		}
		q.acceptEnvironmentFailures(&m, old)
		// Assess the previous fleet before this arrival or replacement can
		// conceal the outage. Pending reconnects are new evidence, not accepted
		// survivors from before recovery, even when several arrive together.
		if old != nil && !old.Gone && !q.inRestartGrace(m.LastSeen) && q.s.NetworkRecoveryAt.IsZero() &&
			(q.networkQuiet || q.fleetQuiet(m.LastSeen, false)) {
			q.startNetworkRecovery(m.LastSeen)
		}
		q.touchMachine(m.ID)
		q.s.Machines[m.ID] = &m
		q.convertPendingParity()
		if old != nil && (old.Gone || q.unreported[m.ID] || q.networkQuiet || !q.s.NetworkRecoveryAt.IsZero() && old.LastSeen.Before(q.s.NetworkRecoveryAt)) {
			q.discontinueContactCadence(m.ID)
		}
		q.acceptContactPeriod(&m, c.period, c.contact)
		q.recordHostLaunchMemory(&m, old)
		q.recordExternalMemory(&m)
		if q.hostMemorySamples == nil {
			q.hostMemorySamples = map[string]map[attemptKey]hostMemorySample{}
		}
		q.retainHostMemoryCharges(m.ID, q.clock.Now())
		q.hostMemorySamples[m.ID] = updateHostMemorySamples(reports, memoryReceivedAt, q.hostMemorySamples[m.ID])
		q.retainHostMemoryCharges(m.ID, q.clock.Now())
		q.refreshHostMemory()
		q.reconcilePreparationPlans(c)
		q.checkinCheckpointExpiry(c, &m, &reply)
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
		q.acceptFaultEvidence(&m)
		q.quarantineDeviceAttempts(&m)
		q.maintain()
		reply.FleetInstructionProtocol, reply.FleetInstructionWaiting = fleetInstructionProtocol, q.fleetInstructionWaiting(hostID(&m))
		q.guardSharedGPU(&m)
		defer func() {
			budget, _ := q.measuredHostMemoryBudget(&m, *q.s.HostMaintenance[hostID(&m)].MemoryBudget)
			reply.HostMemoryBudget = &budget
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
			if c.Recovering || q.workerSuspect(&m, q.clock.Now()) {
				// Preserve retained engines and deliver their cleanup deadlines.
				// A retention-only plan cannot start a missing engine: the worker
				// starts only loading plans. New launch authorization also refuses
				// suspect sessions, including cached preparation plans.
				if !c.Recovering {
					for _, slot := range q.modelPlan(m.ID) {
						if slot.State == "loading" {
							slot.State = "ready"
						}
						reply.ModelPlan = append(reply.ModelPlan, slot)
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
			for _, target := range q.machineTargets(&m) {
				reply.ExtensionQualificationPlan = append(reply.ExtensionQualificationPlan, q.extensionQualificationPlan(target)...)
			}
			reply.ModelPlan = q.modelPlan(m.ID)
			reply.CopyPlan, reply.CacheUsage, reply.CachePins = q.copyPlan(&m)
			q.cancelUnofferedPreparations(&m, reply)
		}()
		q.beginAdmissionCheckin(&m)
		reply.Leave = m.Leaving
		if m.Leaving {
			for _, j := range q.liveJobs() {
				if j.Machine == m.ID && j.Session == m.Session && j.Spec.Resumable {
					q.stop(j, "machine leaving")
				}
			}
		}
		if m.Devices != nil {
			q.checkinMachineAttempts(&m, reports, progresses, c.Recovering, &reply)
			q.settleMaintenance()
			return nil
		}

		if len(reports) != 0 {
			q.skipAdmissionSelection(&m, predicateAttemptReports)
			seen := map[attemptKey]bool{}
			for i := range reports {
				a := q.reconcileAttemptReport(&m, &reports[i], progresses[i])
				reply.Attempts = append(reply.Attempts, a)
				seen[attemptKey{a.ID, a.Attempt, a.Session}] = true
			}
			// A report for another key neither acknowledges an omitted stop nor
			// hides it. Keep every outstanding control individually addressed.
			for _, j := range q.liveJobs() {
				if j.Machine == m.ID && j.Session == m.Session && j.State == "stopping" && !seen[attemptKey{j.ID, j.Attempt, j.Session}] {
					if j.ReleaseConfirmed {
						q.retry(j, j.Reason)
						continue
					}
					reply.Attempts = append(reply.Attempts, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: j.StopAt.Sub(q.clock.Now()).Seconds(), PriorityStop: priorityStop(j), UserCancel: cancelled(j)})
				}
			}
			return nil
		}
		// Re-deliver an assignment if its HTTP response was lost before startup.
		q.skipAdmissionSelection(&m, predicateExistingAssignment)
		for _, j := range q.liveJobs() {
			if j.State == "running" && j.ReleaseConfirmed {
				continue // Final result publication is not an execution offer.
			}
			if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
				if j.State == "stopping" {
					// Omission only finishes a keyed stop after an exact release ack.
					if !j.ReleaseConfirmed {
						reply.Attempts = []AttemptDirective{{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Stop: true, Kill: expired(j.StopAt, q.clock.Now()), GraceSeconds: j.StopAt.Sub(q.clock.Now()).Seconds(), PriorityStop: priorityStop(j), UserCancel: cancelled(j)}}
						return nil
					}
					if cancelled(j) {
						q.settleRelease(Result{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session}, false)
						q.finishCancellation(j)
					} else {
						q.retry(j, j.Reason)
					}
					break
				}
				if c.Recovering || q.maintenanceBlocked(hostID(&m)) || !logPolicyFits(j.Spec.Logs, &m) || !sharedInputsFit(j.Spec, &m) || !q.checkpointReady(j, &m) {
					return nil
				}
				out := cloneJob(j)
				out.LedgerEntries = nil
				reply.Job = &out
				return nil
			}
		}
		if m.Leaving {
			q.skipAdmissionSelection(&m, predicateLeaving)
		} else if c.Recovering {
			q.skipAdmissionSelection(&m, predicateRecovering)
		}
		if !m.Leaving && !c.Recovering {
			q.skipAdmissionSelection(&m, predicateSelectionNotReached)
			scan := q.placementScan()
			j := q.plannedPick(&m, scan)
			if j != nil {
				out := q.assign(j, &m, scan)
				reply.Job = &out
			}
		}
		if m.Leaving && reply.Job == nil && len(q.modelPlan(m.ID)) == 0 {
			q.touchMachine(m.ID)
			m.Gone = true
			m.Models = nil
		}
		q.settleMaintenance()
		return nil
	}, nil)
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
	reply = attemptReply(c, reply)
	if err == nil {
		reply.Attempts = append(reply.Attempts, releaseStops...)
		err = q.hydrateReply(&reply, ctx)
	}
	return reply, err
}
func (q *Queue) current(r Result) (*Job, error) {
	j := q.touchJob(r.ID)
	if j == nil || j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || (j.State != "running" && j.State != "stopping") {
		return nil, ErrStale
	}
	return j, nil
}
func (q *Queue) Complete(r Result) error {
	return q.CompleteContext(context.Background(), r)
}

// CompleteContext keeps bulk output verification outside the queue transaction.
// Only current success outputs or opt-in failure diagnostics need verification;
// release acknowledgements and retransmissions retain their normal handling.
func (q *Queue) CompleteContext(ctx context.Context, r Result) error {
	q.fileLifecycle.RLock()
	defer q.fileLifecycle.RUnlock()
	// Retain validated admission evidence durably before unlocking for bulk
	// diagnostic verification. Maintenance during that I/O must see the wait.
	if launchCapacityRefusal(r) && !r.ReleaseOnly {
		if err := q.updateRequest(ctx, func() error {
			q.retainLaunchCapacityRefusal(r)
			return nil
		}, nil); err != nil {
			return err
		}
	}
	quotaFailure := attemptDiskLimitFailure(r)
	unlockRequest := requestLock(ctx, &q.mu)
	j, err := q.wallTimeVerificationJob(r)
	if err == nil && parkedStorage(j) && !r.ReleaseOnly {
		unlockRequest()
		return errors.New(parkedStorageReason)
	}
	// A quota failure remains terminal when a stop races it. Verify its opt-in
	// diagnostics in either live state, with the same identity fences as outputs.
	verify := err == nil && !(r.Success && wallTimeExpired(j, q.clock.Now())) && j.storedFileResults() && (j.State == "running" || j.State == "stopping" && (quotaFailure || plannedPriorityStop(j) || wallTimeStopped(j)) || wallTimeDiagnosticsAllowed(j, r)) && !cancelled(j) && (r.Success && !r.Interrupted || !r.Success && r.Diagnostics != nil && j.Spec.capturesFailure()) && !r.ReleaseOnly
	var yieldPath string
	if err == nil && yieldMatches(j) && !j.FileTransport && !r.ReleaseOnly && r.Interrupted && r.FinalCheckpointGeneration != nil {
		yieldPath = attemptCheckpoint(*j)
	}
	var outputs map[string]string
	var keep []KeepFile
	var host string
	if verify {
		host = q.executionHost(r.Machine)
		verify = q.s.Machines[r.Machine] != nil && q.s.Machines[r.Machine].Session == r.Session
		if verify {
			if j.Spec.OutputDelivery == asyncNAS && r.Success && !maps.Equal(j.PendingOutputRefs, r.OutputRefs) {
				unlockRequest()
				return errors.New("async_nas requires the reserved immutable output manifest")
			}
			outputs, keep = maps.Clone(j.Spec.Outputs), slices.Clone(j.Spec.KeepOnFailure)
			r.OutputRefs = maps.Clone(r.OutputRefs)
			if r.Diagnostics != nil {
				r.Diagnostics = &AttemptDiagnostics{Files: maps.Clone(r.Diagnostics.Files)}
			}
		}
	}
	unlockRequest()
	if yieldPath != "" {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A heartbeat scan may predate the final save. Give final verification
		// its own observation key, and retry unfinished I/O without accepting a
		// failed-save verdict or discarding the worker's retained receipt.
		key := fmt.Sprintf("%s:yield-final:%d", yieldPath, *r.FinalCheckpointGeneration)
		generation, e := q.nasProgress.run(key, func() (int64, error) { return checkpointProgress(yieldPath) })
		if err := ctx.Err(); err != nil {
			return err
		}
		if errors.Is(e, errCheckpointIOPending) || errors.Is(e, errCheckpointIOTimeout) {
			return e
		}
		if e != nil || generation != *r.FinalCheckpointGeneration {
			r.FinalCheckpointGeneration = nil
		}
	}
	if verify {
		if r.Success {
			if err := validateOutputManifest(outputs, r.OutputRefs); err != nil {
				return err
			}
		}
		verificationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if r.Success {
			if err := q.verifyManifest(verificationCtx, r.OutputRefs); err != nil {
				return err
			}
		} else {
			if err := validateDiagnosticManifest(keep, r.Diagnostics); err != nil {
				return err
			}
			if err := q.verifyManifest(verificationCtx, r.Diagnostics.Files); err != nil {
				return err
			}
		}
		ctx = verificationCtx
	}
	var rejected error
	err = q.updateRequest(ctx, func() error {
		if yieldPath != "" {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if verify {
			q.retainLaunchCapacityRefusal(r)
			q.maintainStartDeadlines()
			j, err := q.wallTimeVerificationJob(r)
			m := q.s.Machines[r.Machine]
			if err != nil || !(j.State == "running" || j.State == "stopping" && (quotaFailure || plannedPriorityStop(j) || wallTimeStopped(j)) || wallTimeDiagnosticsAllowed(j, r)) || cancelled(j) || m == nil || m.Session != r.Session ||
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
			(j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || !terminal(j.State) && j.State != "running" && j.State != "stopping") {
			rejected = ErrStale
		}
		q.maintainWallTimeLimits()
		if j := q.s.Jobs[r.ID]; j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session && wallTimeStopped(j) && r.Success && !r.ReleaseOnly {
			rejected = ErrStale
		}
		return q.complete(r, verify)
	}, nil)
	if err != nil {
		return err
	}
	return rejected
}
func (q *Queue) retainLaunchCapacityRefusal(r Result) {
	// A refusal can race the generated startup cutoff. Validate identity before
	// retaining it, then let caller deadlines/cancellation and custody run normally.
	if launchCapacityRefusal(r) && !r.ReleaseOnly {
		if j, err := q.current(r); err == nil {
			q.recordLaunchCapacityWait(j, r)
		}
	}
}

func (q *Queue) complete(r Result, filesVerified bool) error {
	q.retainLaunchCapacityRefusal(r)
	q.maintainWallTimeLimits()
	if r.MemoryPeak.constrained() && !r.MemoryPeak.Constrained {
		peak := *r.MemoryPeak
		peak.Constrained = true
		r.MemoryPeak = &peak
	}
	q.maintainStartDeadlines()
	q.maintainExtensionQualifications()
	operatorMatched := q.refundOperatorStop(r)
	if r.ReleaseOnly {
		if r.ReleasePending {
			return errors.New("release-only acknowledgement cannot be pending")
		}
		q.settleRelease(r, true)
		if j := q.touchJob(r.ID); j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			if cause := safeAttemptRuntimeCause(j, r.ExecutionCause); !terminal(j.State) && cause != nil && cause.Finalization != nil {
				j.ExecutionCause = cause
				j.CleanupCause = safeAttemptRuntimeCause(j, r.CleanupCause)
				j.Reason = "finalization pending: " + cause.String()
			}
			q.recordAttemptMemory(j, r)
			q.retainAttemptResources(j, r.Resources)
			j.ReleaseConfirmed = true
			if r.Forced {
				j.LastStopOutcome = "forced"
			} else if j.LastStopOutcome == "release pending" {
				j.LastStopOutcome = "released"
			}
			// Native kill-time reports can carry cleanup only. Advance the
			// recorded stop on this exact acknowledgement, rather than using
			// an omitted attempt in the next heartbeat as release evidence.
			if j.State == "stopping" {
				if cancelled(j) {
					q.finishCancellation(j)
				} else {
					q.recordSharedHostLatencyStop(j, r)
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
				q.settleRelease(r, true)
			}
			q.maintain()
			return nil
		}
	}
	settled := false
	pendingTracked := false
	if !r.ReleasePending {
		settled = q.settleRelease(r, true)
		if j := q.touchJob(r.ID); j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			j.ReleaseConfirmed = true
			if j.LastStopOutcome == "release pending" {
				j.LastStopOutcome = "released"
			}
		}
	}
	if r.ReleasePending {
		if j := q.touchJob(r.ID); j != nil && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
			q.retainRelease(j)
		}
		key := releaseKey(HostRelease{Job: r.ID, Machine: r.Machine, Session: r.Session, Attempt: r.Attempt})
		if hold, ok := q.s.HostReleases[key]; ok {
			pendingTracked = true
			hold.CleanupFailed = true
			q.s.HostReleases[key] = hold
		}
	}
	if j := q.touchJob(r.ID); j != nil && r.Forced && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		j.LastStopOutcome = "forced"
		if r.ReleasePending {
			j.LastStopOutcome += "; release pending"
		}
	}
	if settled || pendingTracked {
		if _, err := q.current(r); err != nil {
			q.maintain()
			return nil
		}
	}
	// A retransmitted acknowledgement must not turn a completed job into stale work.
	if j := q.touchJob(r.ID); j != nil && terminal(j.State) && j.Attempt == r.Attempt && j.Machine == r.Machine && j.Session == r.Session {
		if filesVerified && wallTimeDiagnosticsAllowed(j, r) {
			for i := range j.AttemptHistory {
				h := &j.AttemptHistory[i]
				if h.Attempt == r.Attempt && h.Machine == r.Machine && h.Session == r.Session {
					h.Diagnostics = r.Diagnostics
				}
			}
		}
		return nil
	}
	j, err := q.current(r)
	if err != nil {
		return err
	}
	wallFailure := r.ExecutionCause != nil && r.ExecutionCause.Class == wallTimeLimit
	wallFailure = (wallFailure || wallTimeStopped(j) && (r.Success || r.Interrupted)) && !r.OutOfMemory && !attemptDiskLimitFailure(r)
	if wallFailure {
		r.Success, r.Interrupted, r.PriorityStop = false, false, nil
		r.ExecutionCause, r.Error = wallTimeCause(), wallTimeLimit
	}
	r.ExecutionCause = safeAttemptRuntimeCause(j, r.ExecutionCause)
	r.CleanupCause = safeAttemptRuntimeCause(j, r.CleanupCause)
	q.recordColdProbeRefusal(j, r)
	if r.ExecutionCause != nil && (r.ExecutionCause.Operation == "input_staging" || r.ExecutionCause.Finalization != nil) {
		// Keep the durable reason as safe as the structured cause; Error may
		// contain the original worker-supplied diagnostic name or private text.
		r.Error = r.ExecutionCause.String()
	}
	q.retainAttemptResources(j, r.Resources)
	retainInputsStagedAt(j, r.InputsStagedAt)
	if m := q.s.Machines[r.Machine]; m != nil {
		// Contact, but not a report: only a check-in rebuilds the reported
		// half after restart, so it alone clears unreported.
		q.touchMachine(m.ID)
		m.LastSeen = q.clock.Now()
	}
	diskFailure := attemptDiskLimitFailure(r)
	if diskFailure {
		// A denied attempt bound is a payload failure, including during a stop.
		// Retain its exact custody/evidence; never retry it as infrastructure loss
		// or reuse the memory OOM retry without fresh storage admission.
		r.Interrupted, r.PriorityStop = false, nil
	}
	r = withStopProvenance(r, priorityStop(j))
	q.retainYieldFinal(j, r)
	outcome := attemptOutcome(j, r)
	q.recordAttemptStorage(j, r)
	q.recordAttemptMemory(j, r) // Preserve facts before retry clears the envelope.
	j.ExecutionEvidence = safeExecutionEvidence(r.ExecutionEvidence, j.Attempt)
	j.ExecutionCause = safeRuntimeCause(r.ExecutionCause)
	j.CleanupCause = safeRuntimeCause(r.CleanupCause)
	// A kernel OOM is the payload's failure even if preemption raced it.
	ceilingFailure := !r.Success && strings.HasPrefix(ceilingDiagnostic(r.Error, ""), "exceeded its ")
	oomFailure := !r.Success && (r.OutOfMemory || ceilingFailure)
	outcome.OutOfMemory = oomFailure
	// Failed ceiling-bound attempts can hide true need. Successful constrained
	// samples are diagnostic only: they cannot invalidate accepted neighbours.
	// Unenforced process-group pressure never invalidates; a real OOM still does.
	if oomFailure || !r.Success && r.MemoryPeak != nil && r.MemoryPeak.Constrained && r.MemoryPeak.Source != processGroupMemorySource {
		q.recordWorkOOM(j, r, oomFailure)
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
			cards := owner.groupMembers
			if len(cards) == 0 {
				cards = []*Machine{owner}
			}
			for _, card := range cards {
				q.s.BatchReleases[batchReleaseKey(card)] = batchRelease{Session: j.Session, MemoryGB: deviceShareGB(j), Until: until}
			}
		}
	}
	if cancelled(j) || j.State == "stopping" && (!r.Success || !plannedPriorityStop(j)) && !oomFailure && !stageFailure && !diskFailure && !wallFailure && !wallTimeStopped(j) && !launchCapacityRefusal(r) {
		if cancelled(j) {
			q.finishCancellation(j)
		} else {
			q.recordSharedHostLatencyStop(j, r)
			q.retry(j, j.Reason)
		}
	} else if wallFailure {
		j.State, j.Reason = "failed", "execution failed: "+j.ExecutionCause.String()
	} else if diskFailure {
		j.State, j.Reason = "failed", "execution failed: "+j.ExecutionCause.String()
	} else if launchCapacityRefusal(r) {
		q.waitLaunchCapacity(j, r)
	} else if !r.Success && automaticExtensionCheck(j) != nil {
		j.State, j.Reason = "failed", "native check failed: "+r.Error
	} else if !r.Success && !oomFailure && j.StartedAt == nil && j.ExecutionCause != nil && j.ExecutionCause.Operation == "launch" && j.ExecutionCause.Class == "launch_refused" && q.storageRootReason(j, q.s.Machines[j.Machine]) != "" {
		q.requeue(j, q.storageRootReason(j, q.s.Machines[j.Machine]), false)
	} else if !r.Success && !oomFailure && j.StartedAt == nil && j.ExecutionCause != nil && j.ExecutionCause.Operation == "launch" && j.ExecutionCause.Class == "launch_refused" && q.diskHeadroomReason(j, q.s.Machines[j.Machine]) != "" {
		// A changed disk sample can withdraw an offer before exec. This is the
		// same temporary hold as dispatch, including for production stage failures.
		q.requeue(j, q.diskHeadroomReason(j, q.s.Machines[j.Machine]), false)
	} else if !r.Success && !oomFailure && j.ExecutionCause != nil && (j.ExecutionCause.Operation == "start_authorization" || j.ExecutionCause.Operation == "start_handoff") && startRefusalBeforeMain(j, r) {
		// The start fence withdrew placement before exec; like launch_refused,
		// this is a temporary hold, not lost work or an infrastructure retry.
		q.requeueStartRefusal(j)
		if j.State == "queued" {
			admissionRequeueOutcome(&outcome)
		}
	} else if oomFailure && j.Spec.Needs.Memory == "auto" {
		q.retryOOM(j)
	} else if (j.Spec.Priority == "production" || plannedPriorityStop(j)) && (stageFailure || oomFailure) {
		j.State, j.Reason = "failed", "command failed: "+r.Error
	} else if oomFailure && j.Spec.extensionJob() {
		q.retryOOM(j)
	} else if r.ExtensionUnavailable && j.Spec.extensionJob() && j.StartedAt == nil && j.Extension != nil {
		if m := q.s.Machines[r.Machine]; m != nil {
			q.touchMachine(m.ID)
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
			q.touchMachine(m.ID)
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
		reason := "execution interrupted; cause unconfirmed"
		if j.ExecutionCause != nil {
			reason = "execution interrupted: " + j.ExecutionCause.String()
		}
		q.retry(j, reason)
	} else if r.Success {
		if j.storedFileResults() {
			if !filesVerified {
				return ErrStale
			}
			j.OutputRefs = r.OutputRefs
			j.OutputFiles = nil
			if j.Spec.OutputDelivery == asyncNAS {
				now := q.clock.Now()
				j.Delivery = &OutputDelivery{State: "pending", Execution: r.Execution, Machine: r.Machine, Session: r.Session, DestinationID: deliveryRootID(q.deliveryRoot), ManifestSHA256: deliveryDigest(*j), PendingAt: now, Bytes: manifestDiskBytes(r.OutputRefs)}
			}
		}
		q.recordAttemptLearning(j, r)
		q.recordModelUse(j.Spec.Needs.Model)
		q.recordEntryCoverageSuccess(j, r.Machine)
		j.State = "succeeded"
	} else if q.entryCoverageFailure(j, r.Machine) {
		// A coverage finding requeued the job onto another machine type, or
		// failed it with the finding when no proven type remains.
	} else {
		j.State = "failed"
		j.Reason = "command failed: " + r.Error
	}
	if terminal(j.State) {
		now := q.clock.Now()
		j.FinishedAt = &now
	}
	if j.State == "failed" && filesVerified && !r.Success && j.Spec.capturesFailure() {
		outcome.Diagnostics = r.Diagnostics
	}
	q.recordAttemptOutcome(j, outcome)
	q.maintain()
	return nil
}
func (q *Queue) Leave(id string, now bool, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	return q.updateRequest(requestCtx, func() error {
		q.maintain()
		m := q.s.Machines[id]
		if m == nil || m.Gone {
			return errors.New("machine is not in the fleet")
		}
		q.touchMachine(m.ID)
		m.Leaving = true
		for _, j := range q.liveJobs() {
			if j.Machine == id && (j.Spec.Resumable || now) {
				q.stop(j, "machine leaving")
			}
		}
		return nil
	}, nil)
}
func (q *Queue) Snapshot() (Snapshot, error) { return q.snapshot(false) }

func (q *Queue) snapshot(forStatus bool, contexts ...context.Context) (Snapshot, error) {
	if forStatus {
		return q.statusSnapshot(contexts...)
	}
	var out Snapshot
	err := q.updateRequest(requestContext(contexts), func() error { q.maintain(); out = q.projectSnapshot(false); return nil }, nil)
	if err == nil {
		out.Usage, err = q.usageSnapshot(out.Now.Add(-7*24*time.Hour), requestContext(contexts))
	}
	return out, err
}

// Projection runs against either the locked queue or a fully detached read view.
func (q *Queue) projectSnapshot(forStatus bool, budgets ...*requestBudgets) Snapshot {
	compact := q.observationSelection != nil && q.observationSelection.compact
	out := Snapshot{CheckpointExpiry: q.checkpointExpiryCounts(), EvidenceRetention: q.evidenceRetention, Efficiency: q.efficiencyView, HealthAlerts: append([]HealthAlert(nil), q.s.Health.Events...), FleetUpgrade: q.fleetUpgradeStatus(), QueueRelease: CurrentRelease(), Holds: []Hold{}, SyncRequests: append([]SyncRequest(nil), q.s.SyncRequests...), Pins: append([]ModelPin(nil), q.s.Pins...), Jobs: []*Job{}, Machines: []*Machine{}, LoadedModels: []ModelSlot{}}
	out.HealthAlertDelivery = q.healthAlertDelivery()
	if compact {
		out.FleetUpgrade = q.observationSelection.upgrade
	}
	for _, s := range q.s.ModelSlots {
		copy := *s
		model, _ := q.model(s.Model)
		if !compact {
			copy.Placement = q.modelPlacement(model)
		} else {
			copy.Placement = nil
		}
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
	if q.inRestartGrace(q.clock.Now()) && len(q.unreported) > 0 {
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
	hosts := map[string]bool{}
	for _, m := range q.s.Machines {
		hosts[hostID(m)] = true
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

	out.HostStates = make(map[string]string, len(hosts))
	for host := range hosts {
		out.HostStates[host] = q.hostMaintenanceStatus(host).State
		out.DeviceMaintenance = append(out.DeviceMaintenance, q.deviceMaintenanceStatus(host)...)
		if h := q.startupReleaseHold(host); h.Kind != "" {
			out.Holds = append(out.Holds, h)
		}
		if q.maintenanceBlocked(host) {
			h := scopedHold("host_disabled", host, "host "+host+" disabled by operator", true)
			h.Next = strings.ReplaceAll(h.Next, "HOST", host)
			out.Holds = append(out.Holds, h)
		}
	}
	slices.SortFunc(out.DeviceMaintenance, func(a, b DeviceMaintenance) int {
		return strings.Compare(a.Host+"/"+deviceMaintenanceKey(a.Platform, a.Identity), b.Host+"/"+deviceMaintenanceKey(b.Platform, b.Identity))
	})
	// Retained diagnostics are absent from compact captures; explicit section/full
	// reads preserve every existing evidence field.
	for _, key := range slices.Sorted(maps.Keys(q.s.StorageObservations)) {
		out.StorageObservations = append(out.StorageObservations, q.s.StorageObservations[key])
	}
	for _, key := range slices.Sorted(maps.Keys(q.s.WorkObservations)) {
		out.WorkCurves = append(out.WorkCurves, q.s.WorkObservations[key])
	}
	if q.observationSelection != nil {
		page := q.observationSelection.page
		out.JobsPage = &page
	}
	if q.statusAdmissions != nil {
		out.Admissions = *q.statusAdmissions
	} else {
		out.Admissions = q.hostAdmissions()
	}
	out.ComponentExclusions = q.componentExclusions()
	out.Reservations = q.reservations()
	out.Holds = append(out.Holds, operatorHolds(out.Reservations, out.ComponentExclusions, out.DeviceMaintenance)...)
	sortHolds(out.Holds)
	out.PriorityPolicy = q.priorityPolicyRecord()
	out.Now = q.clock.Now()
	order, reasons, holds := map[string]int{}, map[string]string{}, map[string]*Hold{}
	if !compact {
		order, reasons, holds = q.queueProjection()
	}
	projectionSample := startStatusProjection(budgets)
	finishJobWork := projectionSample.measure()
	for _, stored := range q.s.Jobs {
		if q.observationSelection != nil && !q.observationSelection.ids[stored.ID] {
			continue
		}
		if compact {
			copy := *stored
			copy.Spec.Class = jobClass(copy.Spec)
			copy.PriorityPolicy = q.priorityMatch(copy.Spec)
			if copy.State == "queued" && copy.Reason == "" {
				copy.Reason = "waiting for capacity; inspect this job for placement diagnostics"
			}
			q.projectRuntime(&copy)
			copy = statusJobValue(&copy)
			copy.Timing = nil
			out.Jobs = append(out.Jobs, &copy)
		} else {
			j := q.projectJob(stored, order, reasons, forStatus)
			j.wait = holds[stored.ID]
			out.Jobs = append(out.Jobs, j)
		}
	}
	for _, c := range q.calls {
		if q.observationSelection != nil && !q.observationSelection.ids[c.ID] {
			continue
		}
		if compact {
			copy := statusJobValue(compactStatusCall(c))
			copy.Spec.Class = jobClass(copy.Spec)
			copy.PriorityPolicy = q.priorityMatch(copy.Spec)
			q.projectRuntime(&copy)
			out.Jobs = append(out.Jobs, &copy)
		} else {
			out.Jobs = append(out.Jobs, q.projectCall(c, order, reasons))
		}
	}
	finishJobWork()
	for _, m := range q.s.Machines {
		copy := cloneMachine(m)
		copy.Suspect = q.workerSuspect(m, q.clock.Now())
		copy.ExternalMemory = q.externalMemoryStatus(hostID(m))
		if copy.IOPressure != nil {
			copy.IOPressure.AgeSeconds += max(0, q.clock.Now().Sub(copy.IOPressure.ReceivedAt).Seconds())
		}
		for i, v := range copy.DiskVolumes {
			copy.DiskVolumes[i] = q.currentDiskVolume(v, m.DiskReceivedAt)
		}
		copy.RecoveryNeedsAttention = m.Recovering && m.RecoveringSince != nil && q.clock.Now().Sub(*m.RecoveringSince) > recoveryAttentionDelay
		q.machineDeviceStatus(&copy, m)
		copy.AdmissionReason = q.machineLimitReason(m)
		copy.Disabled = ""
		copy.LastResort = q.s.HostMaintenance[hostID(m)].LastResort
		if q.maintenanceBlocked(hostID(m)) {
			copy.Disabled = "planned host maintenance"
		}
		if a := machineAvailability(m); a != nil && !a.Available {
			copy.AdmissionReason = a.Reason
		}
		copy.ReservedMemoryGB, copy.MemoryUsedGB = 0, 0 // Used is total − free.
		for _, slot := range q.s.ModelSlots {
			if slot.Machine == m.ID && slotReservesMemory(slot) && q.slotPinned(slot) {
				model, _ := q.model(slot.Model)
				copy.ReservedMemoryGB += model.MemoryGB
			}
		}
		copy.ModelCopySeconds = map[string]float64{}
		if !compact {
			for _, model := range q.models.Models {
				if q.modelFitsMachine(model, m) {
					copy.ModelCopySeconds[model.Name] = q.loadSeconds(model.Name, m)
				}
			}
		}
		out.Machines = append(out.Machines, &copy)
	}
	finishJobWork = projectionSample.measure()
	for _, j := range out.Jobs {
		if !compact {
			q.projectJobDiagnostics(j, forStatus, false)
		}
	}
	finishJobWork()
	projectionSample.finish(out.Jobs)
	sort.SliceStable(out.Jobs, func(i, k int) bool { return statusBefore(out.Jobs[i], out.Jobs[k]) })
	sort.Slice(out.Machines, func(i, k int) bool { return out.Machines[i].ID < out.Machines[k].ID })
	return out
}

func (q *Queue) projectJob(stored *Job, order map[string]int, reasons map[string]string, forStatus bool) *Job {
	value := *stored
	j := &value
	if stored.Delivery != nil {
		d := *stored.Delivery
		value.Delivery = &d
	}
	if j.State == "queued" {
		j.Reason = reasons[j.ID]
		if j.RequeuedByOperator {
			j.Reason = "requeued: disabled by operator; " + j.Reason
		}
	}
	if q.startupPending(j) || q.startCapacityPaused(j) {
		j.Reason = q.startStatusReason(j)
	}
	if j.State == "running" && j.ReleaseConfirmed {
		j.Reason = "execution released; awaiting final result publication"
		if cause := safeRuntimeCause(j.ExecutionCause); cause != nil && cause.Finalization != nil {
			j.Reason += "; " + cause.String()
		}
	}
	// Eligibility/reasons need the full diagnostic inputs until redaction.
	copy := *j
	if !forStatus {
		copy = cloneJob(j)
		q.projectCancellation(&copy, stored)
	}
	copy.Spec.Class = jobClass(copy.Spec)
	copy.PriorityPolicy = q.priorityMatch(copy.Spec)
	copy.QueueOrder = order[j.ID]
	q.projectRuntime(&copy)
	if terminal(j.State) && j.LastStopOutcome == "release pending" {
		copy.Reason += "; release pending"
	}
	return &copy
}

func (q *Queue) projectCall(c *ModelCall, order map[string]int, reasons map[string]string) *Job {
	call := q.callView(c)
	call.QueueOrder = order[c.ID]
	switch {
	case c.Error != "":
		call.State, call.Reason = "failed", c.Error
	case c.Machine != "":
		call.State, call.Reason = "running", "model call in flight"
	default:
		h := q.waitHold(call)
		call.Reason, call.wait = h.Detail, h
		// The queue-order explanation refines only the call's generic waits.
		if reason := reasons[c.ID]; reason != "" && (h.Kind == "no_eligible_host" || h.Kind == "model_engine" || h.Kind == "waiting_for_worker") {
			call.Reason = reason
		}
	}
	call.Spec.Class = jobClass(call.Spec)
	call.PriorityPolicy = q.priorityMatch(call.Spec)
	return call
}

// callView is a model call as the scheduler sizes it: the model's memory and
// log policy on a queued job.
func (q *Queue) callView(c *ModelCall) *Job {
	call := callJob(c)
	call.Spec.Name = "model call: " + c.Model
	model, _ := q.model(c.Model)
	call.Spec.Logs = model.Logs
	call.Spec.Needs.MemoryGB = model.MemoryGB
	call.Spec.Needs.HostMemoryGB = model.HostMemoryGB
	call.Machine = c.Machine
	call.State = "queued"
	return call
}

// callWait is the scheduler's wait for a model call, in the call's precedence:
// log policy, host RAM, production/disk fences, project cap, model
// placement, engine recovery, then an ordinary wait for a worker.
func (q *Queue) callWait(c *ModelCall) *Hold {
	call := q.callView(c)
	model, _ := q.model(c.Model)
	if reason := q.logsWaitingReason(call); reason != "" {
		return newHold("log_policy", reason)
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
			return newHold("host_memory_headroom", fmt.Sprintf("waiting for RAM: model %s needs %.1f GiB host memory (or telemetry unavailable)", c.Model, model.HostMemoryGB))
		}
	}
	if h := q.holdWait(call); h != nil {
		return h
	}
	if reason := q.capReason(call); reason != "" {
		return newHold("project_cap", reason)
	}
	if c.Machine == "" && (model.Project != "" || len(model.AllowedMachines) > 0 || model.LocalSource != "") {
		eligible := false
		for _, worker := range q.s.Machines {
			if q.projectAllows(c.Project, worker.ID) && q.modelFitsMachine(model, worker) && !worker.Gone && !worker.Leaving {
				eligible = true
				break
			}
		}
		if !eligible {
			return newHold("no_eligible_host", "waiting for model placement: entry/owner/caller allowed_machines or local_source approval excludes available workers")
		}
	}
	if reason := q.engineWaitReason(c); reason != "" {
		return newHold("model_engine", reason)
	}
	return newHold("waiting_for_worker", "waiting for model "+c.Model+" on an eligible worker")
}

func (q *Queue) projectJobDiagnostics(j *Job, forStatus, jobReply bool) {
	projectCommandStarted(j)
	q.projectEfficiency(j)
	j.YieldPlan = q.yieldProjection(j)
	// The code is the scheduler's wait kind, never a reading of its text.
	if j.State == "queued" && j.wait != nil {
		if code := holdDiagnosticCodes[j.wait.Kind]; code != "" {
			j.DiagnosticCode = code
		}
	}
	if j.State != "queued" && j.ExecutionCause != nil && j.ExecutionCause.Class == cpuIsolationUnavailable {
		j.DiagnosticCode = cpuIsolationUnavailable
	}
	if forStatus {
		q.statusEligibility(j)
		q.projectVariantEligibility(j)
		if j.State == "queued" {
			j.Reason = q.statusWaitingReason(j)
		}
		if j.State == "paused" {
			j.Hold = pauseHold(j)
		}
		key := j.Spec.SubmissionKey
		lock := j.EnvironmentLock
		// Status projection owns a fully detached view. Redaction can reuse
		// its fields without another JSON round-trip for every job.
		*j = statusJobValue(j)
		if !jobReply && j.Cancellation != nil {
			ack := *j.Cancellation
			ack.SubmissionKey = nil
			j.Cancellation = &ack
		}
		if jobReply {
			if lock != nil {
				copied := *lock
				copied.SHA256 = maps.Clone(lock.SHA256)
				j.EnvironmentLock = &copied
			}
			j.Spec.SubmissionKey = key
			j.SpecProjection.Omitted = slices.DeleteFunc(j.SpecProjection.Omitted, func(path string) bool {
				return path == "submission_key"
			})
		}
	}
}

func (q *Queue) AppendLog(l LogChunk, requestContexts ...context.Context) error {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestLock(requestCtx, &q.mu)
	defer requestUnlock1()
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
		q.touchMachine(m.ID) // Contact only; see the result path.
		m.LastSeen = q.clock.Now()
		q.republishContact()
	}
	return nil
}

type Log struct {
	Seq     int64  `json:"seq"`
	Attempt int    `json:"attempt"`
	Text    string `json:"text"`
}

func (q *Queue) Logs(id string, after int64, requestContexts ...context.Context) ([]Log, error) {
	requestCtx := requestContext(requestContexts)

	requestUnlock1 := requestLock(requestCtx, &q.mu)
	defer requestUnlock1()
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
	value := *j
	value.LedgerEntries = nil // Immutable entries can be shared by undo/assignment snapshots.
	// FileRef is a value of strings and an integer. Copy the manifest directly
	// rather than encoding and decoding every input in each transaction undo.
	value.Spec.Files = nil
	data, _ := json.Marshal(&value)
	var out Job
	_ = json.Unmarshal(data, &out)
	if len(j.Spec.Files) > 0 {
		out.Spec.Files = maps.Clone(j.Spec.Files)
	}
	out.LedgerEntries = j.LedgerEntries
	return out
}
func cloneMachine(m *Machine) Machine {
	data, _ := json.Marshal(m)
	var out Machine
	_ = json.Unmarshal(data, &out)
	out.toolInventory = m.toolInventory
	if out.HostHealth != nil {
		out.HostHealth.delivery = m.HostHealth.delivery
	}
	return out
}

// Startup acknowledgements use queue time, avoiding cross-machine clock skew.
func (q *Queue) markStarted(j *Job) {
	q.touchJob(j.ID)
	if j.StartedAt == nil {
		now := q.clock.Now()
		j.StartedAt = &now
		// The resume has its place back. An assignment refused before it
		// started keeps it.
		j.MaintenanceResume = nil
		for i := range j.Copies {
			c := &j.Copies[i]
			if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && c.StartedAt == nil {
				c.StartedAt = &now
			}
		}
		q.checkLedgerNativeCoverage(j)
		q.appendJobLedger(j, ledgerAttempt(j, "started", "start_authorized"))
		q.beginWorkTiming(j)
		if j.Spec.StartBy != nil && j.StartWindow == 0 {
			j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
		}
		j.Reason = ""
		since := j.Queued
		if j.Dependency != nil && j.Dependency.ReadyAt != nil {
			since = *j.Dependency.ReadyAt
		}
		j.StartLatencySeconds = now.Sub(since).Seconds()
		if j.Spec.Priority == "production" {
			q.s.ProductionStarts = append(q.s.ProductionStarts, ProductionStart{At: now, Job: j.ID, Seconds: j.StartLatencySeconds})
		}
	}
}
func (q *Queue) Started(r Result, requestContexts ...context.Context) error {
	_, err := q.startedWithWallTime(r, requestContexts...)
	return err
}
func (q *Queue) startedWithWallTime(r Result, requestContexts ...context.Context) (*float64, error) {
	requestCtx := requestContext(requestContexts)

	var remaining *float64
	var rejected error
	err := q.updateRequest(requestCtx, func() error {
		q.maintain()
		j, e := q.current(r)
		if e != nil {
			rejected = e
			return nil
		}
		// An exact-current replay reconciles a lost durable Started reply. It
		// must still pass every live fence, and markStarted never resets time or
		// appends a second start. Acknowledgement is not a second execution.
		if j.State != "running" || j.ReleaseConfirmed || !dependencyReady(j) || cancelled(j) {
			rejected = ErrStale
			return nil
		}
		m := q.s.Machines[r.Machine]
		q.refreshHostMemory()
		predicate := "host_launch"
		if q.assignedLaunchAllowed(m, r.Session, j) {
			predicate = q.startPlacementRejection(j)
		}
		if predicate != "" {
			q.recordStartRefusal(j, predicate)
			rejected = ErrStale
			return nil
		}
		q.clearStartRefusal(j)
		q.retainAttemptResources(j, r.Resources)
		retainInputsStagedAt(j, r.InputsStagedAt)
		q.markStarted(j)
		if at := wallTimeDeadline(j); at != nil {
			left := max(0, at.Sub(q.clock.Now()).Seconds())
			remaining = &left
		}
		return nil
	}, nil)
	if err != nil {
		return nil, err
	}
	return remaining, rejected
}

func (q *Queue) renewRecoveryStartWindow(j *Job) {
	q.touchJob(j.ID)
	// Planned leave keeps its original deadline; queue maintenance stops retain
	// the recovery window for already-started attempts.
	recovering := j.State == "running" || j.State == "stopping" && plannedStop(j)
	if j.Spec.Priority == "production" && recovering && j.StartedAt != nil && j.Spec.StartBy != nil {
		// A started interruption gets the original first-start window, even
		// when its length came from an explicit first-start cutoff. Waiting
		// alone never renews an absolute cutoff.
		if j.StartWindow == 0 {
			j.StartWindow = j.Spec.StartBy.Sub(j.Queued)
		}
		deadline := q.clock.Now().Add(j.StartWindow)
		j.Spec.StartBy = &deadline
		j.StartCapacityBudget = nil
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
	j := q.touchJob(report.ID)
	if j != nil && (j.State == "queued" || j.State == "running" || j.State == "stopping") && !cancelled(j) && (j.State == "queued" || j.Attempt != report.Attempt || j.Machine != m.ID || j.Session != m.Session) {
		for _, copy := range j.Copies {
			canRecover := q.machineRecoveryFits(m, j, copy)
			if canRecover && q.placementPolicyAllowed(j, allocationMachine(m, copy.AllocatedDevices), false) && copy.Recoverable && copy.Attempt == report.Attempt && copy.Machine == m.ID && copy.Session == m.Session && j.State != "queued" && (m.CheckpointUnavailable || !q.checkpointsAvailable() || progress.err != nil || (progress.path != "" && progress.path != attemptCheckpoint(*j))) {
				reply.Hold = true
				return reply
			}
			if canRecover && q.placementPolicyAllowed(j, allocationMachine(m, copy.AllocatedDevices), false) && copy.Recoverable && copy.Attempt == report.Attempt && copy.Machine == m.ID && copy.Session == m.Session && (j.State == "queued" || (j.Spec.Resumable && report.Progress > max(0, progress.durable))) {
				for i := range j.Copies {
					j.Copies[i].Recoverable = false
				}
				j.InputsStagedAt = nil // Recovery adopts a different execution.
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
	if j != nil {
		reply.UserCancel = cancelled(j)
	}
	if j == nil || j.Attempt != report.Attempt || j.Machine != m.ID || j.Session != m.Session || (j.State != "running" && j.State != "stopping") {
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
	if !j.FileTransport && j.Spec.ResumeCompatibility != nil && progress.path == attemptCheckpoint(*j) && progress.err == nil && progress.durable > 0 {
		j.YieldGeneration = max(j.YieldGeneration, progress.durable)
	}
	q.retainAttemptResources(j, report.Resources)
	retainInputsStagedAt(j, report.InputsStagedAt)
	if report.Started && j.Spec.StartBy == nil && j.StartedAt == nil && j.State == "running" && q.startPlacementPolicyAllowed(j) {
		q.markStarted(j)
	}
	j.Progress = report.Progress
	noteCheckpointTarget(j, allocationMachine(m, j.AllocatedDevices), j.Attempt, report.Progress)
	q.retainApplicationProgress(j, report, m)
	q.retainLiveness(j, report)
	if j.State == "stopping" {
		reply.PriorityStop = priorityStop(j)
		reply.UserCancel = cancelled(j)
		reply.Stop = true
		reply.Kill = expired(j.StopAt, q.clock.Now())
		reply.GraceSeconds = j.StopAt.Sub(q.clock.Now()).Seconds()
	}
	return reply
}

// An attempt that is no longer its job's live execution (the job finished or
// moved on) gets no other control, so a lost assignment's custody would stay
// pending forever (#1021). Address a keyed stop to each unconfirmed copy on
// this exact worker session that the worker did not report. The worker
// acknowledges only a key it does not hold, and the start gate refuses launches
// for non-current executions, so no process can exist when that ack arrives.
// Omission alone never confirms release.
func (q *Queue) unheldCopyStops(machine, session string, reports []AttemptReport) []AttemptDirective {
	reported := map[attemptKey]bool{}
	for _, r := range reports {
		reported[attemptKey{r.ID, r.Attempt, r.Session}] = true
	}
	var out []AttemptDirective
	add := func(j *Job) {
		for _, c := range j.Copies {
			if c.ReleaseConfirmed || c.Machine != machine || c.Session != session || reported[attemptKey{j.ID, c.Attempt, c.Session}] {
				continue
			}
			if c.Attempt == j.Attempt && c.Machine == j.Machine && c.Session == j.Session && (j.ReleaseConfirmed || j.State == "running" || j.State == "stopping") {
				continue // The current execution keeps its ordinary offer/stop protocol.
			}
			out = append(out, AttemptDirective{Execution: Execution{ID: j.ID, Attempt: c.Attempt}, Session: c.Session, Stop: true, Kill: true, UserCancel: cancelled(j)})
		}
	}
	// Unconfirmed custody keeps a job mutable, so live and removed-but-mutable
	// records cover every candidate without scanning retained history.
	for _, j := range q.liveJobs() {
		add(j)
	}
	if q.records != nil {
		for id, j := range q.records.removed {
			if q.s.Jobs[id] == nil {
				add(j)
			}
		}
	} else {
		for _, r := range q.s.Submissions {
			if r.Job != nil && q.s.Jobs[r.JobID] == nil {
				add(r.Job)
			}
		}
	}
	slices.SortFunc(out, func(a, b AttemptDirective) int {
		return cmp.Or(strings.Compare(a.ID, b.ID), cmp.Compare(a.Attempt, b.Attempt))
	})
	return out
}

// The bounded legacy adapter retains its original scalar recovery limit.
func (q *Queue) workerHasOtherAttempt(machine, session, job string, attempt int) bool {
	for _, j := range q.liveJobs() {
		if j.Machine == machine && j.Session == session && !(j.ID == job && j.Attempt == attempt) && batchReservesCapacity(j) {
			return true
		}
	}
	for _, r := range q.s.HostReleases {
		if !r.Preparation && r.Machine == machine && !(r.Job == job && r.Session == session && r.Attempt == attempt) {
			return true
		}
	}
	return false
}

// assign persists the exact target before delivering it through either wire form.
func (q *Queue) assign(j *Job, m *Machine, scans ...*placementScan) Job {
	q.touchJob(j.ID)
	values := q.placementValues(q.placementScan(scans...))
	if j.Spec.FanOut != nil {
		q.touchJob(j.ID).PreemptionPlacement = nil
		j = q.assignFanOut(j, m)
	}
	estimate, measured := q.estimateCompletion(j, m)
	// The planner returns a target-specific copy. Persist its exact
	// reservation on the live attempt before charging any resources.
	selected := j
	j = q.touchJob(selected.ID)
	j.PreemptionPlacement = nil
	j.Spec.Needs = selected.Spec.Needs
	j.CPUFallbackFrom = selected.CPUFallbackFrom
	j.MemoryEnvelope, j.MemoryFallback = selected.MemoryEnvelope, selected.MemoryFallback
	if e := j.MemoryEnvelope; e != nil && e.LimitHostGB > 0 && e.ChargeHostGB == j.Spec.Needs.HostMemoryGB {
		// Admission used the learned charge; the typed figure stays the limit.
		j.Spec.Needs.HostMemoryGB = e.LimitHostGB
	}
	q.recordLearnedMemory(j, m)
	if selected.Batch != nil {
		j.Batch = selected.Batch
	}
	if e := qualifiedExtension(selected, m); e != nil {
		j.Extension = &ExtensionSelection{Name: e.Name, Version: e.Version, Digest: e.Digest}
	}
	j.ModelEnvironmentKey = ""
	j.ModelEnvironmentNeedsSource = false
	if model, ok := q.model(j.Spec.Needs.Model); ok {
		recipe := q.environments.Environments[model.Environment][m.GPU]
		j.ModelEnvironmentKey = recipe.Key
		j.ModelEnvironmentNeedsSource = recipe.Binary != "" || recipe.BinaryRelative != ""
	}
	if j.DeviceFaultStartWait != nil {
		q.resumeDeviceFaultStartBudget(j)
	} else if (j.StartCapacityWaiting || q.launchCapacityPending(j)) && !absoluteStartBy(j) && j.Spec.StartBy != nil {
		deadline := q.clock.Now().Add(startCapacityBudget(j))
		j.Spec.StartBy = &deadline
	}
	j.StartCapacityWaiting = false
	q.closeAdmissionWait(j, "assigned")
	j.State = "running"
	j.ExecutionCause, j.CleanupCause = nil, nil
	j.ExecutionEvidence = nil
	j.ReleaseConfirmed = false
	j.RequeuedByOperator = false
	j.StartedAt = nil
	j.InputsStagedAt = nil
	j.Timing = nil
	j.ApplicationProgress = nil
	j.Liveness = nil
	now := q.clock.Now()
	j.PriorityPolicy = q.priorityMatch(j.Spec)
	j.FaultEvidence = nil
	j.AssignedAt = &now
	if !j.Spec.Needs.cpuOnly() {
		q.recordDeviceUse(m)
	}
	j.FinishedAt = nil
	j.StartLatencySeconds = 0
	if q.s.Machines[m.ID] == m {
		q.touchMachine(m.ID)
	}
	m.FreeMemoryGB = max(0, m.FreeMemoryGB-j.Spec.Needs.MemoryGB)
	// Starts plus admission requeues is the issued-attempt high-water mark, so
	// an uncounted refusal or a recovered older copy never reissues a number.
	j.Attempt = max(j.Starts+j.AdmissionRequeues, j.Attempt) + 1
	j.Starts = j.Attempt - j.AdmissionRequeues
	if j.storedFileResults() {
		j.OutputRefs = nil
		j.PendingOutputRefs = nil
	}
	if j.FileTransport {
		j.ResumeFiles = bestFileCheckpoint(j)
	}
	if (j.Spec.extensionJob() || j.Spec.After != nil) && !j.storedFileResults() {
		j.OutputFiles = map[string]string{}
		for name, prefix := range j.Spec.Outputs {
			j.OutputFiles[name] = outputRelative(*j, name, prefix)
		}
	}
	j.Progress = 0
	j.YieldGeneration = 0
	if y := j.ScarceHostYield; y != nil {
		y.Destination, y.DestinationDevice, y.DestinationTarget = m.ID, m.Device, machineTarget(m)
	}
	j.Machine = m.ID
	j.AllocatedDevices = nil
	if m.Devices != nil && !j.Spec.Needs.cpuOnly() {
		j.AllocatedDevices = slices.Clone(targetDevices(m))
	}
	j.Copies = append(j.Copies, Copy{AllocatedDevices: slices.Clone(j.AllocatedDevices), Machine: m.ID, Session: m.Session, Attempt: j.Attempt, Extension: j.Extension, ReleaseReason: "awaiting_release"})
	j.Session = m.Session
	q.claimMemoryExploration(j, m)
	q.recordColdProbeAssignment(j, m)
	j.StorageEnvelope = q.storageEnvelope(j, m)
	j.Reason = ""
	if measured {
		j.Timing = &WorkTiming{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: m.ID, Session: m.Session, PlacementReason: estimate.reason()}
	}
	q.recordValuePlacement(j, m, values)
	q.recordPlacementDecision(j, m)
	entry := ledgerAttempt(j, "assigned", "scheduled")
	entry.Placement = j.Placement
	q.appendJobLedger(j, entry)
	q.refreshHostMemory()
	out := cloneJob(j)
	out.LedgerEntries = nil
	return out
}
