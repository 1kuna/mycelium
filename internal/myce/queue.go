package myce

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
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
	DisabledHosts    map[string]string        `json:"disabled_hosts,omitempty"`
	Hosts            map[string]*HostState    `json:"hosts,omitempty"`
	HostReleases     map[string]HostRelease   `json:"host_releases,omitempty"`
	BatchReleases    map[string]batchRelease  `json:"batch_releases,omitempty"`
	UsageMaintenance time.Time                `json:"usage_maintenance,omitempty"`
	ProductionStarts []ProductionStart        `json:"production_starts,omitempty"`
	Policies         map[string]ProjectPolicy `json:"project_policies,omitempty"`
	FairUsage        map[string]FairUsage     `json:"fair_usage,omitempty"`
	UsageAt          time.Time                `json:"usage_at,omitempty"`
	Preemptions      []Preemption             `json:"preemptions,omitempty"`

	SyncRequests []SyncRequest         `json:"sync_requests,omitempty"`
	ModelUses    map[string]ModelUse   `json:"model_uses,omitempty"`
	Pins         []ModelPin            `json:"pins,omitempty"`
	ModelSlots   map[string]*ModelSlot `json:"model_slots,omitempty"`
	Jobs         map[string]*Job       `json:"jobs"`
	Machines     map[string]*Machine   `json:"machines"`
	Front        int64                 `json:"front"`
}
type Queue struct {
	nasProbe     checkpointIO[bool]
	nasProgress  checkpointIO[int64]
	nasAvailable bool
	// simulation bypasses durable storage only; all scheduling/accounting paths remain shared.
	checkpointProbe  func() bool // Filesystem fault seam.
	simulation       bool
	simID            int64
	uploadSpool      uploadSpool
	environments     EnvironmentCatalog
	productionGrace  time.Duration
	wake             chan struct{}
	models           ModelConfig
	calls            map[string]*ModelCall
	lock             *os.File
	mu               sync.Mutex
	db               *sql.DB
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
func Open(path, root string, clock Clock) (*Queue, error) {
	if clock == nil {
		return nil, errors.New("clock is required")
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
	q := &Queue{uploadSpool: uploadSpool{limit: DefaultUploadSpoolBytes}, productionGrace: 10 * time.Second, wake: make(chan struct{}), lossTimeouts: [3]time.Duration{ProductionLossAfter, DeadlineLossAfter, LossAfter}, lock: lock, db: db, clock: clock, root: abs, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS logs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT NOT NULL, attempt INTEGER NOT NULL, text TEXT NOT NULL); CREATE TABLE IF NOT EXISTS log_offsets (job TEXT, attempt INTEGER, offset INTEGER, PRIMARY KEY(job,attempt,offset)); CREATE TABLE IF NOT EXISTS environment_log_offsets (job TEXT, machine TEXT, session TEXT, key TEXT, offset INTEGER, PRIMARY KEY(job,machine,session,key,offset)); CREATE TABLE IF NOT EXISTS environment_build_logs (machine TEXT, session TEXT, key TEXT, offset INTEGER, text TEXT, PRIMARY KEY(machine,session,key,offset)); CREATE INDEX IF NOT EXISTS logs_job ON logs(job,seq); CREATE TABLE IF NOT EXISTS usage_samples (at INTEGER NOT NULL, span INTEGER NOT NULL, machine TEXT NOT NULL, device TEXT NOT NULL, project TEXT NOT NULL, job TEXT NOT NULL, model TEXT NOT NULL, busy REAL NOT NULL, idle REAL NOT NULL, pinned REAL NOT NULL, memory_seconds REAL NOT NULL, observed REAL NOT NULL, PRIMARY KEY(at,span,machine,device,project,job,model));`); err != nil {
		db.Close()
		return nil, err
	}
	var data []byte
	err = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data)
	if err == nil {
		err = json.Unmarshal(data, &q.s)
	}
	if err != nil && err != sql.ErrNoRows {
		db.Close()
		return nil, err
	}
	q.unreported = map[string]bool{}
	if len(data) > 0 {
		q.restartUntil = clock.Now().Add(RestartGrace)
		for _, j := range q.s.Jobs {
			if j.Attempt > 0 && len(j.Copies) == 0 && j.Machine != "" {
				j.Starts = j.Attempt
				j.Copies = append(j.Copies, Copy{Machine: j.Machine, Session: j.Session, Attempt: j.Attempt})
			}
		}
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
	priority := 2
	for _, j := range q.s.Jobs {
		if j.Machine == id && (j.State == "running" || j.State == "stopping") {
			priority = min(priority, rank(j.Spec.Priority))
		}
	}
	for _, c := range q.calls {
		if c.Machine == id && c.Error == "" {
			priority = min(priority, rank(c.Priority))
		}
	}
	for _, slot := range q.s.ModelSlots {
		if slot.Machine == id && (slot.State == "loading" || slot.InFlight > 0) {
			priority = min(priority, rank(slot.Priority))
		}
	}
	return q.lossTimeouts[priority]
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
		available, _ = q.nasProbe.run(q.root, func() (bool, error) {
			if q.checkpointProbe != nil {
				return q.checkpointProbe(), nil
			}
			return checkpointAvailable(q.root), nil
		})
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
		var data []byte
		data, err = json.Marshal(q.s)
		if err == nil {
			err = q.persistUsage(data)
		}
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
	if err := s.Validate(); err != nil {
		return nil, err
	}
	var out Job
	err := q.update(func() error {
		if e := q.validateProjectPlacement(s.Project, s.AllowedMachines); e != nil {
			return e
		}
		recipes, e := q.resolveEnvironment(s.Environment)
		if e != nil {
			return e
		}
		var id string
		if q.simulation {
			q.simID++
			id = fmt.Sprintf("sim-%012d", q.simID)
		} else {
			id = ID()
		}
		j := &Job{EnvironmentRecipes: recipes, ID: id, Spec: s, State: "queued", Queued: q.clock.Now(), WaitingSince: q.clock.Now(), Checkpoint: filepath.Join(q.root, id)}
		if e := q.validateProjectCapacity(j); e != nil {
			return e
		}
		copy := cloneJob(j)
		j = &copy
		q.s.Jobs[id] = j
		q.maintain()
		out = cloneJob(j)
		return nil
	})
	return &out, err
}
func (q *Queue) retry(j *Job, why string) {
	j.Machine = ""
	j.Session = ""
	j.StopAt = nil
	j.Reason = why
	if j.Losses >= 3 {
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
func (q *Queue) maintain() {
	now := q.clock.Now()
	for _, j := range q.s.Jobs {
		if j.State == "running" || j.State == "stopping" {
			q.retainRelease(j)
		}
	}
	q.maintainHolds()
	for machine, release := range q.s.BatchReleases {
		if !now.Before(release.Until) {
			delete(q.s.BatchReleases, machine)
		}
	}
	active, healthy := 0, 0
	for _, m := range q.s.Machines {
		if !m.Gone && !m.Leaving && m.Disabled == "" {
			active++
			if now.Sub(m.LastSeen) < min(q.lossTimeouts[0], min(q.lossTimeouts[1], q.lossTimeouts[2])) {
				healthy++
			}
		}
	}
	q.networkQuiet = active > 0 && (healthy == 0 || healthy*2 < active)
	for _, m := range q.s.Machines {
		unreported := q.unreported[m.ID]
		grace := unreported && now.Before(q.restartUntil)
		restartExpired := unreported && !grace
		if m.Disabled == "" && !m.Gone && !grace && (restartExpired || (!q.networkQuiet && now.Sub(m.LastSeen) >= q.machineLossTimeout(m.ID))) {
			m.Gone = true
			delete(q.unreported, m.ID)
			m.Models = nil
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
					if j.Reason == "cancel requested" {
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
	for _, j := range q.s.Jobs {
		if j.Reason != "disabled by operator" && !q.networkQuiet && !q.unreported[j.Machine] && j.State == "stopping" && expired(j.StopAt, now) {
			if j.Reason == "cancel requested" {
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
	q.maintainHolds()
	q.refreshHostMemory()
	q.maintainModels()
	q.refreshHostMemory()
	q.maintainHolds()
}
func (q *Queue) Tick() error { return q.update(func() error { q.maintain(); return nil }) }
func (q *Queue) Checkin(c Checkin) (Reply, error) {
	reply := Reply{CheckpointRoot: q.root}
	var durableProgress int64
	var progressErr error
	var progressPath string
	if c.Running != nil {
		q.mu.Lock()
		if j := q.s.Jobs[c.Running.ID]; j != nil && j.Spec.Resumable && j.Attempt != c.Running.Attempt {
			progressPath = attemptCheckpoint(*j)
		}
		q.mu.Unlock()
		if progressPath != "" {
			durableProgress, progressErr = q.nasProgress.run(progressPath, func() (int64, error) {
				progress, err := checkpointProgress(progressPath)
				if errors.Is(err, os.ErrNotExist) {
					return 0, nil
				}
				return progress, err
			})
		}
	}
	err := q.update(func() error {
		m := cloneMachine(&c.Machine)
		m.Disabled = q.s.DisabledHosts[hostID(&m)]
		if old := q.s.Machines[m.ID]; old != nil && old.Disabled != "" {
			m.Disabled = old.Disabled
		}
		if progressErr != nil {
			m.CheckpointUnavailable = true
		}
		if !validMemoryValue(m.HostAvailableMemoryGB) {
			return errors.New("invalid host memory report")
		}
		if m.ID == "" || m.Session == "" || !validGPU(m.GPU) || !validMemoryValue(m.TotalMemoryGB) || m.FreeMemoryGB < 0 || math.IsNaN(m.FreeMemoryGB) || math.IsInf(m.FreeMemoryGB, 0) {
			return errors.New("invalid machine report")
		}
		if m.ModelURL != "" && len(q.models.Models) > 0 && m.ModelConfigHash != modelConfigHash(q.models) {
			return errors.New("worker model config does not match queue")
		}
		old := q.s.Machines[m.ID]
		if old != nil && old.Session != m.Session && !(c.Fresh && c.PreviousSession == old.Session) {
			if h := q.s.Hosts[hostID(old)]; h != nil && h.Policy.ProductionHold {
				for _, slot := range q.s.ModelSlots {
					if slot.Machine == m.ID && slot.Session == old.Session {
						return errors.New("previous model execution release unknown; prior-session cleanup required")
					}
				}
			}
		}
		if c.Running == nil && old != nil && old.Session == m.Session {
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && j.State == "stopping" && q.held(hostID(old)) {
					if j.Reason == "cancel requested" {
						j.State = "cancelled"
						j.StopAt = nil
					} else {
						q.retry(j, j.Reason)
					}
				}
			}
		}
		if m.Disabled != "" {
			m.Leaving = true
			m.LastSeen = q.clock.Now()
			q.s.Machines[m.ID] = &m
			reply.Leave = true
			reply.Stop = c.Running != nil && old != nil && old.Gone
			reply.Kill = reply.Stop
			return nil
		}
		q.reconcileHostReleases(c)
		if m.TotalMemoryGB == 0 {
			m.TotalMemoryGB = m.FreeMemoryGB
			if old != nil {
				m.TotalMemoryGB = max(m.TotalMemoryGB, old.TotalMemoryGB)
			}
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
					if j.Reason == "cancel requested" {
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
		// After fleet silence, give the other workers a full reconnect window.
		if q.networkQuiet && old != nil && !old.Gone {
			for _, other := range q.s.Machines {
				if !other.Gone {
					other.LastSeen = q.clock.Now()
				}
			}
		}
		delete(q.unreported, m.ID)
		m.Gone = false
		m.LastSeen = q.clock.Now()
		q.s.Machines[m.ID] = &m
		delete(q.s.BatchReleases, m.ID) // This report supersedes acknowledgement credit.
		q.reconcileModels(&m, c.ModelReports)
		if e := q.reconcileEnvironments(&m); e != nil {
			return e
		}
		m.FreeMemoryGB = max(0, m.FreeMemoryGB-q.modelReservation(&m, c.ModelReports))
		q.maintain()
		defer func() {
			reply.EnvironmentPlan = q.environmentPlan(&m)
			reply.ModelPlan = q.modelPlan(m.ID)
			reply.CopyPlan, reply.CacheUsage, reply.CachePins = q.copyPlan(&m)
		}()
		reply.Leave = m.Leaving
		if m.Leaving {
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && j.Spec.Resumable {
					q.stop(j, "machine leaving")
				}
			}
		}
		if c.Running != nil {
			j := q.s.Jobs[c.Running.ID]
			if j != nil && !terminal(j.State) && j.Reason != "cancel requested" && (j.State == "queued" || j.Attempt != c.Running.Attempt || j.Machine != m.ID || j.Session != m.Session) {
				for _, copy := range j.Copies {
					if q.placementAllowed(j, &m) && copy.Recoverable && copy.Attempt == c.Running.Attempt && copy.Machine == m.ID && copy.Session == m.Session && j.State != "queued" && (m.CheckpointUnavailable || !q.checkpointsAvailable() || progressErr != nil || (progressPath != "" && progressPath != attemptCheckpoint(*j))) {
						reply.Hold = true
						return nil
					}
					if q.placementAllowed(j, &m) && copy.Recoverable && copy.Attempt == c.Running.Attempt && copy.Machine == m.ID && copy.Session == m.Session && (j.State == "queued" || (j.Spec.Resumable && c.Running.Progress > max(0, durableProgress))) {
						for i := range j.Copies {
							j.Copies[i].Recoverable = false
						}
						j.Attempt = copy.Attempt
						j.Machine = m.ID
						j.Session = m.Session
						j.State = "running"
						j.StopAt = nil
						j.Reason = ""
						j.Progress = c.Running.Progress
						break
					}
				}
			}
			if j == nil || j.Attempt != c.Running.Attempt || j.Machine != m.ID || j.Session != m.Session || terminal(j.State) || j.State == "queued" {
				if j != nil {
					for i := range j.Copies {
						if j.Copies[i].Attempt == c.Running.Attempt {
							j.Copies[i].Recoverable = false
						}
					}
				}
				reply.Stop = true
				reply.Kill = true
				return nil
			}
			if c.Started && j.StartedAt == nil {
				q.markStarted(j)
			}
			j.Progress = c.Running.Progress
			if j.State == "stopping" {
				reply.Stop = true
				reply.Kill = expired(j.StopAt, q.clock.Now())
				reply.GraceSeconds = j.StopAt.Sub(q.clock.Now()).Seconds()
			}
			return nil
		}
		// Re-deliver an assignment if its HTTP response was lost before startup.
		for _, j := range q.s.Jobs {
			if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
				if j.State == "stopping" {
					if j.Reason == "cancel requested" {
						j.State = "cancelled"
						j.StopAt = nil
					} else {
						q.retry(j, j.Reason)
					}
					break
				}
				if !q.checkpointReady(j, &m) {
					return nil
				}
				out := cloneJob(j)
				reply.Job = &out
				return nil
			}
		}
		if !m.Leaving {
			j := q.pick(&m)
			if j != nil {
				for _, c := range q.calls {
					if c.Machine == "" && c.Error == "" && q.callCanUse(&m, c) && q.fairBefore(callJob(c), j, &m) {
						j = nil
						break
					}
				}
			}
			if j != nil && len(j.Spec.Needs.Platform.Preferred) > 0 && !contains(j.Spec.Needs.Platform.Preferred, m.GPU) {
				for _, other := range q.s.Machines {
					if !contains(j.Spec.Needs.Platform.Preferred, other.GPU) || !q.placementAllowed(j, other) || !fits(j, other, other.FreeMemoryGB) || affinity(j, other) < affinity(j, &m) {
						continue
					}
					busy := false
					for _, r := range q.s.Jobs {
						if r.Machine == other.ID && (r.State == "running" || r.State == "stopping") {
							busy = true
						}
					}
					if !busy {
						j = nil
						break
					}
				}
			}
			if j != nil && q.betterPlacement(j, &m) {
				j = nil
			}
			if j != nil {
				j.State = "running"
				j.RequeuedByOperator = false
				j.StartedAt = nil
				now := q.clock.Now()
				j.AssignedAt = &now
				j.FinishedAt = nil
				j.StartLatencySeconds = 0
				m.FreeMemoryGB = max(0, m.FreeMemoryGB-j.Spec.Needs.MemoryGB)
				j.Starts = max(j.Starts, j.Attempt) + 1
				j.Attempt = j.Starts
				j.Progress = 0
				j.Copies = append(j.Copies, Copy{Machine: m.ID, Session: m.Session, Attempt: j.Attempt})
				j.Machine = m.ID
				j.Session = m.Session
				j.Reason = ""
				q.refreshHostMemory()
				out := cloneJob(j)
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
	return reply, err
}
func validGPU(s string) bool { return s == "cuda" || s == "xpu" || s == "metal" || s == "fake" }
func (q *Queue) current(r Result) (*Job, error) {
	j := q.s.Jobs[r.ID]
	if j == nil || j.Attempt != r.Attempt || j.Machine != r.Machine || j.Session != r.Session || (j.State != "running" && j.State != "stopping") {
		return nil, ErrStale
	}
	return j, nil
}
func (q *Queue) Complete(r Result) error {
	return q.update(func() error {
		settled := false
		pendingTracked := false
		if !r.ReleasePending {
			settled = q.settleRelease(r)
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
		if j.State == "stopping" {
			if j.StopAt != nil && q.clock.Now().Before(*j.StopAt) {
				if q.s.BatchReleases == nil {
					q.s.BatchReleases = map[string]batchRelease{}
				}
				q.s.BatchReleases[j.Machine] = batchRelease{Session: j.Session, MemoryGB: j.Spec.Needs.MemoryGB, Until: *j.StopAt}
			}
			if j.Reason == "cancel requested" {
				j.State = "cancelled"
				j.StopAt = nil
			} else {
				q.retry(j, j.Reason)
			}
		} else if r.Interrupted {
			j.Losses++
			q.chargeLoss(j)
			q.retry(j, "worker restarted after cleaning old processes")
		} else if r.Success {
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
	})
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
func (q *Queue) Cancel(id string) error {
	return q.update(func() error {
		j := q.s.Jobs[id]
		if j == nil {
			return errors.New("job not found")
		}
		if j.State == "queued" {
			j.State = "cancelled"
			j.Reason = "cancel requested"
		} else if !terminal(j.State) {
			if j.State == "running" {
				q.stop(j, "cancel requested")
			}
			j.Reason = "cancel requested"
		}
		return nil
	})
}
func (q *Queue) Snapshot() (Snapshot, error) {
	var out Snapshot
	err := q.update(func() error {
		q.maintain()
		out = Snapshot{SyncRequests: append([]SyncRequest(nil), q.s.SyncRequests...), Pins: append([]ModelPin(nil), q.s.Pins...), Jobs: []*Job{}, Machines: []*Machine{}, LoadedModels: []ModelSlot{}}
		for _, s := range q.s.ModelSlots {
			copy := *s
			if s.State == "ready" && s.InFlight == 0 {
				copy.IdleSeconds = max(0, q.clock.Now().Sub(s.LastUsed).Seconds())
			}
			out.LoadedModels = append(out.LoadedModels, copy)
		}
		sort.Slice(out.LoadedModels, func(i, j int) bool {
			return slotKey(out.LoadedModels[i].Machine, out.LoadedModels[i].Model) < slotKey(out.LoadedModels[j].Machine, out.LoadedModels[j].Model)
		})
		if q.networkQuiet {
			out.Warning = "most workers quiet: waiting for the network to return"
		}
		if q.clock.Now().Before(q.restartUntil) && len(q.unreported) > 0 {
			out.Warning = "queue restarted: waiting for workers to report"
		}
		if !q.checkpointsAvailable() {
			out.Warning = "NAS checkpoint root is unreachable; checkpoint-dependent jobs will not start"
		}
		for _, m := range q.s.Machines {
			if !m.Gone && m.CheckpointUnavailable {
				out.Warning = "NAS checkpoint root is unreachable on worker " + m.ID
			}
		}
		for host := range q.s.Hosts {
			if q.held(host) {
				out.HostHolds = append(out.HostHolds, q.holdReason(host))
			}
		}
		sort.Strings(out.HostHolds)
		out.Now = q.clock.Now()
		order, reasons := q.queueProjection()
		for _, j := range q.s.Jobs {
			if j.State == "queued" {
				j.Reason = reasons[j.ID]
				if j.RequeuedByOperator {
					j.Reason = "requeued: disabled by operator; " + j.Reason
				}
				if needsCheckpoint(j) && !q.checkpointsAvailable() {
					j.Reason = "NAS checkpoint root is unreachable"
				}
			}
			copy := cloneJob(j)
			copy.QueueOrder = order[j.ID]
			out.Jobs = append(out.Jobs, &copy)
		}
		for _, c := range q.calls {
			call := callJob(c)
			call.Spec.Name = "model call: " + c.Model
			model, _ := q.model(c.Model)
			call.Spec.Needs.MemoryGB = model.MemoryGB
			call.Spec.Needs.HostMemoryGB = model.HostMemoryGB
			call.Machine = c.Machine
			call.State = "queued"
			call.Reason = "waiting for model " + c.Model + " on an eligible worker"
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
					if m.Gone || m.Leaving || m.ID == c.Exclude || m.ModelURL == "" || !modelFitsMachine(model, m) {
						continue
					}
					eligible = true
					slot := q.s.ModelSlots[slotKey(m.ID, c.Model)]
					if slot != nil && (slot.State == "ready" || slot.State == "loading") || hostFits(m, model.MemoryGB, model.HostMemoryGB) {
						room = true
					}
				}
				if eligible && !room {
					call.Reason = fmt.Sprintf("waiting for RAM: model %s needs %.1f GiB host memory (or telemetry unavailable)", c.Model, model.HostMemoryGB)
				}
			}
			if c.Machine != "" {
				call.State = "running"
				call.Reason = "model call in flight"
			}
			if c.Error != "" {
				call.State = "failed"
				call.Reason = c.Error
			}
			out.Jobs = append(out.Jobs, call)
		}
		for _, m := range q.s.Machines {
			copy := cloneMachine(m)
			copy.ReservedMemoryGB = 0
			copy.MemoryUsedGB = max(0, copy.TotalMemoryGB-copy.FreeMemoryGB)
			for _, slot := range q.s.ModelSlots {
				if slot.Machine == m.ID && q.slotPinned(slot) {
					model, _ := q.model(slot.Model)
					copy.ReservedMemoryGB += model.MemoryGB
				}
			}
			copy.ModelCopySeconds = map[string]float64{}
			for _, model := range q.models.Models {
				if modelFitsMachine(model, m) {
					copy.ModelCopySeconds[model.Name] = q.loadSeconds(model.Name, m)
				}
			}
			out.Machines = append(out.Machines, &copy)
		}
		sort.SliceStable(out.Jobs, func(i, k int) bool { return statusBefore(out.Jobs[i], out.Jobs[k]) })
		sort.Slice(out.Machines, func(i, k int) bool { return out.Machines[i].ID < out.Machines[k].ID })
		return nil
	})
	if err == nil {
		q.mu.Lock()
		out.Usage, err = q.usageLocked(q.clock.Now().Add(-7 * 24 * time.Hour))
		q.mu.Unlock()
	}
	return out, err
}
func (q *Queue) AppendLog(l LogChunk) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, err := q.current(l.Result); err != nil {
		return err
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
		return nil, fmt.Errorf("job not found")
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
		j.StartLatencySeconds = now.Sub(j.Queued).Seconds()
		if j.Spec.Priority == "production" {
			q.s.ProductionStarts = append(q.s.ProductionStarts, ProductionStart{At: now, Job: j.ID, Seconds: j.StartLatencySeconds})
		}
	}
}
func (q *Queue) Started(r Result) error {
	return q.update(func() error {
		j, e := q.current(r)
		if e != nil {
			return e
		}
		q.markStarted(j)
		return nil
	})
}
