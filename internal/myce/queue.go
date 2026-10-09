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
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const LossAfter = 60 * time.Second
const RestartGrace = 3 * time.Minute
const StopGrace = 2 * time.Minute

var ErrStale = errors.New("stale execution: stop this copy")

type state struct {
	ModelSlots map[string]*ModelSlot `json:"model_slots,omitempty"`
	Jobs       map[string]*Job       `json:"jobs"`
	Machines   map[string]*Machine   `json:"machines"`
	Front      int64                 `json:"front"`
}
type Queue struct {
	models       ModelConfig
	calls        map[string]*ModelCall
	lock         *os.File
	mu           sync.Mutex
	db           *sql.DB
	clock        Clock
	root         string
	restartUntil time.Time
	unreported   map[string]bool
	networkQuiet bool
	s            state
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
	q := &Queue{lock: lock, db: db, clock: clock, root: abs, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS logs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT NOT NULL, attempt INTEGER NOT NULL, text TEXT NOT NULL); CREATE TABLE IF NOT EXISTS log_offsets (job TEXT, attempt INTEGER, offset INTEGER, PRIMARY KEY(job,attempt,offset)); CREATE INDEX IF NOT EXISTS logs_job ON logs(job,seq);`); err != nil {
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
	q.mu.Lock()
	defer q.mu.Unlock()
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
	err := fn()
	if err == nil {
		var data []byte
		data, err = json.Marshal(q.s)
		if err == nil {
			_, err = q.db.Exec("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", data)
		}
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
		id := ID()
		j := &Job{ID: id, Spec: s, State: "queued", Queued: q.clock.Now(), Checkpoint: filepath.Join(q.root, id)}
		copy := cloneJob(j)
		j = &copy
		q.s.Jobs[id] = j
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
	if !j.Spec.Resumable && j.Losses >= 3 {
		j.State = "failed"
		j.Reason = "failed after 3 attempts: " + why
		return
	}
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
	active, healthy := 0, 0
	for _, m := range q.s.Machines {
		if !m.Gone && !m.Leaving {
			active++
			if now.Sub(m.LastSeen) < LossAfter {
				healthy++
			}
		}
	}
	q.networkQuiet = active > 0 && healthy == 0
	for _, m := range q.s.Machines {
		unreported := q.unreported[m.ID]
		grace := unreported && now.Before(q.restartUntil)
		restartExpired := unreported && !grace
		if !m.Gone && !grace && (restartExpired || (!q.networkQuiet && now.Sub(m.LastSeen) >= LossAfter)) {
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
								j.Copies[i].Recoverable = true
							}
						}
						j.Losses++
						q.retry(j, "machine lost for 60 seconds")
					}
				}
			}
		}
	}
	for _, j := range q.s.Jobs {
		if !q.networkQuiet && !q.unreported[j.Machine] && j.State == "stopping" && expired(j.StopAt, now) {
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
	q.maintainModels()
	if q.networkQuiet || now.Before(q.restartUntil) || !checkpointAvailable(q.root) {
		return
	}
	// Only preempt if no idle fitting machine has room for the waiting job.
	waiting := []*Job{}
	for _, j := range q.s.Jobs {
		if j.State == "queued" && rank(j.Spec.Priority) < 2 {
			waiting = append(waiting, j)
		}
	}
	sort.Slice(waiting, func(i, k int) bool { return before(waiting[i], waiting[k], &Machine{}) })
	for _, j := range waiting {
		room := false
		for _, m := range q.s.Machines {
			busy := false
			for _, r := range q.s.Jobs {
				if r.Machine == m.ID && (r.State == "running" || r.State == "stopping") {
					busy = true
				}
			}
			for _, r := range q.s.Jobs {
				if r.Machine == m.ID && r.State == "stopping" && fits(j, m, m.FreeMemoryGB+r.Spec.Needs.MemoryGB) {
					room = true
				}
			}
			if room || (!busy && fits(j, m, m.FreeMemoryGB)) {
				room = true
				break
			}
		}
		if room {
			continue
		}
		var victim *Job
		for _, r := range q.s.Jobs {
			m := q.s.Machines[r.Machine]
			if m != nil && r.State == "running" && r.Spec.Resumable && rank(r.Spec.Priority) > rank(j.Spec.Priority) && fits(j, m, m.FreeMemoryGB+r.Spec.Needs.MemoryGB) && (victim == nil || rank(r.Spec.Priority) > rank(victim.Spec.Priority) || rank(r.Spec.Priority) == rank(victim.Spec.Priority) && r.ID < victim.ID) {
				victim = r
			}
		}
		if victim != nil {
			q.stop(victim, "making room for "+j.Spec.Priority+" job")
		}
	}
}
func (q *Queue) Tick() error { return q.update(func() error { q.maintain(); return nil }) }
func (q *Queue) Checkin(c Checkin) (Reply, error) {
	reply := Reply{CheckpointRoot: q.root}
	err := q.update(func() error {
		m := cloneMachine(&c.Machine)
		if m.ID == "" || m.Session == "" || !validGPU(m.GPU) || m.FreeMemoryGB < 0 || math.IsNaN(m.FreeMemoryGB) || math.IsInf(m.FreeMemoryGB, 0) {
			return errors.New("invalid machine report")
		}
		if m.ModelURL != "" && len(q.models.Models) > 0 && m.ModelConfigHash != modelConfigHash(q.models) {
			return errors.New("worker model config does not match queue")
		}
		old := q.s.Machines[m.ID]
		if old != nil && old.Session != m.Session && !old.Gone && !(c.Fresh && c.PreviousSession == old.Session) {
			return errors.New("machine name already joined; leave the other worker first")
		}
		if old != nil && old.Session == m.Session {
			m.Leaving = old.Leaving || m.Leaving
		}
		if old != nil && old.Session != m.Session && c.Fresh {
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == old.Session && (j.State == "running" || j.State == "stopping") {
					j.Losses++
					q.retry(j, "worker restarted after cleaning old processes")
				}
			}
		}
		// After fleet silence, give the other workers a full reconnect window.
		if q.networkQuiet {
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
		q.reconcileModels(&m, c.ModelReports)
		m.FreeMemoryGB = max(0, m.FreeMemoryGB-q.modelMemory(m.ID))
		q.maintain()
		defer func() { reply.ModelPlan = q.modelPlan(m.ID) }()
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
					if copy.Recoverable && copy.Attempt == c.Running.Attempt && copy.Machine == m.ID && copy.Session == m.Session && j.State != "queued" && (m.CheckpointUnavailable || !checkpointAvailable(q.root)) {
						reply.Hold = true
						return nil
					}
					if copy.Recoverable && copy.Attempt == c.Running.Attempt && copy.Machine == m.ID && copy.Session == m.Session && (j.State == "queued" || (j.Spec.Resumable && c.Running.Progress > max(j.Progress, completeProgress(filepath.Join(j.Checkpoint, "attempt-"+strconv.Itoa(j.Attempt)))))) {
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
			j.Progress = max(j.Progress, c.Running.Progress)
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
				if m.CheckpointUnavailable || !checkpointAvailable(q.root) {
					return nil
				}
				out := cloneJob(j)
				reply.Job = &out
				return nil
			}
		}
		if !m.Leaving && !m.CheckpointUnavailable && checkpointAvailable(q.root) {
			j := pick(q.s.Jobs, &m)
			if j != nil {
				for _, c := range q.calls {
					model, ok := q.model(c.Model)
					if c.Machine == "" && c.Error == "" && ok && c.Exclude != m.ID && contains(model.Platforms, m.GPU) && model.MemoryGB <= m.FreeMemoryGB+q.modelMemory(m.ID) && before(callJob(c), j, &m) {
						j = nil
						break
					}
				}
			}
			if j != nil && len(j.Spec.Needs.Platform.Preferred) > 0 && !contains(j.Spec.Needs.Platform.Preferred, m.GPU) {
				for _, other := range q.s.Machines {
					if !contains(j.Spec.Needs.Platform.Preferred, other.GPU) || !fits(j, other, other.FreeMemoryGB) || affinity(j, other) < affinity(j, &m) {
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
			if j != nil {
				j.State = "running"
				m.FreeMemoryGB = max(0, m.FreeMemoryGB-j.Spec.Needs.MemoryGB)
				j.Starts = max(j.Starts, j.Attempt) + 1
				j.Attempt = j.Starts
				j.Progress = 0
				j.Copies = append(j.Copies, Copy{Machine: m.ID, Session: m.Session, Attempt: j.Attempt})
				j.Machine = m.ID
				j.Session = m.Session
				j.Reason = ""
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
			if j.Reason == "cancel requested" {
				j.State = "cancelled"
				j.StopAt = nil
			} else {
				q.retry(j, j.Reason)
			}
		} else if r.Interrupted {
			j.Losses++
			q.retry(j, "worker restarted after cleaning old processes")
		} else if r.Success {
			j.State = "succeeded"
		} else {
			j.State = "failed"
			j.Reason = "command failed: " + r.Error
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
		out = Snapshot{Jobs: []*Job{}, Machines: []*Machine{}, LoadedModels: []ModelSlot{}}
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
			out.Warning = "all workers quiet: waiting for the network to return"
		}
		if q.clock.Now().Before(q.restartUntil) && len(q.unreported) > 0 {
			out.Warning = "queue restarted: waiting for workers to report"
		}
		if !checkpointAvailable(q.root) {
			out.Warning = "NAS checkpoint root is unreachable; jobs will not start"
		}
		for _, m := range q.s.Machines {
			if !m.Gone && m.CheckpointUnavailable {
				out.Warning = "NAS checkpoint root is unreachable on worker " + m.ID
			}
		}
		for _, j := range q.s.Jobs {
			if j.State == "queued" {
				j.Reason = waitingReason(j, q.s.Machines, q.s.Jobs)
				if !checkpointAvailable(q.root) {
					j.Reason = "NAS checkpoint root is unreachable"
				}
			}
			copy := cloneJob(j)
			out.Jobs = append(out.Jobs, &copy)
		}
		for _, m := range q.s.Machines {
			copy := cloneMachine(m)
			out.Machines = append(out.Machines, &copy)
		}
		sort.Slice(out.Jobs, func(i, k int) bool { return out.Jobs[i].ID < out.Jobs[k].ID })
		sort.Slice(out.Machines, func(i, k int) bool { return out.Machines[i].ID < out.Machines[k].ID })
		return nil
	})
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
	return out, rows.Err()
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

// Both reconciliation and command startup use the same checkpoint rules.
func completeProgress(root string) int64 { progress, _ := checkpointProgress(root); return progress }
