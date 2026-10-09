package myce

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const LossAfter = 60 * time.Second
const StopGrace = 2 * time.Minute

var ErrStale = errors.New("stale execution: stop this copy")

type state struct {
	Jobs     map[string]*Job     `json:"jobs"`
	Machines map[string]*Machine `json:"machines"`
	Front    int64               `json:"front"`
}
type Queue struct {
	mu    sync.Mutex
	db    *sql.DB
	clock Clock
	root  string
	s     state
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
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	q := &Queue{db: db, clock: clock, root: abs, s: state{Jobs: map[string]*Job{}, Machines: map[string]*Machine{}}}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS logs (seq INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT NOT NULL, attempt INTEGER NOT NULL, text TEXT NOT NULL); CREATE INDEX IF NOT EXISTS logs_job ON logs(job,seq);`); err != nil {
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
	return q, nil
}
func (q *Queue) Close() error { return q.db.Close() }

// Every mutation is atomic and durable; failed writes restore the previous state.
func (q *Queue) update(fn func() error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	old, _ := json.Marshal(q.s)
	err := fn()
	if err == nil {
		var data []byte
		data, err = json.Marshal(q.s)
		if err == nil {
			_, err = q.db.Exec("INSERT INTO queue(id,data) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", data)
		}
	}
	if err != nil {
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
		q.s.Jobs[id] = j
		out = *j
		return nil
	})
	return &out, err
}
func (q *Queue) retry(j *Job, why string) {
	j.Machine = ""
	j.Session = ""
	j.StopAt = nil
	j.Reason = why
	if j.Attempt >= 3 {
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
	for _, m := range q.s.Machines {
		if !m.Gone && now.Sub(m.LastSeen) >= LossAfter {
			m.Gone = true
			m.Models = nil
			for _, j := range q.s.Jobs {
				if j.Machine == m.ID && j.Session == m.Session && (j.State == "running" || j.State == "stopping") {
					if j.Reason == "cancel requested" {
						j.State = "cancelled"
						j.StopAt = nil
					} else {
						q.retry(j, "machine lost for 60 seconds")
					}
				}
			}
		}
	}
	for _, j := range q.s.Jobs {
		if j.State == "stopping" && expired(j.StopAt, now) {
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
	var reply Reply
	err := q.update(func() error {
		q.maintain()
		m := c.Machine
		if m.ID == "" || m.Session == "" || !validGPU(m.GPU) || m.FreeMemoryGB < 0 || math.IsNaN(m.FreeMemoryGB) || math.IsInf(m.FreeMemoryGB, 0) {
			return errors.New("invalid machine report")
		}
		old := q.s.Machines[m.ID]
		if old != nil && old.Session != m.Session && !old.Gone {
			return errors.New("machine name already joined; leave the other worker first")
		}
		if old != nil && old.Session == m.Session {
			m.Leaving = old.Leaving || m.Leaving
		}
		m.Gone = false
		m.LastSeen = q.clock.Now()
		q.s.Machines[m.ID] = &m
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
			if j == nil || j.Attempt != c.Running.Attempt || j.Machine != m.ID || j.Session != m.Session || terminal(j.State) || j.State == "queued" {
				reply.Stop = true
				reply.Kill = true
				return nil
			}
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
				out := *j
				reply.Job = &out
				return nil
			}
		}
		if !m.Leaving {
			j := pick(q.s.Jobs, &m)
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
				j.Attempt++
				j.Machine = m.ID
				j.Session = m.Session
				j.Reason = ""
				out := *j
				reply.Job = &out
			}
		}
		if m.Leaving && reply.Job == nil {
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
		q.maintain()
		j, err := q.current(r)
		if err != nil {
			return err
		}
		if j.State == "stopping" {
			if j.Reason == "cancel requested" {
				j.State = "cancelled"
				j.StopAt = nil
			} else {
				q.retry(j, j.Reason)
			}
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
		out = Snapshot{Jobs: []*Job{}, Machines: []*Machine{}}
		for _, j := range q.s.Jobs {
			if j.State == "queued" {
				j.Reason = waitingReason(j, q.s.Machines, q.s.Jobs)
			}
			copy := *j
			out.Jobs = append(out.Jobs, &copy)
		}
		for _, m := range q.s.Machines {
			copy := *m
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
	_, err := q.db.Exec("INSERT INTO logs(job,attempt,text) VALUES(?,?,?)", l.ID, l.Attempt, l.Text)
	return err
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
