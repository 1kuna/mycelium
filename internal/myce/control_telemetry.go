package myce

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Reuse the existing telemetry envelope, rather than an installation-sized
// request-rate limit. Overflow is explicit; active control is never blocked on
// diagnostic storage. #811 can resize this shared diagnostic envelope.
const controlEventCapacity = requestLatencyRoutes * requestLatencyWindow
const controlTelemetryProtocol = "control-telemetry-v1"

// ControlEvent contains only generated IDs, finite protocol labels, anonymous
// sources and worker/session digests. It never contains URLs, credentials, argv,
// errors, paths or machine names. Times use the producer's injected clock.
type ControlPhaseSpan struct {
	Phase    string    `json:"phase"`
	Started  time.Time `json:"started_at"`
	Finished time.Time `json:"finished_at"`
}
type ControlTransaction struct {
	Spans [5]ControlPhaseSpan `json:"spans"`
}
type ControlEvent struct {
	DecisionOmitted     bool                             `json:"decision_omitted,omitempty"`
	Decision            *DecisionReceipt                 `json:"decision,omitempty"`
	Epoch               string                           `json:"epoch"`
	ID                  uint64                           `json:"id"`
	Kind                string                           `json:"kind"`
	Operation           string                           `json:"operation,omitempty"`
	Request             uint64                           `json:"request,omitempty"`
	Handler             uint64                           `json:"handler,omitempty"`
	Client              string                           `json:"client,omitempty"`
	Family              string                           `json:"family,omitempty"`
	Worker              string                           `json:"worker,omitempty"`
	Session             string                           `json:"session,omitempty"`
	Mode                string                           `json:"mode,omitempty"`
	Enqueued            time.Time                        `json:"enqueued_at,omitempty"`
	Acquired            time.Time                        `json:"acquired_at,omitempty"`
	Finished            time.Time                        `json:"finished_at,omitempty"`
	Contended           bool                             `json:"contended,omitempty"`
	Transaction         *ControlTransaction              `json:"transaction,omitempty"`
	Blockers            []uint64                         `json:"blockers,omitempty"`
	ReaderGroup         uint64                           `json:"reader_group,omitempty"`
	ReaderGroups        [2]uint64                        `json:"reader_groups,omitempty"`
	FlightDetailOmitted bool                             `json:"flight_detail_omitted,omitempty"`
	ReleaseRecorderWait time.Duration                    `json:"release_recorder_wait_ns,omitempty"`
	Blocker             uint64                           `json:"blocker,omitempty"`
	Phase               string                           `json:"phase,omitempty"`
	Phases              [controlPhaseCount]time.Duration `json:"phase_ns,omitempty"`
	WriterWaitNS        time.Duration                    `json:"writer_wait_ns,omitempty"`
	WaitNS              time.Duration                    `json:"wait_ns,omitempty"`
	HoldNS              time.Duration                    `json:"hold_ns,omitempty"`
	Status              int                              `json:"status,omitempty"`
	Accepted            bool                             `json:"accepted,omitempty"`
	Cancelled           bool                             `json:"cancelled,omitempty"`
	PressureNS          time.Duration                    `json:"pressure_ns,omitempty"`
	BaselineNS          float64                          `json:"baseline_ns,omitempty"`
	PressureSamples     uint64                           `json:"pressure_samples,omitempty"`
	PressureEpoch       uint64                           `json:"pressure_epoch,omitempty"`
	Active              int                              `json:"active,omitempty"`
	Frontier            string                           `json:"frontier,omitempty"`
	Tier                int                              `json:"tier,omitempty"`
	Scope               int                              `json:"scope,omitempty"`
	FrontierTier        int                              `json:"frontier_tier,omitempty"`
	FrontierScope       int                              `json:"frontier_scope,omitempty"`
	CostSamples         uint64                           `json:"cost_samples,omitempty"`
	FrontierCostSamples uint64                           `json:"frontier_cost_samples,omitempty"`
	CostNS              float64                          `json:"cost_ns,omitempty"`
	FrontierCostNS      float64                          `json:"frontier_cost_ns,omitempty"`
	Probe               *PressureProbeSpan               `json:"probe,omitempty"`
}

const (
	controlRecords = iota
	controlUsage
	controlBody
	controlSerialize
	controlSQL
	controlCommit
	controlNotify
	controlOther
	controlPhaseCount
)

var controlPhaseNames = [...]string{"records", "usage", "body", "serialization", "sql_statements", "commit_sync", "notify", "other"}

type ControlTotal struct {
	Kind      string                           `json:"kind"`
	Operation string                           `json:"operation"`
	Count     uint64                           `json:"count"`
	WaitNS    time.Duration                    `json:"wait_ns"`
	HoldNS    time.Duration                    `json:"hold_ns"`
	MaxWaitNS time.Duration                    `json:"max_wait_ns"`
	MaxHoldNS time.Duration                    `json:"max_hold_ns"`
	Phases    [controlPhaseCount]time.Duration `json:"phase_ns"`
	estimate  probeEstimate
}
type ControlFlight struct {
	ID        uint64    `json:"id"`
	Operation string    `json:"operation"`
	Request   uint64    `json:"request,omitempty"`
	Client    string    `json:"client,omitempty"`
	Mode      string    `json:"mode"`
	Enqueued  time.Time `json:"enqueued_at"`
	Acquired  time.Time `json:"acquired_at,omitempty"`
	Phase     string    `json:"phase"`
	PhaseAt   time.Time `json:"phase_at"`
}
type ControlTelemetry struct {
	Epoch                  string          `json:"epoch"`
	At                     time.Time       `json:"at"`
	DurableAt              time.Time       `json:"durable_at"`
	Dropped                uint64          `json:"dropped"`
	Overwritten            uint64          `json:"overwritten"`
	StorageError           bool            `json:"storage_error"`
	PhaseNames             []string        `json:"phase_names"`
	Totals                 []ControlTotal  `json:"totals"`
	Recent                 []ControlEvent  `json:"recent"`
	InFlight               []ControlFlight `json:"in_flight"`
	Pending                int             `json:"pending"`
	AdmissionScope         string          `json:"admission_scope"`
	InFlightTotal          int             `json:"in_flight_total"`
	FlightDetailsDropped   uint64          `json:"flight_details_dropped"`
	StorageDeferred        bool            `json:"storage_deferred"`
	StorageDropped         uint64          `json:"storage_dropped"`
	StorageReason          string          `json:"storage_reason,omitempty"`
	DiagnosticTransactions uint64          `json:"diagnostic_transactions"`
	SerializedBytes        uint64          `json:"serialized_bytes"`
}

func (c *ControlTelemetry) Summary() string {
	if c == nil {
		return ""
	}
	var locks, requests, rejects uint64
	var wait, hold time.Duration
	for _, t := range c.Totals {
		switch t.Kind {
		case "lock":
			locks += t.Count
			wait = max(wait, t.MaxWaitNS)
			hold = max(hold, t.MaxHoldNS)
		case "request":
			requests += t.Count
		case "rejection":
			rejects += t.Count
		}
	}
	return fmt.Sprintf("control: locks=%d requests=%d rejected=%d max_wait=%.1fms max_hold=%.1fms dropped=%d storage_error=%t deferred=%t flight_details_dropped=%d", locks, requests, rejects, holdMS(wait), holdMS(hold), c.Dropped, c.StorageError, c.StorageDeferred, c.FlightDetailsDropped)
}

type controlRecorder struct {
	decisionReleasePrepared                 func()
	decisionRetentionPrepared               func()
	decisionBytes, decisionCapacity         int64
	decisionLimit                           int64
	writeGate                               sync.Mutex
	controlWriters                          atomic.Int64
	backlog                                 []ControlEvent
	drainBuffer                             []ControlEvent
	backlogPending                          int
	pageBytes                               int
	retainedRows                            uint64
	mu                                      sync.Mutex
	flushMu                                 sync.Mutex // Never taken under q.mu.
	clock                                   Clock
	epoch                                   string
	salt                                    [32]byte
	next                                    uint64
	pending                                 []ControlEvent
	spare                                   []ControlEvent
	dropped                                 uint64
	overwritten                             uint64
	durableAt                               time.Time
	storageError                            bool
	totals                                  map[string]ControlTotal
	recent                                  []ControlEvent
	flights                                 map[uint64]ControlFlight
	flightDetailsDropped                    uint64
	inFlightTotal                           int
	readers                                 int
	readerGeneration                        uint64
	writerGeneration                        uint64
	writerTail                              *controlLock
	writer                                  uint64
	writerPhase                             string
	waitingWriters                          int
	pressure                                *queuePressure
	storageProbe                            func() (controlDiskBudget, error)
	storageDeferred                         bool
	storageDropped                          uint64
	storageReason                           string
	commitPressure                          bool
	controlCommitCosts                      map[string]probeEstimate
	diagnosticCommitCost                    probeEstimate
	recoveryRemaining                       float64
	diagnosticTransactions, serializedBytes uint64
}

func newControlRecorder(clock Clock) *controlRecorder {
	c := &controlRecorder{clock: clock, epoch: ID(), pending: make([]ControlEvent, 0, controlEventCapacity), spare: make([]ControlEvent, 0, controlEventCapacity), totals: map[string]ControlTotal{}, flights: make(map[uint64]ControlFlight, requestLatencyWindow)}
	if _, err := rand.Read(c.salt[:]); err != nil {
		panic(err)
	}
	return c
}
func (c *controlRecorder) id() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	return c.next
}
func (c *controlRecorder) anonymous(value string) string {
	if c == nil {
		return ""
	}
	h := hmac.New(sha256.New, c.salt[:])
	_, _ = h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil)[:12])
}
func (c *controlRecorder) append(e ControlEvent) {
	if c == nil {
		return
	}
	if e.Decision != nil {
		c.appendDecision(e, c.decisionBudgetLimit())
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appendLocked(e)
}
func (c *controlRecorder) appendLocked(e ControlEvent) {
	if c.storageDeferred && (e.Kind == "request" || e.Kind == "lock") && !e.Contended {
		c.dropped++
		c.storageDropped++
		return
	}
	if len(c.pending) == cap(c.pending) {
		c.dropped++
		return
	}
	e.Epoch = c.epoch
	c.pending = append(c.pending, e)
}
func (c *controlRecorder) initialize(db *sql.DB) error {
	if err := db.QueryRow(`PRAGMA page_size`).Scan(&c.pageBytes); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS control_events(seq INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS control_totals(key TEXT PRIMARY KEY,data BLOB NOT NULL); CREATE TABLE IF NOT EXISTS control_metadata(id INTEGER PRIMARY KEY CHECK(id=1),data BLOB NOT NULL)`); err != nil {
		return err
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM control_events`).Scan(&c.retainedRows); err != nil {
		return err
	}
	var raw []byte
	if err := db.QueryRow(`SELECT data FROM control_metadata WHERE id=1`).Scan(&raw); err == nil {
		var prior struct {
			Salt                                    [32]byte
			Dropped, Overwritten                    uint64
			StorageDropped, FlightDetailsDropped    uint64
			DiagnosticTransactions, SerializedBytes uint64
			DurableAt                               time.Time
		}
		if err = json.Unmarshal(raw, &prior); err != nil {
			return err
		}
		c.salt = prior.Salt
		c.dropped = prior.Dropped
		c.overwritten = prior.Overwritten
		c.durableAt = prior.DurableAt
		c.storageDropped, c.flightDetailsDropped = prior.StorageDropped, prior.FlightDetailsDropped
		c.diagnosticTransactions, c.serializedBytes = prior.DiagnosticTransactions, prior.SerializedBytes
	} else if err != sql.ErrNoRows {
		return err
	}
	rows, err := db.Query(`SELECT key,data FROM control_totals`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		var raw []byte
		if err = rows.Scan(&key, &raw); err != nil {
			break
		}
		var t ControlTotal
		if err = json.Unmarshal(raw, &t); err != nil {
			break
		}
		c.totals[key] = t
	}
	err = errorsControl(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	// Receipt payloads use targeted durable readback after restart. Do not
	// hydrate their variable-sized history into the count-bounded recent ring.
	rows, err = db.Query(`SELECT json_remove(data,'$.decision') FROM (SELECT seq,data FROM control_events ORDER BY seq DESC LIMIT ?) ORDER BY seq`, requestLatencyWindow)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		var e ControlEvent
		if err = json.Unmarshal(raw, &e); err != nil {
			break
		}
		c.recent = append(c.recent, e)
	}
	return errorsControl(err, rows.Err(), rows.Close())
}
func errorsControl(errs ...error) error { return errors.Join(errs...) }

// Called by queue maintenance and shutdown, outside q.mu. Only exceptional lock
// spans, each rejection and per-worker check-in/probe receipt enter the durable
// event ring. Lifetime scalar aggregates retain every successfully drained span.
// The four-deviation prediction band is the existing probe estimator, not a
// machine-specific slow threshold. No per-acquisition SQL, JSON or stack walk.
func (c *controlRecorder) flush(db *sql.DB) error      { return c.flushMode(db, false) }
func (c *controlRecorder) flushClose(db *sql.DB) error { return c.flushMode(db, true) }

// A diagnostic transaction contains approximately one SQLite page of prepared event
// and aggregate data (or one indivisible larger event). JSON and aggregation
// finish before the writer reservation. Named control writers announce before
// q.mu and take this same reservation FIRST, so diagnostic I/O cannot make a
// control transaction hold q.mu while waiting for its database connection.
func (c *controlRecorder) flushMode(db *sql.DB, closing bool) error {
	if c == nil {
		return nil
	}
	c.flushMu.Lock()
	defer c.releaseDecisionFlush()
	for {
		c.mu.Lock()
		if len(c.backlog) == 0 {
			if len(c.pending) == 0 {
				c.mu.Unlock()
				return nil
			}
			c.drainBuffer = c.pending
			c.backlog = c.drainBuffer
			c.pending = c.spare[:0]
			c.spare = nil
		}
		c.backlogPending = len(c.backlog)
		c.mu.Unlock()
		if !c.storagePermit(0) {
			return nil
		}
		c.mu.Lock()
		totals := make(map[string]ControlTotal, len(c.totals))
		for k, v := range c.totals {
			totals[k] = v
		}
		dropped, overwritten := c.dropped, c.overwritten
		storageDropped, flightDropped := c.storageDropped, c.flightDetailsDropped
		transactions, written := c.diagnosticTransactions, c.serializedBytes
		c.mu.Unlock()
		keep := []ControlEvent{}
		rawEvents := [][]byte{}
		changed := map[string]bool{}
		bytesUsed, n := 0, 0
		for _, e := range c.backlog {
			key := e.Kind + "/" + e.Operation
			t := totals[key]
			t.Kind, t.Operation = e.Kind, e.Operation
			cost := float64(max(e.HoldNS, e.WaitNS))
			exceptional := t.Count == 0 || cost > t.estimate.upper()
			retain := e.Kind != "lock" && e.Kind != "request" || e.Contended || exceptional
			var raw []byte
			if retain {
				raw, _ = json.Marshal(e)
			}
			keyBytes := 0
			if !changed[key] {
				encoded, _ := json.Marshal(t)
				keyBytes = len(encoded) + len(key)
			}
			if n > 0 && bytesUsed+len(raw)+keyBytes > c.pageBytes {
				break
			}
			bytesUsed += len(raw) + keyBytes
			n++
			t.Count++
			t.WaitNS += e.WaitNS
			t.HoldNS += e.HoldNS
			t.MaxWaitNS = max(t.MaxWaitNS, e.WaitNS)
			t.MaxHoldNS = max(t.MaxHoldNS, e.HoldNS)
			for i, d := range e.Phases {
				t.Phases[i] += d
			}
			t.estimate.observe(cost)
			totals[key] = t
			changed[key] = true
			if retain {
				keep = append(keep, e)
				rawEvents = append(rawEvents, raw)
			}
		}
		rawTotals := make(map[string][]byte, len(changed))
		for key := range changed {
			rawTotals[key], _ = json.Marshal(totals[key])
		}
		removed := max(int64(0), int64(c.retainedRows)+int64(len(keep))-controlEventCapacity)
		overwritten += uint64(removed)
		at := c.clock.Now()
		var payloadBytes uint64
		for _, raw := range rawEvents {
			payloadBytes += uint64(len(raw))
		}
		for _, raw := range rawTotals {
			payloadBytes += uint64(len(raw))
		}
		metadata, _ := json.Marshal(struct {
			Salt                                    [32]byte
			Dropped, Overwritten                    uint64
			StorageDropped, FlightDetailsDropped    uint64
			DiagnosticTransactions, SerializedBytes uint64
			DurableAt                               time.Time
		}{c.salt, dropped, overwritten, storageDropped, flightDropped, transactions + 1, written + payloadBytes, at})
		// Count each affected table/index entry, including AUTOINCREMENT and
		// retention deletions, and include metadata/keys in physical admission.
		mutations := uint64(2*len(rawEvents)+2*len(rawTotals)+1) + uint64(removed)
		physicalPayload := payloadBytes + uint64(len(metadata))
		for key := range rawTotals {
			physicalPayload += uint64(len(key))
		}
		if closing {
			c.writeGate.Lock()
		} else {
			if !c.writeGate.TryLock() {
				return nil
			}
			if c.controlWriters.Load() != 0 {
				c.writeGate.Unlock()
				return nil
			}
		}
		// Some maintenance writers (log pruning) bypass writeGate. Own the
		// sole SQLite connection before probing; retain it through commit so
		// those writers cannot invalidate this capacity observation.
		conn, err := db.Conn(context.Background())
		if err != nil {
			c.writeGate.Unlock()
			c.mu.Lock()
			c.storageError = true
			c.mu.Unlock()
			return err
		}
		if !c.storagePermit(c.transactionGrowth(physicalPayload, mutations)) {
			conn.Close()
			c.writeGate.Unlock()
			return nil
		}
		err = func() error {
			defer c.writeGate.Unlock()
			defer conn.Close()
			tx, err := conn.BeginTx(context.Background(), nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			for _, raw := range rawEvents {
				if _, err = tx.Exec(`INSERT INTO control_events(data) VALUES(?)`, raw); err != nil {
					return err
				}
			}
			if removed > 0 {
				if _, err = tx.Exec(`DELETE FROM control_events WHERE seq <= (SELECT COALESCE(MAX(seq),0)-? FROM control_events)`, controlEventCapacity); err != nil {
					return err
				}
			}
			for key, raw := range rawTotals {
				if _, err = tx.Exec(`INSERT INTO control_totals VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data`, key, raw); err != nil {
					return err
				}
			}
			if _, err = tx.Exec(`INSERT INTO control_metadata VALUES(1,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data`, metadata); err != nil {
				return err
			}
			started := c.clock.Now()
			err = tx.Commit()
			if err == nil {
				c.observeCommit(c.clock.Now().Sub(started), true)
			}
			return err
		}()
		c.mu.Lock()
		c.storageError = err != nil
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.totals = totals
		c.diagnosticTransactions++
		c.serializedBytes += payloadBytes
		c.overwritten = overwritten
		c.durableAt = at
		c.retainedRows = c.retainedRows + uint64(len(keep)) - uint64(removed)
		// The reusable drain/spare backing array must not keep consumed payloads.
		clear(c.backlog[:n])
		c.backlog = c.backlog[n:]
		c.backlogPending = len(c.backlog)
		c.recent = append(c.recent, keep...)
		if len(c.recent) > requestLatencyWindow {
			c.recent = append([]ControlEvent(nil), c.recent[len(c.recent)-requestLatencyWindow:]...)
		}
		empty := len(c.backlog) == 0
		if empty {
			c.spare = c.drainBuffer[:0]
			c.drainBuffer = nil
		}
		c.mu.Unlock()
		if empty && !closing {
			return nil
		}
		// Yield between prepared-page turns; any announced control writer wins the
		// next reservation. No timer or installation-dependent rate is introduced.
		runtime.Gosched()
	}
}
func (c *controlRecorder) snapshot() *ControlTelemetry {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	out := &ControlTelemetry{Epoch: c.epoch, At: c.clock.Now(), DurableAt: c.durableAt, Dropped: c.dropped, Overwritten: c.overwritten, StorageError: c.storageError, PhaseNames: append([]string(nil), controlPhaseNames[:]...), Recent: append([]ControlEvent(nil), c.recent...)}
	for _, v := range c.totals {
		out.Totals = append(out.Totals, v)
	}
	for _, v := range c.flights {
		out.InFlight = append(out.InFlight, v)
	}
	out.InFlightTotal = c.inFlightTotal
	out.FlightDetailsDropped = c.flightDetailsDropped
	out.StorageDeferred, out.StorageDropped = c.storageDeferred, c.storageDropped
	out.StorageReason = c.storageReason
	out.DiagnosticTransactions, out.SerializedBytes = c.diagnosticTransactions, c.serializedBytes
	out.Pending = len(c.pending) + c.backlogPending
	out.AdmissionScope = "handler_frontier_queue_pressure"
	c.mu.Unlock()
	sort.Slice(out.InFlight, func(i, j int) bool { return out.InFlight[i].ID < out.InFlight[j].ID })
	sort.Slice(out.Totals, func(i, j int) bool {
		return out.Totals[i].Kind+out.Totals[i].Operation < out.Totals[j].Kind+out.Totals[j].Operation
	})
	return out
}

// An explicit acquisition object makes concurrent read holders independently
// identifiable without goroutine IDs. Its labels are source-code constants.
type controlLock struct {
	mu                         *queueMutex
	recorder                   *controlRecorder
	event                      ControlEvent
	last                       time.Time
	phase                      int
	coarsePhase                int
	writerGeneration           uint64
	previousWriter, nextWriter *controlLock
	dirty                      bool
}

func (m *queueMutex) named(operation string, ctx context.Context) *controlLock {
	l := &controlLock{mu: m, recorder: m.control, phase: controlOther}
	l.event.Kind = "lock"
	l.event.Operation = operation
	if ctx != nil {
		if s := spanFrom(ctx); s != nil {
			l.event.Request = s.id
			l.event.Handler = s.handler
			l.event.Client = s.client
		}
	}
	return l
}
func controlCaller() string {
	pc, _, _, ok := runtime.Caller(2)
	if !ok {
		return "control"
	}
	name := runtime.FuncForPC(pc).Name()
	name = strings.TrimPrefix(name, "mycelium/internal/myce.")
	return name
}
func (l *controlLock) acquire(read bool) {
	c := l.recorder
	l.event.Phases = [controlPhaseCount]time.Duration{}
	l.event.WriterWaitNS = 0
	l.event.ReaderGroup, l.event.ReaderGroups = 0, [2]uint64{}
	l.phase = controlOther
	if c != nil {
		l.event.Enqueued = c.clock.Now()
		c.mu.Lock()
		c.next++
		l.event.ID = c.next
		l.event.Blocker, l.event.Phase = c.writer, c.writerPhase
		l.event.Blockers = nil
		if c.writer != 0 {
			l.event.Blockers = append(l.event.Blockers, c.writer)
		}
		if c.writerTail != nil && c.writerTail.event.ID != c.writer {
			l.event.Blockers = append(l.event.Blockers, c.writerTail.event.ID)
		}
		l.event.Contended = c.writer != 0 || c.waitingWriters > 0 || !read && c.readers > 0
		if !read {
			c.waitingWriters++
			c.writerGeneration++
			l.previousWriter = c.writerTail
			if c.writerTail != nil {
				c.writerTail.nextWriter = l
			}
			c.writerTail = l
			first := c.readerGeneration
			if c.readers == 0 {
				first++
			}
			l.event.ReaderGroups = [2]uint64{first, 0}
		}
		mode := "write"
		if read {
			mode = "read"
		}
		c.inFlightTotal++
		l.event.FlightDetailOmitted = len(c.flights) >= requestLatencyWindow
		if l.event.FlightDetailOmitted {
			c.flightDetailsDropped++
		} else {
			c.flights[l.event.ID] = ControlFlight{ID: l.event.ID, Operation: l.event.Operation, Request: l.event.Request, Client: l.event.Client, Mode: mode, Enqueued: l.event.Enqueued, Phase: "waiting", PhaseAt: l.event.Enqueued}
		}
		c.mu.Unlock()
	}
	var pressure *queuePressure
	var pressureWait *queuePressureWait
	if !read {
		pressure, pressureWait = l.mu.waitPressure()
	}
	if !read && c != nil {
		c.controlWriters.Add(1)
		started := c.clock.Now()
		c.writeGate.Lock()
		l.event.WriterWaitNS = max(0, c.clock.Now().Sub(started))
	}
	if read {
		l.event.Mode = "read"
		l.mu.RLock()
	} else {
		l.event.Mode = "write"
		l.mu.lockPressure(pressure, pressureWait)
	}
	if c != nil {
		l.event.Acquired = c.clock.Now()
		l.last = l.event.Acquired
		c.mu.Lock()
		if !l.event.FlightDetailOmitted {
			flight := c.flights[l.event.ID]
			flight.Acquired = l.event.Acquired
			flight.Phase = "other"
			flight.PhaseAt = l.event.Acquired
			c.flights[l.event.ID] = flight
		}
		if read {
			if c.readers == 0 {
				c.readerGeneration++
			}
			c.readers++
			l.event.ReaderGroup = c.readerGeneration
			l.writerGeneration = c.writerGeneration
			l.event.Contended = l.event.Contended || c.waitingWriters > 0
		} else {
			if c.readerGeneration >= l.event.ReaderGroups[0] {
				l.event.ReaderGroups[1] = c.readerGeneration
			} else {
				l.event.ReaderGroups = [2]uint64{}
			}
			c.waitingWriters--
			c.writer = l.event.ID
			c.writerPhase = "other"
		}
		c.mu.Unlock()
	}
}
func (l *controlLock) Lock()  { l.acquire(false) }
func (l *controlLock) RLock() { l.acquire(true) }
func (l *controlLock) next(phase int) {
	if l.recorder == nil {
		return
	}
	now := l.recorder.clock.Now()
	l.event.Phases[l.phase] += max(0, now.Sub(l.last))
	l.last = now
	l.phase = phase
	l.recorder.mu.Lock()
	if l.event.Mode == "write" {
		l.recorder.writerPhase = controlPhaseNames[phase]
	}
	if !l.event.FlightDetailOmitted {
		flight := l.recorder.flights[l.event.ID]
		flight.Phase = controlPhaseNames[phase]
		flight.PhaseAt = now
		l.recorder.flights[l.event.ID] = flight
	}
	l.recorder.mu.Unlock()
}
func (l *controlLock) release(read bool) {
	c := l.recorder
	if c != nil {
		started := c.clock.Now()
		c.mu.Lock()
		l.event.ReleaseRecorderWait = max(0, c.clock.Now().Sub(started))
		if !read {
			l.event.Contended = l.event.Contended || c.inFlightTotal > 1
		}
		delete(c.flights, l.event.ID)
		c.inFlightTotal--
		if read {
			l.event.Contended = l.event.Contended || l.writerGeneration != c.writerGeneration
			c.readers--
			l.mu.RUnlock()
		} else {
			if l.previousWriter != nil {
				l.previousWriter.nextWriter = l.nextWriter
			}
			if l.nextWriter != nil {
				l.nextWriter.previousWriter = l.previousWriter
			}
			if c.writerTail == l {
				c.writerTail = l.previousWriter
			}
			l.previousWriter, l.nextWriter = nil, nil
			c.writer, c.writerPhase = 0, ""
			l.mu.Unlock()
		}
		// Timestamp after the actual queue unlock, including recorder contention
		// and the pressure sensor's release bookkeeping in the holder span.
		now := c.clock.Now()
		if l.event.Transaction != nil {
			l.event.Transaction.Spans[l.coarsePhase].Finished = now
		}
		l.event.Phases[l.phase] += max(0, now.Sub(l.last))
		l.event.Finished = now
		l.event.WaitNS = max(0, l.event.Acquired.Sub(l.event.Enqueued))
		l.event.HoldNS = max(0, now.Sub(l.event.Acquired))
		l.event.Epoch = c.epoch
		c.appendLocked(l.event)
		c.mu.Unlock()
		if !read {
			c.controlWriters.Add(-1)
			c.writeGate.Unlock()
		}
		return
	}
	if read {
		l.mu.RUnlock()
	} else {
		l.mu.Unlock()
	}
}
func (l *controlLock) Unlock()  { l.release(false) }
func (l *controlLock) RUnlock() { l.release(true) }

func (c *controlRecorder) client(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = "unknown"
	}
	return c.anonymous(host)
}

func controlOperation(fn func() error) string {
	return strings.TrimPrefix(runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), "mycelium/internal/myce.")
}

// ControlEvents reads durable history through the existing WAL reader, without
// q.mu. Sequence cursors and overwrite counters make missing coverage explicit.
type ControlRecord struct {
	Sequence int64 `json:"sequence"`
	ControlEvent
}
type ControlPage struct {
	Telemetry *ControlTelemetry `json:"telemetry"`
	First     int64             `json:"first"`
	HighWater int64             `json:"high_water"`
	Next      int64             `json:"next"`
	Entries   []ControlRecord   `json:"entries"`
}

func (q *Queue) controlPage(ctx context.Context, after int64) (ControlPage, error) {
	page := ControlPage{Telemetry: q.control.snapshot(), Next: after, Entries: []ControlRecord{}}
	for i := range page.Telemetry.Recent {
		e := &page.Telemetry.Recent[i]
		if e.Decision != nil {
			e.Decision, e.DecisionOmitted = nil, true
		}
	}
	if err := q.usageReader.QueryRowContext(ctx, `SELECT COALESCE(MIN(seq),0),COALESCE(MAX(seq),0) FROM control_events`).Scan(&page.First, &page.HighWater); err != nil {
		return page, err
	}
	rows, err := q.usageReader.QueryContext(ctx, `SELECT seq,json_remove(data,'$.decision') FROM control_events WHERE seq>? ORDER BY seq LIMIT ?`, after, requestLatencyWindow)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var record ControlRecord
		var raw []byte
		if err = rows.Scan(&record.Sequence, &raw); err != nil {
			return page, err
		}
		if err = json.Unmarshal(raw, &record.ControlEvent); err != nil {
			return page, err
		}
		if record.Kind == "decision" {
			record.DecisionOmitted = true
		}
		page.Entries = append(page.Entries, record)
		page.Next = record.Sequence
	}
	return page, rows.Err()
}
func (a API) registerControlTelemetry(mux *requestMux) {
	mux.HandleFunc("GET /api/control", func(w http.ResponseWriter, r *http.Request) {
		after := int64(0)
		var err error
		if v := r.URL.Query().Get("after"); v != "" {
			after, err = strconv.ParseInt(v, 10, 64)
		}
		if err != nil || after < 0 {
			respond(w, nil, &HTTPError{Status: 400, Code: "control_cursor_invalid", Message: "after must be a nonnegative sequence"})
			return
		}
		page, err := a.Queue.controlPage(r.Context(), after)
		if err == nil {
			err = chargeProjectionValue(r.Context(), page)
		}
		respond(w, page, err)
	})
}

func validPressureProbeSpans(spans []PressureProbeSpan) bool {
	if len(spans) > hostHealthHistorySize {
		return false
	}
	var sequence uint64
	for _, p := range spans {
		if p.Producer != "" {
			decoded, err := hex.DecodeString(p.Producer)
			if err != nil || len(decoded) != 16 {
				return false
			}
		}
		if p.Sequence <= sequence || p.Started.IsZero() || p.Ready.IsZero() || p.Consumed.IsZero() || len(p.Producer) > 32 || p.Failure != "clock_regressed" && (p.Ready.Before(p.Started) || p.Consumed.Before(p.Ready)) {
			return false
		}
		switch p.Failure {
		case "", "probe_failed", "cancelled", "pressure_unknown", "clock_regressed":
		default:
			return false
		}
		sequence = p.Sequence
	}
	return true
}

func (l *controlLock) coarse(phase int) {
	if l.recorder == nil || l.event.Transaction == nil {
		return
	}
	spans := &l.event.Transaction.Spans
	if phase == l.coarsePhase && !spans[phase].Started.IsZero() {
		return
	}
	now := l.recorder.clock.Now()
	if !spans[l.coarsePhase].Started.IsZero() {
		spans[l.coarsePhase].Finished = now
	}
	names := [...]string{"records", "usage", "body", "persistence", "notify"}
	spans[phase] = ControlPhaseSpan{Phase: names[phase], Started: now}
	l.coarsePhase = phase
}
