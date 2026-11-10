package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Immutable AVL records share unchanged branches across commits. Publication is
// proportional to changed records, not retained history. A reader takes one
// root pointer under q.mu; traversal, decoding and projection own no live lock.
// Old roots are reclaimed normally when their last reader finishes.
type readNode struct {
	key          string
	record       *readRecord
	left, right  *readNode
	height, live int
}
type readRecord struct {
	data    []byte // The persistence journal already owns immutable encodings.
	mutable bool
	once    sync.Once
	decoded atomic.Bool
	value   any
	err     error
	history statusHistoryEntry
}

func readHeight(n *readNode) int {
	if n == nil {
		return 0
	}
	return n.height
}
func readLive(n *readNode) int {
	if n == nil {
		return 0
	}
	return n.live
}
func readBranch(key string, record *readRecord, left, right *readNode) *readNode {
	n := &readNode{key: key, record: record, left: left, right: right, height: 1 + max(readHeight(left), readHeight(right)), live: readLive(left) + readLive(right)}
	if record.mutable {
		n.live++
	}
	return n
}
func readRotateLeft(n *readNode) *readNode {
	r := n.right
	return readBranch(r.key, r.record, readBranch(n.key, n.record, n.left, r.left), r.right)
}
func readRotateRight(n *readNode) *readNode {
	l := n.left
	return readBranch(l.key, l.record, l.left, readBranch(n.key, n.record, l.right, n.right))
}
func readBalance(n *readNode) *readNode {
	if readHeight(n.left)-readHeight(n.right) > 1 {
		if readHeight(n.left.left) < readHeight(n.left.right) {
			n = readBranch(n.key, n.record, readRotateLeft(n.left), n.right)
		}
		return readRotateRight(n)
	}
	if readHeight(n.right)-readHeight(n.left) > 1 {
		if readHeight(n.right.right) < readHeight(n.right.left) {
			n = readBranch(n.key, n.record, n.left, readRotateRight(n.right))
		}
		return readRotateLeft(n)
	}
	return n
}
func readPut(n *readNode, key string, record *readRecord) *readNode {
	if n == nil {
		if record == nil {
			return nil
		}
		return readBranch(key, record, nil, nil)
	}
	if key < n.key {
		return readBalance(readBranch(n.key, n.record, readPut(n.left, key, record), n.right))
	}
	if key > n.key {
		return readBalance(readBranch(n.key, n.record, n.left, readPut(n.right, key, record)))
	}
	if record != nil {
		return readBranch(key, record, n.left, n.right)
	}
	if n.left == nil {
		return n.right
	}
	if n.right == nil {
		return n.left
	}
	next := n.right
	for next.left != nil {
		next = next.left
	}
	return readBalance(readBranch(next.key, next.record, n.left, readPut(n.right, next.key, nil)))
}
func readGet(n *readNode, key string) *readRecord {
	for n != nil {
		if key == n.key {
			return n.record
		}
		if key < n.key {
			n = n.left
		} else {
			n = n.right
		}
	}
	return nil
}
func readWalk(n *readNode, live bool, visit func(string, *readRecord)) {
	if n == nil || live && n.live == 0 {
		return
	}
	readWalk(n.left, live, visit)
	if !live || n.record.mutable {
		visit(n.key, n.record)
	}
	readWalk(n.right, live, visit)
}

// Decode each immutable record once, outside q.mu. Ledger history has a separate
// paginated API and is never an input to receipt/status diagnostics.
type discardReadJSON struct{}

func (*discardReadJSON) UnmarshalJSON([]byte) error { return nil }
func decodeReadJob(data []byte) (*Job, error) {
	type job Job
	out := &Job{}
	wire := struct {
		*job
		LedgerEntries discardReadJSON `json:"ledger_entries"`
	}{job: (*job)(out)}
	err := json.Unmarshal(data, &wire)
	return out, err
}

type readSubmissionJob Job

func (j *readSubmissionJob) UnmarshalJSON(data []byte) error {
	value, err := decodeReadJob(data)
	*j = readSubmissionJob(*value)
	return err
}

func decodeReadSubmission(data []byte) (*submissionRecord, error) {
	type submission submissionRecord
	out := &submissionRecord{}
	wire := struct {
		*submission
		Job *readSubmissionJob `json:"job"`
	}{submission: (*submission)(out)}
	err := json.Unmarshal(data, &wire)
	out.Job = (*Job)(wire.Job)
	return out, err
}

// Persisted JSON is already valid and compact. Locate the job's ledger value,
// without tokenizing its strings or allocating its retained execution history.
// A submission record contains the job one object level deeper.
func readJobLedgerSpan(data []byte, jobDepth int) (int, int) {
	depth, quoted, escaped := 0, false, false
	for i, c := range data {
		if quoted {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		if c == '"' {
			if depth == jobDepth && bytes.HasPrefix(data[i:], []byte(`"ledger_entries":`)) {
				start := i
				end := i + len(`"ledger_entries":`)
				nesting, inString, escape := 0, false, false
				for ; end < len(data); end++ {
					v := data[end]
					if inString {
						if escape {
							escape = false
						} else if v == '\\' {
							escape = true
						} else if v == '"' {
							inString = false
						}
						continue
					}
					if v == '"' {
						inString = true
						continue
					}
					if nesting == 0 && (v == ',' || v == '}') {
						break
					}
					if v == '[' || v == '{' {
						nesting++
					}
					if v == ']' || v == '}' {
						nesting--
					}
				}
				if end < len(data) && data[end] == ',' {
					end++
				} else if start > 0 && data[start-1] == ',' {
					start--
				}
				return start, end
			}
			quoted = true
		} else if c == '{' || c == '[' {
			depth++
		} else if c == '}' || c == ']' {
			depth--
		}
	}
	return 0, 0
}
func (r *readRecord) decode(key string) (any, error) {
	r.once.Do(func() {
		defer r.decoded.Store(true)
		group, _, _ := strings.Cut(key, "/")
		switch group {
		case "jobs":
			r.value, r.err = decodeReadJob(r.data)
			if r.err == nil && !r.mutable {
				j := r.value.(*Job)
				input := statusJobInputs(j)
				input.Spec.SubmissionKey = j.Spec.SubmissionKey
				r.history = statusHistoryEntry{source: j, inputs: &input, dependency: statusDependencyJob(j)}
			}
		case "state":
			out := &state{}
			r.err = json.Unmarshal(r.data, out)
			r.value = out
		case "submissions":
			r.value, r.err = decodeReadSubmission(r.data)
		case "extensions":
			out := &Extension{}
			r.err = json.Unmarshal(r.data, out)
			r.value = out
		case "work_observations":
			out := &WorkObservation{}
			r.err = json.Unmarshal(r.data, out)
			r.value = out
		case "storage_observations":
			out := &StorageObservation{}
			r.err = json.Unmarshal(r.data, out)
			r.value = out
		case "external_memory":
			out := &externalMemoryRing{}
			r.err = json.Unmarshal(r.data, out)
			r.value = out
		case "health_events":
			out := &HealthAlert{}
			r.err = json.Unmarshal(r.data, out)
			r.value = out
		}
	})
	return r.value, r.err
}

// Only successful commits publish. Rollback neither advances the root nor
// discards a reader's lazy decoded records. No SQLite access is used by reads.
func (q *Queue) publishReadRecords(next map[string][]byte) {
	for key, data := range next {
		mutable := false
		if len(data) > 0 {
			if id, ok := strings.CutPrefix(key, "jobs/"); ok {
				mutable = mutableJob(q.s.Jobs[id])
			}
			if id, ok := strings.CutPrefix(key, "submissions/"); ok {
				r := q.s.Submissions[id]
				mutable = r != nil && r.Job != nil && mutableJob(r.Job)
			}
		}
		old := readGet(q.readRoot, key)
		// Reference bytes need not change when their job's custody clears.
		// Liveness belongs to this generation, just like its committed bytes.
		if old != nil && old.mutable == mutable && bytes.Equal(old.data, data) {
			continue
		}
		var record *readRecord
		if len(data) > 0 {
			record = &readRecord{data: data, mutable: mutable}
		}
		q.readRoot = readPut(q.readRoot, key, record)
	}
}

// Configuration is replaced by its setters; compare with detached copies so
// unchanged commits reuse it. Calls and unreported contacts are small ephemeral
// control records and are frozen once per commit, rather than once per reader.
func (q *Queue) publishReadRuntime() {
	if q.readModels == nil || !reflect.DeepEqual(*q.readModels, q.models) {
		value := copyObservationValue(reflect.ValueOf(q.models)).Interface().(ModelConfig)
		q.readModels = &value
	}
	if q.readEnvironments == nil || !reflect.DeepEqual(*q.readEnvironments, q.environments) {
		value := copyObservationValue(reflect.ValueOf(q.environments)).Interface().(EnvironmentCatalog)
		q.readEnvironments = &value
	}
	value := copyObservation(observationState{Calls: q.calls, Unreported: q.unreported})
	q.readRuntime = &value
	q.readGeneration = &readGeneration{root: q.readRoot}
	q.readSamples = make(map[string]map[attemptKey]hostMemorySample, len(q.hostMemorySamples))
	for id, samples := range q.hostMemorySamples {
		q.readSamples[id] = maps.Clone(samples)
	}
	published := &publishedReadIndex{root: q.readRoot, calls: value.Calls, capture: q.freezeReadCapture()}
	// Cache lookups/removal and capture publication share this short fence.
	// Projection and metadata copying remain outside it; unchanged commits keep
	// their entries. An in-flight old response may finish but cannot repopulate
	// a cache entry after the matching new generation is published.
	q.jobSnapshotsMu.Lock()
	for id := range q.readInvalidations {
		delete(q.jobSnapshots, id)
	}
	if q.readDependenciesInvalid {
		for id, entry := range q.jobSnapshots {
			if entry.dependsOnJobs {
				delete(q.jobSnapshots, id)
			}
		}
	}
	q.publishedReads.Store(published)
	q.jobSnapshotsMu.Unlock()
	q.readInvalidations = nil
	q.readDependenciesInvalid = false
}

// Prefix resolution needs only immutable committed identities. A cache hit
// must not queue behind the writer merely to resolve the same job again.
type publishedReadIndex struct {
	root    *readNode
	calls   map[string]*ModelCall
	capture *committedReadCapture
}

// A generation materializes the immutable input maps once, off the writer path.
// Concurrent readers share that assembly and the unchanged records' decoders.
// It never shares a mutable diagnostic planner or a projected response.
type readGeneration struct {
	root      *readNode
	once      sync.Once
	liveOnce  sync.Once
	liveState state
	liveErr   error
	state     state
	records   *queueRecords
	err       error
}

func (g *readGeneration) inputs(ctx context.Context) (state, *queueRecords, error) {
	if err := g.chargeDecode(ctx, false, ""); err != nil {
		return state{}, nil, err
	}
	g.once.Do(func() {
		base := readGet(g.root, "state")
		value, err := base.decode("state")
		if err != nil {
			g.err = err
			return
		}
		g.state = *(value.(*state))
		s := &g.state
		s.Jobs = map[string]*Job{}
		s.Submissions = map[string]*submissionRecord{}
		s.Extensions = map[string]Extension{}
		s.WorkObservations = map[string]WorkObservation{}
		s.StorageObservations = map[string]StorageObservation{}
		s.ExternalMemory = map[string]*externalMemoryRing{}
		g.records = &queueRecords{statusHistory: map[string]statusHistoryEntry{}}
		readWalk(g.root, false, func(key string, record *readRecord) {
			if g.err != nil {
				return
			}
			value, err := record.decode(key)
			if err != nil {
				g.err = err
				return
			}
			group, name, _ := strings.Cut(key, "/")
			switch group {
			case "jobs":
				s.Jobs[name] = value.(*Job)
				if record.history.inputs != nil {
					g.records.statusHistory[name] = record.history
				}
			case "submissions":
				s.Submissions[name] = value.(*submissionRecord)
			case "extensions":
				s.Extensions[name] = *value.(*Extension)
			case "work_observations":
				s.WorkObservations[name] = *value.(*WorkObservation)
			case "storage_observations":
				s.StorageObservations[name] = *value.(*StorageObservation)
			case "external_memory":
				s.ExternalMemory[name] = value.(*externalMemoryRing)
			case "health_events":
				index, e := strconv.Atoi(name)
				if e != nil {
					g.err = e
					return
				}
				for len(s.Health.Events) <= index {
					s.Health.Events = append(s.Health.Events, HealthAlert{})
				}
				s.Health.Events[index] = *value.(*HealthAlert)
			}
		})
		resolveReadSubmissionJobs(s)
	})
	if g.err != nil {
		return state{}, nil, g.err
	}
	return g.state, g.records, ctx.Err()
}

// Live keyed answers share the job in this committed generation. Copy only the
// reference wrapper: decoded records can be shared by older generations whose
// job bytes differ. Removed-job answers already contain their retained snapshot.
func resolveReadSubmissionJobs(s *state) {
	for key, record := range s.Submissions {
		if record.Job == nil && s.Jobs[record.JobID] != nil {
			copy := *record
			copy.Job = s.Jobs[record.JobID]
			s.Submissions[key] = &copy
		}
	}
}

// Selected receipts do not materialize retained execution payloads. A queued
// receipt shares one live/context assembly per revision; started/terminal work
// uses just its own immutable record and the current policy/machine identities.
func (g *readGeneration) jobInputs(ctx context.Context, id string) (state, *queueRecords, error) {
	if err := g.chargeDecode(ctx, false, id); err != nil {
		return state{}, nil, err
	}
	selected := readGet(g.root, "jobs/"+id)
	if selected != nil {
		value, err := selected.decode("jobs/" + id)
		if err != nil {
			return state{}, nil, err
		}
		j := value.(*Job)
		if j.FanOut != nil {
			return g.inputs(ctx)
		}
		if terminal(j.State) || j.State != "queued" && j.StartedAt != nil && !jobHasVariants(j) {
			value, err := readGet(g.root, "state").decode("state")
			if err != nil {
				return state{}, nil, err
			}
			base := value.(*state)
			s := state{Jobs: map[string]*Job{id: j}, PriorityPolicy: base.PriorityPolicy, Policies: base.Policies, Machines: base.Machines, HostReleases: base.HostReleases}
			return s, nil, ctx.Err()
		}
	}
	if err := g.chargeDecode(ctx, true, ""); err != nil {
		return state{}, nil, err
	}
	g.liveOnce.Do(func() {
		value, err := readGet(g.root, "state").decode("state")
		if err != nil {
			g.liveErr = err
			return
		}
		g.liveState = *(value.(*state))
		s := &g.liveState
		s.Jobs = map[string]*Job{}
		s.Submissions = map[string]*submissionRecord{}
		s.Extensions = map[string]Extension{}
		s.WorkObservations = map[string]WorkObservation{}
		s.StorageObservations = map[string]StorageObservation{}
		s.ExternalMemory = nil
		s.Health.Events = nil
		readWalk(g.root, true, func(key string, record *readRecord) {
			if g.liveErr != nil {
				return
			}
			value, err := record.decode(key)
			if err != nil {
				g.liveErr = err
				return
			}
			group, name, _ := strings.Cut(key, "/")
			switch group {
			case "jobs":
				s.Jobs[name] = value.(*Job)
			case "submissions":
				s.Submissions[name] = value.(*submissionRecord)
			}
		})
		readWalk(g.root, false, func(key string, record *readRecord) {
			if g.liveErr != nil {
				return
			}
			group, name, _ := strings.Cut(key, "/")
			if group != "work_observations" && group != "extensions" && group != "storage_observations" {
				return
			}
			value, err := record.decode(key)
			if err != nil {
				g.liveErr = err
				return
			}
			switch group {
			case "work_observations":
				s.WorkObservations[name] = *value.(*WorkObservation)
			case "extensions":
				s.Extensions[name] = *value.(*Extension)
			case "storage_observations":
				s.StorageObservations[name] = *value.(*StorageObservation)
			}
		})
		resolveReadSubmissionJobs(s)
	})
	if g.liveErr != nil {
		return state{}, nil, g.liveErr
	}
	return g.liveState, nil, ctx.Err()
}

// A commit publishes the complete observation context with its immutable root.
// Readers never combine committed records with mutable metadata from a writer.
type readFreshness struct {
	lastSeen   time.Time
	unreported bool
}
type committedReadCapture struct {
	generation    *readGeneration
	models        *ModelConfig
	environments  *EnvironmentCatalog
	runtime       *observationState
	source        *Queue
	contactInputs *Queue // Immutable scalar inputs; derive contact diagnostics off-lock.
	ledgers       map[string]observationMachineLedger
	fresh         map[string]readFreshness
}

func (q *Queue) freezeReadCapture() *committedReadCapture {
	generation, models, environments, runtime := q.readGeneration, q.readModels, q.readEnvironments, q.readRuntime
	source := &Queue{clock: observationClock{q.clock.Now()}, nasAvailable: q.nasAvailable, evidenceRetention: q.evidenceRetention, efficiencyView: q.efficiencyView,
		simulation: q.simulation, productionGrace: q.productionGrace, lossTimeouts: q.lossTimeouts, productionInfrastructureRetries: q.productionInfrastructureRetries,
		restartUntil: q.restartUntil, networkQuiet: q.networkQuiet, root: q.root, files: q.files, fileRetention: q.fileRetention, hostMemorySamples: q.readSamples, detachedRead: true}
	ledgers := make(map[string]observationMachineLedger, len(q.s.Machines))
	fresh := make(map[string]readFreshness, len(q.s.Machines))
	for key, m := range q.s.Machines {
		ledgers[key] = observationMachineLedger{healthDelivery: q.freezeHostHealthDelivery(m),
			toolInventory: m.toolInventory, cpuLedger: m.cpuLedger, hostBudgetGB: m.hostBudgetGB,
			modelReservationDebtGB: m.modelReservationDebtGB, hostReservationDebtGB: m.hostReservationDebtGB}
		fresh[key] = readFreshness{m.LastSeen, q.unreported[key]}
	}
	return &committedReadCapture{generation: generation, models: models, environments: environments, runtime: runtime, source: source,
		contactInputs: q.captureContactInputs(source.clock), ledgers: ledgers, fresh: fresh}
}

// Reference walks, cancellation, capacity scans and recursive copying run on
// an immutable private source after releasing the live queue mutex.
func (q *Queue) captureReadObservation(id string, options *statusOptions, contexts ...context.Context) (*Queue, fileStoreObservation, error) {
	ctx := requestContext(contexts)
	started := readWallStart(q.observeRead)
	var capture *committedReadCapture
	if id != "" {
		if published := q.publishedReads.Load(); published != nil {
			capture = published.capture
		}
	}
	// Offline imports may install unpublished records. Inspect that path only
	// if the writer is immediately available; a committed read never queues.
	imported := false
	if capture != nil && q.mu.TryLock() {
		imported = q.records != nil && (q.records.jobCount != len(q.s.Jobs) || q.records.submissionCount != len(q.s.Submissions))
		q.mu.Unlock()
	}
	if capture == nil {
		unlock, err := requestObservationLock(ctx, &q.mu)
		if err != nil {
			return nil, fileStoreObservation{}, err
		}
		imported = q.records != nil && (q.records.jobCount != len(q.s.Jobs) || q.records.submissionCount != len(q.s.Submissions))
		capture = q.freezeReadCapture()
		unlock()
	}
	// Capture the read clock before any off-lock projection or observation I/O.
	// Freshness includes that work; sampling afterward would report a delayed
	// uncached status as age zero. Published job inputs still use the current
	// injected read time rather than the older commit's runtime clock.
	readAt := q.clock.Now()
	if id == "" {
		readAt = capture.source.clock.Now()
	}
	observeRead(q.observeRead, id, started, started)
	if err := ctx.Err(); err != nil {
		return nil, fileStoreObservation{}, err
	}
	if imported || capture.generation == nil {
		return q.captureStatusObservation(id, options, ctx)
	}
	source, err := capture.observationQueue(ctx, id, readAt)
	if err != nil {
		return nil, fileStoreObservation{}, err
	}
	return source.captureStatusObservation(id, options, ctx)
}

// Materialize immutable committed inputs without taking the writer mutex.
func (capture *committedReadCapture) observationQueue(ctx context.Context, id string, readAt time.Time) (*Queue, error) {
	base := capture.source
	// Preserve the original capture clock and service opportunity while deriving
	// contact. A later read clock may age suspicion, but cannot renew its grace.
	contact := &Queue{clock: base.clock, restartUntil: base.restartUntil}
	capture.contactInputs.freezeContactView(contact)
	source := &Queue{clock: observationClock{readAt}, nasAvailable: base.nasAvailable, evidenceRetention: base.evidenceRetention, efficiencyView: base.efficiencyView,
		simulation: base.simulation, productionGrace: base.productionGrace, lossTimeouts: base.lossTimeouts, productionInfrastructureRetries: base.productionInfrastructureRetries,
		restartUntil: contact.restartUntil, networkQuiet: base.networkQuiet, root: base.root, files: base.files, fileRetention: base.fileRetention, hostMemorySamples: base.hostMemorySamples,
		contacts: contact.contacts, detachedRead: true}
	if contact.mu.control != nil {
		source.mu.control = &controlClock{clock: source.clock.Now, at: source.clock.Now(), opportunity: contact.controlNow()}
	}
	generation, models, environments, runtime := capture.generation, capture.models, capture.environments, capture.runtime
	ledgers, fresh := capture.ledgers, capture.fresh
	var err error
	if id == "" {
		source.s, source.records, err = generation.inputs(ctx)
	} else {
		source.s, source.records, err = generation.jobInputs(ctx, id)
	}
	if err != nil {
		return nil, err
	}
	source.models, source.environments, source.calls = *models, *environments, runtime.Calls
	source.unreported = make(map[string]bool, len(fresh))
	source.s.Machines = maps.Clone(source.s.Machines)
	for key, m := range source.s.Machines {
		copy := *m
		copy.LastSeen = fresh[key].lastSeen
		if fresh[key].unreported {
			source.unreported[key] = true
		}
		l := ledgers[key]
		if m.HostHealth != nil {
			health := *m.HostHealth
			health.delivery = l.healthDelivery
			copy.HostHealth = &health
		}
		copy.toolInventory = l.toolInventory
		copy.cpuLedger = l.cpuLedger
		copy.hostBudgetGB = l.hostBudgetGB
		copy.modelReservationDebtGB = l.modelReservationDebtGB
		copy.hostReservationDebtGB = l.hostReservationDebtGB
		source.s.Machines[key] = &copy
	}
	return source, nil
}

// Prefix resolution walks the immutable key range outside q.mu. Cache/commit
// fences still recheck the resolved exact ID before publishing a receipt.
func (q *Queue) resolveReadJobID(ctx context.Context, id string) (string, error) {
	if index := q.publishedReads.Load(); index != nil {
		resolved, err := resolveReadIndex(ctx, index.root, index.calls, id)
		if err == nil {
			return resolved, nil
		}
		// Offline import/test callers can install unpublished jobs. Preserve
		// that path; ordinary committed IDs never enter the live mutex.
	}
	if err := admitObservationProjection(ctx); err != nil {
		return "", err
	}
	unlock, err := requestObservationLock(ctx, &q.mu)
	if err != nil {
		return "", err
	}
	root, runtime := q.readRoot, q.readRuntime
	imported := q.records != nil && q.records.jobCount != len(q.s.Jobs)
	if root == nil || imported {
		j, e := q.findObservationJob(id)
		unlock()
		if e != nil {
			return "", e
		}
		return j.ID, nil
	}
	unlock()
	var calls map[string]*ModelCall
	if runtime != nil {
		calls = runtime.Calls
	}
	return resolveReadIndex(ctx, root, calls, id)
}

func resolveReadIndex(ctx context.Context, root *readNode, calls map[string]*ModelCall, id string) (string, error) {
	if readGet(root, "jobs/"+id) != nil {
		return id, nil
	}
	if calls[id] != nil {
		return id, nil
	}
	var matches []*Job
	if len(id) >= 8 {
		prefix := "jobs/" + id
		var visit func(*readNode)
		visit = func(n *readNode) {
			if n == nil {
				return
			}
			if n.key >= prefix {
				visit(n.left)
			}
			if strings.HasPrefix(n.key, prefix) {
				matches = append(matches, &Job{ID: strings.TrimPrefix(n.key, "jobs/")})
			}
			if n.key < prefix || strings.HasPrefix(n.key, prefix) {
				visit(n.right)
			}
		}
		visit(root)
		if calls != nil {
			for key := range calls {
				if strings.HasPrefix(key, id) {
					matches = append(matches, &Job{ID: key})
				}
			}
		}
	}
	j, err := FindJob(matches, id)
	if err != nil {
		return "", err
	}
	return j.ID, ctx.Err()
}

// The immutable input cache belongs to the queue. Charge its cold decode before
// allocation; warm records are already included in the host's measured RSS.
// Recursive response copies retain their independent existing charge below.
func (g *readGeneration) chargeDecode(ctx context.Context, live bool, selected string) error {
	allocation := projectionAllocation(ctx)
	if allocation == nil {
		return ctx.Err()
	}
	var failure error
	charge := func(key string, r *readRecord) {
		if r == nil {
			return
		}
		if failure != nil || r.decoded.Load() {
			return
		}
		group, name, _ := strings.Cut(key, "/")
		if selected != "" && key != "state" && (group != "jobs" || name != selected) {
			return
		}
		if live && (group == "jobs" || group == "submissions") && !r.mutable {
			return
		}
		if live && (group == "external_memory" || group == "health_events") {
			return
		}
		var target reflect.Type
		switch group {
		case "state":
			target = reflect.TypeFor[*state]()
		case "jobs":
			target = reflect.TypeFor[*Job]()
		case "submissions":
			target = reflect.TypeFor[*submissionRecord]()
		case "extensions":
			target = reflect.TypeFor[*Extension]()
		case "work_observations":
			target = reflect.TypeFor[*WorkObservation]()
		case "storage_observations":
			target = reflect.TypeFor[*StorageObservation]()
		case "external_memory":
			target = reflect.TypeFor[*externalMemoryRing]()
		case "health_events":
			target = reflect.TypeFor[*HealthAlert]()
		}
		fitting := allocation.limit - allocation.held
		data := r.data
		start, end := 0, 0
		if group == "jobs" {
			start, end = readJobLedgerSpan(data, 1)
		} else if group == "submissions" {
			start, end = readJobLedgerSpan(data, 2)
		}
		// The immutable source bytes are already resident. Only non-ledger
		// bytes require token scratch/copies; discarded ledger values are
		// scanned in place by decodeReadJob, without typed allocations.
		wire := 6 * int64(len(data)-(end-start))
		if !allocation.chargeRetained(wire) {
			failure = projectionRefusal(fitting)
			return
		}
		if end > start {
			data = append(append(make([]byte, 0, len(data)-(end-start)), data[:start]...), data[end:]...)
		}
		cost, err := jsonAllocationCost(data, target, max(0, fitting-wire))
		if err != nil || !allocation.chargeRetained(cost) {
			failure = projectionRefusal(fitting)
		}
	}
	if selected != "" {
		charge("state", readGet(g.root, "state"))
		charge("jobs/"+selected, readGet(g.root, "jobs/"+selected))
	} else {
		readWalk(g.root, false, charge)
	}
	if failure != nil {
		return failure
	}
	return ctx.Err()
}
