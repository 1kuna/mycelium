package myce

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tus/tusd/v2/pkg/memorylocker"
)

// SharedQueueMemberConfig runs one member of the shared queue. Each member
// has its own local SQLite copy and uses the same PostgreSQL authority, file
// store, checkpoint root and fleet token as every other member.
type SharedQueueMemberConfig struct {
	Postgres, Database, Checkpoints, FileStore, Token string
	// PostgresPasswordFile is a private file holding the authority password.
	PostgresPasswordFile string
	Clock                Clock
	// Endpoint is the address other members forward to while this member
	// coordinates. It must be reachable from every member.
	Endpoint string
	// FileStoreBytes limits the shared file store (default 100 GiB).
	FileStoreBytes   int64
	UploadSpoolDir   string
	UploadSpoolBytes int64
	// Options and Configure apply the installed queue settings to the queue
	// each coordinator turn opens. Configure runs before the turn is routable.
	Options   []QueueOption
	Configure func(context.Context, *Queue) error
	// AwaitImport leaves an empty authority untouched so that
	// myce queue cutover can import the single queue first.
	AwaitImport bool

	// Fixtures may shorten the turn and route through a partitionable link.
	lease, interval time.Duration
	peerTransport   http.RoundTripper
	// Fixtures may stand in for a hung store read during bundle verification.
	verifyBundles func(context.Context, *FileStore, map[FileRef]bool) error
}

// SharedQueueMember serves the queue API on behalf of whichever member holds
// the coordinator turn. Create it with OpenSharedQueueMember.
type SharedQueueMember = sharedQueueMember

type sharedQueueMember struct {
	cfg       SharedQueueMemberConfig
	authority *sharedQueueAuthority
	replica   *sql.DB
	lock      *os.File
	owner     string
	endpoint  string
	sequence  int64 // Member loop only.
	timeout   time.Duration
	closeIdle func()

	spool *uploadSpool // Owned by the member across all coordinator turns.
	// tus upload locks, in memory and also held across this member's turns
	// (#2152). tusd's file lock needs hard links that keep one identity,
	// which SMB shares lack, and trusts PIDs, which mean nothing on another
	// host. Only the coordinator serves uploads; other members proxy to it.
	// Retiring a turn interrupts its requests, and a former coordinator's
	// late positional write repeats bytes already there (rootedUpload).
	uploads   *memorylocker.MemoryLocker
	fileSetup sync.Mutex // One outstanding setup even if the mount hangs across turns.
	mu        sync.Mutex // Protects local handler admission, never held for I/O.
	leader    *sharedQueueLeader
}

type sharedQueueLeader struct {
	writer   *sharedQueueWriter
	queue    *Queue
	api      http.Handler
	requests sync.WaitGroup
	stopTick context.CancelFunc
	tickDone chan struct{}
}

// ServeSharedQueueProof runs a member on a loopback listener for the laptop
// proof. Production members use OpenSharedQueueMember.
func ServeSharedQueueProof(ctx context.Context, listener net.Listener, cfg SharedQueueMemberConfig) error {
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !addr.IP.IsLoopback() {
		return errors.New("the shared queue proof requires a loopback listener")
	}
	cfg.Endpoint = "http://" + addr.String()
	m, err := OpenSharedQueueMember(ctx, cfg)
	if err != nil {
		return err
	}
	defer m.Close()
	server := &http.Server{Handler: m, ConnContext: QueueConnectionContext, ReadHeaderTimeout: 5 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		m.Run(loopCtx)
	}()
	select {
	case err = <-serverDone:
		stopLoop()
	case <-ctx.Done():
		stopLoop()
		err = errors.Join(server.Close(), <-serverDone)
	}
	<-loopDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// validSharedEndpoint accepts an HTTP(S) origin that another member can dial.
func validSharedEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.EscapedPath() != "" && u.EscapedPath() != "/" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	return ip == nil || !ip.IsUnspecified()
}

// OpenSharedQueueMember opens the authority and the member's own SQLite copy.
// Serve it as an http.Handler and call Run; call Close after Run returns.
func OpenSharedQueueMember(ctx context.Context, cfg SharedQueueMemberConfig) (*SharedQueueMember, error) {
	if cfg.Postgres == "" || cfg.Database == "" || cfg.Checkpoints == "" || cfg.FileStore == "" || cfg.Token == "" || cfg.Clock == nil {
		return nil, errors.New("a shared queue member requires PostgreSQL, a local database, a checkpoint root, a file store, the fleet token and a clock")
	}
	if !validSharedEndpoint(cfg.Endpoint) {
		return nil, errors.New("a shared queue member requires an HTTP(S) endpoint that other members can reach")
	}
	// Each authority call gets lease/3 (2 s). The NAS authority measured
	// commit p99 about 18 ms and max about 111 ms under load (#1176), so a
	// call that misses 2 s means a stalled authority or host, and the turn
	// ends. The lease bounds crash failover to about lease plus one interval.
	if cfg.interval == 0 {
		cfg.interval = time.Second
	}
	if cfg.lease == 0 {
		cfg.lease = 6 * time.Second
	}
	if cfg.lease < 3*cfg.interval || cfg.interval <= 0 {
		return nil, errors.New("the shared queue turn must span at least three renewal intervals")
	}
	if cfg.FileStoreBytes == 0 {
		cfg.FileStoreBytes = 100 << 30
	}
	if cfg.UploadSpoolBytes == 0 {
		cfg.UploadSpoolBytes = DefaultUploadSpoolBytes
	}
	if cfg.verifyBundles == nil {
		cfg.verifyBundles = verifyFrozenBundleFiles
	}
	closeIdle := func() {}
	if cfg.peerTransport == nil {
		// Direct connections to members, with the joining connect budget so a
		// powered-off coordinator host fails a forward quickly.
		transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
		closeIdle = transport.CloseIdleConnections
		cfg.peerTransport = transport
	}
	if err := PrepareStateRoots(filepath.Dir(cfg.Database), cfg.Database+".uploads"); err != nil {
		return nil, err
	}
	lock, err := lockQueueDatabase(cfg.Database)
	if err != nil {
		return nil, err
	}
	initCtx, cancel := context.WithTimeout(ctx, cfg.lease)
	authority, err := openSharedQueueAuthority(initCtx, cfg.Postgres, cfg.PostgresPasswordFile)
	cancel()
	if err != nil {
		lock.Close()
		return nil, err
	}
	replica, err := openQueueSQLite(cfg.Database)
	if err != nil {
		authority.db.Close()
		lock.Close()
		return nil, err
	}
	m := &sharedQueueMember{
		cfg: cfg, authority: authority, replica: replica, lock: lock, owner: ID(), uploads: memorylocker.New(),
		endpoint: strings.TrimRight(cfg.Endpoint, "/"), timeout: cfg.lease / 3, closeIdle: closeIdle,
	}
	if err := m.bindReplica(ctx); err != nil {
		m.Close()
		return nil, err
	}
	return m, nil
}

// Run takes and renews the coordinator turn until ctx ends, then retires.
func (m *sharedQueueMember) Run(ctx context.Context) { m.run(ctx) }

// Close releases the member's database handles and locks after Run returns.
func (m *sharedQueueMember) Close() error {
	m.closeIdle()
	if m.spool != nil {
		_ = m.spool.lock.Close()
	}
	return errors.Join(m.replica.Close(), m.authority.db.Close(), m.lock.Close())
}

func (m *sharedQueueMember) bindReplica(ctx context.Context) error {
	tx, err := m.replica.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, schema := range []string{sharedQueueCursorSchema, memoryEvidenceSchema, gpuSharingSchema} {
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return err
		}
	}
	var identity string
	err = tx.QueryRowContext(ctx, "SELECT identity,sequence FROM myce_shared_cursor WHERE id=1").Scan(&identity, &m.sequence)
	if err == nil {
		if identity != m.authority.identity {
			return errors.New("the SQLite copy belongs to a different PostgreSQL authority")
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// A single-queue database must never become a shared replica implicitly.
	for name := range sharedQueueTables {
		var populated bool
		err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+name+")").Scan(&populated)
		if err != nil {
			return err
		}
		if populated {
			return errors.New("the shared queue proof requires a fresh or already bound SQLite copy")
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO myce_shared_cursor(id,identity,sequence) VALUES(1,?,0)", m.authority.identity); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *sharedQueueMember) run(ctx context.Context) {
	tick := time.NewTicker(m.cfg.interval)
	defer tick.Stop()
	defer m.retire()
	for {
		m.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (m *sharedQueueMember) step(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if leader := m.leader; leader != nil {
		if !leader.writer.disabled.Load() && m.renew(ctx, leader.writer.turn, m.cfg.lease, true) == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		leader.writer.disabled.Store(true)
		m.retire()
		return
	}
	t, err := m.current(ctx)
	// A turn this member still owns here has no writer: an acquisition or
	// release lost its reply. Replace it now rather than idle out its lease.
	if err != nil || m.pull(ctx, t) != nil || t.ready && t.owner != m.owner || m.cfg.AwaitImport && t.number == 0 && t.head == 0 {
		return
	}
	call, cancel := context.WithTimeout(ctx, m.timeout)
	t, err = m.authority.acquire(call, m.owner, m.endpoint, m.cfg.lease)
	cancel()
	if err != nil {
		return
	}
	if failure := m.promote(ctx, t); failure != "" {
		log.Printf("event=shared_queue_promotion_failed reason=%s", failure)
		m.release(t)
	}
}

func (m *sharedQueueMember) pull(ctx context.Context, t sharedQueueTurn) error {
	head := t.head
	if m.sequence > head {
		return errSharedHead
	}
	if m.sequence < t.floor {
		// The changes this copy needs were pruned. Replacing it with the
		// verified snapshot is the only way forward; replay never skips.
		sequence, err := m.authority.rebuild(ctx, m.replica, m.timeout)
		if err != nil {
			m.readCursor()
			return err
		}
		log.Printf("event=shared_queue_replica_rebuilt from=%d snapshot=%d", m.sequence, sequence)
		m.sequence = sequence
	}
	for m.sequence < head {
		call, cancel := context.WithTimeout(ctx, m.timeout)
		err := m.authority.changes(call, m.sequence, min(head, m.sequence+64), func(change sharedQueueChange) error {
			if err := replaySharedQueueChange(call, m.replica, change); err != nil {
				return err
			}
			m.sequence = change.sequence
			return nil
		})
		cancel()
		if err != nil {
			// A fenced writer may have finished applying its last committed
			// SQLite transaction after the old turn was retired.
			m.readCursor()
			return err
		}
	}
	return nil
}

type sharedPromotionFailure string

const (
	promotionReplay sharedPromotionFailure = "replica_replay"
	promotionHead   sharedPromotionFailure = "replica_sequence_mismatch"
	promotionSpool  sharedPromotionFailure = "local_upload_spool"
	promotionQueue  sharedPromotionFailure = "local_queue_startup"
	promotionTurn   sharedPromotionFailure = "coordinator_turn"
	promotionConfig sharedPromotionFailure = "queue_settings"
	promotionDB     sharedPromotionFailure = "postgresql_renewal"
)

func (m *sharedQueueMember) promote(ctx context.Context, turn sharedQueueTurn) sharedPromotionFailure {
	if err := m.pull(ctx, turn); err != nil {
		if errors.Is(err, errSharedHead) {
			return promotionHead
		}
		return promotionReplay
	}
	if m.spool == nil {
		spool := &uploadSpool{defaultDir: m.cfg.Database + ".uploads"}
		if err := spool.configure(m.cfg.UploadSpoolDir, m.cfg.UploadSpoolBytes); err != nil {
			return promotionSpool
		}
		m.spool = spool
	}
	// Opening a large copy and applying settings can outlast one lease.
	// Keep the turn, still unroutable, until the queue is ready to serve.
	hold, stopHold := context.WithCancel(ctx)
	held := make(chan error, 1)
	go func() { held <- m.holdTurn(hold, turn) }()
	writer := &sharedQueueWriter{authority: m.authority, turn: turn, sequence: turn.head, timeout: m.timeout, spool: m.spool}
	options := append([]QueueOption{WithFileStore(m.cfg.FileStore, m.cfg.FileStoreBytes, true), withUploadLocks(m.uploads)}, m.cfg.Options...)
	q, err := openQueue(m.cfg.Database, m.cfg.Checkpoints, m.cfg.Clock, writer, m.lock, options...)
	if err != nil {
		stopHold()
		<-held
		// Startup may already have committed a projection before failing.
		// Reload the atomic SQLite cursor before trying another promotion.
		m.readCursor()
		if errors.Is(err, errSharedTurn) {
			return promotionTurn
		}
		if errors.Is(err, errSharedHead) {
			return promotionHead
		}
		return promotionQueue
	}
	tickCtx, stop := context.WithCancel(ctx)
	failed := func(reason sharedPromotionFailure) sharedPromotionFailure {
		stop()
		writer.disabled.Store(true)
		_ = q.Close()
		m.readCursor()
		return reason
	}
	if m.cfg.Configure != nil {
		if err := m.cfg.Configure(tickCtx, q); err != nil {
			stopHold()
			<-held
			return failed(promotionConfig)
		}
	}
	stopHold()
	if err := <-held; err != nil {
		return failed(promotionTurn)
	}
	if err := m.renew(ctx, turn, m.cfg.lease, true); err != nil {
		if errors.Is(err, errSharedTurn) {
			return failed(promotionTurn)
		}
		return failed(promotionDB)
	}
	l := &sharedQueueLeader{writer: writer, queue: q, api: API{Queue: q, Token: m.cfg.Token}.Handler(), stopTick: stop, tickDone: make(chan struct{})}
	m.mu.Lock()
	m.leader = l
	m.mu.Unlock()
	go m.prepareStorage(tickCtx, q)
	go m.compact(tickCtx, writer)
	// Best effort: every publication registers again before it checks (#2148).
	go func() { _ = q.registerDeliveryDestinations() }()
	go func() {
		defer close(l.tickDone)
		// The same maintenance loop as a single queue. A failed tick ends
		// this turn; the member loop retires it and may promote again.
		if err := q.Run(tickCtx); err != nil && tickCtx.Err() == nil {
			log.Printf("event=shared_queue_maintenance_failed reason=%s handover=%d", sharedMaintenanceFailure(err), turn.number)
			writer.disabled.Store(true)
		}
	}()
	return ""
}

// compact keeps the change log bounded while this member coordinates. It
// reads this member's copy through its own SQLite handle, so a long snapshot
// read never waits on, or holds, the replica handle or q.mu.
func (m *sharedQueueMember) compact(ctx context.Context, writer *sharedQueueWriter) {
	source, err := openQueueSQLite(m.cfg.Database)
	if err != nil {
		log.Printf("event=shared_queue_compaction_failed reason=local_copy")
		return
	}
	defer source.Close()
	c := &sharedQueueCompactor{authority: m.authority, source: source, writer: writer}
	tick := time.NewTicker(m.cfg.interval)
	defer tick.Stop()
	for {
		if err := c.step(ctx); err != nil && ctx.Err() == nil {
			// PostgreSQL error text can contain connection settings.
			log.Printf("event=shared_queue_compaction_failed reason=%s", sharedMaintenanceFailure(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// sharedMaintenanceFailure names why a tick ended the turn without logging
// PostgreSQL error text, which can contain connection settings.
func sharedMaintenanceFailure(err error) sharedPromotionFailure {
	switch {
	case errors.Is(err, errSharedHead):
		return promotionHead
	case errors.Is(err, errSharedTurn):
		// Includes a write PostgreSQL did not confirm within the call deadline.
		return promotionTurn
	}
	return "local_maintenance"
}

// holdTurn renews an unroutable turn until ctx ends. It reports a lost turn
// or a renewal failure; the caller then gives up this promotion.
func (m *sharedQueueMember) holdTurn(ctx context.Context, turn sharedQueueTurn) error {
	tick := time.NewTicker(m.cfg.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		if err := m.renew(ctx, turn, m.cfg.lease, false); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (m *sharedQueueMember) prepareStorage(ctx context.Context, q *Queue) {
	retry := time.NewTicker(m.cfg.interval)
	defer retry.Stop()
	for ctx.Err() == nil {
		if m.fileSetup.TryLock() {
			store := m.prepareStorageOnce(ctx, q)
			m.fileSetup.Unlock()
			if store != nil {
				m.watchStorage(ctx, q, store)
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-retry.C:
		}
	}
}

func (m *sharedQueueMember) prepareStorageOnce(ctx context.Context, q *Queue) *FileStore {
	q.mu.RLock()
	controlFiles := q.controlStorageFiles()
	q.mu.RUnlock()
	reserve := max(q.fileStoreProductionReserve, 0) // Fixed by startup options.
	store, err := q.prepareFileStore(m.cfg.FileStore, m.cfg.FileStoreBytes, uint64(reserve), controlFiles)
	if err != nil || ctx.Err() != nil {
		return nil
	}
	q.mu.RLock()
	refs, err := q.frozenBundleFiles()
	q.mu.RUnlock()
	if err != nil || m.cfg.verifyBundles(ctx, store, refs) != nil {
		return nil
	}
	store.primeInventory(ctx)
	q.fileLifecycle.Lock()
	defer q.fileLifecycle.Unlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	if ctx.Err() != nil || q.stopping.Load() {
		return nil
	}
	q.files = store
	q.republishReads()
	return store
}

// Health sampling stays independent of both turn renewal and bulk
// maintenance. A stuck reclaim or delivery cannot hide a backing-store fault.
func (m *sharedQueueMember) watchStorage(ctx context.Context, q *Queue, store *FileStore) {
	tick := time.NewTicker(m.cfg.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			available, _ := q.healthStoreProbe.runProgress(store.root, func(progress func()) (bool, error) {
				return checkpointAvailable(store.root, "queue-files", progress), nil
			})
			q.sharedWriter.storageFault.Store(!available)
		}
	}
}

func (m *sharedQueueMember) readCursor() {
	// On failure, a lower in-memory cursor can only cause replay to fail
	// closed. No successor is installed until it agrees with SQLite.
	_ = m.replica.QueryRow("SELECT sequence FROM myce_shared_cursor WHERE id=1").Scan(&m.sequence)
}

func (m *sharedQueueMember) retire() {
	m.mu.Lock()
	l := m.leader
	m.leader = nil
	m.mu.Unlock()
	if l == nil {
		return
	}
	l.queue.BeginShutdown()
	l.stopTick()
	l.writer.disabled.Store(true)
	// Drop authority before waiting for old file handlers or maintenance.
	// A kernel-blocked mount must not prevent a new turn on this member.
	m.release(l.writer.turn)
	go func() {
		<-l.tickDone
		l.requests.Wait()
		_ = l.queue.Close()
	}()

	m.readCursor()
}

func (m *sharedQueueMember) current(ctx context.Context) (sharedQueueTurn, error) {
	call, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.authority.current(call)
}

func (m *sharedQueueMember) renew(ctx context.Context, turn sharedQueueTurn, lease time.Duration, ready bool) error {
	call, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.authority.renew(call, turn, lease, ready)
}

func (m *sharedQueueMember) release(turn sharedQueueTurn) {
	_ = m.renew(context.Background(), turn, 0, false)
}

const (
	sharedTurnPath        = "/api/shared-queue/turn"
	sharedReplicaPath     = "/api/shared-queue/status"
	sharedTurnHeader      = "X-Myce-Shared-Turn"
	sharedVerifyHeader    = "X-Myce-Shared-Turn-Protocol"
	sharedControlProtocol = "3"
	sharedProxyHeader     = "X-Myce-Shared-Forward"
)

func sharedQueueControlRetry() error {
	const code = "shared_queue_control_retry"
	return &HTTPError{Status: http.StatusServiceUnavailable, Code: code, Message: rejectionMessages[code]}
}

func sharedFileStoreUnready() error {
	const code = "shared_file_store_unready"
	return &HTTPError{Status: http.StatusServiceUnavailable, Code: code, Message: rejectionMessages[code]}
}

// A check-in or event poll proposes directives. Launch, start and yield
// authorize an effect only in the second request, with the proposed turn.
func sharedWorkerControl(path string) (proposal, mutation bool) {
	path, _, _ = strings.Cut(path, "?")
	switch path {
	case "/api/workers/checkin":
		return true, false
	case "/api/workers/launch", "/api/workers/started", "/api/workers/yield-stop":
		return false, true
	}
	id, found := strings.CutPrefix(path, "/api/workers/")
	worker, events := strings.CutSuffix(id, "/events")
	return found && events && worker != "", false
}

func (m *sharedQueueMember) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	credential := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if r.Method == http.MethodGet && (r.URL.Path == "/" || r.URL.Path == "/api/status") {
		if user, password, ok := r.BasicAuth(); ok && user == "myce" {
			credential = password
		}
	}
	if subtle.ConstantTimeCompare([]byte(credential), []byte(m.cfg.Token)) != 1 {
		respond(w, nil, &HTTPError{Status: 401, Code: "authentication_required"})
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == sharedReplicaPath {
		m.serveReplica(w, r)
		return
	}
	closeCancelledConnection(w, r)
	turn, err := m.current(r.Context())
	if err != nil || !turn.ready {
		reason := "turn_not_ready"
		if r.Context().Err() != nil {
			reason = "request_cancelled"
		} else if err != nil {
			reason = "turn_unreadable"
		}
		respond(w, nil, queueStoppingBecause(reason))
		return
	}
	proposal, mutation := sharedWorkerControl(r.URL.Path)
	confirm := r.Method == http.MethodPost && r.URL.Path == sharedTurnPath
	if (proposal || mutation || confirm) && r.Header.Get(sharedVerifyHeader) != sharedControlProtocol {
		// Advertise the protocol so a worker can switch after a cutover.
		w.Header().Set(sharedVerifyHeader, sharedControlProtocol)
		respond(w, nil, &HTTPError{Status: http.StatusUpgradeRequired, Code: "worker_protocol_mismatch"})
		return
	}
	number := strconv.FormatInt(turn.number, 10)
	want := r.Header.Get(sharedTurnHeader)
	if want != "" && want != number || confirm && want == "" {
		// A changed turn is transient. The successor still has to check the
		// exact custody when a worker retries a launch or start under its turn.
		respond(w, nil, sharedQueueControlRetry())
		return
	}
	if confirm {
		// The authoritative read above checks readiness and expiry against
		// database time. Handover only increases, so the same number also
		// proves that no takeover committed after the proposal was issued.
		w.Header().Set(sharedTurnHeader, number)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if turn.owner != m.owner {
		m.forward(w, r, turn)
		return
	}
	expected := r.Header.Get(sharedProxyHeader)
	if expected != "" && expected != strconv.FormatInt(turn.number, 10) {
		respond(w, nil, queueStoppingBecause("forwarded_turn_changed"))
		return
	}
	m.mu.Lock()
	l := m.leader
	if l != nil && sameSharedTurn(turn, l.writer.turn) && !l.writer.disabled.Load() {
		l.requests.Add(1)
	} else {
		l = nil
	}
	m.mu.Unlock()
	if l == nil {
		respond(w, nil, queueStoppingBecause("turn_not_serving"))
		return
	}
	defer l.requests.Done()
	if r.Method == http.MethodGet && r.URL.Path == sharedTurnPath {
		// The retired verification protocol can never authorize an effect.
		respond(w, nil, queueStoppingBecause("retired_turn_protocol"))
		return
	}
	if proposal || mutation {
		w.Header().Set(sharedTurnHeader, number)
	}
	if mutation && want == "" {
		// This reply carries no authorization, even if delayed past a handover.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	files := strings.HasPrefix(r.URL.Path, "/api/files/") && r.URL.Path != "/api/files/config"
	extensions := r.URL.Path == "/api/extensions" && r.Method == http.MethodPost || strings.HasPrefix(r.URL.Path, "/api/extensions/")
	if files || extensions {
		l.queue.mu.RLock()
		ready := l.queue.sharedFilesReady()
		l.queue.mu.RUnlock()
		if !ready {
			respond(w, nil, sharedFileStoreUnready())
			return
		}
	}
	l.api.ServeHTTP(w, r)
}

func (m *sharedQueueMember) forward(w http.ResponseWriter, r *http.Request, turn sharedQueueTurn) {
	target, err := url.Parse(turn.endpoint)
	if err != nil || !validSharedEndpoint(turn.endpoint) || turn.endpoint == m.endpoint || r.Header.Get(sharedProxyHeader) != "" {
		respond(w, nil, queueStoppingBecause("forward_target_invalid"))
		return
	}
	proxy := httputil.ReverseProxy{
		Transport: m.cfg.peerTransport,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.SetURL(target)
			p.Out.Header.Set(sharedProxyHeader, strconv.FormatInt(turn.number, 10))
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			respond(w, nil, queueStoppingBecause("forward_failed"))
		},
	}
	proxy.ServeHTTP(w, r)
}

func (m *sharedQueueMember) serveReplica(w http.ResponseWriter, r *http.Request) {
	// The persisted job also contains its execution payload. Replica reads
	// expose only lifecycle fields and leave full projections to the API.
	type replicaJob struct {
		ID        string     `json:"id"`
		State     string     `json:"state"`
		StartedAt *time.Time `json:"started_at,omitempty"`
	}
	type replicaStatus struct {
		LastKnown bool        `json:"last_known"`
		Sequence  int64       `json:"sequence"`
		Job       *replicaJob `json:"job,omitempty"`
	}
	out := replicaStatus{LastKnown: true}
	ctx := r.Context()
	tx, err := m.replica.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err == nil {
		defer tx.Rollback()
		err = tx.QueryRowContext(ctx, "SELECT sequence FROM myce_shared_cursor WHERE id=1").Scan(&out.Sequence)
	}
	if id := r.URL.Query().Get("job"); id != "" && err == nil {
		var data []byte
		err = tx.QueryRowContext(ctx, "SELECT data FROM queue_state WHERE key=?", "jobs/"+id).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		} else if err == nil {
			data, err = expandJobRecord(data)
			if err == nil {
				err = json.Unmarshal(data, &out.Job)
			}
		}
	}
	if err != nil {
		respond(w, nil, queueStoppingBecause("replica_unreadable"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	respond(w, out, nil)
}
