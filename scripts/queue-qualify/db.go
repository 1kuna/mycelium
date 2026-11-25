package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// The qualification tables live in their own schema. The coordinator and
// change tables copy the stage 2 shapes (myce_coordinator_v1 and
// myce_changes_v1 in internal/myce/shared_queue_postgres.go on the stage 2
// branch at e5d9287a) so the measured writes match the real ones.
//
// The harness never drops, truncates or deletes. Every run writes rows under
// its own run ID or sequence, and the storage owner removes the whole
// qualification database or container afterwards.
const qualSchema = "myce_qualification"

// prepare creates the schema with an ownership comment and refuses a schema
// of that name without it.
const qualSchemaComment = "created by queue-qualify; the storage owner removes the qualification database"

// The harness runs only in a database whose name starts with this, in any
// case, such as the plan's default "qualify". A production queue database
// never does, so a mistaken connection string fails here even if every
// catalog check passed.
const qualDatabasePrefix = "qualif"

// pinnedSearchPath makes every unqualified name resolve in the system catalog
// first. A role's or database's search_path could otherwise put a schema
// ahead of pg_catalog whose pg_class view or current_database() function
// hides a queue. Every catalog reference is also written pg_catalog.*.
const pinnedSearchPath = "pg_catalog, pg_temp"

const qualDDL = `
CREATE TABLE IF NOT EXISTS myce_qualification.myce_coordinator_v1 (
	id integer PRIMARY KEY CHECK (id = 1),
	identity text NOT NULL,
	owner text NOT NULL DEFAULT '',
	endpoint text NOT NULL DEFAULT '',
	handover bigint NOT NULL DEFAULT 0,
	head bigint NOT NULL DEFAULT 0,
	expires timestamptz NOT NULL DEFAULT 'epoch',
	ready boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS myce_qualification.myce_changes_v1 (
	sequence bigint PRIMARY KEY,
	handover bigint NOT NULL,
	operation text UNIQUE NOT NULL,
	payload bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS myce_qualification.catchup_changes_v1 (
	run text NOT NULL,
	sequence bigint NOT NULL,
	handover bigint NOT NULL,
	operation text NOT NULL,
	payload bytea NOT NULL,
	PRIMARY KEY (run, sequence)
);
CREATE TABLE IF NOT EXISTS myce_qualification.durability_v1 (
	run text NOT NULL,
	seq bigint NOT NULL,
	sha bytea NOT NULL,
	payload bytea NOT NULL,
	PRIMARY KEY (run, seq)
);
INSERT INTO myce_qualification.myce_coordinator_v1(id, identity)
	VALUES (1, 'queue-qualify') ON CONFLICT DO NOTHING;
`

// Distinct from the stage 2 first-boot lock (679900481).
const (
	ddlLockKey         = 679900491
	commitLockKey      = 679900492
	catchupLockKey     = 679900493
	commitRenewLockKey = 679900494
)

// redactor replaces every connection detail in a message. Driver errors name
// the host, user and database, and some echo the connection string.
type redactor struct{ secrets []string }

var activeRedactor redactor

func newRedactor(raw string, cfg *pgx.ConnConfig) redactor {
	values := map[string]string{raw: "<connection>"}
	if cfg != nil {
		values[cfg.Password] = "<password>"
		values[cfg.User] = "<user>"
		values[cfg.Database] = "<database>"
		values[cfg.Host] = "<host>"
		for _, f := range cfg.Fallbacks {
			values[f.Host] = "<host>"
		}
	}
	r := redactor{}
	for secret, label := range values {
		if strings.TrimSpace(secret) != "" {
			r.secrets = append(r.secrets, secret, label)
		}
	}
	// Longest first, so a host inside the raw string never leaves a fragment.
	pairs := make([][2]string, 0, len(r.secrets)/2)
	for i := 0; i < len(r.secrets); i += 2 {
		pairs = append(pairs, [2]string{r.secrets[i], r.secrets[i+1]})
	}
	sort.Slice(pairs, func(i, j int) bool { return len(pairs[i][0]) > len(pairs[j][0]) })
	r.secrets = r.secrets[:0]
	for _, p := range pairs {
		r.secrets = append(r.secrets, p[0], p[1])
	}
	return r
}

func (r redactor) text(s string) string {
	if len(r.secrets) == 0 {
		return s
	}
	return strings.NewReplacer(r.secrets...).Replace(s)
}

type connFlags struct{ env, file string }

func addConnFlags(fs *flag.FlagSet) *connFlags {
	c := &connFlags{}
	fs.StringVar(&c.env, "dsn-env", "MYCE_QUAL_PG_DSN", "environment variable holding the PostgreSQL connection string (never printed)")
	fs.StringVar(&c.file, "dsn-file", "", "file holding the connection string; overrides --dsn-env")
	return c
}

type target struct {
	cfg   *pgx.ConnConfig
	class string

	// identity is what the first vetted session saw; every later session,
	// including each reconnect, must match it before it writes.
	mu       sync.Mutex
	identity *sessionIdentity
}

// sessionIdentity names the server and database a session reached. A
// hostname or failover address can lead a reconnect somewhere else.
type sessionIdentity struct {
	Database         string
	DatabaseOID      uint32
	SystemIdentifier string
}

func (c *connFlags) load() (*target, error) {
	var raw string
	if c.file != "" {
		info, err := os.Stat(c.file)
		if err != nil {
			return nil, errors.New("cannot read the --dsn-file")
		}
		if info.Mode().Perm()&0o077 != 0 {
			fmt.Fprintln(os.Stderr, "queue-qualify: warning: the --dsn-file is readable by other users; chmod 600 it")
		}
		b, err := os.ReadFile(c.file)
		if err != nil {
			return nil, errors.New("cannot read the --dsn-file")
		}
		raw = strings.TrimSpace(string(b))
	} else {
		raw = strings.TrimSpace(os.Getenv(c.env))
	}
	if raw == "" {
		return nil, fmt.Errorf("no connection string: set the variable named by --dsn-env (%s) or pass --dsn-file", c.env)
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		// pgx's parse error quotes the connection string; never pass it on.
		activeRedactor = newRedactor(raw, nil)
		return nil, errors.New("the connection string does not parse")
	}
	activeRedactor = newRedactor(raw, cfg)
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	cfg.RuntimeParams["application_name"] = "queue-qualify"
	cfg.RuntimeParams["search_path"] = pinnedSearchPath
	return &target{cfg: cfg, class: endpointClass(cfg.Host)}, nil
}

// endpointClass records where the server is without naming it.
func endpointClass(host string) string {
	switch {
	case strings.HasPrefix(host, "/"):
		return "unix-socket"
	case host == "localhost":
		return "loopback"
	}
	ip := net.ParseIP(host)
	switch {
	case ip == nil:
		return "hostname"
	case ip.IsLoopback():
		return "loopback"
	case ip.IsPrivate(), ip.IsLinkLocalUnicast():
		return "private-network"
	default:
		return "public-network"
	}
}

func (t *target) connect(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, t.cfg.Copy())
	if err != nil {
		return nil, fmt.Errorf("connect: %s", activeRedactor.text(err.Error()))
	}
	return conn, nil
}

var (
	// errRefused marks a target the harness must never write to. It is final:
	// a reconnect loop stops on it rather than retrying.
	errRefused = errors.New("refused")
	// errLockBusy means another session holds this run's advisory lock. On a
	// reconnect it can be this run's own dead session the server has not yet
	// noticed, so callers may retry it.
	errLockBusy = errors.New("another queue-qualify run holds this database")
	// errTargetChanged is the refusal for a session that reached a different
	// server or database than the run's first session.
	errTargetChanged = errors.New("target changed")
)

func refused(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errRefused}, args...)...)
}

// open connects and vets the session before anything is written: pinned
// search_path, a dedicated qualification database, no shared-queue tables in
// any schema, durable settings, and the same server and database as the
// first session. With write it also prepares the qualification tables. A
// nonzero lock is the session's advisory lock, taken again on every
// reconnect, because a session lock does not outlive its connection.
func (t *target) open(ctx context.Context, write bool, lock int64) (*pgx.Conn, error) {
	conn, err := t.connect(ctx)
	if err != nil {
		return nil, err
	}
	if err := t.vet(ctx, conn, write, lock); err != nil {
		conn.Close(context.Background())
		return nil, err
	}
	return conn, nil
}

func (t *target) vet(ctx context.Context, conn *pgx.Conn, write bool, lock int64) error {
	var path string
	if err := conn.QueryRow(ctx, "SELECT pg_catalog.set_config('search_path', $1, false)", pinnedSearchPath).Scan(&path); err != nil {
		return err
	}
	if path != pinnedSearchPath {
		return refused("search_path is %q, not %q", path, pinnedSearchPath)
	}
	if err := requireDedicated(ctx, conn); err != nil {
		return err
	}
	if err := refuseQueueTables(ctx, conn); err != nil {
		return err
	}
	if err := requireDurable(ctx, conn); err != nil {
		return err
	}
	id, err := readIdentity(ctx, conn)
	if err != nil {
		return err
	}
	if err := t.sameTarget(id); err != nil {
		return err
	}
	if write {
		if err := prepare(ctx, conn); err != nil {
			return err
		}
	}
	if lock != 0 {
		var locked bool
		if err := conn.QueryRow(ctx, "SELECT pg_catalog.pg_try_advisory_lock($1)", lock).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return errLockBusy
		}
	}
	return nil
}

// sameTarget records the first identity and refuses any other later.
func (t *target) sameTarget(id sessionIdentity) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.identity == nil {
		t.identity = &id
		return nil
	}
	if *t.identity != id {
		return fmt.Errorf("%w: %w: this connection reached a different server or database than the run started on (system identifier or database changed); not writing", errRefused, errTargetChanged)
	}
	return nil
}

func readIdentity(ctx context.Context, conn *pgx.Conn) (sessionIdentity, error) {
	var id sessionIdentity
	var oid int64
	if err := conn.QueryRow(ctx, `SELECT d.datname::text, d.oid::pg_catalog.int8 FROM pg_catalog.pg_database d
		WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()`).Scan(&id.Database, &oid); err != nil {
		return id, err
	}
	id.DatabaseOID = uint32(oid)
	id.SystemIdentifier = readSystemIdentifier(ctx, conn)
	return id, nil
}

// readSystemIdentifier returns "" when the role may not read pg_control.
func readSystemIdentifier(ctx context.Context, conn *pgx.Conn) string {
	var id string
	if err := conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_catalog.pg_control_system()").Scan(&id); err != nil {
		return ""
	}
	return id
}

// requireDedicated refuses a database not named for qualification.
func requireDedicated(ctx context.Context, conn *pgx.Conn) error {
	var name string
	if err := conn.QueryRow(ctx, "SELECT pg_catalog.current_database()::text").Scan(&name); err != nil {
		return err
	}
	if !strings.HasPrefix(strings.ToLower(name), qualDatabasePrefix) {
		return refused("the connected database is not a dedicated qualification database: its name must start with %q, as in the plan's compose sketch", qualDatabasePrefix)
	}
	return nil
}

// refuseQueueTables refuses a database holding any myce_* relation outside
// the harness's schema, in any case. Stage 2 creates its tables unqualified,
// so they land in whatever schema leads the search_path, and a quoted name
// can be upper case. pg_class lists relations in every schema whatever the
// role's privileges.
func refuseQueueTables(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `SELECT n.nspname::text OPERATOR(pg_catalog.||) '.' OPERATOR(pg_catalog.||) c.relname::text
		FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
		WHERE pg_catalog.starts_with(pg_catalog.lower(c.relname::text), 'myce_')
		  AND n.nspname OPERATOR(pg_catalog.<>) $1
		ORDER BY 1 LIMIT 5`, qualSchema)
	if err != nil {
		return err
	}
	found, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}
	if len(found) > 0 {
		return refused("this database holds shared queue tables (%s); qualify on a dedicated database", strings.Join(found, ", "))
	}
	if exists, ours, err := qualSchemaState(ctx, conn); err != nil {
		return err
	} else if exists && !ours {
		return refused("schema %s exists but queue-qualify did not create it; qualify on a dedicated database", qualSchema)
	}
	return nil
}

func requireDurable(ctx context.Context, conn *pgx.Conn) error {
	var durable bool
	if err := conn.QueryRow(ctx, `SELECT pg_catalog.current_setting('fsync')::pg_catalog.bool
		AND pg_catalog.current_setting('full_page_writes')::pg_catalog.bool`).Scan(&durable); err != nil {
		return err
	}
	if !durable {
		return refused("fsync and full_page_writes must be on; measurements without them say nothing about durable commits")
	}
	return nil
}

// qualSchemaState reports whether the qualification schema exists and
// whether this tool created it.
func qualSchemaState(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (exists, ours bool, err error) {
	var comment *string
	err = q.QueryRow(ctx, `SELECT pg_catalog.obj_description(n.oid, 'pg_namespace') FROM pg_catalog.pg_namespace n
		WHERE n.nspname OPERATOR(pg_catalog.=) $1`, qualSchema).Scan(&comment)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return true, comment != nil && *comment == qualSchemaComment, nil
}

// prepare creates the qualification tables. open has already vetted the
// session; the schema check is repeated under the DDL lock.
func prepare(ctx context.Context, conn *pgx.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT pg_catalog.pg_advisory_xact_lock($1)", ddlLockKey); err != nil {
		return err
	}
	exists, ours, err := qualSchemaState(ctx, tx)
	switch {
	case err != nil:
		return err
	case exists && !ours:
		return refused("schema %s exists but queue-qualify did not create it", qualSchema)
	case !exists:
		if _, err := tx.Exec(ctx, "CREATE SCHEMA "+qualSchema+"; COMMENT ON SCHEMA "+qualSchema+" IS '"+qualSchemaComment+"'"); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, qualDDL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type serverInfo struct {
	Version          string            `json:"version"`
	VersionNum       int               `json:"version_num"`
	Settings         map[string]string `json:"settings"`
	SystemIdentifier string            `json:"system_identifier,omitempty"`
	Timeline         int               `json:"timeline,omitempty"`
	StartedAt        time.Time         `json:"started_at"`
	ClockOffsetMs    float64           `json:"clock_offset_ms"`
	InRecovery       bool              `json:"in_recovery"`
	Superuser        bool              `json:"superuser"`
	EndpointClass    string            `json:"endpoint_class"`
	RoundTrip        latencyStats      `json:"round_trip"`
}

var infoSettings = []string{
	"fsync", "full_page_writes", "synchronous_commit", "wal_sync_method",
	"data_checksums", "shared_buffers", "max_wal_size", "checkpoint_timeout",
	"wal_level", "max_connections", "commit_delay", "wal_compression",
	"lc_collate", "server_encoding",
}

func readServerInfo(ctx context.Context, conn *pgx.Conn, class string, pings int) (*serverInfo, error) {
	s := &serverInfo{Settings: map[string]string{}, EndpointClass: class}
	before := time.Now()
	var server time.Time
	err := conn.QueryRow(ctx, `SELECT pg_catalog.version(), pg_catalog.current_setting('server_version_num')::pg_catalog.int4,
		pg_catalog.pg_postmaster_start_time(), pg_catalog.clock_timestamp(), pg_catalog.pg_is_in_recovery(),
		(SELECT r.rolsuper FROM pg_catalog.pg_roles r WHERE r.rolname OPERATOR(pg_catalog.=) current_user)`).
		Scan(&s.Version, &s.VersionNum, &s.StartedAt, &server, &s.InRecovery, &s.Superuser)
	if err != nil {
		return nil, err
	}
	mid := before.Add(time.Since(before) / 2)
	s.ClockOffsetMs = ms(server.Sub(mid))
	rows, err := conn.Query(ctx, "SELECT s.name, pg_catalog.current_setting(s.name) FROM pg_catalog.pg_settings s WHERE s.name OPERATOR(pg_catalog.=) ANY($1)", infoSettings)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return nil, err
		}
		s.Settings[k] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// The identifier changes only when the data directory is recreated, so a
	// match across a restart or OS update shows the same cluster came back.
	// The timeline changes when a backup is restored and promoted. Both may
	// be restricted to privileged roles.
	s.SystemIdentifier = readSystemIdentifier(ctx, conn)
	var timeline int32
	if err := conn.QueryRow(ctx, "SELECT timeline_id FROM pg_catalog.pg_control_checkpoint()").Scan(&timeline); err == nil {
		s.Timeline = int(timeline)
	}
	if pings > 0 {
		var lat []float64
		for i := 0; i < pings; i++ {
			start := time.Now()
			if _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
				return nil, err
			}
			lat = append(lat, ms(time.Since(start)))
		}
		s.RoundTrip = summarize(lat, 0)
	}
	return s, nil
}

func infoChecks(s *serverInfo) []check {
	on := func(name string) bool { return s.Settings[name] == "on" }
	status := func(ok bool, otherwise string) string {
		if ok {
			return statusPass
		}
		return otherwise
	}
	cs := []check{
		{Name: "fsync on", Status: status(on("fsync"), statusFail), Measured: s.Settings["fsync"], Limit: "on",
			Prevents: "acknowledged commits living only in memory and vanishing on a crash"},
		{Name: "full_page_writes on", Status: status(on("full_page_writes"), statusFail), Measured: s.Settings["full_page_writes"], Limit: "on",
			Prevents: "torn pages after a power cut that WAL replay cannot repair"},
		{Name: "synchronous_commit on by default", Status: status(on("synchronous_commit"), statusWarn), Measured: s.Settings["synchronous_commit"], Limit: "on",
			Prevents: "a session that forgets SET LOCAL acknowledging before the WAL flush (the queue sets it per transaction)"},
		{Name: "data_checksums on", Status: status(on("data_checksums"), statusWarn), Measured: s.Settings["data_checksums"], Limit: "on",
			Prevents: "silent page corruption on the NAS volume going unnoticed until a replay"},
		{Name: "commit_delay 0", Status: status(s.Settings["commit_delay"] == "0", statusWarn), Measured: s.Settings["commit_delay"], Limit: "0",
			Prevents: "group-commit waits inflating every commit at the queue's low concurrency"},
		{Name: "PostgreSQL 17", Status: status(s.VersionNum/10000 == 17, statusWarn), Measured: fmt.Sprint(s.VersionNum), Limit: "17xxxx",
			Prevents: "qualifying a major version the stage 2 proof did not run on"},
		{Name: "primary, not a standby", Status: status(!s.InRecovery, statusFail), Measured: fmt.Sprint(s.InRecovery), Limit: "false",
			Prevents: "measuring a read-only replica that cannot hold the turn"},
	}
	return cs
}

func cmdInfo(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("info", out)
	cf := addConnFlags(fs)
	o := addOutputFlags(fs)
	pings := fs.Int("pings", 200, "SELECT 1 round trips to time")
	if err := fs.Parse(args); err != nil {
		return err
	}
	t, err := cf.load()
	if err != nil {
		return err
	}
	conn, err := t.open(ctx, false, 0)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	s, err := readServerInfo(ctx, conn, t.class, *pings)
	if err != nil {
		return err
	}
	r := newReport("info", o)
	r.Server = s
	r.Checks = infoChecks(s)
	return r.finish(out)
}
