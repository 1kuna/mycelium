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
	"time"

	"github.com/jackc/pgx/v5"
)

// The qualification tables live in their own schema. The coordinator and
// change tables copy the stage 2 shapes (myce_coordinator_v1 and
// myce_changes_v1 in internal/myce/shared_queue_postgres.go on the stage 2
// branch at e5d9287a) so the measured writes match the real ones.
const qualSchema = "myce_qualification"

const qualDDL = `
CREATE SCHEMA IF NOT EXISTS myce_qualification;
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
	sequence bigint PRIMARY KEY,
	handover bigint NOT NULL,
	operation text UNIQUE NOT NULL,
	payload bytea NOT NULL
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
	ddlLockKey     = 679900491
	commitLockKey  = 679900492
	catchupLockKey = 679900493
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

// prepare refuses a database that holds a real shared-queue authority or
// lacks durable writes, then creates the qualification tables.
func prepare(ctx context.Context, conn *pgx.Conn) error {
	var live bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass('public.myce_coordinator_v1') IS NOT NULL").Scan(&live); err != nil {
		return err
	}
	if live {
		return errors.New("this database holds a shared queue authority (public.myce_coordinator_v1); qualify on a dedicated database")
	}
	var durable bool
	if err := conn.QueryRow(ctx, "SELECT current_setting('fsync')::boolean AND current_setting('full_page_writes')::boolean").Scan(&durable); err != nil {
		return err
	}
	if !durable {
		return errors.New("fsync and full_page_writes must be on; measurements without them say nothing about durable commits")
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", ddlLockKey); err != nil {
		return err
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
	err := conn.QueryRow(ctx, `SELECT version(), current_setting('server_version_num')::int,
		pg_postmaster_start_time(), clock_timestamp(), pg_is_in_recovery(),
		(SELECT rolsuper FROM pg_roles WHERE rolname = current_user)`).
		Scan(&s.Version, &s.VersionNum, &s.StartedAt, &server, &s.InRecovery, &s.Superuser)
	if err != nil {
		return nil, err
	}
	mid := before.Add(time.Since(before) / 2)
	s.ClockOffsetMs = ms(server.Sub(mid))
	rows, err := conn.Query(ctx, "SELECT name, current_setting(name) FROM pg_settings WHERE name = ANY($1)", infoSettings)
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
	// match across a restart or OS update proves the same cluster came back.
	// pg_control_system may be restricted to privileged roles.
	var id string
	if err := conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&id); err == nil {
		s.SystemIdentifier = id
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
	conn, err := t.connect(ctx)
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

func cmdCleanup(ctx context.Context, args []string, out io.Writer) error {
	fs := newFlags("cleanup", out)
	cf := addConnFlags(fs)
	loadDir := fs.String("load-dir", "", "also remove leftover load directories (marked myce-qual-load-*) under this directory")
	skipDB := fs.Bool("skip-db", false, "do not connect; only remove load directories")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*skipDB {
		t, err := cf.load()
		if err != nil {
			return err
		}
		conn, err := t.connect(ctx)
		if err != nil {
			return err
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+qualSchema+" CASCADE"); err != nil {
			return err
		}
		fmt.Fprintln(out, "dropped schema", qualSchema)
	}
	if *loadDir != "" {
		removed, err := removeLoadLeftovers(*loadDir)
		for _, r := range removed {
			fmt.Fprintln(out, "removed", r)
		}
		return err
	}
	return nil
}
