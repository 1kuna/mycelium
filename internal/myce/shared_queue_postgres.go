package myce

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

var (
	errSharedTurn               = errors.New("shared queue coordinator turn is unavailable")
	errSharedHead               = errors.New("shared queue copy does not match the committed sequence")
	errSharedPostgres           = errors.New("shared queue PostgreSQL authority could not be opened")
	errSharedPostgresDurability = errors.New("PostgreSQL must enable fsync and full_page_writes for the shared queue")
)

type sharedQueueTurn struct {
	owner, endpoint string
	number, head    int64
	expires         time.Time
	ready           bool
}

type sharedQueueAuthority struct {
	db       *sql.DB
	identity string
	// A fixture can discard the response after PostgreSQL acknowledges COMMIT.
	commit func(*sql.Tx) error
}

const sharedQueuePostgresSchema = `
CREATE TABLE IF NOT EXISTS myce_coordinator_v1 (
	id integer PRIMARY KEY CHECK (id = 1),
	identity text NOT NULL,
	owner text NOT NULL DEFAULT '',
	endpoint text NOT NULL DEFAULT '',
	handover bigint NOT NULL DEFAULT 0,
	head bigint NOT NULL DEFAULT 0,
	expires timestamptz NOT NULL DEFAULT 'epoch',
	ready boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS myce_changes_v1 (
	sequence bigint PRIMARY KEY,
	handover bigint NOT NULL,
	operation text UNIQUE NOT NULL,
	payload bytea NOT NULL
);
`

// openSharedQueueAuthority connects to the PostgreSQL authority. The DSN
// names the server, port, database and user; it may be local or on the LAN.
// The password comes only from passwordFile, a private regular file, so it is
// never in argv, the environment or a log. Without an sslmode in the DSN, pgx
// uses TLS when the server offers it and plaintext otherwise ("prefer").
func openSharedQueueAuthority(ctx context.Context, dsn, passwordFile string) (*sharedQueueAuthority, error) {
	if sharedQueueDSNHasPassword(dsn) {
		return nil, errSharedPostgresDSNPassword
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection settings")
	}
	if passwordFile != "" {
		if cfg.Password, err = readSharedQueuePassword(passwordFile); err != nil {
			return nil, err
		}
	}
	db := stdlib.OpenDB(*cfg)
	a := &sharedQueueAuthority{db: db, commit: (*sql.Tx).Commit}
	if err := a.initialize(ctx); err != nil {
		db.Close()
		if errors.Is(err, errSharedPostgresDurability) {
			return nil, errSharedPostgresDurability
		}
		// pgx errors can contain connection settings or server-supplied
		// text. This is also the boundary used by the CLI startup logger.
		return nil, errSharedPostgres
	}
	return a, nil
}

var errSharedPostgresDSNPassword = errors.New("put the PostgreSQL password in the password file, not the connection settings")

// sharedQueueDSNHasPassword refuses a password in either DSN form, so a
// secret never reaches a command line or saved configuration.
func sharedQueueDSNHasPassword(dsn string) bool {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return true // Refuse what cannot be inspected.
		}
		_, set := u.User.Password()
		return set || u.Query().Has("password")
	}
	return sharedDSNPasswordKey.MatchString(dsn)
}

var sharedDSNPasswordKey = regexp.MustCompile(`(^|\s)password\s*=`)

func readSharedQueuePassword(path string) (string, error) {
	private := errors.New("the PostgreSQL password file must be a private regular file (chmod 600) holding one line")
	info, err := os.Lstat(path)
	if err != nil {
		return "", errors.New("cannot read the PostgreSQL password file")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", private
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("cannot read the PostgreSQL password file")
	}
	password := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if password == "" || strings.ContainsAny(password, "\r\n\x00") {
		return "", private
	}
	return password, nil
}

func (a *sharedQueueAuthority) initialize(ctx context.Context) error {
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var durable bool
	if err := tx.QueryRowContext(ctx, "SELECT current_setting('fsync')::boolean AND current_setting('full_page_writes')::boolean").Scan(&durable); err != nil {
		return err
	}
	if !durable {
		return errSharedPostgresDurability
	}
	// Serialize first boot even when the coordinator table does not exist.
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(679900481)"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, sharedQueuePostgresSchema); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO myce_coordinator_v1(id,identity) VALUES(1,$1) ON CONFLICT DO NOTHING", ID()); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, "SELECT identity FROM myce_coordinator_v1 WHERE id=1").Scan(&a.identity); err != nil {
		return err
	}
	return tx.Commit()
}

func (a *sharedQueueAuthority) begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

const sharedTurnColumns = "owner,endpoint,handover,head,expires,ready"

func scanSharedTurn(row *sql.Row) (sharedQueueTurn, error) {
	var t sharedQueueTurn
	err := row.Scan(&t.owner, &t.endpoint, &t.number, &t.head, &t.expires, &t.ready)
	return t, err
}

func (a *sharedQueueAuthority) current(ctx context.Context) (sharedQueueTurn, error) {
	return scanSharedTurn(a.db.QueryRowContext(ctx, `SELECT owner,endpoint,handover,head,expires,
		ready AND expires > clock_timestamp() FROM myce_coordinator_v1 WHERE id=1`))
}

func lockedSharedTurn(ctx context.Context, tx *sql.Tx) (sharedQueueTurn, time.Time, error) {
	t, err := scanSharedTurn(tx.QueryRowContext(ctx, "SELECT "+sharedTurnColumns+" FROM myce_coordinator_v1 WHERE id=1 FOR UPDATE"))
	if err != nil {
		return sharedQueueTurn{}, time.Time{}, err
	}
	// The owner might have expired while this transaction was waiting for the lock.
	var now time.Time
	err = tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now)
	return t, now, err
}

// acquire takes the next turn once the current one has expired. A member may
// also replace an unexpired turn it owns: it calls acquire only while it
// serves no turn, so its own turn there is left by an acquisition or release
// whose reply was lost. The new number fences any writer of the old one.
func (a *sharedQueueAuthority) acquire(ctx context.Context, owner, endpoint string, lease time.Duration) (sharedQueueTurn, error) {
	tx, err := a.begin(ctx)
	if err != nil {
		return sharedQueueTurn{}, err
	}
	defer tx.Rollback()
	t, now, err := lockedSharedTurn(ctx, tx)
	if err != nil {
		return sharedQueueTurn{}, err
	}
	if t.expires.After(now) && t.owner != owner {
		return sharedQueueTurn{}, errSharedTurn
	}
	t, err = scanSharedTurn(tx.QueryRowContext(ctx, `UPDATE myce_coordinator_v1
		SET owner=$1,endpoint=$2,handover=handover+1,expires=$3,ready=false
		WHERE id=1 RETURNING `+sharedTurnColumns, owner, endpoint, now.Add(lease)))
	if err != nil {
		return sharedQueueTurn{}, err
	}
	return t, tx.Commit()
}

func sameSharedTurn(a, b sharedQueueTurn) bool {
	return a.owner == b.owner && a.number == b.number
}

// renew marks the turn routable only after the member has replayed the
// handover point and opened the queue. Release also works after expiry, but
// only if the turn has not already changed hands.
func (a *sharedQueueAuthority) renew(ctx context.Context, want sharedQueueTurn, lease time.Duration, ready bool) error {
	tx, err := a.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	t, now, err := lockedSharedTurn(ctx, tx)
	if err != nil {
		return err
	}
	if !sameSharedTurn(t, want) || lease > 0 && !t.expires.After(now) {
		return errSharedTurn
	}
	if _, err := tx.ExecContext(ctx, `UPDATE myce_coordinator_v1 SET expires=$1,ready=$2 WHERE id=1`, now.Add(lease), ready); err != nil {
		return err
	}
	return tx.Commit()
}

type sharedQueueChange struct {
	sequence  int64
	handover  int64
	operation string
	payload   []byte
}

func (a *sharedQueueAuthority) append(ctx context.Context, want sharedQueueTurn, change sharedQueueChange) (uncertain bool, err error) {
	tx, err := a.begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	t, now, err := lockedSharedTurn(ctx, tx)
	if err != nil {
		return false, err
	}
	if !sameSharedTurn(t, want) || !t.expires.After(now) {
		return false, errSharedTurn
	}
	if t.head != change.sequence-1 || change.handover != t.number {
		return false, errSharedHead
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO myce_changes_v1(sequence,handover,operation,payload) VALUES($1,$2,$3,$4)`,
		change.sequence, change.handover, change.operation, change.payload); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE myce_coordinator_v1 SET head=$1 WHERE id=1", change.sequence); err != nil {
		return false, err
	}
	err = a.commit(tx)
	return err != nil, err
}

func (a *sharedQueueAuthority) lookup(ctx context.Context, operation string) (sequence, handover int64, err error) {
	err = a.db.QueryRowContext(ctx, `SELECT sequence,handover FROM myce_changes_v1 WHERE operation=$1`, operation).Scan(&sequence, &handover)
	return
}

func (a *sharedQueueAuthority) changes(ctx context.Context, after, through int64, apply func(sharedQueueChange) error) error {
	rows, err := a.db.QueryContext(ctx, `SELECT sequence,handover,operation,payload FROM myce_changes_v1
		WHERE sequence > $1 AND sequence <= $2 ORDER BY sequence`, after, through)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var c sharedQueueChange
		if err := rows.Scan(&c.sequence, &c.handover, &c.operation, &c.payload); err != nil {
			return err
		}
		if c.sequence != after+1 {
			return errSharedHead
		}
		if err := apply(c); err != nil {
			return err
		}
		after = c.sequence
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if after != through {
		return errSharedHead
	}
	return nil
}
