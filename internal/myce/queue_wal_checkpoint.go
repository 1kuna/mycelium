package myce

import (
	"context"
	"database/sql"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// A FULL commit still syncs the WAL before publishing the journal. Copying WAL
// frames back to the database has no queue state to protect and can run on a
// separate connection, without keeping either queue lock through that I/O.
type queueWALCheckpoint struct {
	pages atomic.Uint64
	done  chan struct{}
}

const queueWALCheckpointFrames = 1000 // SQLite's default foreground threshold.

func queueCheckpointDSNs(path string) (writer, checkpoint string, err error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	u := url.URL{Scheme: "file", Path: abs}
	u.RawQuery = "mode=rw&_pragma=busy_timeout(0)&_pragma=synchronous(FULL)"
	checkpoint = u.String()
	// The setting belongs to each SQLite connection. Apply it on every open,
	// including a pool reconnection or a connection retained after startup.
	u.RawQuery = "mode=rwc&_pragma=wal_autocheckpoint(0)&_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)"
	return u.String(), checkpoint, nil
}

func (c *queueWALCheckpoint) start(ctx context.Context, dsn, walPath string, pageBytes int) {
	c.done = make(chan struct{})
	c.pages.Add(queueWALCheckpointFrames) // Include frames recovered on open.
	go func() {
		defer close(c.done)
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			log.Print("event=queue_wal_checkpoint outcome=open_failed")
			return
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		const walHeader, frameHeader = 32, 24
		minBytes := int64(walHeader + queueWALCheckpointFrames*(pageBytes+frameHeader))
		var checkpointedSize int64
		var pending bool
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if ctx.Err() != nil {
					return
				}
				work := c.pages.Swap(0)
				if work == 0 && !pending {
					continue
				}
				info, err := os.Stat(walPath)
				if err != nil {
					c.pages.Add(work)
					continue
				}
				size := info.Size()
				if size < minBytes {
					checkpointedSize, pending = 0, false
					continue
				}
				// A reused WAL can retain its allocated size. Size growth and
				// unfinished passes trigger independently of the write estimate.
				if pending || size > checkpointedSize || work >= queueWALCheckpointFrames {
					checkpointedSize = size
					pending = checkpointQueueWAL(ctx, db)
				} else {
					c.pages.Add(work)
				}
			}
		}
	}()
}

// PASSIVE does not wait for writers or pinned readers. A short or busy pass
// leaves the remaining frames durable in the WAL for the next pass.
func checkpointQueueWAL(ctx context.Context, db *sql.DB) (pending bool) {
	started := holdNow(nil)
	var busy, frames, copied int
	err := db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &frames, &copied)
	duration := holdNow(nil).Sub(started)
	budget := holdTelemetry("queue_wal_checkpoint")
	if ctx.Err() == nil && (err != nil || budget.admit(duration)) {
		log.Printf("event=queue_wal_checkpoint duration_ms=%.3f busy=%d wal_frames=%d checkpointed_frames=%d failed=%t", holdMS(duration), busy, frames, copied, err != nil)
	}
	// SQLite returns -1/-1 when no WAL has been opened. Another writer can
	// have been active when this connection tried to open the database.
	return err != nil || busy != 0 || frames < 0 || copied < frames
}

// BeginShutdown cancels the checkpoint lane. Join it before taking the queue
// writer turn and before releasing the queue database's exclusive file lock.
func (c *queueWALCheckpoint) wait() {
	if c.done != nil {
		<-c.done
	}
}
