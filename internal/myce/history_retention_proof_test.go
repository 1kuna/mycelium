package myce

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt-in proof on a real myce dev database: MYCE_JOB_EXPIRY_PROOF=<jobs>.
// It fills a dev queue through the real API and command runner, copies the
// database, opens the copy as an upgraded queue, expires the records and
// restarts it, and logs the bytes before and after.
func TestDevJobRecordExpiryProof(t *testing.T) {
	count, _ := strconv.Atoi(os.Getenv("MYCE_JOB_EXPIRY_PROOF"))
	if count <= 0 {
		t.Skip("MYCE_JOB_EXPIRY_PROOF unset")
	}
	root := t.TempDir()
	if err := PrepareStateRoots(root); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	start := func(db string) *Dev {
		t.Helper()
		d, err := StartDev(DevConfig{EnvironmentCacheLimit: 1 << 30, Database: db, Checkpoints: filepath.Join(root, "checkpoints"), Address: "127.0.0.1:0", Token: "proof", Workers: 4, MemoryGB: 32})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	original := filepath.Join(root, "queue.db")
	d := start(original)
	c := d.Client()
	ids := make([]string, 0, count)
	for i := range count {
		s := Spec{Name: fmt.Sprintf("proof-%d", i%12), Project: fmt.Sprintf("project-%d", i%3), Priority: "experiment"}
		s.Run.Command = []string{"echo", strconv.Itoa(i)}
		s.Needs.Platform = Platform{Required: []string{"cpu"}}
		s.Needs.MemoryGB = 1
		if i%5 == 0 {
			s.SubmissionKey = fmt.Sprintf("key-%d", i)
		}
		var j Job
		if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	settled := func(q *Queue) (done, open int) {
		q.mu.RLock()
		defer q.mu.RUnlock()
		for _, j := range q.s.Jobs {
			if terminal(j.State) && !mutableJob(j) {
				done++
			} else {
				open++
			}
		}
		return
	}
	for {
		if done, _ := settled(d.Queue); done == count {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("jobs did not settle")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	copied := filepath.Join(root, "copy.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := copyProofFile(original+suffix, copied+suffix); err != nil {
			t.Fatal(err)
		}
	}
	// The copy stands for a database written before this change.
	if err := proofExec(copied, "DROP TABLE job_summaries"); err != nil {
		t.Fatal(err)
	}
	before := measureProof(t, copied)

	d = start(copied)
	c = d.Client()
	advance := func(seconds float64) {
		t.Helper()
		if err := c.Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": seconds}, nil); err != nil {
			t.Fatal(err)
		}
	}
	advance(jobRecordRetention.Seconds() + 3600)
	passes := 1
	for {
		if done, _ := settled(d.Queue); done == 0 {
			break
		}
		if passes > 2*count+20 {
			q := d.Queue
			q.mu.RLock()
			for id, j := range q.s.Jobs {
				t.Logf("%s state=%s fin=%v files=%v/%v logs=%v ck=%+v/%v deps=%d sched=%v now=%v", id, j.State, j.FinishedAt, hasJobFiles(j), j.FilesExpired, q.records.logs.scheduled[id], q.records.checkpointExpiry[id], q.records.checkpointUndated[id], q.records.dependents[id], q.records.history.scheduled[id], q.clock.Now())
				break
			}
			q.mu.RUnlock()
			t.Fatal("expiry did not finish")
		}
		// Workers remove settled checkpoint folders on their check-ins; a
		// record waits for that, then is looked at again after the recheck.
		time.Sleep(time.Second)
		advance(jobExpiryRecheck.Seconds())
		passes++
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	after := measureProof(t, copied)

	d = start(copied) // Restart on the expired database.
	defer d.Close()
	c = d.Client()
	if done, open := settled(d.Queue); done != 0 || open != 0 {
		t.Fatalf("restart kept %d settled and %d open records", done, open)
	}
	var j Job
	err := c.Call(ctx, "GET", "/api/jobs/"+ids[1], nil, &j)
	var gone *HTTPError
	if !errors.As(err, &gone) || gone.Code != "job_expired" {
		t.Fatalf("expired lookup: %v", err)
	}
	s := Spec{Name: "proof-0", Project: "project-0", Priority: "experiment", SubmissionKey: "key-0"}
	s.Run.Command = []string{"echo", "0"}
	s.Needs.Platform = Platform{Required: []string{"cpu"}}
	s.Needs.MemoryGB = 1
	if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil || j.ID != ids[0] || j.State != "succeeded" {
		t.Fatalf("keyed replay after expiry: %+v %v", j, err)
	}
	if after.summaries != count {
		t.Fatalf("%d summaries for %d jobs", after.summaries, count)
	}
	t.Logf("%d jobs, %d expiry passes", count, passes)
	t.Logf("before: job records %d B, keyed answers %d B, queue_state %d B, summaries %d B, vacuumed file %d B", before.jobs, before.submissions, before.state, before.summaryBytes, before.file)
	t.Logf("after:  job records %d B, keyed answers %d B, queue_state %d B, summaries %d B, vacuumed file %d B", after.jobs, after.submissions, after.state, after.summaryBytes, after.file)
}

type proofBytes struct {
	jobs, submissions, state, summaryBytes, file int64
	summaries                                    int
}

func measureProof(t *testing.T, path string) proofBytes {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var out proofBytes
	rows, err := db.Query("SELECT key, length(data) FROM queue_state")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var key string
		var n int64
		if err := rows.Scan(&key, &n); err != nil {
			t.Fatal(err)
		}
		out.state += n
		if record, ok := strings.CutPrefix(key, "fields/"); ok {
			if name, _, ok := strings.Cut(record, "/"); ok {
				if decoded, err := decodeFieldKey(name); err == nil {
					key = decoded
				}
			}
		}
		switch {
		case strings.HasPrefix(key, "jobs/"):
			out.jobs += n
		case strings.HasPrefix(key, "submissions/"):
			out.submissions += n
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT count(*), coalesce(sum(length(summary)+coalesce(length(learned),0)),0) FROM job_summaries").Scan(&out.summaries, &out.summaryBytes); err != nil && !strings.Contains(err.Error(), "no such table") {
		t.Fatal(err)
	}
	vacuumed := path + ".vacuum"
	_ = os.Remove(vacuumed)
	if _, err := db.Exec("VACUUM INTO ?", vacuumed); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(vacuumed)
	if err != nil {
		t.Fatal(err)
	}
	out.file = info.Size()
	_ = os.Remove(vacuumed)
	return out
}

func decodeFieldKey(name string) (string, error) {
	data, err := base64.RawURLEncoding.DecodeString(name)
	return string(data), err
}

func proofExec(path, statement string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(statement)
	return err
}

func copyProofFile(from, to string) error {
	in, err := os.Open(from)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
