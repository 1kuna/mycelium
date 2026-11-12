package myce

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// A retained set produced by real queue paths: several programs, failures,
// out-of-memory retries, storage and memory evidence, outputs and ledgers.
func retainedJobRecordFixture(t *testing.T, jobs int) (path string, files string, clock *fakeClock) {
	t.Helper()
	q, clock, m, s, out := storageFixture(t)
	q.slimJobRecords = false // Rows as a release before the writer wrote them.
	for i := 0; i < jobs; i++ {
		spec := s
		spec.Name = fmt.Sprintf("storage-profile-%d", i%5)
		j := startObservation(t, q, m, spec)
		clock.Advance(time.Duration(3+i%11) * time.Second)
		r := storageResult(&j, out, uint64(1000+i*37))
		switch i % 7 {
		case 3:
			r.Success = false
		case 5:
			r.Success, r.OutOfMemory = false, true
		}
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if live := q.s.Jobs[j.ID]; live != nil && !terminal(live.State) {
			again := check(t, q, m, nil)
			if again.Job == nil || again.Job.ID != j.ID {
				t.Fatalf("retry not assigned: %s %s", live.State, live.Reason)
			}
			if err := q.Started(Result{Machine: again.Job.Machine, Session: again.Job.Session, Execution: Execution{ID: j.ID, Attempt: again.Job.Attempt}}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(4 * time.Second)
			if err := q.Complete(storageResult(again.Job, out, uint64(2000+i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	path, files = strings.TrimSuffix(q.lock.Name(), ".lock"), q.files.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	return path, files, clock
}

func copyQueueDatabase(t *testing.T, path string) string {
	t.Helper()
	dir := t.TempDir()
	if err := PrepareStateRoots(dir); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "queue.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		data, err := os.ReadFile(path + suffix)
		if os.IsNotExist(err) && suffix != "" {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(target+suffix, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return target
}

func openRetainedJobRecords(t *testing.T, path, files string, clock *fakeClock, slim bool) *Queue {
	t.Helper()
	q, err := Open(path, t.TempDir(), clock, func(q *Queue) { q.slimJobRecords = slim })
	if err != nil {
		t.Fatal(err)
	}
	if err = q.ConfigureFileStore(files, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	return q
}

func retainedJobRows(t *testing.T, q *Queue) (rows map[string][]byte, bytes int) {
	t.Helper()
	records, err := readQueueRecords(q.db)
	if err != nil {
		t.Fatal(err)
	}
	rows = map[string][]byte{}
	for key, data := range records {
		if strings.HasPrefix(key, "jobs/") {
			rows[key] = data
		}
	}
	err = q.db.QueryRow(`SELECT COALESCE(SUM(LENGTH(data)),0) FROM queue_state WHERE key GLOB 'jobs/*' OR key GLOB ?`, "fields/"+strings.TrimSuffix(fieldPrefix("jobs/")[len("fields/"):], "/")+"*").Scan(&bytes)
	if err != nil {
		t.Fatal(err)
	}
	return rows, bytes
}

// Every reader consumers use, as raw wire bytes.
func jobRecordReaders(t *testing.T, q *Queue) map[string][]byte {
	t.Helper()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	get := func(method, route, body string) []byte {
		req, err := http.NewRequest(method, server.URL+route, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer test")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("%s %s: %d %s %v", method, route, res.StatusCode, data, err)
		}
		return data
	}
	out := map[string][]byte{
		"status full":   get("GET", "/api/status?detail=full", ""),
		"status recent": get("GET", "/api/status?jobs=recent", ""),
	}
	ids := []string{}
	q.mu.Lock()
	for id := range q.s.Jobs {
		ids = append(ids, id)
	}
	q.mu.Unlock()
	sort.Strings(ids)
	for _, id := range ids {
		out["job "+id] = get("GET", "/api/jobs/"+id, "")
		out["ledger "+id] = get("GET", "/api/jobs/"+id+"/ledger", "")
	}
	out["wait"] = get("POST", "/api/wait", `{"ids":["`+strings.Join(ids, `","`)+`"],"seconds":0}`)
	return out
}

func TestJobRecordSharedValuesKeepReadersAndRollback(t *testing.T) {
	path, files, clock := retainedJobRecordFixture(t, 28)

	ordinary := openRetainedJobRecords(t, copyQueueDatabase(t, path), files, clock, false)
	before, beforeBytes := retainedJobRows(t, ordinary)
	want := jobRecordReaders(t, ordinary)
	ordinary.Close()
	if len(before) != 28 {
		t.Fatalf("fixture retained %d jobs", len(before))
	}
	for key, data := range before {
		if bytes.HasPrefix(data, jobRecordRefsPrefix) {
			t.Fatal("writer is off, yet a shared row was written", key)
		}
		back, err := expandJobRecord(slimJobRecord(data))
		if err != nil || !bytes.Equal(back, data) {
			t.Fatalf("%s did not round-trip: %v", key, err)
		}
	}

	// Open converts every retained record; readers see identical bytes.
	converted := copyQueueDatabase(t, path)
	q := openRetainedJobRecords(t, converted, files, clock, true)
	after, afterBytes := retainedJobRows(t, q)
	for key, data := range after {
		if !bytes.HasPrefix(data, jobRecordRefsPrefix) {
			t.Fatal("open left an ordinary retained record", key)
		}
	}
	compareJobRecordReaders(t, want, jobRecordReaders(t, q))
	t.Logf("retained job bytes: ordinary %d, shared %d (%.1f%% smaller)", beforeBytes, afterBytes, 100*float64(beforeBytes-afterBytes)/float64(beforeBytes))
	if afterBytes*10 > beforeBytes*8 {
		t.Fatalf("expected at least 20%% smaller: %d -> %d", beforeBytes, afterBytes)
	}
	q.Close()

	// Rollback to a reader-only release: shared rows open, read the same and
	// stay as written until a job changes.
	q = openRetainedJobRecords(t, converted, files, clock, false)
	defer q.Close()
	compareJobRecordReaders(t, want, jobRecordReaders(t, q))
	again, _ := retainedJobRows(t, q)
	changed := 0
	for key, data := range after {
		if !bytes.Equal(again[key], data) {
			changed++
			t.Log(key, string(again[key][:min(200, len(again[key]))]))
		}
	}
	if changed > 0 {
		t.Fatal("reader-only open rewrote shared records:", changed)
	}
}

func compareJobRecordReaders(t *testing.T, want, got map[string][]byte) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("reader count %d != %d", len(got), len(want))
	}
	// SQLite file sizes are physical readings of the database, and a wait
	// revision is this process's change counter; neither is a job value.
	process := regexp.MustCompile(`"control":\{[^}]*\}|"revision":\d+`)
	for name, data := range want {
		data, other := process.ReplaceAll(data, nil), process.ReplaceAll(got[name], nil)
		got[name] = other
		if !bytes.Equal(data, got[name]) {
			at := 0
			for at < min(len(data), len(got[name])) && data[at] == got[name][at] {
				at++
			}
			from := max(0, at-300)
			t.Fatalf("%s changed at byte %d:\nwant %s\ngot  %s", name, at, data[from:min(len(data), at+300)], got[name][from:min(len(got[name]), at+300)])
		}
	}
}

func TestJobRecordSharedValuesEncoding(t *testing.T) {
	for _, data := range []string{
		`{"a":"0123456789abcdef0123","b":"0123456789abcdef0123","c":"0123456789abcdef0123"}`,
		`{"x":{"k":"0123456789abcdef0123","v":[1,2,3]},"y":[{"k":"0123456789abcdef0123","v":[1,2,3]}],"z":"0123456789abcdef0123"}`,
		`{"s":"quote \" and {\"$ref\":0} text, long enough","t":"quote \" and {\"$ref\":0} text, long enough"}`,
		`{"m":{"$ref":1},"n":"0123456789abcdef0123","o":"0123456789abcdef0123"}`,
		`{"short":"ab","other":"ab"}`,
	} {
		slim := slimJobRecord([]byte(data))
		back, err := expandJobRecord(slim)
		if err != nil || string(back) != data {
			t.Fatalf("%s -> %s -> %s (%v)", data, slim, back, err)
		}
		if strings.Contains(data, `"$ref`) && string(slim) != data {
			t.Fatal("a record containing the marker must stay ordinary", data)
		}
	}
	if got := string(slimJobRecord([]byte(`{"a":"0123456789abcdef0123","b":"0123456789abcdef0123","c":"0123456789abcdef0123"}`))); got != `{"$refs":["0123456789abcdef0123"],"a":{"$ref":0},"b":{"$ref":0},"c":{"$ref":0}}` {
		t.Fatal(got)
	}
	for _, bad := range []string{`{"$refs":[],"a":{"$ref":0}}`, `{"$refs":[{"$ref":0}],"a":{"$ref":0}}`, `{"$refs":["x"],"a":{"$ref":}}`} {
		if _, err := expandJobRecord([]byte(bad)); err == nil {
			t.Fatal("accepted a broken reference", bad)
		}
	}
}

// Measures a copy of a real queue database read-only, without opening a queue:
// MYCE_JOB_RECORD_DB=/path/to/copy/queue.db go test -run TestJobRecordSharedValuesMeasure
func TestJobRecordSharedValuesMeasure(t *testing.T) {
	path := os.Getenv("MYCE_JOB_RECORD_DB")
	if path == "" {
		t.Skip("MYCE_JOB_RECORD_DB unset")
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	records, err := readQueueRecords(db)
	if err != nil {
		t.Fatal(err)
	}
	jobs, ordinary, shared := 0, 0, 0
	for key, data := range records {
		if !strings.HasPrefix(key, "jobs/") {
			continue
		}
		full, err := expandJobRecord(data)
		if err != nil {
			t.Fatal(key, err)
		}
		slim := slimJobRecord(full)
		if back, err := expandJobRecord(slim); err != nil || !bytes.Equal(back, full) {
			t.Fatal(key, "did not round-trip", err)
		}
		jobs, ordinary, shared = jobs+1, ordinary+len(full), shared+len(slim)
	}
	t.Logf("%d job records: ordinary %d bytes, shared %d bytes (%.1f%% smaller)", jobs, ordinary, shared, 100*float64(ordinary-shared)/float64(max(1, ordinary)))
}
