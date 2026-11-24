package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The incident-sized fixture exercises real SQLite commits, offset dedup and
// the HTTP decoder. Its holds are observations, not a loaded-host speed claim.
func TestWorkerLogBatchReplay(t *testing.T) {
	for _, predecessor := range []bool{false, true} {
		t.Run(fmt.Sprintf("predecessor=%t", predecessor), func(t *testing.T) {
			q, _ := fixture(t)
			submit(t, q, spec("experiment"))
			j := check(t, q, machine("a", "fake"), nil).Job
			w := recoveryWorker(t)
			w.Machine.ID = j.Machine
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			s.state.Job, s.state.Session, s.state.Log = j, j.Session, "replay.log"
			text := strings.Repeat("x", 5670640)
			if err := os.WriteFile(filepath.Join(s.dir, s.state.Log), []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			api := API{Queue: q, Token: "test"}.Handler()
			calls, batches := 0, 0
			var holds []time.Duration
			q.observeRequest = func(route string, duration, wait, hold, after time.Duration) {
				if route == "POST /api/workers/log" || route == "POST /api/workers/log-batch" {
					holds = append(holds, hold)
				}
			}
			w.Client = Client{URL: "http://fixture.invalid", Token: "test", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				rw := httptest.NewRecorder()
				if r.URL.Path == "/api/workers/log-batch" {
					batches++
					if predecessor {
						rw.WriteHeader(404)
						return rw.Result(), nil
					}
				}
				api.ServeHTTP(rw, r)
				return rw.Result(), nil
			})}}
			if err := w.replay(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			if !predecessor && (calls != 22 || batches != 22) {
				t.Fatalf("requests=%d batches=%d", calls, batches)
			}
			if predecessor && (calls != 348 || batches != 1) {
				t.Fatalf("predecessor requests=%d batches=%d", calls, batches)
			}
			if s.state.Offset != int64(len(text)) || s.state.Pending != 0 {
				t.Fatal(s.state.Offset, s.state.Pending)
			}
			var got strings.Builder
			var seq int64
			for {
				logs, err := q.Logs(j.ID, seq)
				if err != nil {
					t.Fatal(err)
				}
				if len(logs) == 0 {
					break
				}
				for _, l := range logs {
					got.WriteString(l.Text)
					seq = l.Seq
				}
			}
			if got.String() != text {
				t.Fatal("replayed bytes changed")
			}
			var total, maxHold time.Duration
			for _, h := range holds {
				total += h
				maxHold = max(maxHold, h)
			}
			t.Logf("bytes=%d requests=%d commits=%d total_hold=%s max_hold=%s", len(text), calls, len(holds), total, maxHold)
		})
	}
}

// Changing admission after an ambiguous commit must not change any record's
// offset or bytes. A restart also retains the final short record when logs grow.
func TestWorkerLogBatchLostAckResizeAndGrowth(t *testing.T) {
	for _, fullRecords := range []int{0, 2} {
		t.Run(fmt.Sprintf("full_records=%d", fullRecords), func(t *testing.T) {
			q, _ := fixture(t)
			submit(t, q, spec("experiment"))
			j := check(t, q, machine("a", "fake"), nil).Job
			w := recoveryWorker(t)
			w.Machine.ID = j.Machine
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			s.state.Job, s.state.Session, s.state.Log = j, j.Session, "replay.log"
			text := strings.Repeat("x", fullRecords*logReplayRecordSize) + "short"
			path := filepath.Join(s.dir, s.state.Log)
			if err := os.WriteFile(path, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			api := API{Queue: q, Token: "test"}.Handler()
			var original []LogChunk
			replayed := 0
			w.Client = Client{URL: "http://fixture.invalid", Token: "test", HTTP: &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
				data, _ := io.ReadAll(r.Body)
				r.Body = io.NopCloser(strings.NewReader(string(data)))
				rw := httptest.NewRecorder()
				if original == nil {
					if r.URL.Path == "/api/workers/log-batch" {
						var batch logBatch
						if err := json.Unmarshal(data, &batch); err != nil {
							t.Error(err)
						}
						original = batch.Chunks
					} else {
						var chunk LogChunk
						if err := json.Unmarshal(data, &chunk); err != nil {
							t.Error(err)
						}
						original = []LogChunk{chunk}
					}
					api.ServeHTTP(rw, r)
					if rw.Code != 200 {
						t.Error(rw.Code)
					}
					return nil, io.ErrUnexpectedEOF
				}
				if r.URL.Path == "/api/workers/log-batch" {
					rw.WriteHeader(413)
					io.WriteString(rw, `{"code":"request_too_large"}`)
					return rw.Result(), nil
				}
				var c LogChunk
				json.Unmarshal(data, &c)
				if replayed < len(original) && !reflect.DeepEqual(c, original[replayed]) {
					t.Error("lost-ack record changed")
				}
				replayed++
				api.ServeHTTP(rw, r)
				return rw.Result(), nil
			})}}
			if err := w.replay(context.Background(), s); err == nil {
				t.Fatal("lost ACK not surfaced")
			}
			if s.state.Offset != 0 || s.state.Pending != len(text) {
				t.Fatal("ambiguous boundary not retained")
			}
			s.close()
			s, err = w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if err := os.WriteFile(path, []byte(text+"new bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := w.replay(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			logs, err := q.Logs(j.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			for _, l := range logs {
				got += l.Text
			}
			if got != text+"new bytes" || len(logs) != len(original)+1 || replayed != len(original)+1 {
				t.Fatalf("duplicate/lost records: count=%d length=%d", len(logs), len(got))
			}
		})
	}
}

func TestLogBatchAtomicFencesAndLegacyReplay(t *testing.T) {
	q, _ := fixture(t)
	submit(t, q, spec("experiment"))
	j := check(t, q, machine("a", "fake"), nil).Job
	a := LogChunk{Result: result(j, true), Offset: 1, Text: "first"}
	b := LogChunk{Result: a.Result, Offset: 6, Text: "second"}
	batch := logBatch{Chunks: []LogChunk{a, b}}
	if _, err := q.db.Exec(`CREATE TRIGGER fail_log BEFORE INSERT ON logs WHEN NEW.text='second' BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if err := q.appendLogBatch(batch, context.Background()); err == nil {
		t.Fatal("partial commit accepted")
	}
	var count int
	q.db.QueryRow("SELECT count(*) FROM log_offsets").Scan(&count)
	if count != 0 {
		t.Fatal("partial offsets committed", count)
	}
	q.db.Exec("DROP TRIGGER fail_log")
	// Old worker -> new queue, then duplicate plus new record in a batch.
	if err := q.AppendLog(a); err != nil {
		t.Fatal(err)
	}
	if err := q.appendLogBatch(batch, context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.appendLogBatch(batch, context.Background()); err != nil {
		t.Fatal(err)
	}
	bad := batch
	bad.Chunks = append([]LogChunk(nil), batch.Chunks...)
	bad.Chunks[1].Attempt++
	if err := q.appendLogBatch(bad, context.Background()); err == nil {
		t.Fatal("mixed executions accepted")
	}
	stale := batch
	stale.Chunks = append([]LogChunk(nil), batch.Chunks...)
	for i := range stale.Chunks {
		stale.Chunks[i].Session = "stale"
	}
	if err := q.appendLogBatch(stale, context.Background()); !errors.Is(err, ErrStale) {
		t.Fatal("stale accepted", err)
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) != 2 || logs[0].Text+logs[1].Text != "firstsecond" {
		t.Fatal(logs, err)
	}
}
