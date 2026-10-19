package myce

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func releaseFixture(t *testing.T) string { return releaseFixtureKind(t, "release-7285a88") }
func releaseFixtureKind(t *testing.T, kind string) string {
	t.Helper()
	dest := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	err := filepath.WalkDir(filepath.Join("testdata", kind), func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, _ := filepath.Rel(filepath.Join("testdata", kind), path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dest, rel), 0700)
		}
		return copyConversionOriginal(path, filepath.Join(dest, rel), 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dest
}
func conversionOptions(t *testing.T, source string) PersistedConversionOptions {
	t.Helper()
	return PersistedConversionOptions{Source: source, Target: filepath.Join(t.TempDir(), "current"), Host: "fixture-host", Clock: &fakeClock{now: time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)}, Apply: true}
}
func readFixtureDB(t *testing.T, path string) (map[string]json.RawMessage, map[string][][]any) {
	t.Helper()
	u := url.URL{Scheme: "file", Path: path}
	db, e := sql.Open("sqlite", u.String()+"?mode=ro&immutable=1")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	var b []byte
	if e = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&b); e != nil {
		t.Fatal(e)
	}
	var raw map[string]json.RawMessage
	if e = json.Unmarshal(b, &raw); e != nil {
		t.Fatal(e)
	}
	rows, e := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name!='queue' ORDER BY name")
	if e != nil {
		t.Fatal(e)
	}
	var names []string
	for rows.Next() {
		var n string
		if e = rows.Scan(&n); e != nil {
			t.Fatal(e)
		}
		names = append(names, n)
	}
	rows.Close()
	result := map[string][][]any{}
	for _, n := range names {
		r, e := db.Query("SELECT * FROM \"" + strings.ReplaceAll(n, "\"", "\"\"") + "\" ORDER BY rowid")
		if e != nil {
			t.Fatal(e)
		}
		cols, _ := r.Columns()
		result[n] = [][]any{}
		for r.Next() {
			v := make([]any, len(cols))
			p := make([]any, len(cols))
			for i := range v {
				p[i] = &v[i]
			}
			if e = r.Scan(p...); e != nil {
				t.Fatal(e)
			}
			result[n] = append(result[n], v)
		}
		r.Close()
	}
	return raw, result
}
func sourceHashes(t *testing.T, source string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(source, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			h, e := conversionFileHash(path)
			if e != nil {
				return e
			}
			rel, _ := filepath.Rel(source, path)
			out[rel] = h
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPersistedConversionMatrix(t *testing.T) {
	for _, change := range []string{"release", "open-controls", "missing-session", "opaque-owner", "missing-engine-attempt", "unsequenced-checkpoint", "corrupt-state"} {
		t.Run(change, func(t *testing.T) {
			source := releaseFixture(t)
			o := conversionOptions(t, source)
			switch change {
			case "open-controls":
				if e := os.MkdirAll(filepath.Join(source, "host"), 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(source, "host", "enable-pending.json"), []byte(`{"generation":7,"workers":[],"enable_generation":8}`), 0600); e != nil {
					t.Fatal(e)
				}
			case "missing-session":
				b, _ := os.ReadFile(filepath.Join(source, "workers/one/state.json"))
				var s map[string]any
				json.Unmarshal(b, &s)
				s["session"] = ""
				b, _ = json.Marshal(s)
				os.WriteFile(filepath.Join(source, "workers/one/state.json"), b, 0600)
			case "opaque-owner":
				p := filepath.Join(source, "workers/one/fixture-execution.docker.json")
				b, _ := os.ReadFile(p)
				var s map[string]any
				json.Unmarshal(b, &s)
				delete(s, "owner")
				b, _ = json.Marshal(s)
				os.WriteFile(p, b, 0600)
			case "missing-engine-attempt":
				db, e := sql.Open("sqlite", filepath.Join(source, "queue.db"))
				if e != nil {
					t.Fatal(e)
				}
				var b []byte
				db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&b)
				var s map[string]any
				json.Unmarshal(b, &s)
				for _, slot := range s["model_slots"].(map[string]any) {
					delete(slot.(map[string]any), "attempt")
				}
				b, _ = json.Marshal(s)
				db.Exec("UPDATE queue SET data=?", b)
				db.Close()
			case "unsequenced-checkpoint":
				dir := filepath.Join(source, "checkpoints/job-000/attempt-2")
				os.MkdirAll(dir, 0700)
				os.WriteFile(filepath.Join(dir, "save.bin"), []byte("unique-save"), 0600)
			case "corrupt-state":
				os.WriteFile(filepath.Join(source, "workers/one/state.json"), []byte("broken"), 0600)
			}
			before := sourceHashes(t, source)
			old, tables := readFixtureDB(t, filepath.Join(source, "queue.db"))
			dry := o
			dry.Apply = false
			manifest, err := ConvertPersistedState(dry)
			blocked := change != "release" && change != "open-controls"
			if blocked {
				if err == nil || len(manifest.Blockers) == 0 {
					t.Fatalf("expected blocker: %+v %v", manifest, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if _, e := os.Stat(o.Target); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("dry-run published")
			}
			manifest, err = ConvertPersistedState(o)
			if !reflect.DeepEqual(before, sourceHashes(t, source)) {
				t.Fatal("source changed")
			}
			if blocked {
				if err == nil {
					t.Fatal("unsafe conversion activated")
				}
				if _, e := os.Stat(o.Target); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("failed conversion published")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Jobs != 103 || manifest.Copies != 206 || manifest.Releases != 1 || manifest.Slots != 1 || manifest.Reports != 1 {
				t.Fatalf("inventory: %+v", manifest)
			}
			current, currentTables := readFixtureDB(t, filepath.Join(o.Target, "queue.db"))
			if !reflect.DeepEqual(tables, currentTables) {
				t.Fatal("SQLite logs/usage/custody tables changed")
			}
			var history map[string]json.RawMessage
			if e := json.Unmarshal(current["conversion_history"], &history); e != nil {
				t.Fatal(e)
			}
			for key := range old {
				var a, b any
				json.Unmarshal(old[key], &a)
				json.Unmarshal(current[key], &b)
				// Retired flags remain reconstructable historical evidence, without
				// keeping an operational decoder or cooldown in the runtime.
				if key == "machines" || key == "hosts" {
					var retired map[string]map[string]any
					json.Unmarshal(history["retired_"+key+"_fields"], &retired)
					for id, fields := range retired {
						record := b.(map[string]any)[id].(map[string]any)
						for field, value := range fields {
							if _, active := record[field]; active {
								t.Fatal("retired active field", field)
							}
							if field == "production_idle" {
								policy := record["policy"].(map[string]any)
								if _, active := policy[field]; active {
									t.Fatal("retired policy active")
								}
								policy[field] = value
							} else {
								record[field] = value
							}
						}
					}
				}
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("%s did not round-trip", key)
				}
			}
			if err = verifyPersistedOriginals(o.Target, manifest); err != nil {
				t.Fatal(err)
			}
			intent, err := ReadHostIntent(filepath.Join(o.Target, "host"))
			if err != nil || intent.Desired != "closed" || intent.LastClose == nil || intent.LastClose.Settlement != "pending" {
				t.Fatalf("not CLOSED/unconfirmed: %+v %v", intent, err)
			}
			var fences map[string]QueueHostIntent
			json.Unmarshal(current["host_intents"], &fences)
			if fences[o.Host].HostRevision != intent.HostRevision || fences[o.Host].Desired != "closed" {
				t.Fatal("queue/local conversion fences differ")
			}
			if intent.Revision <= 23 || len(intent.Pending) != 1 || intent.Pending[0].Result.Attempt != 2 {
				t.Fatal("lost host counters or exact pending custody", intent)
			}
			expectedSpool, e := filepath.EvalSymlinks(filepath.Join(o.Target, "workers/one"))
			if e != nil {
				t.Fatal(e)
			}
			workers, e := HostWorkers(filepath.Join(o.Target, "host"))
			if e != nil || len(workers) != 1 || workers[0].State != expectedSpool || workers[0].Config != "" || len(workers[0].Command) != 0 {
				t.Fatal("registry retained executable source references", workers, e)
			}
			if e = AdoptHostWorker(filepath.Join(o.Target, "host"), workers[0]); e != nil {
				t.Fatal(e)
			}
			if current, e := HostWorkers(filepath.Join(o.Target, "host")); e != nil || len(current) != 1 {
				t.Fatal("registry duplicated on adoption", e)
			}
			if _, err = ConvertPersistedState(o); err != nil {
				t.Fatal("idempotent retry:", err)
			}
		})
	}
}

func TestPersistedConversionCrashResume(t *testing.T) {
	for _, phase := range []string{"validated", "published"} {
		t.Run(phase, func(t *testing.T) {
			source := releaseFixture(t)
			before := sourceHashes(t, source)
			o := conversionOptions(t, source)
			o.boundary = func(p string) error {
				if p == phase {
					return errors.New("injected crash")
				}
				return nil
			}
			if _, e := ConvertPersistedState(o); e == nil {
				t.Fatal("fault did not fire")
			}
			if !reflect.DeepEqual(before, sourceHashes(t, source)) {
				t.Fatal("failure altered source")
			}
			if phase == "validated" {
				if _, e := os.Stat(o.Target); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("partial publication")
				}
			}
			o.boundary = nil
			m, e := ConvertPersistedState(o)
			if e != nil {
				t.Fatal(e)
			}
			if e = verifyPersistedOriginals(o.Target, m); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestPersistedConversionDevRestart(t *testing.T) {
	source := releaseFixtureKind(t, "release-7285a88-dev")
	o := conversionOptions(t, source)
	// Bind the synthetic release fixture's generic checkpoint root to this test's
	// isolated destination before conversion, just as the old writer configured it.
	fixtureDB, e := sql.Open("sqlite", filepath.Join(source, "queue.db"))
	if e != nil {
		t.Fatal(e)
	}
	var fixtureData []byte
	if e = fixtureDB.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&fixtureData); e != nil {
		t.Fatal(e)
	}
	fixtureData = bytes.ReplaceAll(fixtureData, []byte("/fixture/checkpoints"), []byte(filepath.Join(o.Target, "checkpoints")))
	if _, e = fixtureDB.Exec("UPDATE queue SET data=? WHERE id=1", fixtureData); e != nil {
		t.Fatal(e)
	}
	fixtureDB.Close()

	if _, e := ConvertPersistedState(o); e != nil {
		t.Fatal(e)
	}
	config := DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(o.Target, "queue.db"), Checkpoints: filepath.Join(o.Target, "checkpoints"), HostStateDir: filepath.Join(o.Target, "host"), HostID: o.Host, Token: "test", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}
	d, e := StartDev(config)
	if e != nil {
		t.Fatal(e)
	}
	closed := true
	t.Cleanup(func() {
		if !closed {
			d.Close()
		}
	})
	closed = false
	if j := job(t, d.Queue, "job-000"); j.State != "queued" || j.Attempt != 0 {
		t.Fatal("converted job ran while CLOSED")
	}
	initial, e := ReadHostIntent(config.HostStateDir)
	if e != nil {
		t.Fatal(e)
	}
	// Test-only current-boot fake exclusion, followed by the real explicit CAS
	// open. Production conversion never writes confirmation or opens the host.
	testConfirmClose(t, config.HostStateDir)
	if _, e = changeHostIntent(config.HostStateDir, d.Queue.clock, hostIntentChange{Desired: "open", Expected: initial.HostRevision}); e != nil {
		t.Fatal(e)
	}
	await(t, func() bool {
		select {
		case e := <-d.Errors():
			t.Fatal("dev worker:", e)
		default:
		}
		j := job(t, d.Queue, "job-000")
		if j.State == "failed" {
			t.Fatal("failed job:", j.Reason)
		}
		return j.State == "succeeded"
	})
	completed := job(t, d.Queue, "job-000")
	if completed.Attempt != 1 {
		t.Fatal("duplicate execution", completed.Attempt)
	}
	logs, e := d.Queue.Logs(completed.ID, 0)
	if e != nil || len(logs) != 1 || logs[0].Text != "once" {
		t.Fatalf("payload execution: %+v %v", logs, e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	closed = true
	d, e = StartDev(config)
	if e != nil {
		t.Fatal(e)
	}
	closed = false
	await(t, func() bool {
		s, e := d.Queue.ReadHostObservation(context.Background(), o.Host)
		return e == nil && len(s.Workers) == 1 && s.Workers[0].Healthy
	})
	if got := job(t, d.Queue, completed.ID); got.State != "succeeded" || got.Attempt != 1 {
		t.Fatal("terminal attempt replayed", got)
	}
	if terminal := job(t, d.Queue, "job-102"); terminal.State != "succeeded" || terminal.Attempt != 2 {
		t.Fatal("historical terminal replayed")
	}
	logs, e = d.Queue.Logs(completed.ID, 0)
	if e != nil || len(logs) != 1 {
		t.Fatal("execution repeated after restart", e)
	}
	dir := filepath.Join(o.Target, "checkpoints", "job-102", "attempt-2")
	if generation, e := checkpointProgress(dir); e != nil || generation != 7 {
		t.Fatal("checkpoint generation", generation, e)
	}
}

func TestPersistedConversionSourceAndBackupVerification(t *testing.T) {
	for _, change := range []string{"add", "alter", "backup", "output", "different-source"} {
		t.Run(change, func(t *testing.T) {
			source := releaseFixture(t)
			o := conversionOptions(t, source)
			if change == "add" || change == "alter" {
				o.boundary = func(phase string) error {
					if phase == "validated" {
						name := "new-file"
						if change == "alter" {
							name = "workers/one/fixture-execution.log"
						}
						return os.WriteFile(filepath.Join(source, name), []byte("changed-source"), 0600)
					}
					return nil
				}
				if _, e := ConvertPersistedState(o); e == nil {
					t.Fatal("published changed source")
				}
				if _, e := os.Stat(o.Target); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("published changed source")
				}
				return
			}
			if _, e := ConvertPersistedState(o); e != nil {
				t.Fatal(e)
			}
			if change == "output" {
				if e := os.WriteFile(filepath.Join(o.Target, "workers/one/fixture-execution.log"), []byte("tampered"), 0600); e != nil {
					t.Fatal(e)
				}
			} else if change == "backup" {
				if e := os.WriteFile(filepath.Join(o.Target, ".conversion/originals/workers/one/fixture-execution.log"), []byte("tampered"), 0600); e != nil {
					t.Fatal(e)
				}
			} else {
				o.Source = releaseFixture(t)
				if e := os.WriteFile(filepath.Join(o.Source, "other"), []byte("different"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := ConvertPersistedState(o); e == nil {
				t.Fatal("accepted unverified retry")
			}
		})
	}
}

func TestPersistedConversionWALAndCredentials(t *testing.T) {
	source := releaseFixture(t)
	// Preserve an uncheckpointed WAL using SQLite's persistent WAL option.
	db, e := sql.Open("sqlite", filepath.Join(source, "queue.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; INSERT INTO logs(job,attempt,text) VALUES('job-102',2,'wal-only')"); e != nil {
		t.Fatal(e)
	}
	// This connection represents a stopped copy: no further writes while converting.
	if st, e := os.Stat(filepath.Join(source, "queue.db-wal")); e != nil || st.Size() == 0 {
		t.Fatal("missing WAL fixture", e)
	}
	configDir := filepath.Join(source, "install/worker")
	if e = os.MkdirAll(configDir, 0700); e != nil {
		t.Fatal(e)
	}
	secret := `synthetic-$literal-"quoted"`
	b, _ := json.Marshal(map[string]string{"token": secret})
	if e = os.WriteFile(filepath.Join(configDir, "credentials.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(configDir, "config.json"), []byte(`{"queue":"http://fixture-queue.invalid"}`), 0600); e != nil {
		t.Fatal(e)
	}
	before := sourceHashes(t, source)
	o := conversionOptions(t, source)
	if _, e = ConvertPersistedState(o); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, source)) {
		t.Fatal("source/WAL changed")
	}
	_, tables := readFixtureDB(t, filepath.Join(o.Target, "queue.db"))
	if len(tables["logs"]) != 2 || tables["logs"][1][3] != "wal-only" {
		t.Fatal("lost WAL records", tables["logs"])
	}
	got, queue, e := ReadFleetEnvironment(filepath.Join(o.Target, "install/worker/credentials.json"))
	if e != nil || got != secret || queue != "http://fixture-queue.invalid" {
		t.Fatal("credential bytes changed", e)
	}
}

func TestPersistedConversionRuntimeRejectsOld(t *testing.T) {
	source := releaseFixture(t)
	if q, e := Open(filepath.Join(source, "queue.db"), t.TempDir(), RealClock{}); e == nil {
		q.Close()
		t.Fatal("old SQLite schema accepted")
	}
	if s, e := (Worker{StateDir: filepath.Join(source, "workers/one")}).openStore(); e == nil {
		s.close()
		t.Fatal("old spool accepted")
	}
}

func TestPersistedConversionCredentialRefusal(t *testing.T) {
	for _, kind := range []string{"control-token", "invalid-queue"} {
		t.Run(kind, func(t *testing.T) {
			source := releaseFixture(t)
			dir := filepath.Join(source, "install/worker")
			if e := os.MkdirAll(dir, 0700); e != nil {
				t.Fatal(e)
			}
			token, queue := "synthetic-token", "http://fixture-queue.invalid"
			if kind == "control-token" {
				token += "\tcontrol"
			} else {
				queue = "file:///unsupported"
			}
			for name, value := range map[string]any{"credentials.json": map[string]string{"token": token}, "config.json": ServiceConfig{Queue: queue}} {
				b, e := json.Marshal(value)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(dir, name), b, 0600); e != nil {
					t.Fatal(e)
				}
			}
			before := sourceHashes(t, source)
			o := conversionOptions(t, source)
			m, e := ConvertPersistedState(o)
			if e == nil || len(m.Blockers) == 0 {
				t.Fatal("unsupported credential conversion published")
			}
			if _, e = os.Stat(o.Target); !errors.Is(e, os.ErrNotExist) || !reflect.DeepEqual(before, sourceHashes(t, source)) {
				t.Fatal("failed credential conversion modified source or published target", e)
			}
		})
	}
}

func TestPersistedConversionRejectsMissingBlobSchema(t *testing.T) {
	source := releaseFixtureKind(t, "release-7285a88-dev")
	db, e := sql.Open("sqlite", filepath.Join(source, "queue.db"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec("PRAGMA user_version=3"); e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	before := sourceHashes(t, source)
	if q, e := Open(filepath.Join(source, "queue.db"), t.TempDir(), RealClock{}); e == nil {
		q.Close()
		t.Fatal("old blob with current SQLite version accepted")
	}
	if !reflect.DeepEqual(before, sourceHashes(t, source)) {
		// Open may create its lock but must not rewrite the old database.
		if after := sourceHashes(t, source); after["queue.db"] != before["queue.db"] {
			t.Fatal("refused source DB changed")
		}
	}
}

func TestPersistedConversionCancellationPreservesCustody(t *testing.T) {
	source := releaseFixture(t)
	db, e := sql.Open("sqlite", filepath.Join(source, "queue.db"))
	if e != nil {
		t.Fatal(e)
	}
	var data []byte
	if e = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data); e != nil {
		t.Fatal(e)
	}
	var raw map[string]json.RawMessage
	var jobs map[string]map[string]json.RawMessage
	if e = json.Unmarshal(data, &raw); e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw["jobs"], &jobs); e != nil {
		t.Fatal(e)
	}
	jobs["job-102"]["state"] = json.RawMessage(`"cancelled"`)
	jobs["job-102"]["reason"] = json.RawMessage(`"cancel requested"`)
	raw["jobs"], _ = json.Marshal(jobs)
	data, _ = json.Marshal(raw)
	if _, e = db.Exec("UPDATE queue SET data=? WHERE id=1", data); e != nil {
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	before := sourceHashes(t, source)
	o := conversionOptions(t, source)
	if _, e = ConvertPersistedState(o); e != nil {
		t.Fatal(e)
	}
	q, e := Open(filepath.Join(o.Target, "queue.db"), t.TempDir(), o.Clock)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	j := q.s.Jobs["job-102"]
	if !cancelled(j) || j.CancelledAt == nil || len(j.Copies) != 2 || len(q.s.HostReleases) != 1 {
		t.Fatal("cancellation or custody lost")
	}
	for _, copy := range j.Copies {
		if copy.ReleaseConfirmed {
			t.Fatal("conversion inferred release")
		}
	}
	if !reflect.DeepEqual(before, sourceHashes(t, source)) {
		t.Fatal("conversion changed source")
	}
}

func TestPersistedConversionUnsupportedAndMixed(t *testing.T) {
	for _, kind := range []string{"future", "mixed", "unsequenced", "unsequenced-files", "missing-copy"} {
		t.Run(kind, func(t *testing.T) {
			source := releaseFixture(t)
			db, e := sql.Open("sqlite", filepath.Join(source, "queue.db"))
			if e != nil {
				t.Fatal(e)
			}
			var b []byte
			if e = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&b); e != nil {
				t.Fatal(e)
			}
			var raw map[string]json.RawMessage
			if e = json.Unmarshal(b, &raw); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "future":
				_, e = db.Exec("PRAGMA user_version=99")
			case "mixed":
				raw["schema_version"] = json.RawMessage("3")
				raw["host_controls"] = json.RawMessage(`{"fixture-host":{"generation":22}}`)
				_, e = db.Exec("PRAGMA user_version=3")
			case "missing-copy":
				var jobs map[string]map[string]json.RawMessage
				json.Unmarshal(raw["jobs"], &jobs)
				delete(jobs["job-102"], "copies")
				raw["jobs"], _ = json.Marshal(jobs)
			case "unsequenced":
				e = os.WriteFile(filepath.Join(source, "checkpoints/job-102/attempt-2/save.json"), []byte(`{"value":7}`), 0600)
			case "unsequenced-files":
				var jobs map[string]map[string]json.RawMessage
				json.Unmarshal(raw["jobs"], &jobs)
				jobs["job-102"]["file_checkpoints"] = json.RawMessage(`{"2":{"generation":0,"files":{}}}`)
				raw["jobs"], _ = json.Marshal(jobs)
			}
			if e != nil {
				t.Fatal(e)
			}
			b, e = json.Marshal(raw)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = db.Exec("UPDATE queue SET data=? WHERE id=1", b); e != nil {
				t.Fatal(e)
			}
			if e = db.Close(); e != nil {
				t.Fatal(e)
			}
			before := sourceHashes(t, source)
			o := conversionOptions(t, source)
			m, e := ConvertPersistedState(o)
			if kind == "missing-copy" {
				if e != nil || m.Copies != 205 {
					t.Fatal("exact copy reconstruction", m.Copies, e)
				}
				current, _ := readFixtureDB(t, filepath.Join(o.Target, "queue.db"))
				var jobs map[string]Job
				json.Unmarshal(current["jobs"], &jobs)
				j := jobs["job-102"]
				if len(j.Copies) != 1 || j.Copies[0].Session != "fixture-session" || j.Copies[0].Attempt != 2 || j.Attempt != 2 || j.Losses != 1 || j.Starts != 2 {
					t.Fatal("incorrect reconstruction", j)
				}
			} else if e == nil {
				t.Fatal("unsupported/mixed source activated")
			}
			if !reflect.DeepEqual(before, sourceHashes(t, source)) {
				t.Fatal("source changed")
			}
			if kind == "mixed" {
				if q, e := Open(filepath.Join(source, "queue.db"), t.TempDir(), RealClock{}); e == nil {
					q.Close()
					t.Fatal("mixed queue accepted")
				}
			}
		})
	}
}

func TestPersistedConversionFixtureProvenance(t *testing.T) {
	var p struct {
		Fixtures map[string]string `json:"fixtures_sha256"`
	}
	b, e := os.ReadFile("testdata/release-fixture-provenance.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(b, &p); e != nil {
		t.Fatal(e)
	}
	if len(p.Fixtures) < 20 {
		t.Fatal("incomplete provenance")
	}
	for path, want := range p.Fixtures {
		got, e := conversionFileHash(filepath.Join("testdata", path))
		if e != nil || got != want {
			t.Fatal("fixture provenance mismatch", path, e)
		}
	}
}
