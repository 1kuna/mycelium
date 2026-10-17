package myce

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These are the literal JSON formats from 8bc2a83: host_disable.go's flag,
// hostReceipt, uint64 control-generation and HostWorker; queue.go's SQLite
// queue(id,data). The unit counter is intentionally unrelated to the host one.
func oldHostFixture(t *testing.T) HostConversionOptions {
	t.Helper()
	root := t.TempDir()
	spool := t.TempDir()
	queue := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", queue)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE queue (id INTEGER PRIMARY KEY CHECK(id=1), data BLOB NOT NULL); INSERT INTO queue VALUES(1,'{"jobs":{},"machines":{},"host_controls":{"test-host":{"generation":30,"enabled":true,"cleared":true}}}');`)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	writeOld := func(path, data string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0640); err != nil {
			t.Fatal(err)
		}
	}
	writeOld(filepath.Join(root, "control-generation.json"), `7`)
	writeOld(filepath.Join(root, "units", "myce-worker@card-a.service", "control-generation.json"), `19`)
	writeOld(filepath.Join(spool, "state.json"), `{"session":"session-a","fresh":false,"offset":0}`)
	w := HostWorker{Host: "test-host", Name: "card-a", Unit: "myce-worker@card-a.service", State: spool, Queue: "http://offline", Session: "session-a", Generation: 19}
	b, err := json.Marshal(w)
	if err != nil {
		t.Fatal(err)
	}
	writeOld(filepath.Join(root, "worker-a.json"), string(b))
	return HostConversionOptions{Dir: root, Host: "test-host", QueueDatabases: map[string]string{"http://offline": queue}, Clock: &fakeClock{now: time.Unix(1000, 0)}}
}
func oldFlag(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "DISABLED"), []byte(`{"generation":22,"who":"1000","when":"2026-10-17T00:00:00Z","reason":"operator close","grace_seconds":10}`), 0640); err != nil {
		t.Fatal(err)
	}
}
func TestHostConversionFlagMatrix(t *testing.T) {
	for mask := 0; mask < 8; mask++ {
		t.Run(string(rune('0'+mask)), func(t *testing.T) {
			o := oldHostFixture(t)
			dirs := []string{o.Dir, filepath.Join(o.Dir, "units", "myce-worker@card-a.service"), filepath.Join(o.Dir, "units", "myce-worker@card-b.service")}
			for i, dir := range dirs {
				if mask&(1<<i) != 0 {
					oldFlag(t, dir)
				}
			}
			before, _ := os.ReadFile(filepath.Join(o.Dir, "control-generation.json"))
			dry, err := ConvertHostControl(o)
			if err != nil {
				t.Fatal(err)
			}
			if dry.Intent.Revision != 31 || dry.Intent.Observed != nil || dry.Intent.Desired == "closed" != (mask != 0) {
				t.Fatal(dry)
			}
			if _, err = os.Stat(filepath.Join(o.Dir, hostStateFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("dry-run published state", err)
			}
			o.Apply = true
			m, err := ConvertHostControl(o)
			if err != nil {
				t.Fatal(err)
			}
			s, err := ReadHostIntent(o.Dir)
			if err != nil || s.Desired != dry.Intent.Desired || s.Revision != 31 || s.Observed != nil {
				t.Fatal(s, err)
			}
			if mask != 0 {
				if _, err = hostLaunchGate(o.Dir); err == nil {
					t.Fatal("closed conversion launched")
				}
			}
			for _, src := range m.Sources {
				if err = verifyConversionSource(filepath.Join(conversionDir(o.Dir), src.Original), src); err != nil {
					t.Fatal(err)
				}
			}
			again, err := ConvertHostControl(o)
			if err != nil || again.Intent.HostRevision != m.Intent.HostRevision {
				t.Fatal(again, err)
			}
			found := false
			for _, src := range m.Sources {
				if src.Path == filepath.Join(o.Dir, "control-generation.json") {
					b, _ := os.ReadFile(filepath.Join(conversionDir(o.Dir), src.Original))
					if string(b) != string(before) {
						t.Fatal("bytes changed")
					}
					found = true
				}
			}
			if !found {
				t.Fatal("original counter missing")
			}
		})
	}
}
func TestHostConversionMalformedAndPartialReceipts(t *testing.T) {
	for _, receipt := range []string{`{`, `null`, `{}`, `{"generation":29}`, `{"generation":29,"workers":[]}`, `{"generation":29,"workers":[{"host_id":"test-host","name":"card-a","state_dir":"STATE","queue":"http://offline","executions":[{"machine":"card-a","session":"session-a","id":"job-a","attempt":2}]}]}`} {
		t.Run(receipt, func(t *testing.T) {
			o := oldHostFixture(t)
			workers, _ := HostWorkers(o.Dir)
			receipt = strings.ReplaceAll(receipt, "STATE", workers[0].State)
			if err := os.WriteFile(filepath.Join(o.Dir, "disable-receipt.json"), []byte(receipt), 0600); err != nil {
				t.Fatal(err)
			}
			o.Apply = true
			m, err := ConvertHostControl(o)
			if err != nil {
				t.Fatal(err)
			}
			if m.Intent.Desired != "closed" || m.Intent.Observed != nil {
				t.Fatal(m)
			}
			if strings.Contains(receipt, "job-a") {
				if m.Pending != 1 || !m.Intent.Pending[0].Result.ReleasePending {
					t.Fatal(m)
				}
			}
		})
	}
}
func TestHostConversionInterruptedPhases(t *testing.T) {
	for _, phase := range []string{"inventory", "original", "preserved", "staged", "retired-source", "retired", "published", "unfenced"} {
		t.Run(phase, func(t *testing.T) {
			o := oldHostFixture(t)
			oldFlag(t, o.Dir)
			o.Apply = true
			o.boundary = func(step string) error {
				if step == phase {
					return errors.New("power loss")
				}
				return nil
			}
			first, err := ConvertHostControl(o)
			if err == nil {
				t.Fatal("missing fault")
			}
			if phase != "unfenced" {
				if _, err = ReadHostIntent(o.Dir); !errors.Is(err, ErrHostState) {
					t.Fatal("partial conversion active", err)
				}
			}
			o.boundary = nil
			m, err := ConvertHostControl(o)
			if err != nil {
				t.Fatal(err)
			}
			if m.Intent.HostRevision != first.Intent.HostRevision || m.Phase != "complete" {
				t.Fatal(m)
			}
			for _, src := range m.Sources {
				if err = verifyConversionSource(filepath.Join(conversionDir(o.Dir), src.Original), src); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestHostConversionRefusesUnknownIdentityOverflowAndChangedOriginal(t *testing.T) {
	for _, bad := range []string{"identity", "overflow", "unknown-queue", "corrupt-counter", "busy-worker"} {
		t.Run(bad, func(t *testing.T) {
			o := oldHostFixture(t)
			switch bad {
			case "identity":
				o.Host = "another-host"
			case "overflow":
				os.WriteFile(filepath.Join(o.Dir, "control-generation.json"), []byte(`18446744073709551615`), 0600)
			case "unknown-queue":
				o.QueueDatabases = nil
			case "corrupt-counter":
				os.WriteFile(filepath.Join(o.Dir, "control-generation.json"), []byte(`{}`), 0600)
			case "busy-worker":
				workers, _ := HostWorkers(o.Dir)
				unlock, err := hostOperationLock(workers[0].State, "worker.lock", false)
				if err != nil {
					t.Fatal(err)
				}
				defer unlock()
			}
			o.Apply = true
			if _, err := ConvertHostControl(o); err == nil {
				t.Fatal("bad conversion accepted")
			}
			if _, err := os.Stat(filepath.Join(o.Dir, hostStateFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid state published")
			}
		})
	}
	o := oldHostFixture(t)
	o.Apply = true
	o.boundary = func(phase string) error {
		if phase == "preserved" {
			return errors.New("power loss")
		}
		return nil
	}
	m, err := ConvertHostControl(o)
	if err == nil {
		t.Fatal("fault missing")
	}
	os.WriteFile(filepath.Join(conversionDir(o.Dir), m.Sources[0].Original), []byte("changed"), 0600)
	o.boundary = nil
	if _, err = ConvertHostControl(o); err == nil {
		t.Fatal("changed original accepted")
	}
}

func TestHostConversionQueuePendingAndWALOriginals(t *testing.T) {
	o := oldHostFixture(t)
	path := o.QueueDatabases["http://offline"]
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; UPDATE queue SET data='{"jobs":{"job-a":{"id":"job-a","state":"running","copies":[{"machine":"card-a","session":"session-a","attempt":2,"recoverable":true}]}},"machines":{"card-a":{"id":"card-a","host_id":"test-host"}},"host_controls":{"test-host":{"generation":30,"enabled":true,"cleared":true}},"machine_controls":{"card-a":{"host":"test-host","session":"session-a","generation":65,"enabled":false,"cleared":false}},"host_releases":{"release-a":{"Host":"test-host","Job":"job-a","Machine":"card-a","Session":"session-a","Attempt":2,"preparation":true}},"model_slots":{"engine-a":{"machine":"card-a","session":"session-a","model":"model-a","attempt":"engine-attempt-a","state":"ready"}}}' WHERE id=1;`)
	if err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		b, e := os.ReadFile(path + suffix)
		if e != nil {
			t.Fatal(e)
		}
		before[path+suffix] = b
	}
	dry, err := ConvertHostControl(o)
	if err != nil {
		t.Fatal(err)
	}
	if dry.Intent.Revision != 66 || dry.Intent.Desired != "closed" || dry.Pending != 2 {
		t.Fatal(dry)
	}
	preparations, engines := 0, 0
	for _, p := range dry.Intent.Pending {
		if p.Preparation {
			preparations++
		}
		if p.Engine != nil {
			engines++
		}
	}
	if preparations != 1 || engines != 1 {
		t.Fatal(dry.Intent.Pending)
	}
	for path, b := range before {
		after, e := os.ReadFile(path)
		if e != nil || string(after) != string(b) {
			t.Fatal("dry-run changed source WAL bytes", e)
		}
	}
	o.Apply = true
	m, err := ConvertHostControl(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, src := range m.Sources {
		if err = verifyConversionSource(filepath.Join(conversionDir(o.Dir), src.Original), src); err != nil {
			t.Fatal(err)
		}
	}
	for path, b := range before {
		after, e := os.ReadFile(path)
		if e != nil || string(after) != string(b) {
			t.Fatal("apply changed source WAL bytes", e)
		}
	}
}

func TestHostConversionUnitOnlyRegistryPreserved(t *testing.T) {
	o := oldHostFixture(t)
	source := filepath.Join(o.Dir, "worker-a.json")
	unit := filepath.Join(o.Dir, "units", "myce-worker@card-a.service", "worker-a.json")
	if err := os.Rename(source, unit); err != nil {
		t.Fatal(err)
	}
	o.Apply = true
	m, err := ConvertHostControl(o)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := HostWorkers(o.Dir)
	if err != nil || len(workers) != 1 || workers[0].Name != "card-a" {
		t.Fatal(workers, err)
	}
	if _, err = ConvertHostControl(o); err != nil {
		t.Fatal(err)
	}
	for _, src := range m.Sources {
		if err = verifyConversionSource(filepath.Join(conversionDir(o.Dir), src.Original), src); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHostConversionArchivedCountersAndReceipts(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		t.Run(fmt.Sprint(exhausted), func(t *testing.T) {
			o := oldHostFixture(t)
			workers, err := HostWorkers(o.Dir)
			if err != nil {
				t.Fatal(err)
			}
			w := workers[0]
			w.Generation = 117
			w.Executions = []Result{{Machine: w.Name, Session: w.Session, Execution: Execution{ID: "old-job", Attempt: 3}}}
			receipt, err := json.Marshal(hostReceipt{Generation: 91, EnableGeneration: 92, Workers: []HostWorker{w}})
			if err != nil {
				t.Fatal(err)
			}
			trash := filepath.Join(o.Dir, "units", w.Unit, ".Trash")
			if err = os.MkdirAll(trash, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(trash, "old-enable-pending.json")
			if err = os.WriteFile(path, receipt, 0640); err != nil {
				t.Fatal(err)
			}
			if exhausted {
				if err = os.WriteFile(filepath.Join(trash, "old-control-generation.json"), []byte(`18446744073709551615`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			o.Apply = true
			m, err := ConvertHostControl(o)
			if exhausted {
				if err == nil {
					t.Fatal("archived counter overflow accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if m.Intent.Revision != 118 || m.Intent.Desired != "closed" || m.Pending != 1 {
				t.Fatalf("archive omitted: revision=%d desired=%s pending=%d", m.Intent.Revision, m.Intent.Desired, m.Pending)
			}
			found := false
			for _, src := range m.Sources {
				if src.Path == path {
					found = true
					if err = verifyConversionSource(filepath.Join(conversionDir(o.Dir), src.Original), src); err != nil {
						t.Fatal(err)
					}
					if src.Mode != 0640 || src.SHA256 != conversionHash(receipt) {
						t.Fatal("archived bytes or mode lost")
					}
				}
			}
			if !found {
				t.Fatal("archived receipt not preserved")
			}
		})
	}
}
