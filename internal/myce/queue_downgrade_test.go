package myce

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func downgradeState(t *testing.T, db *sql.DB) ([]byte, []byte) {
	t.Helper()
	var marker []byte
	if err := db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&marker); err != nil {
		t.Fatal(err)
	}
	data, err := readQueueState(db, marker)
	if err != nil {
		t.Fatal(err)
	}
	return marker, data
}

func TestQueueDowngradePreservesDecisions(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "interrupted"}[interrupt], func(t *testing.T) {
			q, clock := fixture(t)
			s := keyedSpec()
			s.Priority = "experiment"
			j := submit(t, q, s)
			a := check(t, q, machine("one", "fake"), nil).Job
			if a == nil {
				t.Fatal("fixture job was not assigned")
			}
			ledgerCustody(t, q, a, "held", "reserved")
			ledgerCustody(t, q, a, "held", "started")
			r := result(a, true)
			r.ReleasePending = true
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			s.SubmissionKey = "queued-after-upgrade"
			queued := submit(t, q, s)
			var path string
			if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
				t.Fatal(err)
			}
			if err := DowngradeQueue(path); err == nil || !strings.Contains(err.Error(), "already in use") {
				t.Fatal("downgrade did not fence the running queue", err)
			}
			// Exercise fragmented state and preserve future/unknown decisions as
			// raw JSON, rather than serializing through this binary's state type.
			records, err := readQueueRecords(q.db)
			if err != nil {
				t.Fatal(err)
			}
			var base map[string]json.RawMessage
			if err = json.Unmarshal(records["state"], &base); err != nil {
				t.Fatal(err)
			}
			base["future_decision"], _ = json.Marshal(strings.Repeat("preserved", q.persistencePageBytes))
			data, _ := json.Marshal(base)
			if err = q.writeFields("state", records["state"], data, &queuePersistenceTiming{}, q.db.Exec, nil); err != nil {
				t.Fatal(err)
			}
			if _, err = q.db.Exec("INSERT INTO logs(job,attempt,text) VALUES(?,1,'durable-log')", j.ID); err != nil {
				t.Fatal(err)
			}
			marker, before := downgradeState(t, q.db)
			if !bytes.Equal(marker, normalizedQueueMarker) {
				t.Fatal("fixture did not write v3")
			}
			root := q.root
			if err = q.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if interrupt {
				if _, err = db.Exec(`CREATE TRIGGER reject_downgrade BEFORE UPDATE ON queue BEGIN SELECT RAISE(ABORT,'interrupted downgrade'); END`); err != nil {
					t.Fatal(err)
				}
				physical, err := readQueueRecords(db)
				if err != nil {
					t.Fatal(err)
				}
				if err = DowngradeQueue(path); err == nil {
					t.Fatal("interrupted downgrade succeeded")
				}
				stillMarker, stillState := downgradeState(t, db)
				stillRecords, err := readQueueRecords(db)
				if err != nil || !bytes.Equal(stillMarker, marker) || !bytes.Equal(stillState, before) || !reflect.DeepEqual(physical, stillRecords) {
					t.Fatal("failed downgrade changed durable records", err)
				}
				if _, err = db.Exec("DROP TRIGGER reject_downgrade"); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				if err = DowngradeQueue(path); err != nil {
					t.Fatal(err)
				}
			}
			marker, after := downgradeState(t, db)
			if !bytes.Equal(marker, fieldQueueMarker) || !bytes.Equal(before, after) {
				t.Fatal("downgrade changed jobs, custody or decisions")
			}
			var machines, logs int
			if err = db.QueryRow("SELECT count(*) FROM queue_state WHERE key GLOB 'machines/*'").Scan(&machines); err != nil || machines != 0 {
				t.Fatal("v2 retained incompatible machine records", machines, err)
			}
			if err = db.QueryRow("SELECT count(*) FROM logs WHERE job=? AND text='durable-log'", j.ID).Scan(&logs); err != nil || logs != 1 {
				t.Fatal("downgrade changed auxiliary tables", logs, err)
			}
			q, err = Open(path, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			if q.s.Jobs[j.ID] == nil || q.s.Jobs[queued.ID] == nil || !attemptReleasePending(q.s.Jobs[j.ID]) || len(q.s.Jobs[j.ID].LedgerEntries) == 0 {
				t.Fatal("re-upgrade lost IDs or pending custody")
			}
			if got := submit(t, q, s); got.ID != queued.ID {
				t.Fatal("re-upgrade lost keyed submission")
			}
			marker, _ = downgradeState(t, q.db)
			if !bytes.Equal(marker, normalizedQueueMarker) {
				t.Fatal("re-upgrade did not write v3")
			}
		})
	}
}

func TestQueueDowngradeRejectsUnsupported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	if err := DowngradeQueue(path); err == nil {
		t.Fatal("created a missing database")
	}
	q, _ := fixture(t)
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.Exec("UPDATE queue SET data=?", []byte(`"future-format"`)); err != nil {
		t.Fatal(err)
	}
	q.Close()
	if err := DowngradeQueue(path); err == nil || !strings.Contains(err.Error(), "requires normalized") {
		t.Fatal("accepted unsupported format", err)
	}
}
