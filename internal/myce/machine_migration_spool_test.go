package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type m4LegacyRetentionFixture struct {
	MissingAck        []string                   `json:"missing_ack"`
	SupersededResults []string                   `json:"superseded_results"`
	Jobs              []*Job                     `json:"jobs"`
	Spool             map[string]json.RawMessage `json:"spool"`
}

func m4LegacyRetention(t *testing.T) m4LegacyRetentionFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/m4-legacy-retained.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture m4LegacyRetentionFixture
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func writeM4LegacySpool(t *testing.T, dir string, fixture m4LegacyRetentionFixture) {
	t.Helper()
	for name, data := range fixture.Spool {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestM4VerifiedEmptyLegacyRetainedSpool(t *testing.T) {
	fixture := m4LegacyRetention(t)
	if len(fixture.MissingAck) != 3 || len(fixture.SupersededResults) != 4 || len(fixture.Jobs) != 5 {
		t.Fatal("fixture no longer covers the seven production blockers and normal records")
	}
	c, clock, mapping, opts, run := m4EmptyFixture(t)
	source, err := ReadServiceConfig(opts.WorkerConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	writeM4LegacySpool(t, source.State, fixture)
	q, err := openConversionQueue(c, clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range fixture.Jobs {
		q.s.Jobs[j.ID] = j
	}
	if err = q.update(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	q.Close()
	before, err := migrationSpoolDigest(source.State)
	if err != nil {
		t.Fatal(err)
	}
	for _, dry := range []bool{true, false} {
		if err = ConvertQueueInstall(context.Background(), c, mapping, dry, false, clock, run, opts); err != nil {
			t.Fatalf("dry=%t: settled legacy retention refused: %v", dry, err)
		}
	}
	after, err := migrationSpoolDigest(source.State)
	if err != nil || before != after {
		t.Fatal("conversion changed historical spool", err)
	}
}

func TestM4LegacyRetentionEachRecord(t *testing.T) {
	fixture := m4LegacyRetention(t)
	for name, data := range fixture.Spool {
		if !strings.HasSuffix(name, ".docker.json") && !strings.HasSuffix(name, ".result.json") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
				t.Fatal(err)
			}
			ack := strings.TrimSuffix(name, ".docker.json") + ".docker-ack.json"
			if data, ok := fixture.Spool[ack]; ok {
				if err := os.WriteFile(filepath.Join(dir, ack), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := settledMigrationSpool(dir, Snapshot{Jobs: fixture.Jobs}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestM4LegacyRetentionCustodyRefusals(t *testing.T) {
	const identity = "record-kill-1"
	const dockerFile = identity + ".docker.json"
	const resultFile = identity + ".result.json"
	cases := []struct{ name, file, reason string }{
		{"unknown_job", dockerFile, "queue_attempt_unknown"},
		{"live_current", "record-kill-2.result.json", "queue_attempt_live"},
		{"unsettled_current", "record-kill-2.result.json", "queue_release_unsettled"},
		{"unsettled_copy", resultFile, "queue_release_unsettled"},
		{"missing_copy", resultFile, "queue_attempt_unknown"},
		{"wrong_session", resultFile, "queue_attempt_unknown"},
		{"wrong_machine", resultFile, "queue_attempt_unknown"},
		{"unreleased_docker", dockerFile, "docker_custody_unsettled"},
		{"pending_docker", dockerFile, "docker_custody_unsettled"},
		{"operator_confirmed", dockerFile, "docker_custody_unsettled"},
		{"wrong_identity", dockerFile, "docker_identity_mismatch"},
		{"wrong_owner", dockerFile, "docker_owner_mismatch"},
		{"malformed_ack", dockerFile, "docker_ack_invalid"},
		{"unreadable_ack", dockerFile, "docker_ack_unreadable"},
		{"malformed_result", resultFile, "spool_invalid"},
		{"malformed_docker", dockerFile, "spool_invalid"},
		{"nonregular_result", resultFile, "spool_not_regular"},
		{"unbound_docker", dockerFile, "queue_attempt_identity_missing"},
		{"active_state", "state.json", "spool_active_state"},
		{"pending_launch", "state.json", "spool_active_state"},
		{"no_snapshot", resultFile, "queue_attempt_unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := m4LegacyRetention(t)
			dir := t.TempDir()
			var target *Job
			for _, j := range fixture.Jobs {
				if j.ID == "job-kill" {
					target = j
				}
			}
			var docker dockerExecution
			if err := json.Unmarshal(fixture.Spool[dockerFile], &docker); err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "unknown_job":
				fixture.Jobs = nil
			case "live_current":
				target.State = "running" // Even a released current attempt cannot be called terminal.
			case "unsettled_current":
				target.ReleaseConfirmed = false
			case "unsettled_copy":
				target.Copies[0].ReleaseConfirmed = false
			case "missing_copy":
				target.Copies = target.Copies[1:]
			case "wrong_session":
				target.Copies[0].Session = "other-session"
			case "wrong_machine":
				target.Copies[0].Machine = "other-worker"
			case "unreleased_docker":
				docker.Released = false
			case "pending_docker":
				docker.Pending = true
			case "operator_confirmed":
				docker.OperatorConfirmed = true
			case "wrong_identity":
				docker.Identity = "other-identity"
			case "wrong_owner":
				docker.Owner.Session = "other-session"
			case "unbound_docker":
				docker.Owner = nil
				delete(fixture.Spool, resultFile)
			case "active_state":
				fixture.Spool["state.json"] = json.RawMessage(`{"session":"session-host-gpu0","fresh":false,"offset":0,"job":{"id":"job-kill","attempt":1}}`)
			case "pending_launch":
				fixture.Spool["state.json"] = json.RawMessage(`{"session":"session-host-gpu0","fresh":false,"offset":0,"launch_pending":true}`)
			}
			fixture.Spool[dockerFile], _ = json.Marshal(docker)
			writeM4LegacySpool(t, dir, fixture)
			switch tc.name {
			case "malformed_ack", "unreadable_ack":
				path := filepath.Join(dir, identity+".docker-ack.json")
				if tc.name == "malformed_ack" {
					if err := os.WriteFile(path, []byte(`{"worker":"host-gpu0"}`), 0600); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "malformed_result", "malformed_docker":
				if err := os.WriteFile(filepath.Join(dir, tc.file), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "nonregular_result":
				if err := os.Remove(filepath.Join(dir, resultFile)); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("absent", filepath.Join(dir, resultFile)); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if tc.name == "no_snapshot" {
				err = settledMigrationSpool(dir)
			} else {
				err = settledMigrationSpool(dir, Snapshot{Jobs: fixture.Jobs})
			}
			var typed *HTTPError
			if !errors.As(err, &typed) || typed.Code != "machine_migration_incomplete" || !strings.Contains(typed.Message, tc.file+": "+tc.reason) {
				t.Fatalf("want %s: %s, got %v", tc.file, tc.reason, err)
			}
		})
	}
}

func TestM4VerifiedEmptyLegacyRefusalNamesEveryFile(t *testing.T) {
	fixture := m4LegacyRetention(t)
	c, clock, mapping, opts, run := m4EmptyFixture(t)
	source, err := ReadServiceConfig(opts.WorkerConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	// With no queue bindings every result and Docker record must refuse, including
	// old successful records with a perfectly valid retention acknowledgement.
	writeM4LegacySpool(t, source.State, fixture)
	before, err := migrationSpoolDigest(source.State)
	if err != nil {
		t.Fatal(err)
	}
	for _, dry := range []bool{true, false} {
		err = ConvertQueueInstall(context.Background(), c, mapping, dry, false, clock, run, opts)
		var typed *HTTPError
		if !errors.As(err, &typed) || typed.Code != "machine_drain_spool_unsettled" {
			t.Fatal(err)
		}
		for name := range fixture.Spool {
			if (strings.HasSuffix(name, ".docker.json") || strings.HasSuffix(name, ".result.json")) && !strings.Contains(typed.Message, name+": queue_attempt_unknown") {
				t.Errorf("missing file/reason %s in %v", name, err)
			}
		}
	}
	after, err := migrationSpoolDigest(source.State)
	if err != nil || before != after {
		t.Fatal("refusal changed historical spool", err)
	}
}

func TestM4LegacyDockerWithoutOwnerUsesExactResult(t *testing.T) {
	fixture := m4LegacyRetention(t)
	dir := t.TempDir()
	for name, data := range fixture.Spool {
		if strings.HasSuffix(name, ".docker.json") {
			var d dockerExecution
			if err := json.Unmarshal(data, &d); err != nil {
				t.Fatal(err)
			}
			d.Owner = nil
			data, _ = json.Marshal(d)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := settledMigrationSpool(dir, Snapshot{Jobs: fixture.Jobs}); err != nil {
		t.Fatal(err)
	}
}
