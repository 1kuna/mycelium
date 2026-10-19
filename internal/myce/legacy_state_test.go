package myce

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func stateTree(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, _ := filepath.Rel(root, p)
		if d.IsDir() {
			out[rel] = sha256.Sum256([]byte("directory"))
			return nil
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		out[rel] = sha256.Sum256(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLegacyReleaseRefusal(t *testing.T) {
	for _, kind := range []string{"release-7285a88", "release-7285a88-native", "release-7285a88-idle", "release-7285a88-dev"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			if e := os.CopyFS(root, os.DirFS(filepath.Join("testdata", kind))); e != nil {
				t.Fatal(e)
			}
			before := stateTree(t, root)
			check := func(e error, path string) {
				t.Helper()
				var typed *LegacyStateError
				if !errors.Is(e, ErrLegacyState) || !errors.As(e, &typed) || !strings.Contains(e.Error(), "legacy_state_present") || !strings.Contains(e.Error(), path) || !strings.Contains(e.Error(), "fresh install") {
					t.Fatalf("missing actionable refusal: %v", e)
				}
			}
			db := filepath.Join(root, "queue.db")
			q, e := Open(db, root, RealClock{})
			if q != nil {
				q.Close()
			}
			check(e, db)
			dir := filepath.Join(root, "workers", "one")
			s, e := (Worker{StateDir: dir}).openStore()
			if s != nil {
				s.close()
			}
			check(e, dir)
			if kind != "release-7285a88-dev" {
				host := filepath.Join(root, "host")
				_, e = ReadHostIntent(host)
				check(e, host)
				_, e = InitializeHostIntent(host, "fixture-host", RealClock{})
				check(e, host)
				_, e = HostWorkers(host)
				check(e, host)
			}
			after := stateTree(t, root)
			if len(before) != len(after) {
				t.Fatal("refusal changed tree", len(before), len(after))
			}
			for p, h := range before {
				if after[p] != h {
					t.Fatal("refusal changed", p)
				}
			}
		})
	}
}

func TestLegacyQueueWALRefusal(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	db, e := sql.Open("sqlite", source)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE queue(id INTEGER PRIMARY KEY, data BLOB); INSERT INTO queue VALUES(1,'{"jobs":{},"machines":{}}')`); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "queue.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		b, e := os.ReadFile(source + suffix)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(target+suffix, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	before := stateTree(t, filepath.Dir(target))
	q, e := Open(target, root, RealClock{})
	if q != nil {
		q.Close()
	}
	if !errors.Is(e, ErrLegacyState) {
		t.Fatalf("WAL-only RC accepted: %v", e)
	}
	after := stateTree(t, filepath.Dir(target))
	if len(before) != len(after) {
		t.Fatal("WAL refusal created files")
	}
	for p, h := range before {
		if after[p] != h {
			t.Fatal("WAL refusal changed", p)
		}
	}
}

func TestLegacyHostErrorProtocol(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host")
	original := legacyState(path)
	if hostProtocolCode(original) != "legacy_state_present" || !errors.Is(hostProtocolError("legacy_state_present"), ErrLegacyState) {
		t.Fatal("host child lost refusal code")
	}
	envelope := HostEnvelopeError(original)
	if envelope.Code != "legacy_state_present" || !strings.Contains(envelope.Message, path) {
		t.Fatal("host envelope lost archive path")
	}
	v := HostInvocation{Privileged: true}
	_, e := v.Mutate(context.Background(), HostMutation{Operation: "status", Timeout: 5 * time.Second}, func(context.Context, HostChildLaunch) ([]byte, error) {
		return json.Marshal(hostChildResponse{Version: 1, Operation: "status", Code: "legacy_state_present", LegacyPaths: []string{path}})
	}, nil)
	var typed *LegacyStateError
	if !errors.As(e, &typed) || len(typed.Paths) != 1 || typed.Paths[0] != path {
		t.Fatal("child refusal lost archive path", e)
	}
}

func TestCurrentQueueWALRestart(t *testing.T) {
	source := filepath.Join(t.TempDir(), "queue.db")
	q, e := Open(source, t.TempDir(), RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	submitted := submit(t, q, spec("experiment"))
	target := filepath.Join(t.TempDir(), "queue.db")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		b, e := os.ReadFile(source + suffix)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(target+suffix, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	restarted, e := Open(target, t.TempDir(), RealClock{})
	if e != nil {
		t.Fatal("current crash snapshot refused", e)
	}
	defer restarted.Close()
	if len(restarted.s.Jobs) != 1 || restarted.s.Jobs[submitted.ID] == nil {
		t.Fatal("current WAL state lost")
	}
}

func TestFreshInstallSurvivingDataRefused(t *testing.T) {
	for _, field := range []string{"state", "model-cache", "environment-cache"} {
		t.Run(field, func(t *testing.T) {
			c, _ := preserveWorkerFixture(t)
			path := c.State
			if field == "model-cache" {
				path = c.ModelCache
			}
			if field == "environment-cache" {
				path = c.EnvironmentCache
			}
			if e := os.MkdirAll(path, 0700); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(filepath.Join(path, "build-state.json"), []byte(`{"identity":"RC-build","launch_pending":true}`), 0600); e != nil {
				t.Fatal(e)
			}
			before := stateTree(t, path)
			if e := prepareClosedWorkerInstall(c); e == nil {
				t.Fatal("surviving custody falsely settled")
			}
			if _, e := os.Stat(filepath.Join(c.HostStateDir, hostStateFile)); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("surviving custody provisioned host")
			}
			after := stateTree(t, path)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("surviving data changed")
			}
		})
	}
}

func TestLegacyStandaloneCustodyRefusal(t *testing.T) {
	old, e := os.ReadFile("testdata/release-7285a88/workers/one/engine-engine-attempt.json")
	if e != nil {
		t.Fatal(e)
	}
	for _, record := range []string{"engine-old.json", "attempt-old.json", "external/recipe.partial/build-state.json"} {
		t.Run(record, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "state")
			environment := filepath.Join(root, "external")
			path := filepath.Join(dir, record)
			archive := dir
			if strings.HasPrefix(record, "external/") {
				path = filepath.Join(root, record)
				archive = environment
			}
			if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
				t.Fatal(e)
			}
			if e := os.WriteFile(path, old, 0600); e != nil {
				t.Fatal(e)
			}
			before := stateTree(t, root)
			s, e := (Worker{StateDir: dir, EnvironmentRoot: environment}).openStore()
			if s != nil {
				s.close()
			}
			if !errors.Is(e, ErrLegacyState) || !strings.Contains(e.Error(), archive) {
				t.Fatal("old orphan custody accepted", e)
			}
			if !reflect.DeepEqual(before, stateTree(t, root)) {
				t.Fatal("old orphan custody changed")
			}
		})
	}
}
