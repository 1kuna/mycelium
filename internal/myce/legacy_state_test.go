package myce

import (
	"context"
	"crypto/sha256"
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
	marker, e := os.ReadFile(filepath.Join(filepath.Dir(source), FormatMarker))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(filepath.Dir(target), FormatMarker), marker, 0600); e != nil {
		t.Fatal(e)
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
			if e := os.WriteFile(path, []byte("surviving data"), 0600); e != nil {
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
