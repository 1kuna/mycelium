package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestExtensionCheckMaintenanceBeforeSetup(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, action := range []string{"disable", "drain"} {
			t.Run(fmt.Sprintf("legacy=%t/%s", legacy, action), func(t *testing.T) {
				e := compositionExtension(t, false)
				setup := []string{"sh", "-c", "printf authorized > {cache}/review-setup-marker"}
				if legacy {
					e = fakeExtension(t)
					e.Setup = setup
				} else {
					e.Kinds = nil
					e.Variants[0].Baselines = nil
					e.Variants[0].Setup = setup
					e.Variants[0].Check = []string{"sh", "-c", "exit 0"}
				}
				e = freezeComposition(t, e)
				root := t.TempDir()
				ext := filepath.Join(root, "extensions")
				if err := InstallExtension(ext, e); err != nil {
					t.Fatal(err)
				}
				d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "state", "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: ext, HostID: "local-host", Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				await(t, func() bool { s, _ := d.Queue.Snapshot(); return len(s.Machines) > 0 })
				if _, err = d.Queue.ChangeHostMaintenance("local-host", HostMaintenanceRequest{Action: action}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
				reports, checkErr := CheckExtension(ctx, ext, e, "fake", "0", "local-host", io.Discard, &EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 4 << 20}, d.Client())
				cancel()
				if checkErr == nil {
					t.Fatal("blocked check succeeded", reports)
				}
				marker := filepath.Join(extensionCache(ext, e, "fake"), "review-setup-marker")
				if _, err = os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("setup ran without authorization", err)
				}
				status, err := d.Queue.HostMaintenanceStatus("local-host")
				if err != nil || !status.Quiescent {
					t.Fatal("blocked check left custody", status, err)
				}
				if _, err = d.Queue.ChangeHostMaintenance("local-host", HostMaintenanceRequest{Action: "resume"}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				reports, err = CheckExtension(ctx, ext, e, "fake", "0", "local-host", io.Discard, &EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 4 << 20}, d.Client())
				if err != nil {
					t.Fatal("resumed check failed", reports, err)
				}
				if _, err = os.Stat(marker); err != nil {
					t.Fatal("authorized setup never ran", err)
				}
			})
		}
	}
}
func TestClientUnknownResultJob(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	err = d.Client().DownloadResult(context.Background(), "missing-job", "result", filepath.Join(root, "result"))
	var api *HTTPError
	if !errors.As(err, &api) || api.Code != "job_unknown" {
		t.Errorf("missing result job has no typed job_unknown error: %T %v", err, err)
	}
}

func TestClientResultAtomicReplacement(t *testing.T) {
	body := []byte("new-result")
	h := sha256.Sum256(body)
	ref := FileRef{SHA256: hex.EncodeToString(h[:]), SizeBytes: int64(len(body))}
	var corrupt atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			json.NewEncoder(w).Encode(Snapshot{Jobs: []*Job{{ID: "job", State: "succeeded", OutputRefs: FileManifest{"result": ref}}}})
			return
		}
		if r.Header.Get("Range") != "" {
			t.Error("application output used as a resumable partial")
		}
		w.Header().Set("ETag", `"`+ref.SHA256+`"`)
		if corrupt.Load() {
			w.Write([]byte("bad-result"))
		} else {
			w.Write(body)
		}
	}))
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	for _, previous := range []string{"old-result", "longer prior output", "old", ""} {
		t.Run(previous, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "nested", "result")
			if previous != "" {
				if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(dest, []byte(previous), 0600); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := c.DownloadResult(context.Background(), "job", "result", dest); err != nil {
					t.Fatal(err)
				}
				b, err := os.ReadFile(dest)
				if err != nil || string(b) != string(body) {
					t.Fatal("wrong result", string(b), err)
				}
			}
			corrupt.Store(true)
			if err := c.DownloadResult(context.Background(), "job", "result", dest); err == nil {
				t.Fatal("accepted corrupt result")
			}
			corrupt.Store(false)
			b, err := os.ReadFile(dest)
			if err != nil || string(b) != string(body) {
				t.Fatal("failed download replaced output", string(b), err)
			}
			entries, err := os.ReadDir(filepath.Dir(dest))
			if err != nil || len(entries) != 1 {
				t.Fatal("temporary result leaked", entries, err)
			}
		})
	}
}

func TestClientResultTypedErrors(t *testing.T) {
	for _, tc := range []struct {
		name, state, code string
		status            int
	}{
		{"unknown", "", "job_unknown", 404},
		{"queued", "queued", "request_stale", 409},
		{"running", "running", "request_stale", 409},
		{"failed", "failed", "request_stale", 409},
		{"cancelled", "cancelled", "request_stale", 409},
		{"missing-artifact", "succeeded", "artifact_unknown", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				snapshot := Snapshot{}
				if tc.state != "" {
					snapshot.Jobs = []*Job{{ID: "job", State: tc.state}}
				}
				json.NewEncoder(w).Encode(snapshot)
			}))
			defer server.Close()
			err := (Client{URL: server.URL, Token: "fixture"}).DownloadResult(context.Background(), "job", "result", filepath.Join(t.TempDir(), "result"))
			var api *HTTPError
			if !errors.As(err, &api) || api.Code != tc.code || api.Status != tc.status {
				t.Fatalf("wrong result error: %T %v", err, err)
			}
		})
	}
}
