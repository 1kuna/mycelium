package myce

import (
	"bytes"
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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExtensionCheckMaintenanceBeforeSetup(t *testing.T) {
	// The cases share only the enrolled dev runtime. Each installs a distinct
	// extension and keeps its own cache, setup marker and check receipts.
	root := t.TempDir()
	ext := filepath.Join(root, "extensions")
	d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "state", "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: ext, HostID: "local-host", Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond}, func(q *Queue) { q.clock = &workerFakeClock{now: q.clock.Now()} })
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	awaitTestDevDeviceFingerprints(t, d)
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
				e.Name = fmt.Sprintf("maintenance-%t-%s", legacy, action)
				e = freezeComposition(t, e)
				root := t.TempDir()
				if err := InstallExtension(ext, e); err != nil {
					t.Fatal(err)
				}
				if _, err = d.Queue.ChangeHostMaintenance("local-host", HostMaintenanceRequest{Action: action}); err != nil {
					t.Fatal(err)
				}
				cache := &EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 4 << 20}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				client := d.Client()
				var blocked *Job
				var blockedErr error
				client.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
					// Cancel only after observing the accepted check under
					// maintenance. The cleanup calls use a separate context.
					if id, ok := strings.CutPrefix(r.URL.Path, "/api/jobs/"); ok && r.Method == http.MethodGet && ctx.Err() == nil {
						blocked, blockedErr = d.Client().GetJob(r.Context(), id)
						cancel()
						return nil, context.Canceled
					}
					return http.DefaultTransport.RoundTrip(r)
				})}
				reports, checkErr := CheckExtension(ctx, ext, e, "fake", "0", "local-host", io.Discard, cache, client)
				cancel()
				if blockedErr != nil || blocked == nil {
					t.Fatalf("blocked check was never observed: read=%v check=%v", blockedErr, checkErr)
				}
				if blocked.State != "queued" || blocked.Attempt != 0 || blocked.StartedAt != nil || !errors.Is(checkErr, context.Canceled) {
					t.Fatalf("expected an unassigned check before cancellation: state=%s attempt=%d started=%v check=%v", blocked.State, blocked.Attempt, blocked.StartedAt, checkErr)
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
				// A legacy check waits for each work in series. Its admission,
				// execution and release have no shared ten-second contract.
				var commandLog bytes.Buffer
				reports, err = CheckExtension(t.Context(), ext, e, "fake", "0", "local-host", &commandLog, cache, d.Client())
				if err != nil {
					t.Fatalf("resumed check failed: %v\nreports: %+v\ncommand output:\n%s", err, reports, commandLog.String())
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
	d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
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
		if r.URL.Path == "/api/jobs/job" {
			json.NewEncoder(w).Encode(Job{ID: "job", State: "succeeded", OutputRefs: FileManifest{"result": ref}})
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
				if tc.state == "" {
					respond(w, nil, &HTTPError{Status: 404, Code: "job_unknown", Message: "job is unknown"})
					return
				}
				json.NewEncoder(w).Encode(Job{ID: "job", State: tc.state})
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
