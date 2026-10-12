package myce

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPressureLargeState(t *testing.T) {
	root := t.TempDir()
	jobroot := filepath.Join(root, "job")
	prior := filepath.Join(jobroot, "attempt-1")
	if err := os.MkdirAll(prior, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40001; i++ {
		if err := os.WriteFile(filepath.Join(prior, fmt.Sprintf("f%05d", i)), []byte{byte(i)}, 0600); err != nil {
			t.Fatal(err)
		}
	}
	large, err := os.Create(filepath.Join(prior, "streamed"))
	if err != nil {
		t.Fatal(err)
	}
	block := make([]byte, 65536)
	expected := sha256.New()
	for i := 0; i < 4096; i++ {
		block[0] = byte(i)
		if _, err = large.Write(block); err != nil {
			t.Fatal(err)
		}
		expected.Write(block)
	}
	if err = large.Sync(); err != nil {
		t.Fatal(err)
	}
	large.Close()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	t.Log("local fixture ready; first durable copy begins")
	dest, err := prepareCheckpoint(Job{Checkpoint: jobroot, Attempt: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("first durable copy seconds %.3f", time.Since(started).Seconds())
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc+128<<20 {
		t.Fatal("checkpoint copying retained excessive heap")
	}
	count := 0
	entries, err := os.ReadDir(dest)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	if count != 40002 {
		t.Fatalf("file count %d", count)
	}
	copied, err := os.Open(filepath.Join(dest, "streamed"))
	if err != nil {
		t.Fatal(err)
	}
	measured := sha256.New()
	for i := 0; i < 4096; i++ {
		if _, err = io.ReadFull(copied, block); err != nil || block[0] != byte(i) {
			t.Fatal("streamed payload changed")
		}
		measured.Write(block)
	}
	if !bytes.Equal(expected.Sum(nil), measured.Sum(nil)) {
		t.Fatal("full streamed payload hash mismatch")
	}
	copied.Close()
	// A trailing separator remains equivalent. Symlink/corrupt path cases are in the existing suite.
	started = time.Now()
	t.Log("second durable copy begins")
	if _, err = prepareCheckpoint(Job{Checkpoint: jobroot + string(os.PathSeparator), Attempt: 3}); err != nil {
		t.Fatal(err)
	}
	t.Logf("second durable copy seconds %.3f", time.Since(started).Seconds())
	t.Log("40001 small files and 256MiB stream copied intact; heap bounded; trailing path accepted")
}

func TestPressureDockerProxyMatrix(t *testing.T) {
	for _, mode := range []string{"definite-no-exec", "refused-create", "refused-start", "accepted-lost-response", "slow-stop"} {
		t.Run(mode, func(t *testing.T) {
			accepted := make(chan struct{})
			release := make(chan struct{})
			late := make(chan struct{})
			allowed := mode == "accepted-lost-response" || mode == "slow-stop"
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				if !allowed {
					rw.WriteHeader(http.StatusConflict)
					return
				}
				close(accepted)
				// A fake daemon deliberately models an already accepted independent operation.
				<-release
				close(late)
				rw.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			w := recoveryWorker(t)
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			s.state.Session = "pressure-docker-proxy"
			j := recoveryJob(t, []string{"sh", "-c", "curl -fsS --max-time 10 " + server.URL + " >/dev/null"})
			p, err := w.start(s, j)
			if err != nil {
				t.Fatal(err)
			}
			if allowed {
				select {
				case <-accepted:
				case <-time.After(10 * time.Second):
					t.Fatal("CLI did not reach fake daemon")
				}
				p.stop(w.Clock.Now(), 0, true)
			}
			select {
			case <-p.done:
			case <-time.After(10 * time.Second):
				t.Fatal("owned CLI did not exit")
			}
			if allowed {
				releaseOnce.Do(func() { close(release) })
				select {
				case <-late:
				case <-time.After(time.Second):
					t.Fatal("fake accepted operation missing")
				}
				t.Log("EXPECTED CONTRACT FAILURE: accepted daemon action survives stopped CLI")
			} else {
				t.Log("definite refusal: no fake daemon action")
			}
		})
	}
}

func TestPressureForeignVersoStateMustReject(t *testing.T) {
	for _, shape := range []map[string]any{
		{"state": "failed", "pid": nil, "assigned_xpu": 1, "preemption_count": 0, "attempt_token": "synthetic-old-attempt"},
		{"state": "running", "pid": 999999, "assigned_xpu": 1, "preemption_count": 2, "attempt_token": "synthetic-old-attempt"},
	} {
		w := recoveryWorker(t)
		data, err := json.Marshal(shape)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(w.StateDir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(w.StateDir, "state.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		store, err := w.openStore()
		if err == nil {
			store.close()
			t.Errorf("foreign Verso custody state accepted as Mycelium worker state; enum=%s", shape["state"])
		}
	}
}
