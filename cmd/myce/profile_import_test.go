package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Exercise the real dev API/file store with a sparse local input. A transport
// barrier models a slow import without moving multi-GB fixtures or depending on
// the test machine's disk speed. Cancellation belongs to the caller.
func TestProfileImportDevPhaseAndProgress(t *testing.T) {
	for _, phase := range []string{"upload", "commit", "arming"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			for _, dir := range []string{"source", ".mycelium/profiles"} {
				if err := os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
					t.Fatal(err)
				}
			}
			const size = 9 << 20
			f, err := os.Create(filepath.Join(root, "input"))
			if err != nil {
				t.Fatal(err)
			}
			if err = f.Truncate(size); err != nil {
				t.Fatal(err)
			}
			if err = f.Close(); err != nil {
				t.Fatal(err)
			}
			profile := `name: import
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 1, memory_gb: 0.125}
targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
check: [true]
shared_inputs: {data: {local: input}}
`
			path := filepath.Join(root, ".mycelium/profiles/import.yaml")
			if err = os.WriteFile(path, []byte(profile), 0600); err != nil {
				t.Fatal(err)
			}
			state := t.TempDir()
			d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := d.Close(); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			target, _ := url.Parse(d.URL)
			proxy := httputil.NewSingleHostReverseProxy(target)
			var blocked atomic.Bool
			ctx = myce.WithFileProgress(ctx, func(p myce.FileProgress) {
				if phase == "upload" && p.Phase == "upload" && p.Bytes == 4<<20 {
					blocked.Store(true)
					cancel()
				}
			})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				stop := phase == "commit" && r.Method == "POST" && r.URL.Path == "/api/jobs"
				if stop {
					blocked.Store(true)
					cancel()
					return
				}
				proxy.ServeHTTP(w, r)
			}))
			defer server.Close()
			var out bytes.Buffer
			notify := ""
			wantExit, wantCause := 69, "cancelled"
			if phase == "arming" {
				notify = filepath.Join(root, "missing-notifier")
				wantExit, wantCause = 74, "ENOENT"
			}
			err = runProfile(ctx, path, nil, nil, "import", notify, "", "", server.URL, "fixture", &out, io.Discard)
			server.Close()
			if phase != "arming" && !blocked.Load() || exitCode(err) != wantExit {
				t.Fatalf("did not reach %s: %v", phase, err)
			}
			var r profileReceipt
			if e := json.Unmarshal(out.Bytes(), &r); e != nil {
				t.Fatal(e)
			}
			failure := r.ImportFailure
			if failure == nil || failure.Code != "input_import_failed" || failure.Phase != phase || failure.Cause != wantCause || !strings.Contains(failure.NextStep, "shared_inputs.NAME.nas") {
				t.Fatalf("missing phase/cause/next step: %v %s", err, out.String())
			}
			if phase == "upload" && (failure.ProgressBytes != 4<<20 || failure.SizeBytes != size || failure.ProgressKind != "acknowledged") {
				t.Fatalf("lost upload offset: %+v", failure)
			}
			if phase == "commit" && (failure.ProgressBytes < size || failure.ProgressBytes != failure.SizeBytes || failure.ProgressKind != "uploaded") {
				t.Fatalf("lost completed upload: %+v", failure)
			}
			if phase != "arming" && (r.JobID != "" || r.Status != "intent") || r.Observation != "not_armed" || phase == "arming" && r.JobID == "" {
				t.Fatalf("incorrect receipt: %+v", r)
			}
			var saved profileReceipt
			if e := readProfileJSON(filepath.Join(root, ".mycelium/receipts", profileHash([]string{"test", "import"}), "receipt.json"), &saved); e != nil || saved.ImportFailure == nil || saved.ImportFailure.Phase != phase {
				t.Fatalf("failure not persisted: %v", e)
			}
			if strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), "fixture") || strings.Contains(err.Error(), root) {
				t.Fatal("diagnostic leaked transport or path")
			}
			t.Logf("%v", err)
		})
	}
}
