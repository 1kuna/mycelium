package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestAuthoringOriginReaderPreparation(t *testing.T) {
	for _, origin := range []string{myce.OriginProfile, myce.OriginAdapter} {
		t.Run(origin, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(root)
			conversionWrite(t, "source/run.sh", "echo ok\n")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
			defer server.Close()
			var out bytes.Buffer
			var err error
			if origin == myce.OriginProfile {
				conversionWrite(t, "profile.yaml", `name: work
project: example
class: experiment
source: {directory: source}
run: {command: [sh, run.sh]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`)
				err = runProfile(t.Context(), "profile.yaml", nil, nil, "origin", "", "", "", server.URL, "fixture", &out, io.Discard)
			} else {
				conversionWrite(t, "adapter.yaml", cpuAdapterYAML(filepath.Join(root, "source")))
				err = runAdapter(t.Context(), "adapter.yaml", "", nil, nil, "origin", "", "", "", server.URL, "fixture", &out, io.Discard)
			}
			// The unavailable queue leaves the exact prepared submission in its
			// receipt for recovery. No observer or job is created by this test.
			var receipt profileReceipt
			if exitCode(err) != 69 || json.Unmarshal(out.Bytes(), &receipt) != nil || receipt.Spec.AuthoringOrigin != origin {
				t.Fatalf("reader lost frozen origin: %v %s", err, &out)
			}
		})
	}
}

func TestAuthoringOriginJobSpecCommandAndStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.yaml")
	conversionWrite(t, path, validJobYAML)
	s, err := readJobSpec(path)
	if err != nil || s.AuthoringOrigin != myce.OriginJobSpec {
		t.Fatalf("job spec origin missing: %+v %v", s, err)
	}
	var submitted myce.Spec
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(myce.Job{ID: "origin-job", State: "queued", Spec: submitted})
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := runCommand(context.Background(), []string{"--queue", server.URL, "--token", "fixture", "--detach", "--", "echo", "ok"}, &out, io.Discard); err != nil || submitted.AuthoringOrigin != myce.OriginCommand {
		t.Fatalf("command origin missing: %+v %v", submitted, err)
	}
	output := captureJoiningOutput(t, func() error {
		printStatus(myce.Snapshot{Jobs: []*myce.Job{{ID: "origin-job", Spec: myce.Spec{AuthoringOrigin: myce.OriginAdapter}}}}, false)
		return nil
	})
	if !strings.Contains(output, "authoring origin: adapter") {
		t.Fatal(output)
	}
}
