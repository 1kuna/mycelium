package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// A real native command exits zero; all three supported reads must explain the
// later output refusal, even when optional diagnostic capture was not requested.
func TestResultFinalizationFailure(t *testing.T) {
	root := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "q.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), OutputDeliveryRoot: filepath.Join(root, "delivery"), Token: "fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c := d.Client()
	t.Setenv("MYCE_TOKEN", "fixture")
	input := filepath.Join(root, "input")
	if err := os.WriteFile(input, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := myce.IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.UploadJobInput(ctx, input, ref); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, command, step, class string
		async, diagnostics         bool
	}{
		{name: "missing", command: `test -z "$MYCE_TOKEN"; printf ok > command-receipt`, step: "output_path", class: "not_found"},
		{name: "directory", command: `test -z "$MYCE_TOKEN"; mkdir output; printf ok > command-receipt`, step: "output_validation", class: "not_regular"},
		{name: "async-directory", command: `test -z "$MYCE_TOKEN"; mkdir output; printf ok > command-receipt`, step: "output_validation", class: "not_regular", async: true},
		{name: "captured-directory", command: `test -z "$MYCE_TOKEN"; mkdir output`, step: "output_validation", class: "not_regular", diagnostics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := myce.Spec{Name: tc.name, Project: "test", SubmissionKey: tc.name, Files: myce.FileManifest{"input": ref}, Outputs: map[string]string{"output": "results"}, Run: myce.RunSpec{Command: []string{"sh", "-ec", tc.command}}, Needs: myce.ResourceNeeds{CPUCores: 1, MemoryGB: 0.125, Platform: myce.Platform{Required: []string{"cpu"}}}}
			if tc.async {
				spec.OutputDelivery = "async_nas"
				spec.Disk = &myce.DiskNeeds{OutputBytes: 1024}
			}
			if tc.diagnostics {
				spec.Diagnostics = &myce.DiagnosticSpec{}
			}
			j, err := c.SubmitAndWait(ctx, spec)
			if err != nil || j == nil || j.State != "failed" {
				t.Fatalf("terminal result: %+v %v", j, err)
			}
			got, err := c.GetJob(ctx, j.ID)
			if err != nil {
				t.Fatal(err)
			}
			cause := got.ExecutionCause
			if cause == nil || cause.Finalization == nil {
				t.Fatalf("GET lost finalization: %+v", got)
			}
			detail := cause.Finalization
			if cause.Class != tc.class || detail.Step != tc.step || detail.Output != "output" || detail.CommandExitCode == nil || *detail.CommandExitCode != 0 || len(got.OutputRefs) != 0 || got.Losses != 0 {
				t.Fatalf("wrong finalization: %+v cause=%+v", got, detail)
			}
			status := captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", c.URL, j.ID}) })
			for _, want := range []string{tc.step, tc.class, `output "output"`, "command exited 0"} {
				if !strings.Contains(status, want) {
					t.Fatalf("status missing %q: %s", want, status)
				}
			}
			dest := filepath.Join(t.TempDir(), "diagnostics")
			var stderr bytes.Buffer
			if err := fetchResult(ctx, c, j.ID, dest, j.Attempt, "", &stderr); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dest, "summary.json"))
			if err != nil {
				t.Fatal(err)
			}
			var summary myce.FailureSummary
			if err = json.Unmarshal(data, &summary); err != nil {
				t.Fatal(err)
			}
			if summary.Success || summary.ID != j.ID || summary.Attempt != j.Attempt || summary.ExecutionCause == nil || summary.ExecutionCause.Finalization == nil || summary.ExecutionCause.Finalization.Step != tc.step {
				t.Fatalf("result diagnostics lost cause: %s", data)
			}
			if strings.Contains(string(data), root) || strings.Contains(string(data), "http://") {
				t.Fatalf("summary leaked physical paths: %s", data)
			}
		})
	}
}
