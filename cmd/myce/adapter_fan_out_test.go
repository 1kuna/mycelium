package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// A generic staged pipeline: the adapter must submit the existing API graph,
// including its frozen native sidecar, rather than invent an orchestration layer.
func fanOutAdapterYAML(parent string) string {
	after := ""
	if parent != "" {
		after = fmt.Sprintf("    after: {job: %s, outputs: {first/input: first/result}}\n", parent)
	}
	return fmt.Sprintf(`schema: 1
project: pipeline-fixture
default: stage
entries:
  stage:
    source: {directory: source}
    class: production
    start_window: 600s
    execution:
      main: {command: [sh, '{extension}/run.sh']}
      sidecars: [{name: helper, command: [sh, '{extension}/helper.sh'], memory_gb: 0.05}]
    needs: {cpu_cores: 1, memory_gb: 0.25, platform: cpu}
    targets: [{os: %s, arch: %s, platform: cpu}]
    checks: {qualification: [sh, '{extension}/check.sh']}
    baselines:
      extension:stage: {input: baseline, output: baseline, measure: exact, native: fixture, variation: exact}
    outputs: {result: results}
    workload: {input_shape: fixture, units: 1}
    fan_out: {max_concurrency: 1, pieces: [{key: first, args: [first]}, {key: tail, args: [tail]}]}
%s`, runtime.GOOS, runtime.GOARCH, after)
}

func TestAdapterFanOutSharingOptIn(t *testing.T) {
	var defaultSpec []byte
	for _, policy := range []string{"", "share_xpu: false, ", "share_xpu: true, "} {
		data := strings.Replace(fanOutAdapterYAML(""), "fan_out: {", "fan_out: {"+policy, 1)
		a, err := decodeAdapter([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		p, _, err := adapterProfile(a, "stage")
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(p.Spec)
		if err != nil {
			t.Fatal(err)
		}
		if policy == "" {
			defaultSpec = encoded
		}
		if policy != "share_xpu: true, " {
			if !bytes.Equal(encoded, defaultSpec) || bytes.Contains(encoded, []byte("share_xpu")) {
				t.Fatal("default/false changed frozen submission bytes", string(encoded))
			}
		} else if !p.FanOut.ShareXPU || !bytes.Contains(encoded, []byte(`"share_xpu":true`)) {
			t.Fatal("adapter lost explicit sharing intent", string(encoded))
		}
	}
}

func TestAdapterWholeParentFanOutGraph(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	for name, data := range map[string]string{
		"run.sh": "printf '%s' \"$1\" > result\n", "helper.sh": "cat\n",
		"check.sh": "cat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\n", "baseline": "fixture\n",
	} {
		conversionWrite(t, filepath.Join(root, "source", name), data)
	}
	q, clock, db, checkpoints := waitQueue(t)
	if err := q.ConfigureFileStore(db+".files", 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(myce.API{Queue: q, Token: "fanout-fixture"}.Handler())
	defer server.Close()
	c := myce.Client{URL: server.URL, Token: "fanout-fixture"}

	// Independently author the native API recipe. Comparing the entire recipe
	// catches loss of sidecars or checks during adapter lowering.
	target := myce.ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}
	recipe := myce.Extension{Name: "stage", Version: "1", Purpose: "Project entry stage", DefaultClass: "production", Outputs: []string{"result"},
		Variants: []myce.ExtensionVariant{{Target: target,
			Needs: myce.ResourceNeeds{CPUCores: 1, MemoryGB: 0.25},
			Execution: myce.ExecutionSpec{Main: myce.ExecutionMember{Command: []string{"sh", "{extension}/run.sh"}},
				Sidecars: []myce.NativeSidecar{{Name: "helper", Command: []string{"sh", "{extension}/helper.sh"}, MemoryGB: 0.05}}},
			Check:     []string{"sh", "{extension}/check.sh"},
			Baselines: map[string]myce.ExtensionBaseline{"extension:stage": {Input: "baseline", Output: "baseline", Measure: "exact", Native: "fixture", Variation: "exact"}},
		}}}
	archive := filepath.Join(t.TempDir(), "source.tar")
	ref, err := freezeProfileSource(ctx, root, profileSource{Directory: "source"}, archive)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := freezeAdapterExtension(recipe, archive)
	if err != nil {
		t.Fatal(err)
	}
	// Registration records the queue's default native sandbox policy separately
	// from the author-supplied recipe and its content digest.
	registered := frozen
	registered.SandboxDefault = true

	submitAdapter := func(parent, key string) (*myce.Job, myce.Spec) {
		t.Helper()
		a, err := decodeAdapter([]byte(fanOutAdapterYAML(parent)))
		if err != nil {
			t.Fatal(err)
		}
		p, data, err := adapterProfile(a, "")
		if err != nil || !reflect.DeepEqual(p.Native, &recipe) {
			t.Fatalf("native recipe differs from API recipe: %+v, %v", p.Native, err)
		}
		var out, diagnostics bytes.Buffer
		// Missing notification configuration returns 74 after durable submit,
		// keeping this queue test free of detached observer processes.
		if err := runPreparedProfile(ctx, root, p, data, nil, nil, key, filepath.Join(root, "absent-notifier"), "", "", server.URL, "fanout-fixture", &out, &diagnostics); exitCode(err) != 74 {
			t.Fatalf("adapter submit: %v %s %s", err, &out, &diagnostics)
		}
		var receipt profileReceipt
		if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
			t.Fatal(err)
		}
		want := myce.Spec{Name: "stage", Project: "pipeline-fixture", Class: "production", StartWindow: "600s", SubmissionKey: key,
			AuthoringOrigin: myce.OriginAdapter,
			Extension:       frozen.Name + "@" + frozen.Version, ExtensionDigest: frozen.Digest, PrepareSource: true,
			Files: myce.FileManifest{".myce-source.tar": ref}, Targets: []myce.ExecutionTarget{target},
			Outputs: map[string]string{"result": "results"}, Workload: &myce.Workload{InputShape: "fixture", Units: 1},
			FanOut: &myce.FanOut{MaxConcurrency: 1, Pieces: []myce.FanOutPiece{{Key: "first", Args: []string{"first"}}, {Key: "tail", Args: []string{"tail"}}}}}
		if parent != "" {
			want.After = &myce.PieceDependency{Job: parent, Outputs: map[string]string{"first/input": "first/result"}}
		}
		if !reflect.DeepEqual(receipt.Spec, want) {
			t.Fatalf("submitted graph node differs structurally:\ngot %+v\nwant %+v", receipt.Spec, want)
		}
		// Same durable key and independently authored API intent must adopt the
		// adapter's actual queue node, not create or conflict with another node.
		api, err := c.Submit(ctx, want)
		if err != nil || api.ID != receipt.JobID {
			t.Fatalf("API intent did not adopt adapter node %s: %v", receipt.JobID, err)
		}
		// Compare the wire structure: JSON omits empty maps in the frozen recipe.
		gotRecipe, _ := json.Marshal(api.Extensions)
		wantRecipe, _ := json.Marshal([]myce.Extension{registered})
		if !bytes.Equal(gotRecipe, wantRecipe) {
			t.Fatalf("frozen API recipe differs:\ngot %s\nwant %s", gotRecipe, wantRecipe)
		}
		return api, want
	}
	parent, _ := submitAdapter("", "producer")
	child, childSpec := submitAdapter(parent.ID, "consumer")
	clock.advance(20 * time.Minute)
	blocked, err := c.GetJob(ctx, child.ID)
	if err != nil || blocked.Dependency == nil || blocked.Dependency.State != "blocked" || blocked.Attempt != 0 || blocked.FanOut.Next != 0 || blocked.State != "queued" {
		t.Fatalf("blocked fan-out spent its start window or ran early: %+v %v", blocked, err)
	}
	if _, err := q.CancelJob(parent.ID); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Second)
	cancelled, err := c.GetJob(ctx, child.ID)
	if err != nil || cancelled.State != "cancelled" || cancelled.Attempt != 0 || cancelled.FanOut.Next != 0 {
		t.Fatalf("upstream cancellation missed blocked adapter fan-out: %+v %v", cancelled, err)
	}
	// Reopen through the queue API; no direct storage access. The same key and
	// future dependents must retain the cancellation fence.
	server.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = waitOpenQueue(t, db, checkpoints, clock)
	replay, err := q.Submit(childSpec)
	if err != nil || replay.ID != child.ID || replay.State != "cancelled" {
		t.Fatalf("restart lost keyed cancellation: %+v %v", replay, err)
	}
	childSpec.SubmissionKey = "late-consumer"
	late, err := q.Submit(childSpec)
	if err != nil || late.State != "cancelled" || late.Attempt != 0 || late.FanOut.Next != 0 {
		t.Fatalf("future dependent escaped cancellation fence: %+v %v", late, err)
	}
}

func TestAdapterWholeParentFanOutRestrictions(t *testing.T) {
	valid := fanOutAdapterYAML("parent")
	for _, tc := range []struct{ name, yaml, reason string }{
		{"piece", strings.Replace(valid, "job: parent,", "job: parent, piece: first,", 1), "omit after.piece"},
		{"result-check", strings.Replace(valid, "checks: {qualification:", "checks: {result: [true], qualification:", 1), "after/check"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeAdapter([]byte(tc.yaml)); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("unsupported dependent fan-out accepted: %v", err)
			}
		})
	}
}
