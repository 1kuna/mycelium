package myce

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The upstream stage writes a declared audio output beside the segments. An
// item's args name a gate file it waits for and whether it fails.
const warmUpstreamMain = `
import os, sys, time
from pathlib import Path
from warm_pieces import WarmPieces

with WarmPieces() as pieces:
    while item := pieces.next():
        gate, mode = item.args
        while gate != "-" and not Path(gate).exists():
            time.sleep(0.05)
        print("upstream:" + item.key, flush=True)
        if mode == "fail":
            pieces.failure()
            continue
        (item.output_dir / "out").write_text("segments-" + item.key)
        (item.output_dir / "audio.wav").write_bytes(b"RIFF" + item.key.encode())
        pieces.success()
`

// The downstream stage loads once, then checks that each item's folder holds
// exactly its declared input, that no audio reached the attempt, and that the
// previous item's staged inputs were removed once its receipt was accepted.
const warmDownstreamMain = `
import os, sys, time
from pathlib import Path
from warm_pieces import WarmPieces

time.sleep(float(sys.argv[1]))
print("model-loaded", flush=True)
previous = None
with WarmPieces() as pieces:
    while item := pieces.next():
        print("item:" + item.key, flush=True)
        assert set(item.inputs) == {"segments.json"}, item.inputs
        folder = item.inputs["segments.json"].parent
        assert sorted(os.listdir(folder)) == ["segments.json"], os.listdir(folder)
        root = folder.parent.parent
        assert not [p for p in root.rglob("*") if "audio" in p.name or p.name.startswith("RIFF")], list(root.rglob("*"))
        assert previous is None or not previous.exists(), previous
        previous = folder
        segments = item.inputs["segments.json"].read_text()
        (item.output_dir / "labels").write_text(segments + ":labels")
        pieces.success()
`

func warmStageExtension(t *testing.T, name, main string, command ...string) Extension {
	t.Helper()
	python := warmStagePython(t)
	helper, err := os.ReadFile(filepath.Join("..", "..", "clients", "python", "src", "mycelium", "warm_pieces.py"))
	if err != nil {
		t.Fatal(err)
	}
	e := compositionExtension(t, false)
	e.Name, e.Version, e.Kinds = name, "1", nil
	e.Files["main.py"] = []byte(main)
	e.Files["warm_pieces.py"] = helper
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "fake"}
	v.Execution = ExecutionSpec{Main: ExecutionMember{Command: append([]string{python, "-u", "main.py"}, command...), Workdir: "{extension}"}}
	v.Baselines = map[string]ExtensionBaseline{"extension:" + name: v.Baselines["extension:sample"]}
	v.References = map[string]WorkReference{"extension:" + name: {InputShape: "item", Unit: "item", Units: 1}}
	return freezeComposition(t, e)
}

func startWarmStagesDev(t *testing.T, root string, extensions ...Extension) (*Dev, Client, context.Context) {
	t.Helper()
	c := DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"),
		FileStore: filepath.Join(root, "files"), ExtensionRoot: filepath.Join(root, "extensions"), Token: "warm-stages-fixture",
		Workers: 2, MemoryGB: 8, Interval: defaultWorkerInterval}
	for _, e := range extensions {
		if err := InstallExtension(c.ExtensionRoot, e); err != nil {
			t.Fatal(err)
		}
	}
	d, err := startTestDev(t, c, func(q *Queue) {
		for _, e := range extensions {
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() { logWarmStageFailures(t, d) })
	if err := d.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	return d, d.Client(), ctx
}

// Each stage keeps its own fake card. Automatic qualification checks share
// one card, which a gated upstream item would otherwise hold.
func warmStageSpec(e Extension, name, machine string, outputs map[string]string) Spec {
	return Spec{Name: name, Project: "warm", SubmissionKey: name, Extension: extensionKey(e), ExtensionDigest: e.Digest,
		Workload: &Workload{InputShape: "item", Units: 1}, Advanced: &Advanced{WarmPieces: warmPiecesFixed},
		Outputs: outputs, FanOut: &FanOut{}, AllowedMachines: []string{machine}}
}

func warmDownstreamPiece(producer, key string) FanOutPiece {
	return FanOutPiece{Key: key, After: &PieceDependency{Job: producer, Piece: key, Outputs: map[string]string{"segments.json": "out"}}}
}

// The test drives the producer from its own attempt tree, so neither payload
// needs to write outside its sandbox or see the host's temporary directory.
func releaseWarmStage(t *testing.T, root, producer, gate string) {
	t.Helper()
	work := attemptStorageRoot(Job{ID: producer, Attempt: 1}, filepath.Join(root, "workers", "fake-1"))
	if err := os.WriteFile(filepath.Join(work, ".myce-extension", gate), nil, 0600); err != nil {
		t.Fatal(err)
	}
}

// A downstream warm parent whose items depend on an upstream warm parent's
// items receives them in readiness order, not declaration order, with one
// model load. The upstream failure fails only the dependent item, and each
// item receives exactly its declared input, never the producer's audio.
func TestWarmPieceDependenciesDevReadyOrder(t *testing.T) {
	upstream := warmStageExtension(t, "upstreamstage", warmUpstreamMain)
	downstream := warmStageExtension(t, "downstreamstage", warmDownstreamMain, "3")
	root := newTestStateRoot(t)
	_, client, ctx := startWarmStagesDev(t, root, upstream, downstream)
	const gate = "downstream-accepted-r3"

	up := warmStageSpec(upstream, "upstream", "fake-1", map[string]string{"out": "results", "audio.wav": "results"})
	up.FanOut.Pieces = []FanOutPiece{{Key: "r3", Args: []string{"-", "ok"}}, {Key: "r1", Args: []string{gate, "ok"}}, {Key: "r2", Args: []string{"-", "fail"}}}
	producer, err := client.Submit(ctx, up)
	if err != nil {
		t.Fatal(err)
	}
	down := warmStageSpec(downstream, "downstream", "fake-2", map[string]string{"labels": "results"})
	down.FanOut.Pieces = []FanOutPiece{warmDownstreamPiece(producer.ID, "r1"), warmDownstreamPiece(producer.ID, "r2"), warmDownstreamPiece(producer.ID, "r3")}
	consumer, err := client.Submit(ctx, down)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		j, err := client.GetJob(ctx, consumer.ID)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(j.State) {
			t.Fatalf("downstream ended before r1 was released: %s (%s)", j.State, j.Reason)
		}
		_, accepted := j.FanOut.Results["r3"]
		return accepted
	})
	releaseWarmStage(t, root, producer.ID, gate)
	if final := awaitTerminal(t, ctx, client, producer.ID); final.State != "failed" || len(final.FanOut.Results) != 2 {
		t.Fatalf("upstream %s (%s) %+v", final.State, final.Reason, final.FanOut)
	}
	final := awaitTerminal(t, ctx, client, consumer.ID)
	logs := warmDevLogs(t, ctx, client, consumer.ID)
	t.Logf("downstream log:\n%s", logs)
	r3, r1 := strings.Index(logs, "item:r3"), strings.Index(logs, "item:r1")
	if final.State != "failed" || final.Attempt != 1 || strings.Count(logs, "model-loaded") != 1 || final.FanOut.Warm.Loads != 1 ||
		r3 < 0 || r1 < r3 || strings.Contains(logs, "item:r2") || len(final.FanOut.Warm.Failures) != 1 || final.FanOut.Warm.Failures["r2"] != (Execution{}) ||
		final.FanOut.Warm.Dependencies["r2"].State != "upstream_failed" || len(final.FanOut.Results) != 2 {
		t.Fatalf("downstream %s (%s), attempt %d, warm %+v", final.State, final.Reason, final.Attempt, final.FanOut.Warm)
	}
	for _, key := range []string{"r1", "r3"} {
		dst := filepath.Join(t.TempDir(), "labels")
		if err := client.DownloadResult(ctx, consumer.ID, key+"/labels", dst); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(dst); err != nil || string(got) != "segments-"+key+":labels" {
			t.Fatalf("%s labels = %q, %v", key, got, err)
		}
	}
}

// With no ready item the attempt keeps its reservation for at most its own
// measured load time, then requeues with no penalty and runs again once the
// item is ready.
func TestWarmPieceDependenciesDevIdleRequeue(t *testing.T) {
	upstream := warmStageExtension(t, "upstreamstage", warmUpstreamMain)
	downstream := warmStageExtension(t, "downstreamstage", warmDownstreamMain, "1")
	root := newTestStateRoot(t)
	d, client, ctx := startWarmStagesDev(t, root, upstream, downstream)
	const gate = "release-k2"

	up := warmStageSpec(upstream, "upstream", "fake-1", map[string]string{"out": "results", "audio.wav": "results"})
	up.FanOut.Pieces = []FanOutPiece{{Key: "k1", Args: []string{"-", "ok"}}, {Key: "k2", Args: []string{gate, "ok"}}}
	producer, err := client.Submit(ctx, up)
	if err != nil {
		t.Fatal(err)
	}
	down := warmStageSpec(downstream, "downstream", "fake-2", map[string]string{"labels": "results"})
	down.FanOut.Pieces = []FanOutPiece{warmDownstreamPiece(producer.ID, "k1"), warmDownstreamPiece(producer.ID, "k2")}
	consumer, err := client.Submit(ctx, down)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		snapshot, err := d.Queue.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range snapshot.Jobs {
			if j.ID == consumer.ID {
				return j.State == "queued" && j.Attempt == 1 && len(j.FanOut.Results) == 1
			}
		}
		return false
	})
	releaseWarmStage(t, root, producer.ID, gate)
	final := awaitTerminal(t, ctx, client, consumer.ID)
	logs := warmDevLogs(t, ctx, client, consumer.ID)
	if final.State != "succeeded" || final.Attempt != 2 || final.FanOut.Warm.Loads != 2 || final.Losses != 0 || final.InfrastructureRetries != 0 ||
		final.FanOut.Results["k1"].Execution.Attempt != 1 || final.FanOut.Results["k2"].Execution.Attempt != 2 {
		t.Fatalf("downstream %s (%s), attempt %d, losses %d, retries %d, warm %+v\n%s", final.State, final.Reason, final.Attempt, final.Losses, final.InfrastructureRetries, final.FanOut.Warm, logs)
	}
	if got := final.FanOut.Warm.LoadSeconds; got <= 0 {
		t.Fatalf("no measured load: %+v", final.FanOut.Warm)
	}
}
