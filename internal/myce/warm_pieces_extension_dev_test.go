package myce

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The variant runs main from its private extension copy, not the staged
// input directory, so item paths must come from the worker.
const warmExtensionMain = `
import json, os, urllib.request
from pathlib import Path
from warm_pieces import WarmPieces

assert "MYCE_TOKEN" not in os.environ
assert Path.cwd().name == ".myce-extension", Path.cwd()
weights = json.loads(Path("model.json").read_text())
print("model-loaded", flush=True)
with WarmPieces() as pieces:
    while item := pieces.next():
        print("item:" + item.key, flush=True)
        assert urllib.request.urlopen(os.environ["MYCE_SIDECAR_HELPER_URL"], timeout=5).read() == b"ready"
        value = int(item.inputs["in"].read_text())
        (item.output_dir / "out").write_text(f"{item.key}:{value * weights['multiplier']}:{item.args[0]}:{os.environ['MYCE_DEVICE']}")
        pieces.success()
`

func warmExtension(t *testing.T) Extension {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	helper, err := os.ReadFile(filepath.Join("..", "..", "clients", "python", "src", "mycelium", "warm_pieces.py"))
	if err != nil {
		t.Fatal(err)
	}
	e := compositionExtension(t, false)
	e.Name, e.Version, e.Kinds = "warmstage", "1", nil
	e.Files["main.py"] = []byte(warmExtensionMain)
	e.Files["warm_pieces.py"] = helper
	e.Files["sidecar.py"] = []byte(warmDevSidecar)
	e.Files["model.json"] = []byte(`{"multiplier":3}`)
	v := &e.Variants[0]
	v.Target = ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "fake"}
	port := 0
	v.Execution = ExecutionSpec{
		Main:     ExecutionMember{Command: []string{python, "-u", "main.py"}, Workdir: "{extension}"},
		Sidecars: []NativeSidecar{{Name: "helper", Command: []string{python, "-u", "{extension}/sidecar.py", "{port}"}, Port: &port, Readiness: &NativeReadiness{Path: "/", Timeout: "15s"}}},
	}
	v.Baselines = map[string]ExtensionBaseline{"extension:warmstage": v.Baselines["extension:sample"]}
	v.References = map[string]WorkReference{"extension:warmstage": {InputShape: "item", Unit: "item", Units: 1}}
	return freezeComposition(t, e)
}

// An extension stage with a sidecar runs every declared item in one placed
// attempt on a fake accelerator: one model load, one sidecar start and one
// launch, with each item accepted under its key (#2187 slice 1).
func TestWarmPiecesDevExtensionSingleLoad(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "inherited-fixture-token")
	root := newTestStateRoot(t)
	c := DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"),
		FileStore: filepath.Join(root, "files"), ExtensionRoot: filepath.Join(root, "extensions"), Token: "warm-extension-fixture",
		Workers: 1, MemoryGB: 8, Interval: defaultWorkerInterval}
	e := warmExtension(t)
	if err := InstallExtension(c.ExtensionRoot, e); err != nil {
		t.Fatal(err)
	}
	d, err := startTestDev(t, c, func(q *Queue) {
		if err := q.AddExtension(e); err != nil {
			t.Fatal(err)
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
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	if err := d.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	client := d.Client()
	keys := []string{"a", "b", "c", "d"}
	s := Spec{Name: "warm-stage", Project: "warm", SubmissionKey: "warm-extension-stage", Extension: extensionKey(e), ExtensionDigest: e.Digest,
		Workload: &Workload{InputShape: "item", Units: 1}, Advanced: &Advanced{WarmPieces: warmPiecesFixed},
		Outputs: map[string]string{"out": "results"}, Files: FileManifest{}, FanOut: &FanOut{}}
	for i, key := range keys {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: key, Args: []string{"arg-" + key}})
		s.Files[PlainPiecePrefix+key+"/in"] = warmDevFile(t, ctx, client, strconv.Itoa(i+1))
	}
	j, err := client.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	final := awaitTerminal(t, ctx, client, j.ID)
	logs := warmDevLogs(t, ctx, client, j.ID)
	loads, sidecars := strings.Count(logs, "model-loaded"), strings.Count(logs, "sidecar-loaded")
	t.Logf("model loads=%d, sidecar loads=%d, accepted keys=%d, attempts=%d", loads, sidecars, len(final.FanOut.Results), final.Attempt)
	if final.State != "succeeded" || final.Attempt != 1 || loads != 1 || sidecars != 1 || final.FanOut.Warm.Loads != 1 ||
		len(final.FanOut.Results) != len(keys) || len(final.FanOut.Children) != 0 || final.Extension == nil || final.Extension.Digest != e.Digest {
		t.Fatalf("state %s (%s), attempt %d, warm %+v, log:\n%s", final.State, final.Reason, final.Attempt, final.FanOut.Warm, logs)
	}
	for i, key := range keys {
		if got := final.FanOut.Results[key].Execution; got != (Execution{ID: j.ID, Attempt: 1}) {
			t.Fatalf("%s accepted from %+v", key, got)
		}
		warmDevCheckOutput(t, ctx, client, j.ID, key, key+":"+strconv.Itoa(3*(i+1))+":arg-"+key+":"+ExtensionDevice("fake"))
	}
	// Only the useful parent and its automatic qualification check ran: no
	// per-item child jobs and no per-item launches.
	snapshot, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var other []string
	for _, got := range snapshot.Jobs {
		if got.ID != j.ID && automaticExtensionCheck(got) == nil {
			other = append(other, got.ID)
		}
	}
	if len(other) != 0 || len(final.FanOut.Warm.Failures) != 0 {
		t.Fatalf("unexpected per-item jobs %v", other)
	}
}
