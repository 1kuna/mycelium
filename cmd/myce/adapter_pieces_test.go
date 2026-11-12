package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func piecesAdapterYAML(source string) string {
	return fmt.Sprintf(`schema: 1
project: pieces-fixture
default: work
entries:
  work:
    source: {directory: %q}
    run: {command: [sh, run.sh]}
    needs: {cpu_cores: 1, memory_gb: 0.25, platform: cpu}
    targets: [{os: %s, arch: %s, platform: cpu}]
    outputs: {result: results}
    pieces:
      - {key: a, args: [first], inputs: {slot.txt: slots/a.txt}}
      - {key: b, args: [second], inputs: {slot.txt: slots/b.txt}}
`, source, runtime.GOOS, runtime.GOARCH)
}

func TestAdapterPiecesLowering(t *testing.T) {
	data := piecesAdapterYAML("source")
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	if p.FanOut == nil || len(p.FanOut.Pieces) != 2 || p.FanOut.Pieces[1].Key != "b" || !slices.Equal(p.FanOut.Pieces[1].Args, []string{"second"}) || p.FanOut.MaxConcurrency != 0 {
		t.Fatalf("pieces not lowered to fan_out: %+v", p.FanOut)
	}
	if p.Inputs[myce.PlainPiecePrefix+"a/slot.txt"] != "slots/a.txt" || len(a.Entries["work"].Inputs) != 0 {
		t.Fatalf("per-piece input not lowered, or entry mutated: %v", p.Inputs)
	}
	for name, bad := range map[string]string{
		"duplicate key": strings.Replace(data, "key: b", "key: a", 1),
		"reserved name": strings.Replace(data, "    outputs:", "    inputs: {.myce-pieces/a/x: x}\n    outputs:", 1),
		"with fan_out":  strings.Replace(data, "    outputs:", "    fan_out: {pieces: [{key: c}]}\n    outputs:", 1),
		"unclean dest":  strings.Replace(data, "slot.txt: slots/a.txt", "../slot.txt: slots/a.txt", 1),
	} {
		if _, err := decodeAdapter([]byte(bad)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// One submission and one wait run every piece and return results by key.
func TestAdapterPiecesDevResultsByKey(t *testing.T) {
	root := t.TempDir()
	for name, data := range map[string]string{
		"source/run.sh": "printf '%s:%s' \"$1\" \"$(cat slot.txt)\" > result\n",
		"slots/a.txt":   "alpha", "slots/b.txt": "beta",
	} {
		conversionWrite(t, filepath.Join(root, name), data)
	}
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "pieces-fixture-token", GPU: "cpu", Workers: 2, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	a, err := decodeAdapter([]byte(piecesAdapterYAML("source")))
	if err != nil {
		t.Fatal(err)
	}
	p, data, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	// Missing notification configuration returns 74 after durable submit.
	if err := runPreparedProfile(ctx, root, p, data, nil, nil, "pieces", filepath.Join(root, "absent-notifier"), "", "", d.URL, "pieces-fixture-token", &out, &diagnostics); exitCode(err) != 74 {
		t.Fatalf("submit: %v %s %s", err, &out, &diagnostics)
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	j, err := d.Client().Wait(ctx, receipt.JobID)
	if err != nil || j.State != "succeeded" {
		// A failed parent alone hides which piece failed and why (r6.12 Darwin full).
		var children []any
		if snap, serr := d.Queue.Snapshot(); serr == nil {
			for _, c := range snap.Jobs {
				children = append(children, map[string]any{"id": c.ID, "state": c.State, "reason": c.Reason, "attempt": c.Attempt, "losses": c.Losses, "finding": c.Finding, "history": c.AttemptHistory})
			}
		}
		detail, _ := json.Marshal(children)
		t.Fatalf("parent wait err=%v jobs=%s", err, detail)
	}
	for key, want := range map[string]string{"a": "first:alpha", "b": "second:beta"} {
		dest := filepath.Join(root, "out", key, "result")
		if err := d.Client().DownloadResult(ctx, j.ID, key+"/result", dest); err != nil {
			t.Fatal(err)
		}
		if b, err := os.ReadFile(dest); err != nil || string(b) != want {
			t.Fatalf("result for %s = %q %v", key, b, err)
		}
	}
}
