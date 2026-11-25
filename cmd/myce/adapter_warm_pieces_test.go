package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

func TestAdapterPiecesWarmDevResults(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	helper, err := os.ReadFile(filepath.Join("..", "..", "clients", "python", "src", "mycelium", "warm_pieces.py"))
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{
		"source/run.py": `from warm_pieces import WarmPieces
with WarmPieces() as pieces:
    while item := pieces.next():
        assert len(item.args) == 1
        (item.output_dir / "result").write_text(item.args[0] + ":" + item.inputs["slot.txt"].read_text())
        pieces.success()
`,
		"source/warm_pieces.py": string(helper),
		"slots/a.txt":           "alpha",
		"slots/b.txt":           "beta",
	} {
		conversionWrite(t, filepath.Join(root, name), data)
	}
	d, err := myce.StartDev(myce.DevConfig{
		EnvironmentCacheLimit: 1 << 30, Address: net.JoinHostPort(net.IPv4(127, 0, 0, 1).String(), "0"),
		Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"),
		Token: "adapter-warm-fixture", GPU: "cpu", Workers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	yaml := strings.Replace(piecesAdapterYAML("source"), "[sh, run.sh]", "["+python+", run.py]", 1)
	yaml = strings.Replace(yaml, "    outputs:", "    advanced: {warm_pieces: fixed}\n    outputs:", 1)
	a, err := decodeAdapter([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	p, data, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	if p.WarmPieces != "fixed" || len(p.FanOut.Pieces) != 2 {
		t.Fatal("adapter lost authored warm mode or piece declarations")
	}
	var out, diagnostics bytes.Buffer
	ctx, cancel := context.WithTimeout(t.Context(), devTestTimeout)
	defer cancel()
	if err := runPreparedProfile(ctx, root, p, data, nil, nil, "warm-pieces", filepath.Join(root, "absent-notifier"), "", "", d.URL, "adapter-warm-fixture", &out, &diagnostics); exitCode(err) != 74 {
		t.Fatalf("submit: %v %s %s", err, &out, &diagnostics)
	}
	var receipt profileReceipt
	if err := json.Unmarshal(out.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	j, err := d.Client().Wait(ctx, receipt.JobID)
	if err != nil || j.State != "succeeded" || j.Attempt != 1 || len(j.FanOut.Children) != 0 {
		t.Fatalf("warm adapter wait: %v, job: %v", err, j)
	}
	destination := t.TempDir()
	if err := fetchResult(ctx, d.Client(), j.ID, destination, 0, "", &diagnostics); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"a": "first:alpha", "b": "second:beta"} {
		got, err := os.ReadFile(filepath.Join(destination, key, "result"))
		if err != nil || string(got) != want {
			t.Fatalf("result for %s = %q %v", key, got, err)
		}
	}
}
