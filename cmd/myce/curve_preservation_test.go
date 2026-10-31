package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestCurvePreservationCommand(t *testing.T) {
	root := t.TempDir()
	write := func(name string, value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, name)
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	snap := myce.Snapshot{Now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	before := write("before.json", snap)
	after := write("after.json", snap)
	ledgers := write("ledgers.json", []myce.JobLedgerPage{})
	args := []string{"--before", before, "--after", after, "--ledgers", ledgers}
	var out bytes.Buffer
	if err := curvePreservationCommand(args, &out); err != nil || !json.Valid(out.Bytes()) {
		t.Fatal(out.String(), err)
	}
	snap.WorkCurves = []myce.WorkObservation{{Samples: 1}}
	write("after.json", snap)
	out.Reset()
	if err := curvePreservationCommand(args, &out); err == nil || !json.Valid(out.Bytes()) {
		t.Fatal("missing accepted provenance passed or missing receipt", out.String(), err)
	}
}
