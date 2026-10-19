package main

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/myce"
)

func TestPersistedConversionCLI(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	fixture := filepath.Join("../../internal/myce/testdata/release-7285a88")
	if e := filepath.WalkDir(fixture, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(fixture, path)
		if e != nil {
			return e
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(source, rel), 0700)
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		return os.WriteFile(filepath.Join(source, rel), b, 0600)
	}); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(t.TempDir(), "current")
	args := []string{"--source", source, "--target", target, "--host-id", "fixture-host"}
	for _, mode := range []string{"--dry-run", "--apply"} {
		var out bytes.Buffer
		if e := stateConvertCommand(append(args, mode), &out); e != nil {
			t.Fatal(e)
		}
		var m myce.PersistedConversionManifest
		if e := json.Unmarshal(out.Bytes(), &m); e != nil || m.Jobs != 103 || m.Copies != 206 {
			t.Fatal("conversion output", e)
		}
	}
	// Dispatch must happen before ordinary token/config loading. No credentials
	// exist in this synthetic HOME and no live environment file is consulted.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	if e := runWithProbe(append([]string{"state", "convert"}, append(args, "--dry-run")...), myce.MachineProbe{}); e != nil {
		t.Fatal(e)
	}
}
