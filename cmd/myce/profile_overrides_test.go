package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProfileUrgencyOverridesFrozenReceipt(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	profile := `name: fixture
project: test
class: experiment
priority: experiment
deadline: 2030-01-02T03:04:05Z
source: {directory: source}
run: {command: [true]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	path := filepath.Join(dir, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(refuseProfilePreparation))
	defer server.Close()
	invoke := func(extra ...string) (profileReceipt, error) {
		var out bytes.Buffer
		args := append([]string{"--profile", path, "--queue", server.URL, "--token", "fixture"}, extra...)
		err := runCommand(context.Background(), args, &out, io.Discard)
		var receipt profileReceipt
		if out.Len() > 0 {
			if e := json.Unmarshal(out.Bytes(), &receipt); e != nil {
				t.Fatal(e)
			}
		}
		return receipt, err
	}
	base, err := invoke("--key", "base")
	if exitCode(err) != 69 || base.Spec.Class != "experiment" || base.Spec.Priority != "experiment" || base.Spec.Deadline == nil || base.Spec.Deadline.Format("2006-01-02T15:04:05Z07:00") != "2030-01-02T03:04:05Z" {
		t.Fatalf("base: %+v %v", base.Spec, err)
	}
	got, err := invoke("--key", "override", "--class", "deadline", "--deadline", "2031-02-03T04:05:06Z")
	if exitCode(err) != 69 || got.Spec.Class != "deadline" || got.Spec.Priority != "deadline" || got.Spec.Deadline == nil || got.Spec.Deadline.Format("2006-01-02T15:04:05Z07:00") != "2031-02-03T04:05:06Z" {
		t.Fatalf("override: %+v %v", got.Spec, err)
	}
	custom, err := invoke("--key", "custom", "--class", "custom")
	if exitCode(err) != 69 || custom.Spec.Class != "custom" || custom.Spec.Priority != "" {
		t.Fatalf("custom class: %+v %v", custom.Spec, err)
	}
	var saved profileReceipt
	if err := readProfileJSON(filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", "override"}), "receipt.json"), &saved); err != nil || saved.Spec.Class != got.Spec.Class || !saved.Spec.Deadline.Equal(*got.Spec.Deadline) {
		t.Fatalf("saved receipt: %+v %v", saved.Spec, err)
	}
	_, err = invoke("--key", "override", "--class", "production")
	if err == nil || !strings.Contains(err.Error(), "intent") {
		t.Fatalf("changed intent: %v", err)
	}
	for _, flags := range [][]string{{"--class", "bad/class"}, {"--class", ""}, {"--deadline", "invalid"}, {"--cpu", "2"}, {"--detach"}, {"--priority", "production"}} {
		if _, err := invoke(flags...); err == nil || exitCode(err) == 69 {
			t.Errorf("%v accepted: %v", flags, err)
		}
	}
}
