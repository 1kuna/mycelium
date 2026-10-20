package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestExtensionCLIComposition(t *testing.T) {
	folder, root := t.TempDir(), t.TempDir()
	description := fmt.Sprintf(`name: fixture.stage
version: '1'
purpose: Wrap an unchanged command
variants:
  - target: {os: %s, arch: %s, platform: fake}
    needs: {memory_gb: 1}
    execution:
      main: {command: [sh, stage.sh]}
    check: [sh, check.sh]
    baselines:
      extension:fixture.stage:
        input: input
        output: output
        measure: exact
        native: synthetic laptop fixture
        variation: exact fixture
`, runtime.GOOS, runtime.GOARCH)
	for name, body := range map[string]string{"extension.yaml": description, "stage.sh": "exit 0\n", "check.sh": "test -z \"$MYCE_TOKEN\" || exit 99\ncat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\n", "input": "fixture\n", "output": "fixture\n"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	devRoot := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(devRoot, "queue.db"), Checkpoints: filepath.Join(devRoot, "checkpoints"), HostID: "cli-host", Token: "fixture-not-inherited", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	client := d.Client()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var snapshot myce.Snapshot
		if err = client.Call(context.Background(), "GET", "/api/status", nil, &snapshot); err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Machines) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker startup")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Setenv("MYCE_QUEUE", d.URL)
	t.Setenv("MYCE_TOKEN", "fixture-not-inherited")
	if err := extensionCommand([]string{"add", "--local", "--root", root, folder}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"setup", "check"} {
		if err := extensionCommand([]string{action, "--root", root, "--gpu", "fake", "--device", "0", "--host", "cli-host", "--environment-cache", t.TempDir(), "--environment-cache-bytes", "1048576", "fixture.stage@1"}); err != nil {
			t.Fatal(err)
		}
	}
	reports := myce.LocalExtensionReports(root, "fake", "")
	if len(reports) != 1 || reports[0].Target == nil || reports[0].Target.OS != runtime.GOOS {
		t.Fatal("variant target lost", reports)
	}
}
