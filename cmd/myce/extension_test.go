package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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
	t.Setenv("MYCE_TOKEN", "fixture-not-inherited")
	if err := extensionCommand([]string{"add", "--local", "--root", root, folder}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"setup", "check"} {
		if err := extensionCommand([]string{action, "--root", root, "--gpu", "fake", "--environment-cache", t.TempDir(), "--environment-cache-bytes", "1048576", "fixture.stage@1"}); err != nil {
			t.Fatal(err)
		}
	}
	reports := myce.LocalExtensionReports(root, "fake", "")
	if len(reports) != 1 || reports[0].Target == nil || reports[0].Target.OS != runtime.GOOS {
		t.Fatal("variant target lost", reports)
	}
}
