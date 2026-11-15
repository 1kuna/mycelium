package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/statusdoc"
)

func TestGenerateCommandPreservesProse(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "docs"), 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "docs/status.md")
	prefix, suffix := "# Authored explanation\n\n", "\n\nKeep this explanation.\n"
	stale := "<!-- BEGIN GENERATED STATUS FIELDS -->\nstale\n<!-- END GENERATED STATUS FIELDS -->"
	if err := os.WriteFile(path, []byte(prefix+stale+suffix), 0644); err != nil {
		t.Fatal(err)
	}
	previousFlags, previousArgs := flag.CommandLine, os.Args
	t.Cleanup(func() { flag.CommandLine, os.Args = previousFlags, previousArgs })
	for range 2 {
		flag.CommandLine = flag.NewFlagSet("statusdoc", flag.ContinueOnError)
		os.Args = []string{"statusdoc", "-root", root}
		main()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != prefix+statusdoc.Fields()+suffix {
			t.Fatal("generation changed authored prose or failed to replace stale fields")
		}
	}
}
