package myce

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExtensionSmokeSuccessRemainsUnqualified(t *testing.T) {
	for _, mode := range []string{"main-smoke", "check-without-baseline"} {
		t.Run(mode, func(t *testing.T) {
			e := compositionExtension(t, false)
			e.Kinds = nil
			e.Variants[0].Check = nil
			if mode == "check-without-baseline" {
				e.Variants[0].Check = []string{"sh", "-c", "exit 0"}
			}
			e.Variants[0].Baselines = nil
			e.Variants[0].Execution.Main.Command = []string{"sh", "-c", "exit 0"}
			e = freezeComposition(t, e)
			root := t.TempDir()
			ext := filepath.Join(root, "extensions")
			if err := InstallExtension(ext, e); err != nil {
				t.Fatal(err)
			}
			d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "state", "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: ext, Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			awaitTestDevDeviceFingerprints(t, d)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reports, err := CheckExtension(ctx, ext, e, "fake", "0", "dev-host", io.Discard, &EnvironmentCache{Root: filepath.Join(root, "cache"), Limit: 4 << 20}, d.Client())
			if err != nil || len(reports) != 1 || !reports[0].Setup {
				t.Fatal(reports, err)
			}
			for _, r := range append(reports, readExtensionReports(ext, e, "fake")...) {
				if r.Qualified || !strings.Contains(r.Reason, "ran OK but not qualified") || !strings.Contains(r.Reason, "baselines[") || !strings.Contains(r.Reason, "extension:"+e.Name) {
					t.Fatalf("false smoke qualification: %+v", r)
				}
			}
			j := submit(t, d.Queue, Spec{Name: "ordinary", Project: "demo", Extension: extensionKey(e)})
			snapshot, err := d.Queue.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range snapshot.Machines {
				m.ExtensionReports = reports
				if extensionCanRun(j, m) {
					t.Fatal("smoke-only extension eligible for placement")
				}
			}
		})
	}
}
