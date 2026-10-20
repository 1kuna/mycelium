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
			d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "state", "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: ext, Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			await(t, func() bool { s, _ := d.Queue.Snapshot(); return len(s.Machines) > 0 })
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

func TestExtensionQualificationPassImpliesPlacementEligibleHost(t *testing.T) {
	for _, mode := range []string{"qualified", "no-baseline", "no-check", "failed-check", "wrong-host", "wrong-target", "wrong-digest", "wrong-platform", "wrong-environment", "wrong-check", "no-check-id", "no-setup", "wrong-fingerprint"} {
		t.Run(mode, func(t *testing.T) {
			e := compositionExtension(t, false)
			m := machine("local", "fake")
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
			m.HostID = "local-host"
			m.HostMemoryMeasured = true
			m.FreeHostMemoryGB = 64
			m.HostAvailableMemoryGB = 64
			m.ExtensionFingerprint = "fingerprint"
			r := newExtensionReport(e, &m, "extension:"+e.Name)
			r.Setup, r.Qualified, r.CheckID = true, true, "checked"
			switch mode {
			case "no-baseline":
				e.Variants[0].Baselines = nil
			case "no-check":
				e.Variants[0].Check = nil
			case "failed-check":
				r.Qualified = false
			case "wrong-host":
				r.Host = "another-host"
			case "wrong-target":
				r.Target = &ExecutionTarget{OS: "other", Arch: "other", Platform: "fake"}
			case "wrong-digest":
				r.Digest = "different"
			case "wrong-platform":
				r.Platform = "cuda"
			case "wrong-environment":
				r.EnvironmentIdentity = "different"
			case "wrong-check":
				r.CheckDigest = "different"
			case "no-check-id":
				r.CheckID = ""
			case "no-setup":
				r.Setup = false
			case "wrong-fingerprint":
				r.Fingerprint = "different"
			}
			m.ExtensionReports = []ExtensionReport{r}
			j := &Job{Spec: Spec{Extension: extensionKey(e)}, Extensions: []Extension{e}}
			passed := extensionQualificationReason(e, r, &m) == ""
			if passed != (mode == "qualified") {
				t.Fatalf("predicate result %t for %s", passed, mode)
			}
			if extensionCanRun(j, &m) != passed {
				t.Fatal("check verdict and placement eligibility disagree")
			}
			if passed {
				q, _ := fixture(t)
				if err := q.AddExtension(e); err != nil {
					t.Fatal(err)
				}
				placed := submit(t, q, Spec{Name: "qualified", Project: "demo", Extension: extensionKey(e)})
				if !fits(placed, &m, m.FreeMemoryGB) {
					t.Fatal("PASS host excluded by placement")
				}
				reply := check(t, q, m, nil)
				if reply.Job == nil || reply.Job.ID != placed.ID {
					t.Fatal("PASS host did not receive job", reply)
				}
			}
		})
	}
}
