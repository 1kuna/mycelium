package myce

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// This is the myce dev runtime, real authenticated API, qualification, command
// runner and verified artifact transfer; no queue result is fabricated.
func TestPieceDependencyDevCPU(t *testing.T) {
	for _, transport := range []string{"file-store", "nas"} {
		t.Run(transport, func(t *testing.T) {
			root := newTestStateRoot(t)
			c := DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), ExtensionRoot: filepath.Join(root, "extensions"), JobFileRoot: filepath.Join(root, "nas"), Token: "piece-dev-test", GPU: "cpu", Workers: 2, CPUBudgetCores: 4, Interval: 10 * time.Millisecond}
			if transport == "file-store" {
				c.FileStore = filepath.Join(root, "files")
			} else {
				c.NASCheckpoints = true
			}
			if err := os.MkdirAll(c.JobFileRoot, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("MYCE_TOKEN", c.Token)
			// Two plain commands use an external fixture marker to hold the final piece.
			// The marker is test coordination, never a readiness/dispatch signal.
			e := compositionExtension(t, false)
			e.Kinds = nil
			e.Name = "piece-dev"
			e.Version = "1"
			v := &e.Variants[0]
			v.Target = ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}
			v.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}
			v.Execution.Main.Command = []string{"sh", "{extension}/produce.sh"}
			v.Baselines = map[string]ExtensionBaseline{"extension:piece-dev": e.Variants[0].Baselines["extension:sample"]}
			v.References = map[string]WorkReference{"extension:piece-dev": {InputShape: "tiny", Unit: "piece", Units: 1}}
			e.Files["produce.sh"] = []byte("test -z \"$MYCE_TOKEN\" || exit 99\nif [ \"$1\" = tail ]; then while [ ! -f \"$2\" ]; do sleep .02; done; fi\nprintf '%s' \"$1\" > out\n")
			e = freezeComposition(t, e)
			if err := InstallExtension(c.ExtensionRoot, e); err != nil {
				t.Fatal(err)
			}
			sink := compositionExtension(t, false)
			sink.Kinds = nil
			sink.Name = "piece-sink"
			sink.Version = "1"
			sv := &sink.Variants[0]
			sv.Target = v.Target
			sv.Needs = v.Needs
			sv.Execution.Main.Command = []string{"sh", "{extension}/consume.sh"}
			sv.Baselines = map[string]ExtensionBaseline{"extension:piece-sink": sv.Baselines["extension:sample"]}
			sink.Files["consume.sh"] = []byte("test -z \"$MYCE_TOKEN\" || exit 99\ncat input > final\n")
			sink = freezeComposition(t, sink)
			if err := InstallExtension(c.ExtensionRoot, sink); err != nil {
				t.Fatal(err)
			}
			d, err := StartDev(c)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if d != nil {
					if err := d.Close(); err != nil {
						t.Error(err)
					}
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			probeWait(t, ctx, func() bool { snap, err := d.Queue.Snapshot(); return err == nil && len(snap.Machines) == 2 })
			cache := &EnvironmentCache{Root: filepath.Join(root, "workers", "fake-1", "environment-cache")}
			if _, err := CheckExtension(ctx, c.ExtensionRoot, e, "cpu", "", "dev-host", io.Discard, cache, d.Client()); err != nil {
				t.Fatal("CPU qualification", err)
			}
			if _, err := CheckExtension(ctx, c.ExtensionRoot, sink, "cpu", "", "dev-host", io.Discard, cache, d.Client()); err != nil {
				t.Fatal("consumer CPU qualification", err)
			}
			gate := filepath.Join(root, "finish-tail")
			producer := Spec{Name: "producer", Project: "pieces", Priority: "experiment", SubmissionKey: "producer", Extension: extensionKey(e), Workload: &Workload{InputShape: "tiny", Units: 1}, Outputs: map[string]string{"out": "results"}, FanOut: &FanOut{MaxConcurrency: 1, Pieces: []FanOutPiece{{Key: "first", Args: []string{"first", gate}}, {Key: "tail", Args: []string{"tail", gate}}}}}
			p, err := d.Client().Submit(ctx, producer)
			if err != nil {
				t.Fatal(err)
			}
			consumer := Spec{Name: "consumer", Project: "pieces", Priority: "experiment", SubmissionKey: "consumer", After: &PieceDependency{Job: p.ID, Piece: "first", Outputs: map[string]string{"input": "out"}}, Run: RunSpec{Command: []string{"sh", "-c", "test -z \"$MYCE_TOKEN\" || exit 99; cat input > final"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}, Outputs: map[string]string{"final": "results"}}
			consumer.Extension = extensionKey(sink)
			consumer.Run = RunSpec{}
			consumer.Needs = ResourceNeeds{}
			first, err := d.Client().SubmitAndWait(ctx, consumer)
			if err != nil || first.State != "succeeded" {
				t.Fatalf("consumer %v %+v", err, first)
			}
			pending, err := d.Client().GetJob(ctx, p.ID)
			if err != nil || pending.State != "queued" || len(pending.FanOut.Results) != 1 {
				t.Fatal("consumer did not precede gather", pending, err)
			}
			output := filepath.Join(root, "consumed")
			if transport == "file-store" {
				err = d.Client().DownloadResult(ctx, first.ID, "final", output)
			} else {
				var b []byte
				b, err = os.ReadFile(filepath.Join(c.JobFileRoot, first.OutputFiles["final"]))
				if err == nil {
					err = os.WriteFile(output, b, 0600)
				}
			}
			data, readErr := os.ReadFile(output)
			if err != nil || readErr != nil || string(data) != "first" {
				t.Fatalf("consumed bytes %q %v %v", data, err, readErr)
			}
			if err := os.WriteFile(gate, []byte("release"), 0600); err != nil {
				t.Fatal(err)
			}
			if p, err = d.Client().Wait(ctx, p.ID); err != nil || p.State != "succeeded" {
				t.Fatal("gather", err)
			}
			accepted := first.Dependency.Execution
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = nil
			d, err = StartDev(c)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := d.Client().Submit(ctx, consumer)
			if err != nil || replay.ID != first.ID || replay.Dependency.Execution != accepted {
				t.Fatal("restart/replay lost immutable binding", err)
			}
			// Abort a running registered consumer even though producer gather succeeded.
			abort := consumer
			abort.Name = "abort-consumer"
			abort.SubmissionKey = "abort-consumer"
			abort.Outputs = nil
			abort.Extension = ""
			abort.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 0.1}
			abort.Run.Command = []string{"sh", "-c", "test -z \"$MYCE_TOKEN\" || exit 99; cat input >/dev/null; while :; do sleep .02; done"}
			running, err := d.Client().Submit(ctx, abort)
			if err != nil {
				t.Fatal(err)
			}
			probeWait(t, ctx, func() bool { j, err := d.Client().GetJob(ctx, running.ID); return err == nil && j.StartedAt != nil })
			ack, err := d.Client().Cancel(ctx, p.ID)
			if err != nil || !ack.WillNotRun || *ack.Release != "pending" {
				t.Fatal("abort did not aggregate downstream custody", ack, err)
			}
			probeWait(t, ctx, func() bool { ack, err := d.Client().Cancel(ctx, p.ID); return err == nil && *ack.Release != "pending" })
			stopped, err := d.Client().GetJob(ctx, running.ID)
			if err != nil || stopped.State != "cancelled" || !stopped.ReleaseConfirmed {
				t.Fatal("downstream cleanup", stopped, err)
			}
			future := consumer
			future.SubmissionKey = "after-abort"
			excluded, err := d.Client().Submit(ctx, future)
			if err != nil || excluded.State != "cancelled" {
				t.Fatal("late consumer escaped abort", err)
			}
			t.Logf("%s: consumed %q before gather; bound execution %s attempt %d survived restart/replay; parent abort confirmed downstream cleanup", transport, data, accepted.ID, accepted.Attempt)
		})
	}
}
