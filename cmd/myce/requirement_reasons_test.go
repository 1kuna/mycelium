package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestRequirementReasonsAdapterAndProfileSubmission(t *testing.T) {
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "reasons-fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	var host string
	wait(t, ctx, func() bool {
		s, err := status(ctx, d.Client())
		if err == nil && len(s.Machines) == 1 {
			host = s.Machines[0].ID
			return true
		}
		return false
	})
	binary := devTestBinary(t)
	for _, format := range []string{"adapter", "profile"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "source", "run.sh"), []byte("printf ok > result\n"), 0600); err != nil {
				t.Fatal(err)
			}
			text := strings.Replace(cpuAdapterYAML("source"), "    source:", fmt.Sprintf("    allowed_machines: [%q]\n    requirement_reasons: {host: 'private explanation /private/input'}\n    source:", host), 1)
			if format == "profile" {
				text = fmt.Sprintf("name: work\nproject: test\nclass: experiment\nsource: {directory: source}\nrun: {command: [sh, run.sh]}\nneeds: {cpu_cores: 1, memory_gb: 0.25, platform: cpu}\ntargets: [{os: %s, arch: %s, platform: cpu}]\noutputs: {result: results}\ncheck: [test, -s, result]\nallowed_machines: [%q]\nrequirement_reasons: {host: 'private explanation /private/input'}\n", runtime.GOOS, runtime.GOARCH, host)
			}
			file := filepath.Join(root, format+".yaml")
			if err := os.WriteFile(file, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, binary, "run", "--"+format, file, "--queue", d.URL, "--key", "reason-"+format)
			command.Dir = root
			command.Env = append(os.Environ(), "MYCE_TOKEN=reasons-fixture", "MYCE_NOTIFY=")
			var stderr bytes.Buffer
			command.Stderr = &stderr
			out, err := command.Output()
			if err != nil {
				t.Fatal("submission", err, &stderr)
			}
			var receipt profileReceipt
			if err := json.Unmarshal(out, &receipt); err != nil || receipt.Spec.RequirementReasons["host"] != "private explanation /private/input" {
				t.Fatal("receipt lost authored map", err)
			}
			var saved profileReceipt
			path := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{receipt.Project, receipt.Key}), "receipt.json")
			if err := readProfileJSON(path, &saved); err != nil || saved.Spec.RequirementReasons["host"] != receipt.Spec.RequirementReasons["host"] {
				t.Fatal("durable receipt lost map", err)
			}
			got, err := d.Client().Wait(ctx, receipt.JobID)
			if err != nil || got.State != "succeeded" || len(got.RequirementReasons) != 1 || got.RequirementReasons[0] != (myce.RequirementReasonStatus{Constraint: "host", ReasonSource: "authored", Source: "spec.requirement_reasons[host]"}) || len(got.Spec.RequirementReasons) != 0 {
				t.Fatal("job GET lost safe reason/provenance", got.RequirementReasons, err)
			}
			snapshot, err := status(ctx, d.Client())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, j := range snapshot.Jobs {
				if j.ID == receipt.JobID {
					found = len(j.RequirementReasons) == 1 && j.RequirementReasons[0].ReasonSource == "authored"
					data, _ := json.Marshal(j)
					if bytes.Contains(data, []byte("private")) {
						t.Fatal("raw prose leaked in status")
					}
				}
			}
			if !found {
				t.Fatal("status lost reason provenance")
			}
		})
	}
	// Raw job YAML has only the shared Spec field, not the legacy request map.
	raw := filepath.Join(t.TempDir(), "job.yaml")
	text := fmt.Sprintf("name: raw-reason\nproject: test\nclass: experiment\nrun: {command: [echo, ok]}\nneeds: {cpu_cores: 1, memory_gb: 1, platform: cpu}\nallowed_machines: [%q]\nrequirement_reasons: {host: 'private raw-job explanation'}\n", host)
	if err := os.WriteFile(raw, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	if err := specCommand([]string{"check", "--resolve", "--json", "--queue", d.URL, "--token", "reasons-fixture", raw}, &report, &report); err != nil {
		t.Fatal("raw-job resolve", err, &report)
	}
	if strings.Contains(report.String(), "Host pin has no declared") || strings.Contains(report.String(), "private raw-job") {
		t.Fatal("raw-job explanation ignored or leaked", &report)
	}
}
