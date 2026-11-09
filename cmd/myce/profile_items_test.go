package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mycelium/internal/myce"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func profileGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid"}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
}

func TestProfilePerItemDev(t *testing.T) {
	root := t.TempDir()
	write := func(name, data string) {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// One committed profile, common data, and a caller-supplied item per run.
	// Hermetic worker defaults disable process-memory sampling; queue tests
	// exercise measured memory reuse with deterministic accepted peaks.
	profile := fmt.Sprintf(`name: items
project: items
class: experiment
source: {git: HEAD, paths: [scripts/]}
run: {command: [sh, scripts/process.sh]}
inputs: {common: common.txt}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: %s, arch: %s, platform: cpu}]
outputs: {result: results}
check: [test, -s, result]
`, runtime.GOOS, runtime.GOARCH)
	write(".mycelium/profiles/items.yaml", profile)
	write(".gitignore", ".mycelium/receipts/\nitems/\n")
	write("scripts/process.sh", "test -z \"$MYCE_TOKEN\"\ncat common item > result\nprintf '%s' \"$1\" >> result\nsleep 1\n")
	write("common.txt", "common\n")
	write("docs.txt", "unrelated\n")
	for _, item := range []string{"one", "two"} {
		write("items/"+item, item+"\n")
	}
	profileGit(t, root, "init", "--quiet")
	profileGit(t, root, "add", ".")
	profileGit(t, root, "commit", "--quiet", "-m", "fixture")
	binary := devTestBinary(t)
	state := t.TempDir()
	const token = "item-fixture-token"
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), FileStore: filepath.Join(state, "files"), Checkpoints: filepath.Join(state, "checkpoints"), Token: token, GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	invoke := func(checkout, item, arg string) profileReceipt {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "run", "--profile", "items", "--queue", d.URL, "--input", "item="+filepath.Join(checkout, "items", item), "--", arg)
		cmd.Dir = checkout
		cmd.Env = append(os.Environ(), "MYCE_TOKEN="+token, "MYCE_NOTIFY=")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("run: %v %s %s", err, stderr.String(), out)
		}
		var r profileReceipt
		if err = json.Unmarshal(out, &r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	finish := func(r profileReceipt) *myce.Job {
		t.Helper()
		j, err := d.Client().Wait(ctx, r.JobID)
		if err != nil || j.State != "succeeded" {
			t.Fatalf("job: %+v %v", j, err)
		}
		dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"items", r.Key}))
		wait(t, ctx, func() bool {
			lock, e := profileLock(filepath.Join(dir, "watch.lock"))
			if e != nil {
				return false
			}
			lock.Close()
			return true
		})
		return j
	}
	var first profileReceipt
	for i, item := range []string{"one", "two"} {
		r := invoke(root, item, item)
		j := finish(r)
		if i == 0 {
			first = r
		} else {
			if r.Spec.ProfileMemory != first.Spec.ProfileMemory {
				t.Fatal("items have different profile memory identities")
			}
			if r.JobID == first.JobID {
				t.Fatal("different items adopted same job")
			}
		}
		output := filepath.Join(t.TempDir(), "result")
		if err := d.Client().DownloadResult(ctx, j.ID, "result", output); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(output)
		if string(got) != "common\n"+item+"\n"+item {
			t.Fatalf("output: %q", got)
		}
	}
	// A fresh clone has no local receipts. Same bytes at a different input path
	// and an unrelated source commit must still produce the identical wire spec.
	write("docs.txt", "changed unrelated\n")
	profileGit(t, root, "add", "docs.txt")
	profileGit(t, root, "commit", "--quiet", "-m", "unrelated")
	clone := filepath.Join(t.TempDir(), "clone")
	profileGit(t, root, "clone", "--quiet", root, clone)
	if err := os.MkdirAll(filepath.Join(clone, "items"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(clone, "items", "one"), []byte("one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if r := invoke(clone, "one", "one"); r.JobID != first.JobID {
		t.Fatalf("fresh unrelated replay: %s != %s", r.JobID, first.JobID)
	}
	// Changed content must create another job.
	write("items/one", "changed item\n")
	changed := invoke(root, "one", "one")
	finish(changed)
	if changed.JobID == first.JobID {
		t.Fatal("changed item did not rerun")
	}

}
