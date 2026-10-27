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

	"gopkg.in/yaml.v3"
	"mycelium/internal/myce"
)

func cpuAdapterYAML(source string) string {
	return fmt.Sprintf(`schema: 1
project: adapter-fixture
default: work
entries:
  work:
    source: {directory: %q}
    run: {command: [sh, run.sh]}
    needs: {cpu_cores: 1, memory_gb: 0.25, platform: cpu}
    targets: [{os: %s, arch: %s, platform: cpu}]
    outputs: {result: results}
    checks: {result: [test, -s, result]}
`, source, runtime.GOOS, runtime.GOARCH)
}
func TestAdapterStrictSelectedEntryAndLiteralArgs(t *testing.T) {
	data := cpuAdapterYAML("source")
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	_, one, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	a.Entries["other"] = a.Entries["work"]
	_, two, err := adapterProfile(a, "")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(one, two) {
		t.Fatal("unrelated entry changed selected intent")
	}
	cases := []string{
		strings.Replace(data, "schema: 1", "schema: 2", 1),
		strings.Replace(data, "    checks:", "    unknown: value\n    checks:", 1),
		strings.Replace(data, "    checks:", "    environment: {name: tools, lock_file: requirements.txt}\n    checks:", 1),
		strings.Replace(data, "    checks:", "    variants: [{target: {os: linux, arch: amd64, platform: fake}}]\n    checks:", 1),
		strings.Replace(data, "    checks:", "    after: {job: parent, piece: item, outputs: {input: result}}\n    checks:", 1),
	}
	for i, bad := range cases {
		if _, e := decodeAdapter([]byte(bad)); e == nil {
			t.Errorf("case %d accepted invalid recipe", i)
		}
	}
	root := t.TempDir()
	value := "literal ' ; $(touch should-not-exist)"
	cmd := checkedAdapterCommand([]string{"sh", "-c", `printf '%s' "$1" > result`, "fixture"}, []string{"test", "-s", "result"})
	cmd = append(cmd, value)
	run := exec.Command(cmd[0], cmd[1:]...)
	run.Dir = root
	if b, e := run.CombinedOutput(); e != nil {
		t.Fatalf("%v %s", e, b)
	}
	b, e := os.ReadFile(filepath.Join(root, "result"))
	if e != nil || string(b) != value {
		t.Fatalf("argv %q %v", b, e)
	}
	if _, e = os.Stat(filepath.Join(root, "should-not-exist")); !os.IsNotExist(e) {
		t.Fatal("argv evaluated as shell")
	}
}
func TestAdapterDevCheckedArtifact(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("test -z \"${MYCE_TOKEN:-}\"\nprintf '%s' \"$1\" > result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "adapter.yaml")
	data := cpuAdapterYAML(source)
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "adapter-fixture-token", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	binary := devTestBinary(t)
	invoke := func(key string) (profileReceipt, error) {
		command := exec.CommandContext(ctx, binary, "run", "--adapter", path, "--queue", d.URL, "--key", key, "--", "literal; $()")
		command.Dir = root
		command.Env = append(os.Environ(), "MYCE_TOKEN=adapter-fixture-token", "MYCE_NOTIFY=")
		var out, stderr bytes.Buffer
		command.Stdout = &out
		command.Stderr = &stderr
		e := command.Run()
		var r profileReceipt
		if e != nil {
			return r, fmt.Errorf("%w: %s %s", e, out.String(), stderr.String())
		}
		e = json.Unmarshal(out.Bytes(), &r)
		return r, e
	}
	r, err := invoke("checked")
	if err != nil {
		t.Fatal(err)
	}
	j, err := d.Client().Wait(ctx, r.JobID)
	if err != nil || j.State != "succeeded" {
		t.Fatalf("job %v %v", j, err)
	}
	dest := filepath.Join(root, "accepted")
	if err = d.Client().DownloadResult(ctx, j.ID, "result", dest); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(dest)
	if err != nil || string(b) != "literal; $()" {
		t.Fatalf("result %q %v", b, err)
	}
	replay, err := invoke("checked")
	if err != nil || replay.JobID != r.JobID {
		t.Fatalf("replay %+v %v", replay, err)
	}
	// A successful main cannot publish an artifact whose authored result check fails.
	data = strings.Replace(data, "[test, -s, result]", "[false]", 1)
	if err = os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = invoke("failed-result")
	if err != nil {
		t.Fatal(err)
	}
	j, err = d.Client().Wait(ctx, r.JobID)
	if err != nil || j.State != "failed" || len(j.OutputRefs) != 0 {
		t.Fatalf("failed check %+v %v", j, err)
	}
}

func TestAdapterSourceExcludesOtherEntries(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	git := func(args ...string) {
		t.Helper()
		c := exec.Command("git", args...)
		c.Dir = root
		if b, e := c.CombinedOutput(); e != nil {
			t.Fatalf("git %v %s", e, b)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "script"), []byte("source"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, ".mycelium"), 0700); err != nil {
		t.Fatal(err)
	}
	adapter := filepath.Join(root, ".mycelium", "adapter.yaml")
	if err := os.WriteFile(adapter, []byte("first authored entries"), 0600); err != nil {
		t.Fatal(err)
	}
	git("init", "--quiet")
	git("add", ".")
	git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture")
	source := profileSource{Git: "HEAD", AdapterFile: ".mycelium/adapter.yaml"}
	one, id, err := identifyProfileSource(ctx, root, source)
	if err != nil {
		t.Fatal(err)
	}
	first, err := freezeProfileSource(ctx, root, one, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(adapter, []byte("unrelated entry edit"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = identifyProfileSource(ctx, root, source); err != nil {
		t.Fatal("uncommitted instruction file contaminated source", err)
	}
	git("add", ".")
	git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "other entry")
	two, id2, err := identifyProfileSource(ctx, root, source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := freezeProfileSource(ctx, root, two, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil {
		t.Fatal(err)
	}
	if id != id2 || first != second {
		t.Fatal("unrelated entry changed source or its frozen identity")
	}
}

func TestAdapterDevNativeColdAndWarmQualification(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"run.sh": "test -z \"${MYCE_TOKEN:-}\"\nprintf '%s' \"$1\" > result\n", "check.sh": "cat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\n", "input": "baseline", "native": "baseline"} {
		if err := os.WriteFile(filepath.Join(source, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	a := projectAdapter{Schema: 1, Project: "native-fixture", Default: "native-fixture", Entries: map[string]adapterEntry{"native-fixture": {
		Source: profileSource{Directory: source}, Spec: myce.Spec{Outputs: map[string]string{"result": "results"}},
		Variants: []adapterVariant{{Target: myce.ExecutionTarget{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "fake"}, Needs: myce.ResourceNeeds{MemoryGB: 0.25, HostMemoryGB: 0.25}, Execution: myce.ExecutionSpec{Main: myce.ExecutionMember{Command: []string{"sh", "{extension}/run.sh"}}}, Checks: adapterChecks{Result: []string{"test", "-s", "result"}, Qualification: []string{"sh", "{extension}/check.sh"}}, Baselines: map[string]myce.ExtensionBaseline{"extension:native-fixture": {Input: "input", Output: "native", Measure: "exact", Native: "fixture shell copy", Variation: "exact deterministic bytes"}}}},
	}}}
	data, err := yaml.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var authored map[string]any
	if err = yaml.Unmarshal(data, &authored); err != nil {
		t.Fatal(err)
	}
	nativeEntry := authored["entries"].(map[string]any)["native-fixture"].(map[string]any)
	delete(nativeEntry, "run")
	delete(nativeEntry, "needs")
	data, err = yaml.Marshal(authored)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "adapter.yaml")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), ExtensionRoot: filepath.Join(state, "extensions"), Token: "native-adapter-token", GPU: "fake", MemoryGB: 2, Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	binary := devTestBinary(t)
	invoke := func(key string) profileReceipt {
		t.Helper()
		command := exec.CommandContext(ctx, binary, "run", "--adapter", path, "--queue", d.URL, "--key", key, "--", "native literal; $()")
		command.Dir = root
		command.Env = append(os.Environ(), "MYCE_TOKEN=native-adapter-token", "MYCE_NOTIFY=")
		b, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("native submit: %v %s", err, b)
		}
		var r profileReceipt
		if err = json.Unmarshal(b, &r); err != nil {
			t.Fatalf("receipt %v %s", err, b)
		}
		return r
	}
	var checkID, digest string
	for _, key := range []string{"cold", "warm"} {
		r := invoke(key)
		j, err := d.Client().Wait(ctx, r.JobID)
		if err != nil || j.State != "succeeded" {
			t.Fatalf("%s job %+v %v", key, j, err)
		}
		dest := filepath.Join(root, key+"-result")
		if err = d.Client().DownloadResult(ctx, j.ID, "result", dest); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(dest)
		if err != nil || string(b) != "native literal; $()" {
			t.Fatalf("native result %q %v", b, err)
		}
		snapshot, err := d.Queue.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, job := range snapshot.Jobs {
			if job.Spec.ExtensionCheck != nil {
				count++
				if job.State != "succeeded" {
					t.Fatalf("check %+v", job)
				}
			}
		}
		if count != 1 {
			t.Fatalf("%s made %d checks", key, count)
		}
		found := false
		for _, machine := range snapshot.Machines {
			for _, report := range machine.ExtensionReports {
				if report.Qualified && report.Work == "extension:native-fixture" {
					found = true
					if key == "cold" {
						checkID, digest = report.CheckID, report.Digest
					}
					if report.CheckID == "" || report.CheckID != checkID || report.Digest != digest {
						t.Fatalf("warm qualification changed %+v", report)
					}
				}
			}
		}
		if !found {
			t.Fatal("useful result has no exact native qualification")
		}
	}
}
