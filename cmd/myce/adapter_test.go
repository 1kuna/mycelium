package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
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

func TestAdapterStartWindow(t *testing.T) {
	data := strings.Replace(cpuAdapterYAML("source"), "    source:", "    class: production\n    start_window: 600s\n    source:", 1)
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "")
	if err != nil || p.StartWindow != "600s" || p.Class != "production" {
		t.Fatalf("entry lost stage policy: %+v %v", p.Spec, err)
	}
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
		strings.Replace(data, "    checks:", "    assets: {model.bin: {sha256: "+strings.Repeat("a", 64)+", size_bytes: 4}}\n    checks:", 1),
		strings.Replace(data, "    checks:", "    references: {extension:work: {input_shape: fixture, unit: item, units: 1}}\n    checks:", 1),
		strings.Replace(data, "    checks:", "    sweeps: {extension:work: {run: [echo, fixture]}}\n    checks:", 1),
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

func TestAdapterSharedInputUploadNamespaces(t *testing.T) {
	binary := devTestBinary(t)
	for _, storage := range []string{"local", "nas"} {
		t.Run(storage, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			privateBytes, sharedBytes := []byte("private bytes"), []byte("shared runtime bytes")
			for name, data := range map[string][]byte{"private": privateBytes, "runtime": sharedBytes, "source/run.sh": []byte("true\n")} {
				if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			privateRef := myce.FileRef{SHA256: fmt.Sprintf("%x", sha256.Sum256(privateBytes)), SizeBytes: int64(len(privateBytes))}
			sharedRef := myce.FileRef{SHA256: fmt.Sprintf("%x", sha256.Sum256(sharedBytes)), SizeBytes: int64(len(sharedBytes))}
			shared := "{local: runtime}"
			if storage == "nas" {
				shared = fmt.Sprintf("{nas: media/runtime, sha256: %s, size_bytes: %d}", sharedRef.SHA256, sharedRef.SizeBytes)
			}
			path := filepath.Join(root, "adapter.yaml")
			data := cpuAdapterYAML(source) + "    inputs: {shared/runtime: private}\n    shared_inputs: {runtime: " + shared + "}\n"
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			q, _, _, _ := waitQueue(t)
			if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "files"), 1<<20, true); err != nil {
				t.Fatal(err)
			}
			m := myce.Machine{Capabilities: []string{"native_isolation_landlock", "shared_inputs_v1"}, ID: "worker", Session: "fixture", GPU: "cpu", CPUIsolation: "select-none",
				HostMemoryMeasured: true, HostAvailableMemoryGB: 32, HostMemoryBudgetGB: 8, MemoryCeilingSlackGB: .125,
				CapabilityProfile: &myce.CapabilityProfile{OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: 8, MemoryLayout: "separate", SystemMemoryBytes: 32 << 30}}
			if _, err := q.Checkin(myce.Checkin{Machine: m}); err != nil {
				t.Fatal(err)
			}
			const token = "adapter-input-fixture-token"
			var online atomic.Bool
			handler := myce.API{Queue: q, Token: token}.Handler()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !online.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
			defer cancel()
			invoke := func(wantExit int) profileReceipt {
				t.Helper()
				cmd := exec.CommandContext(ctx, binary, "run", "--adapter", path, "--queue", server.URL, "--key", "shared-input", "--notify", filepath.Join(root, "absent-notifier"))
				cmd.Dir = root
				cmd.Env = append(os.Environ(), "MYCE_TOKEN="+token)
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				out, err := cmd.Output()
				if exitCodeForTest(err) != wantExit {
					t.Fatalf("exit=%v want=%d stdout=%s stderr=%s", err, wantExit, out, &stderr)
				}
				var receipt profileReceipt
				if err := json.Unmarshal(out, &receipt); err != nil {
					t.Fatal(err)
				}
				return receipt
			}
			frozen := invoke(69)
			for _, name := range []string{"source", "private", "runtime"} {
				if err := os.RemoveAll(filepath.Join(root, name)); err != nil {
					t.Fatal(err)
				}
			}
			online.Store(true)
			// An absent notifier returns 74 after submission, without starting a watcher.
			receipt := invoke(74)
			if receipt.JobID == "" || receipt.Key != frozen.Key || len(receipt.Uploads) != 0 {
				t.Fatalf("saved submission not recovered: %+v", receipt)
			}
			client := myce.Client{URL: server.URL, Token: token}
			snapshot, err := q.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			job, err := myce.FindJob(snapshot.Jobs, receipt.JobID)
			if err != nil {
				t.Fatal(err)
			}
			input := job.Spec.SharedInputs["runtime"]
			if job.Spec.Files["shared/runtime"] != privateRef || input.SHA256 != sharedRef.SHA256 || input.SizeBytes != sharedRef.SizeBytes || input.Local != "" {
				t.Fatalf("private/shared identities changed: %+v / %+v", job.Spec.Files, input)
			}
			for _, blob := range []struct {
				name string
				ref  myce.FileRef
				data []byte
			}{{"private", privateRef, privateBytes}, {"shared", sharedRef, sharedBytes}} {
				if blob.name == "shared" && storage == "nas" {
					if input.NAS != "media/runtime" || client.HasFile(ctx, sharedRef) == nil {
						t.Fatal("NAS input lost its reference or was uploaded")
					}
					continue
				}
				dest := filepath.Join(root, "downloaded-"+blob.name)
				if err := client.DownloadFile(ctx, blob.ref, dest); err != nil {
					t.Fatal(err)
				}
				if got, err := os.ReadFile(dest); err != nil || !bytes.Equal(got, blob.data) {
					t.Fatalf("%s bytes=%q err=%v", blob.name, got, err)
				}
			}
			if replay := invoke(74); replay.JobID != receipt.JobID {
				t.Fatalf("replay created another job: %+v", replay)
			}
		})
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
	for name, value := range map[string]string{".gitattributes": "omitted export-ignore\n", "omitted": "excluded source bytes"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
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
	// Unscoped adapter sources retain git archive export-ignore behavior while
	// excluding only the instruction file; selected-path sources use raw blobs.
	checkArchive := filepath.Join(t.TempDir(), "export.tar")
	if _, err := freezeProfileSource(ctx, root, one, checkArchive); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(checkArchive)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(f)
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if h.Name == "omitted" || h.Name == ".mycelium/adapter.yaml" {
			t.Fatal("excluded source was transferred", h.Name)
		}
	}
	if err := f.Close(); err != nil {
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
	deadline := time.Now().Add(time.Hour)
	a := projectAdapter{Schema: 1, Project: "native-fixture", Default: "native-fixture", Entries: map[string]adapterEntry{"native-fixture": {
		Version: strings.Repeat("v", 64),
		Source:  profileSource{Directory: source}, Spec: myce.Spec{Class: "deadline", Deadline: &deadline, Outputs: map[string]string{"result": "results"}},
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

// Saved auto-memory identity must distinguish materially different composed
// commands and member environments while old plain-profile identity stays stable.
func TestAdapterComposedMemoryIdentity(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "input"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	base := executionProfile{Adapter: true, Source: profileSource{Directory: source}, Spec: myce.Spec{Name: "composed", Project: "fixture", Class: "test", Needs: myce.ResourceNeeds{CPUCores: 1, Memory: "auto", Platform: myce.Platform{Required: []string{"cpu"}}}, Targets: []myce.ExecutionTarget{{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}}}}
	var previous string
	for i, member := range []myce.ExecutionMember{{Command: []string{"echo", "first"}}, {Command: []string{"echo", "second"}}, {Command: []string{"echo", "second"}, Env: map[string]string{"MODE": "other"}}} {
		p := base
		p.Execution = &myce.ExecutionSpec{Main: member}
		var out bytes.Buffer
		err := runPreparedProfile(context.Background(), root, p, []byte(fmt.Sprint(i)), nil, nil, fmt.Sprint(i), "", "", "", server.URL, "fixture-token", &out, &out)
		if exitCode(err) != 69 {
			t.Fatalf("expected unavailable queue with saved intent: %v %s", err, out.String())
		}
		var r profileReceipt
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		if r.Spec.ProfileMemory == "" || r.Spec.ProfileMemory == previous {
			t.Fatal("changed composed command/environment reused memory identity")
		}
		previous = r.Spec.ProfileMemory
	}
}

func TestAdapterNativeRecipeBeforeResultWrapping(t *testing.T) {
	e := adapterEntry{Spec: myce.Spec{Run: myce.RunSpec{Command: []string{"echo", "work"}}, Needs: myce.ResourceNeeds{MemoryGB: 1, Platform: myce.Platform{Required: []string{"cuda"}}}, Targets: []myce.ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cuda"}}}, Checks: adapterChecks{Result: []string{"true"}, Qualification: []string{"echo", "qualification"}}, Baselines: map[string]myce.ExtensionBaseline{"extension:work": {}}, Sweeps: map[string]myce.ReferenceSweep{"extension:work": {Command: []string{"echo", "measure"}}}}
	lower := func(entry adapterEntry) (executionProfile, error) {
		p, _, err := adapterProfile(projectAdapter{Project: "fixture", Default: "work", Entries: map[string]adapterEntry{"work": entry}}, "")
		return p, err
	}
	p, err := lower(e)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Native.Variants[0].Sweeps["extension:work"].Run) != 0 {
		t.Fatal("result check manufactured an omitted wave command")
	}
	conflict := e
	conflict.Targets = append(slices.Clone(e.Targets), myce.ExecutionTarget{OS: "darwin", Arch: "arm64", Platform: "metal"})
	if _, err := lower(conflict); err == nil {
		t.Fatal("native lowering widened required platforms")
	}
	v := adapterEntry{Variants: []adapterVariant{{Target: e.Targets[0], Needs: myce.ResourceNeeds{MemoryGB: 1}, Checks: e.Checks, Baselines: e.Baselines}}}
	if _, err := lower(v); err == nil {
		t.Fatal("result check manufactured a missing native main command")
	}
	v.Spec.After = &myce.PieceDependency{Job: "parent", Piece: "piece"}
	v.Variants[0].Execution.Main.Command = []string{"echo", "work"}
	if err := validateAdapterEntry(v); err == nil {
		t.Fatal("variant result check bypassed after restriction")
	}
	v.Spec.After = nil
	v.Variants[0].Execution.Main.Command = []string{""}
	if _, err := lower(v); err == nil {
		t.Fatal("result check manufactured an empty native executable")
	}
}
