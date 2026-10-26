package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"mycelium/internal/myce"
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
	for _, item := range []string{"one", "two", "three"} {
		write("items/"+item, item+"\n")
	}
	profileGit(t, root, "init", "--quiet")
	profileGit(t, root, "add", ".")
	profileGit(t, root, "commit", "--quiet", "-m", "fixture")
	binary := devTestBinary(t)
	state := t.TempDir()
	const token = "item-fixture-token"
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), FileStore: filepath.Join(state, "files"), Checkpoints: filepath.Join(state, "checkpoints"), Token: token, GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	wait(t, ctx, func() bool { s, e := status(ctx, d.Client()); return e == nil && len(s.Machines) == 1 })
	target, _ := url.Parse(d.URL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	patches := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PATCH" {
			mu.Lock()
			patches[filepath.Base(r.URL.Path)]++
			mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	invoke := func(checkout, item, arg string) profileReceipt {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "run", "--profile", "items", "--queue", server.URL, "--input", "item="+filepath.Join(checkout, "items", item), "--", arg)
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
	for i, item := range []string{"one", "two", "three"} {
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
	write("scripts/process.sh", "cat common item > result\nprintf '%s' \"$1\" >> result\nsleep 1\n# relevant change\n")
	profileGit(t, root, "add", "scripts/process.sh")
	profileGit(t, root, "commit", "--quiet", "-m", "relevant")
	relevant := invoke(root, "one", "one")
	finish(relevant)
	if relevant.JobID == changed.JobID {
		t.Fatal("relevant source did not rerun")
	}
	// Opting in separates items and appended arguments into their own classes.
	write(".mycelium/profiles/items.yaml", profile+"memory_class: per_item\n")
	perItem := invoke(root, "two", "two")
	finish(perItem)
	perArgs := invoke(root, "two", "other-args")
	finish(perArgs)
	perFile := invoke(root, "three", "two")
	finish(perFile)
	if perItem.Spec.ProfileMemory == perArgs.Spec.ProfileMemory || perItem.Spec.ProfileMemory == perFile.Spec.ProfileMemory {
		t.Fatal("per_item must separate both arguments and item content")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, ref := range []myce.FileRef{first.Spec.Files[".myce-source.tar"], first.Spec.Files[".myce-inputs/"+profileHash("common")]} {
		if patches[ref.SHA256] != 1 {
			t.Errorf("common blob uploaded %d times", patches[ref.SHA256])
		}
	}
	t.Log("one committed profile ran three items; common blobs uploaded once; shared memory identity; fresh clone after unrelated commit adopted; changed item/code reran; per_item separated args")
}

func TestProfileSourcePaths(t *testing.T) {
	root := t.TempDir()
	for name, data := range map[string]string{"script": "$Format:%H$", "outside": "other", ".gitattributes": "script export-subst\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	profileGit(t, root, "init", "--quiet")
	profileGit(t, root, "add", ".")
	profileGit(t, root, "commit", "--quiet", "-m", "fixture")
	ctx := context.Background()
	source := profileSource{Git: "HEAD", Paths: []string{"script"}}
	pinned, id, err := identifyProfileSource(ctx, root, source)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "source.tar")
	ref, err := freezeProfileSource(ctx, root, pinned, archive)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tr := tar.NewReader(f)
	h, err := tr.Next()
	if err != nil || h.Name != "script" {
		t.Fatal(h, err)
	}
	content, err := io.ReadAll(tr)
	if err != nil || string(content) != "$Format:%H$" {
		t.Fatalf("substituted blob: %q %v", content, err)
	}
	if _, err = tr.Next(); err != io.EOF {
		t.Fatal("unselected file archived", err)
	}
	if err = os.WriteFile(filepath.Join(root, "outside"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	again, newID, err := identifyProfileSource(ctx, root, source)
	if err != nil || id != newID {
		t.Fatal("unrelated dirt changed source", err)
	}
	next, err := freezeProfileSource(ctx, root, again, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil || next != ref {
		t.Fatal("unstable archive", err)
	}
	for _, paths := range [][]string{{"../outside"}, {":(glob)*"}, {"missing"}} {
		if _, _, err = identifyProfileSource(ctx, root, profileSource{Git: "HEAD", Paths: paths}); err == nil {
			t.Errorf("accepted invalid paths %q", paths)
		}
	}
	if err = os.WriteFile(filepath.Join(root, "script"), []byte("dirty"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = identifyProfileSource(ctx, root, source); err == nil {
		t.Fatal("dirty selected source accepted")
	}
}

func TestProfilePerItemInputValidation(t *testing.T) {
	for _, flags := range [][]string{
		{"--input", "missing-equals"},
		{"--input", "../escape=file"},
		{"--input", ".myce-inputs/private=file"},
		{"--input", "item=one", "--input", "item=two"},
	} {
		if err := runCommand(context.Background(), append([]string{"--profile", "unused"}, flags...), io.Discard, io.Discard); exitCode(err) != 64 {
			t.Errorf("flags %q: %v", flags, err)
		}
	}
	root := t.TempDir()
	profileDir := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profileDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profileDir, "fixture.yaml")
	profile := `name: fixture
project: test
class: experiment
source: {directory: source}
run: {command: [true]}
inputs: {common: file}
needs: {cpu_cores: 1, memory: auto}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"common", "common/child"} {
		err := runProfile(context.Background(), path, nil, map[string]string{name: "file"}, "", "", "", "", "http://127.0.0.1:1", "fixture", io.Discard, io.Discard)
		if exitCode(err) != 64 {
			t.Errorf("overlap %q: %v", name, err)
		}
	}
	// On an uncertain submission the new file has its own durable frozen copy;
	// source edits after preparation cannot mutate the saved bytes.
	var out bytes.Buffer
	err := runProfile(context.Background(), path, []string{"literal"}, map[string]string{"item": "file"}, "frozen", "", "", "", "http://127.0.0.1:1", "fixture", &out, io.Discard)
	if exitCode(err) != 69 {
		t.Fatal(err)
	}
	var r profileReceipt
	if err = json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "file"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	frozen := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", r.Key}), r.Uploads[".myce-inputs/"+profileHash("item")])
	got, err := os.ReadFile(frozen)
	if err != nil || string(got) != "original" {
		t.Fatalf("frozen item: %q %v", got, err)
	}
}
