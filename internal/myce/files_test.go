package myce

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFilesResumableAndHashRefusal(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(filepath.Join(t.TempDir(), "store"), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	c := Client{URL: api.URL, Token: "test"}
	source := filepath.Join(t.TempDir(), "input")
	data := bytes.Repeat([]byte("verified bytes"), 100)
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	// A real tus upload survives an interrupted PATCH and a server restart.
	upload := "/api/files/uploads/" + ref.SHA256
	req, _ := http.NewRequest("POST", api.URL+"/api/files/uploads/", nil)
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Tus-Resumable", "1.0.0")
	setUploadHeaders(req, ref)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 201 {
		t.Fatal(res.StatusCode)
	}
	req, _ = http.NewRequest("PATCH", api.URL+upload, bytes.NewReader(data[:17]))
	req.Header.Set("Authorization", "Bearer test")
	req.Header.Set("Tus-Resumable", "1.0.0")
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 204 {
		t.Fatal(res.StatusCode)
	}
	api.Close()
	api = httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	c.URL = api.URL
	if err = c.UploadFile(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "download")
	// Preserve a prefix across an interrupted download and request only the remainder.
	if err = os.WriteFile(dest, data[:23], 0600); err != nil {
		t.Fatal(err)
	}
	ranges := 0
	transport := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=23-" {
			ranges++
		}
		API{Queue: q, Token: "test"}.Handler().ServeHTTP(w, r)
	})
	proxy := httptest.NewServer(transport)
	defer proxy.Close()
	c.URL = proxy.URL
	if err = c.DownloadFile(context.Background(), ref, dest); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dest)
	if !bytes.Equal(got, data) || ranges != 1 {
		t.Fatal("range resume failed", ranges)
	}
	bad := ref
	bad.SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err = c.UploadFile(context.Background(), source, bad); err == nil {
		t.Fatal("accepted mismatched digest")
	}
	req, _ = http.NewRequest("GET", proxy.URL+"/api/files/blobs/"+bad.SHA256, nil)
	req.Header.Set("Authorization", "Bearer test")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	if res.StatusCode != 404 {
		t.Fatal("unverified upload published", res.StatusCode)
	}
}

func TestFilesNASInputs(t *testing.T) {
	root := t.TempDir()
	checkpoints := filepath.Join(root, "checkpoints")
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: checkpoints, FileStore: filepath.Join(root, "files"), FileStoreLimit: 1 << 20, NASCheckpoints: true, Token: "nas-input-fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := d.Client()
	source := filepath.Join(root, "input")
	if err := os.WriteFile(source, []byte("NAS-mode input"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UploadFile(ctx, source, ref); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Needs.CPUCores, s.Needs.MemoryGB = 1, 1
	s.Needs.Platform = Platform{Required: []string{"cpu"}}
	s.Resumable = true
	s.Run.Command = []string{"sh", "-c", `test -z "$MYCE_TOKEN" && cat input > output && printf '{"myce_checkpoint":{"generation":1},"value":42}' > "$MYCE_CHECKPOINT_DIR/save.json"`}
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"output": "results"}
	var accepted Job
	if err := c.Call(ctx, "POST", "/api/jobs", s, &accepted); err != nil {
		t.Fatal("NAS-mode queue rejected stored inputs:", err)
	}
	finished, err := c.Wait(ctx, accepted.ID)
	if err != nil || finished.State != "succeeded" {
		t.Fatalf("NAS-mode execution: %+v %v", finished, err)
	}
	if finished.FileTransport || len(finished.FileCheckpoints) != 0 || finished.ResumeFiles != nil {
		t.Fatal("stored inputs changed NAS checkpoint transport", finished)
	}
	save := filepath.Join(checkpoints, accepted.ID, "attempt-1", "save.json")
	if data, err := os.ReadFile(save); err != nil || !bytes.Contains(data, []byte(`"value":42`)) {
		t.Fatalf("NAS checkpoint missing: %s %v", data, err)
	}
	dest := filepath.Join(root, "downloaded")
	if err := c.DownloadResult(ctx, accepted.ID, "output", dest); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(dest); err != nil || string(data) != "NAS-mode input" {
		t.Fatalf("stored result: %s %v", data, err)
	}
	// NAS-mode admission must retain the same input hash verification.
	if err := os.WriteFile(d.Queue.files.blob(ref), []byte("corrupt input!"), 0600); err != nil {
		t.Fatal(err)
	}
	err = c.Call(ctx, "POST", "/api/jobs", s, nil)
	var rejection *HTTPError
	if !errors.As(err, &rejection) || rejection.Status != 400 || rejection.Code != "job_files_invalid" {
		t.Fatal("corrupt NAS-mode input accepted", err)
	}
}

func TestFileCacheBoundedEvictionAndPrivateCopies(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 128, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	clock := &fakeClock{}
	cache := FileCache{Root: t.TempDir(), Limit: 8, Clock: clock, Client: c}
	refs := []FileRef{}
	for _, data := range []string{"aaaa", "bbbb", "cccc"} {
		src := filepath.Join(t.TempDir(), "input")
		os.WriteFile(src, []byte(data), 0600)
		ref, err := IdentifyFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.UploadFile(context.Background(), src, ref); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
		clock.Advance(time.Second)
		dest := filepath.Join(t.TempDir(), "private")
		if err = cache.copyTo(context.Background(), ref, dest); err != nil {
			t.Fatal(err)
		}
		// Changing a private copy must not poison future jobs or shared cached data.
		os.WriteFile(dest, []byte("edited"), 0600)
	}
	if _, err := os.Stat(filepath.Join(cache.Root, refs[0].SHA256)); !os.IsNotExist(err) {
		t.Fatal("oldest cache entry retained", err)
	}
	entries, _ := os.ReadDir(cache.Root)
	var total int64
	for _, entry := range entries {
		if entry.Name() == FormatMarker {
			continue // The cache limit covers payloads, not its fixed format metadata.
		}
		info, _ := entry.Info()
		total += info.Size()
	}
	if total > 8 {
		t.Fatal("cache exceeds bound", total)
	}
	if marker, err := os.ReadFile(filepath.Join(cache.Root, FormatMarker)); err != nil || string(marker) != stateFormat {
		t.Fatal("cache eviction lost the format marker", err)
	}
	if err := q.files.verify(context.Background(), refs[0]); err != nil {
		t.Fatal("authoritative copy evicted", err)
	}
}

func TestStoredCheckpointDurabilityStalenessAndOutputAcceptance(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	s := spec("experiment")
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := q.Checkin(Checkin{Machine: Machine{CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: "a", Session: "a", GPU: "fake", FreeMemoryGB: 8}})
	if err != nil || reply.Job == nil {
		t.Fatal(err, reply)
	}
	r := Result{Machine: "a", Session: "a", Execution: Execution{ID: j.ID, Attempt: reply.Job.Attempt}}
	src := filepath.Join(t.TempDir(), "save.json")
	os.WriteFile(src, []byte(`{"myce_checkpoint":{"generation":7},"value":5}`), 0600)
	ref, _ := IdentifyFile(src)
	save := checkpointPublication{Result: r, Checkpoint: FileCheckpoint{7, FileManifest{"checkpoint.json": ref}}}
	if err = q.publishCheckpoint(context.Background(), save); err == nil {
		t.Fatal("accepted missing payload")
	}
	if err = c.UploadFile(context.Background(), src, ref); err != nil {
		t.Fatal(err)
	}
	q.files.syncDir = func(string) error { return errors.New("injected fsync failure") }
	if err = q.publishCheckpoint(context.Background(), save); err == nil {
		t.Fatal("accepted failed durability barrier")
	}
	snap, _ := q.Snapshot()
	if len(snap.Jobs[0].FileCheckpoints) > 0 {
		t.Fatal("failed checkpoint published")
	}
	q.files.syncDir = syncWorkerDir
	if err = q.publishCheckpoint(context.Background(), save); err != nil {
		t.Fatal(err)
	}
	bad := save
	bad.Attempt++
	if err = q.publishCheckpoint(context.Background(), bad); !errors.Is(err, ErrStale) {
		t.Fatal("accepted stale publisher", err)
	}
	// Reconciliation ignores a worker's untransferred local generation.
	reply, err = q.Checkin(Checkin{Machine: Machine{CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: "a", Session: "a", GPU: "fake", FreeMemoryGB: 0}, Running: &Execution{ID: j.ID, Attempt: r.Attempt, Progress: 999}})
	if err != nil {
		t.Fatal(err)
	}
	snap, _ = q.Snapshot()
	if snap.Jobs[0].Progress != 7 {
		t.Fatal("local-only progress accepted", snap.Jobs[0].Progress)
	}
}

func TestFilesDifferentHyphaResume(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 required for atomic checkpoint example")
	}
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	code := []byte(`import os,pathlib,json,signal,time
assert 'MYCE_TOKEN' not in os.environ
assert pathlib.Path('input').read_text()=='payload'
assert pathlib.Path('model').read_text()=='weights'
p=pathlib.Path(os.environ['MYCE_CHECKPOINT_DIR'])/'checkpoint.json'
save=json.loads(p.read_text()) if p.exists() else {'value':0,'myce_checkpoint':{'generation':0}}
n=save['value']; g=save['myce_checkpoint']['generation']; stopping=False
print('resumed='+str(n),flush=True)
def stop(*args):
 global stopping
 stopping=True
signal.signal(signal.SIGTERM,stop)
while n<30 and not stopping:
 n+=1; g+=1
 tmp=p.with_suffix('.tmp')
 with tmp.open('w') as f:
  json.dump({'myce_checkpoint':{'generation':g},'value':n},f); f.flush(); os.fsync(f.fileno())
 tmp.replace(p)
 fd=os.open(p.parent,os.O_RDONLY); os.fsync(fd); os.close(fd)
 time.sleep(.05)
if not stopping: pathlib.Path('output').write_text(str(n))
`)
	manifest := FileManifest{}
	for name, data := range map[string][]byte{"train.py": code, "input": []byte("payload"), "model": []byte("weights")} {
		src := filepath.Join(t.TempDir(), "file")
		os.WriteFile(src, data, 0600)
		ref, _ := IdentifyFile(src)
		if err := c.UploadFile(context.Background(), src, ref); err != nil {
			t.Fatal(err)
		}
		manifest[name] = ref
	}
	s := spec("experiment")
	s.Run.Command = []string{"python3", "train.py"}
	s.Files = manifest
	s.Outputs = map[string]string{"output": "results"}
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	start := func(name string) (context.CancelFunc, chan error, string) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		root := newTestStateRoot(t)
		worker := Worker{Client: c, StateDir: root, Interval: 20 * time.Millisecond, Clock: RealClock{}, Machine: Machine{CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: name, HostID: name, Session: name, GPU: "fake", FreeMemoryGB: 8}, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }}
		go func() { done <- worker.Run(ctx) }()
		return cancel, done, root
	}
	cancelA, doneA, rootA := start("a")
	defer cancelA()
	await(t, func() bool { snap, _ := q.Snapshot(); return len(snap.Jobs[0].FileCheckpoints) > 0 })
	cancelB, doneB, rootB := start("b")
	defer cancelB()
	if err = q.Leave("a", false); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-doneA:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hypha a did not leave")
	}
	await(t, func() bool {
		snap, _ := q.Snapshot()
		return snap.Jobs[0].State == "succeeded" || snap.Jobs[0].State == "failed"
	})
	snap, _ := q.Snapshot()
	got := snap.Jobs[0]
	if got.State != "succeeded" || got.Machine != "b" || got.Attempt < 2 {
		t.Fatalf("different-hypha resume: %+v", got)
	}
	ref, ok := got.OutputRefs["output"]
	if !ok {
		t.Fatal("output not returned")
	}
	dest := filepath.Join(t.TempDir(), "output")
	if err = c.DownloadFile(context.Background(), ref, dest); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(dest)
	if string(data) != "30" {
		t.Fatal(string(data))
	}
	logs, _ := q.Logs(j.ID, 0)
	resumed := false
	for _, line := range logs {
		if line.Attempt == got.Attempt && strings.Contains(line.Text, "resumed=") && !strings.Contains(line.Text, "resumed=0") {
			resumed = true
		}
	}
	if !resumed {
		t.Fatal("second hypha restarted instead of resuming", logs)
	}
	if rootA == rootB {
		t.Fatal("shared state")
	}
	cancelB()
	select {
	case err = <-doneB:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hypha b did not leave")
	}
}

func TestFileStoreCorruptUploadOffsetCapacityAndRestart(t *testing.T) {
	t.Parallel()
	db := filepath.Join(t.TempDir(), "queue.db")
	checkpoint := t.TempDir()
	store := t.TempDir()
	clock := &fakeClock{}
	q, err := Open(db, checkpoint, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.ConfigureFileStore(store, 8, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	c := Client{URL: server.URL, Token: "test"}
	src := filepath.Join(t.TempDir(), "file")
	os.WriteFile(src, []byte("good"), 0600)
	ref, _ := IdentifyFile(src)
	request := func(method, path string, data []byte, headers map[string]string) int {
		t.Helper()
		res, e := c.fileRequest(context.Background(), method, path, bytes.NewReader(data), headers)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		io.Copy(io.Discard, res.Body)
		return res.StatusCode
	}
	create := map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": "4", "Upload-Metadata": "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256))}
	if got := request("POST", "/api/files/uploads/", nil, create); got != 201 {
		t.Fatal(got)
	}
	patch := map[string]string{"Tus-Resumable": "1.0.0", "Content-Type": "application/offset+octet-stream", "Upload-Offset": "0"}
	if got := request("PATCH", "/api/files/uploads/"+ref.SHA256, []byte("ba"), patch); got != 204 {
		t.Fatal(got)
	}
	if got := request("PATCH", "/api/files/uploads/"+ref.SHA256, []byte("ba"), patch); got != 409 {
		t.Fatal("wrong offset accepted", got)
	}
	server.Close()
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(db, checkpoint, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err = q.ConfigureFileStore(store, 8, true); err != nil {
		t.Fatal(err)
	}
	server = httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c.URL = server.URL
	patch["Upload-Offset"] = "2"
	if got := request("PATCH", "/api/files/uploads/"+ref.SHA256, []byte("dd"), patch); got < 400 {
		t.Fatal("corrupt upload accepted", got)
	}
	if err = c.HasFile(context.Background(), ref); err == nil {
		t.Fatal("corrupt upload published")
	}
	// A bad request cannot consume more than its declared reservation.
	create["Upload-Length"] = "5"
	create["Upload-Metadata"] = "sha256 " + base64.StdEncoding.EncodeToString([]byte(strings.Repeat("a", 64)))
	if got := request("POST", "/api/files/uploads/", nil, create); got != 507 {
		t.Fatal("store capacity ignored", got)
	}
	if got := request("GET", "/api/files/blobs/"+ref.SHA256, nil, map[string]string{"Authorization": "Bearer invalid"}); got != 401 {
		t.Fatal("missing authorization", got)
	}
}

func TestStoredModelCacheSingleAndBundle(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	files := []ModelFile{}
	for name, data := range map[string]string{"weights": "model", "config.json": "{}"} {
		src := filepath.Join(t.TempDir(), "file")
		os.WriteFile(src, []byte(data), 0600)
		ref, _ := IdentifyFile(src)
		if err := c.UploadFile(context.Background(), src, ref); err != nil {
			t.Fatal(err)
		}
		files = append(files, ModelFile{Path: name, SHA256: ref.SHA256, SizeBytes: ref.SizeBytes})
	}
	root, _ := filepath.EvalSymlinks(t.TempDir())
	cache := ModelCache{Root: root, Limit: 9, Clock: RealClock{}, FileClient: &c}
	if err := cache.init(); err != nil {
		t.Fatal(err)
	}
	bundleSize, bundleHash, _ := ModelFilesIdentity(files)
	var weight ModelFile
	for _, f := range files {
		if f.Path == "weights" {
			weight = f
		}
	}
	models := []Model{{Name: "one", Path: "stored", FileStore: true, SizeBytes: weight.SizeBytes, SHA256: weight.SHA256}, {Name: "bundle", Path: "stored", FileStore: true, SizeBytes: bundleSize, SHA256: bundleHash, Files: files}}
	for _, model := range models {
		path, err := cache.Prepare(context.Background(), model)
		if err != nil {
			t.Fatal(err)
		}
		if err = verifyModel(path, model); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(cache.Root, cacheKey(models[0]))); !os.IsNotExist(err) {
		t.Fatal("unloaded stored model was not evicted", err)
	}
}

func TestStoredExtensionInputAndOutput(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	reports := installQualified(t, root, e, "")
	source := filepath.Join(t.TempDir(), "input")
	os.WriteFile(source, []byte("[1,2]\n"), 0600)
	ref, _ := IdentifyFile(source)
	if err := c.UploadFile(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "stored-extension", Project: "test", Priority: "experiment", Kind: "sample", Files: FileManifest{"input": ref}, Outputs: map[string]string{"output": "results"}}
	j, err := q.Submit(s)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	defer cancel()
	state := newTestStateRoot(t)
	w := Worker{Client: c, ExtensionRoot: root, StateDir: state, Clock: RealClock{}, Interval: 20 * time.Millisecond, HostMemorySource: func(context.Context) (float64, error) { return 16, nil }, Machine: Machine{CapabilityProfile: machine("profile", "fake").CapabilityProfile, ID: "extension", HostID: "extension", GPU: "fake", FreeMemoryGB: 8, ExtensionReports: reports}}
	go func() { done <- w.Run(ctx) }()
	await(t, func() bool {
		snap, _ := q.Snapshot()
		return snap.Jobs[0].State == "succeeded" || snap.Jobs[0].State == "failed"
	})
	snap, _ := q.Snapshot()
	got := snap.Jobs[0]
	if got.ID != j.ID || got.State != "succeeded" || got.OutputRefs["output"] != ref {
		t.Fatalf("extension file transport: %+v", got)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("extension worker stuck")
	}
}
