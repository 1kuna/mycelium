package myce

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func cacheFixture(t *testing.T) (*ModelCache, func(string, string) Model) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	c := &ModelCache{Root: filepath.Join(home, "cache"), NASRoot: filepath.Join(home, "nas"), Limit: 8, Clock: &fakeClock{now: time.Now()}, Free: func(string) (int64, error) { return 1000, nil }}
	if e := c.init(); e != nil {
		t.Fatal(e)
	}
	if e := os.MkdirAll(c.NASRoot, 0700); e != nil {
		t.Fatal(e)
	}
	return c, func(name, data string) Model {
		t.Helper()
		p := filepath.Join(c.NASRoot, name)
		if e := os.WriteFile(p, []byte(data), 0600); e != nil {
			t.Fatal(e)
		}
		return Model{Name: name, Path: name, SizeBytes: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(data))), Engine: "fake"}
	}
}
func TestCacheCopyVerifyReuseAndLRU(t *testing.T) {
	c, model := cacheFixture(t)
	a, b, d := model("a", "aaaa"), model("b", "bbbb"), model("d", "dddd")
	ctx := context.Background()
	pa, e := c.Prepare(ctx, a)
	if e != nil {
		t.Fatal(e)
	}
	if pa == filepath.Join(c.NASRoot, a.Path) {
		t.Fatal("loaded NAS")
	}
	c.Clock.(*fakeClock).Advance(time.Second)
	pb, e := c.Prepare(ctx, b)
	if e != nil {
		t.Fatal(e)
	}
	c.Clock.(*fakeClock).Advance(time.Second)
	if _, e = c.Prepare(ctx, a); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Prepare(ctx, d); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(pb); !os.IsNotExist(e) {
		t.Fatal("LRU not evicted")
	}
	if _, e = os.Stat(pa); e != nil {
		t.Fatal("recent model evicted")
	}
	names, free, e := c.Report([]Model{a, b, d})
	if e != nil || len(names) != 2 || free != 0 {
		t.Fatalf("report %v %d %v", names, free, e)
	}
	trash, _ := filepath.Glob(filepath.Join(c.NASRoot, ".Trash", "myce-model-cache", "*"))
	if len(trash) != 1 {
		t.Fatal("eviction not recoverable")
	}
}
func TestCacheDiskFullAndProtected(t *testing.T) {
	c, model := cacheFixture(t)
	a, b := model("a", "aaaaaaaa"), model("b", "bbbbbbbb")
	if _, e := c.Prepare(context.Background(), a); e != nil {
		t.Fatal(e)
	}
	c.Protected = func(key string) bool { return key == cacheKey(a) }
	if _, e := c.Prepare(context.Background(), b); e == nil || !strings.Contains(e.Error(), "insufficient") {
		t.Fatal("evicted loaded/pinned model", e)
	}
	c.Free = func(string) (int64, error) { return 0, nil }
	if _, e := c.Prepare(context.Background(), b); e == nil {
		t.Fatal("disk full accepted")
	}
	if _, e := os.Stat(filepath.Join(c.Root, cacheKey(a))); e != nil {
		t.Fatal("protected model missing")
	}
}
func TestCacheInterruptedCopyRetainedChecksumAndDirect(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "data")
	partial := filepath.Join(c.Root, cacheKey(m)+".partial")
	if e := os.WriteFile(partial, []byte("da"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := c.init(); e != nil {
		t.Fatal(e)
	}
	if st, e := os.Stat(partial); e != nil || st.Size() != 2 {
		t.Fatal("resumable prefix lost", e)
	}
	m.SHA256 = strings.Repeat("0", 64)
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("bad checksum accepted")
	}
	entries, _ := os.ReadDir(c.Root)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".model") {
			t.Fatal("bad copy published")
		}
	}
	m = model("m", "data")
	m.DirectNAS = true
	p, e := c.Prepare(context.Background(), m)
	if e != nil || p != filepath.Join(c.NASRoot, m.Path) {
		t.Fatal(p, e)
	}
	m.Path = "../escape"
	if _, e = c.Prepare(context.Background(), m); e == nil {
		t.Fatal("path traversal")
	}
}
func TestCacheCancelledCopyAndSizeMismatch(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "data")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.Prepare(ctx, m); e == nil {
		t.Fatal("cancel accepted")
	}
	m.SizeBytes = 3
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("size mismatch accepted")
	}
}
func TestFetchRejectsInternetAtLoadAndOverwrite(t *testing.T) {
	if e := FetchModel(context.Background(), "http://huggingface.co/a", "unused", 1, strings.Repeat("0", 64)); e == nil {
		t.Fatal("insecure URL accepted")
	}
	if e := FetchModel(context.Background(), "https://example.com/a", "unused", 1, strings.Repeat("0", 64)); e == nil {
		t.Fatal("unapproved source")
	}
	p := filepath.Join(t.TempDir(), "master")
	os.WriteFile(p, []byte("existing"), 0600)
	if e := FetchModel(context.Background(), "https://huggingface.co/a", p, 1, strings.Repeat("0", 64)); e == nil {
		t.Fatal("overwrite accepted")
	}
}

func TestDiskPressureEvictsUntilFree(t *testing.T) {
	c, model := cacheFixture(t)
	c.Limit = 100
	a, b := model("a", "aaaaaaaa"), model("b", "bbbbbbbb")
	if _, e := c.Prepare(context.Background(), a); e != nil {
		t.Fatal(e)
	}
	c.Free = func(root string) (int64, error) {
		entries, e := os.ReadDir(root)
		var used int64
		for _, entry := range entries {
			st, err := entry.Info()
			if err != nil {
				return 0, err
			}
			used += st.Size()
		}
		return 8 + int64(len(stateFormat)) - used, e
	}
	if _, e := c.Prepare(context.Background(), b); e != nil {
		t.Fatal("disk-space eviction failed", e)
	}
	if _, e := os.Stat(filepath.Join(c.Root, cacheKey(a))); !os.IsNotExist(e) {
		t.Fatal("disk pressure did not evict LRU")
	}
}

func TestCatalogRefusesRemoteLoadArgs(t *testing.T) {
	for _, arg := range []string{"--hf-repo", "--hf-file=weights.gguf", "-hf", "--model-url=https://example.com/m", "--mmproj-url"} {
		c := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{{Name: "m", Path: "m.gguf", Engine: "llama.cpp", SizeBytes: 4, SHA256: strings.Repeat("a", 64), MemoryGB: 1, Platforms: []string{"fake"}, Args: []string{arg}}}}
		if e := validateModels(c); e == nil {
			t.Fatal("remote load option accepted", arg)
		}
	}
	t.Setenv("LLAMA_ARG_HF_REPO", "repo")
	if e := (&LlamaEngine{}).Load(context.Background(), Model{}); e == nil {
		t.Fatal("remote environment accepted")
	}
}

type fetchTransport func(*http.Request) (*http.Response, error)

func (f fetchTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type interruptedFetchReader struct{}

func (interruptedFetchReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestFetchRejectsConcurrentResume(t *testing.T) {
	old := http.DefaultClient
	defer func() { http.DefaultClient = old }()
	started, release := make(chan struct{}), make(chan struct{})
	http.DefaultClient = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-release
		return &http.Response{StatusCode: http.StatusPartialContent, Header: http.Header{"Content-Range": []string{"bytes 2-3/4"}}, Body: io.NopCloser(strings.NewReader("ta"))}, nil
	})}
	dest := filepath.Join(t.TempDir(), "model")
	if err := os.WriteFile(dest+".partial", []byte("da"), 0600); err != nil {
		t.Fatal(err)
	}
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte("data")))
	url := "https://huggingface.co/repo/resolve/hash/file"
	done := make(chan error, 1)
	go func() { done <- FetchModel(context.Background(), url, dest, 4, sum) }()
	<-started
	err := FetchModel(context.Background(), url, dest, 4, sum)
	close(release)
	if err == nil {
		t.Fatal("concurrent resume accepted")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := verifyModel(dest, Model{Name: "model", SizeBytes: 4, SHA256: sum}); err != nil {
		t.Fatal(err)
	}
	if err := FetchModel(context.Background(), url, dest, 4, sum); err == nil {
		t.Fatal("published destination overwritten")
	}
	if _, err := os.Lstat(dest + ".partial"); !os.IsNotExist(err) {
		t.Fatal("concurrent or late fetch left an extra partial", err)
	}
}

func TestFetchRecoversInvalidPartial(t *testing.T) {
	for _, prefix := range []string{"bad!", "bad", "oversized"} {
		t.Run(prefix, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			dest := filepath.Join(home, "bundle", "model")
			if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dest+".partial", []byte(prefix), 0600); err != nil {
				t.Fatal(err)
			}
			old := http.DefaultClient
			defer func() { http.DefaultClient = old }()
			http.DefaultClient = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
				res := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data"))}
				if r.Header.Get("Range") != "" {
					res.StatusCode = http.StatusPartialContent
					res.Header.Set("Content-Range", "bytes 3-3/4")
					res.Body = io.NopCloser(strings.NewReader("a"))
				}
				return res, nil
			})}
			sum := fmt.Sprintf("%x", sha256.Sum256([]byte("data")))
			if err := FetchModel(context.Background(), "https://huggingface.co/a", dest, 4, sum); err != nil {
				t.Fatal(err)
			}
			if err := verifyModel(dest, Model{Name: "model", SizeBytes: 4, SHA256: sum}); err != nil {
				t.Fatal(err)
			}
			trash, err := os.ReadDir(filepath.Join(home, ".Trash", "myce-model-cache"))
			if err != nil || len(trash) != 1 {
				t.Fatalf("invalid partial not preserved: %v %v", trash, err)
			}
			if _, err := os.Lstat(dest + ".partial"); !os.IsNotExist(err) {
				t.Fatal("partial remains", err)
			}
		})
	}
}

func TestFetchSyncsPublicationAndRecovers(t *testing.T) {
	home := t.TempDir()
	dest := filepath.Join(home, "model")
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte("data")))
	if err := os.WriteFile(dest+".partial", []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	syncDir := func(dir string) error {
		calls++
		if _, err := os.Stat(dest); err != nil {
			t.Fatal(err)
		}
		_, err := os.Stat(dest + ".partial")
		if calls == 1 {
			if err != nil {
				t.Fatal("partial missing before link sync", err)
			}
			return errors.New("injected sync failure")
		}
		if calls == 2 && err != nil {
			t.Fatal("partial missing before retry sync", err)
		}
		if calls == 3 && !os.IsNotExist(err) {
			t.Fatal("partial remains at cleanup sync", err)
		}
		return syncWorkerDir(dir)
	}
	url := "https://huggingface.co/a"
	if err := fetchModel(context.Background(), url, dest, 4, sum, syncDir); err == nil {
		t.Fatal("sync failure ignored")
	}
	if err := fetchModel(context.Background(), url, dest, 4, sum, syncDir); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("directory sync calls = %d", calls)
	}
}

func TestFetchBundleResumesAndVerifies(t *testing.T) {
	old := http.DefaultClient
	defer func() { http.DefaultClient = old }()
	attempt := 0
	http.DefaultClient = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		attempt++
		response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}
		switch attempt {
		case 1:
			response.Body = io.NopCloser(io.MultiReader(strings.NewReader("wei"), interruptedFetchReader{}))
		case 2:
			if got := r.Header.Get("Range"); got != "bytes=3-" {
				t.Fatalf("resume range = %q", got)
			}
			response.StatusCode = http.StatusPartialContent
			response.Header.Set("Content-Range", "bytes 3-6/7")
			response.Body = io.NopCloser(strings.NewReader("ghts"))
		default:
			response.Body = io.NopCloser(strings.NewReader("{}"))
		}
		return response, nil
	})}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	files := []ModelFile{
		{Path: "weights/model.bin", SizeBytes: 7, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("weights")))},
		{Path: "config.json", SizeBytes: 2, SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("{}")))},
	}
	url := "https://huggingface.co/repo/resolve/hash/file"
	dest := filepath.Join(root, files[0].Path)
	if err := FetchModel(context.Background(), url, dest, files[0].SizeBytes, files[0].SHA256); err == nil {
		t.Fatal("interruption accepted")
	}
	for _, f := range files {
		if err := FetchModel(context.Background(), url, filepath.Join(root, f.Path), f.SizeBytes, f.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	size, sum, err := ModelFilesIdentity(files)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyModelFiles(context.Background(), root, Model{Name: "bundle", Files: files, SizeBytes: size, SHA256: sum}); err != nil {
		t.Fatal(err)
	}
}

func TestExplicitFetchVerifiesMaster(t *testing.T) {
	old := http.DefaultClient
	defer func() { http.DefaultClient = old }()
	http.DefaultClient = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("data")), Header: make(http.Header)}, nil
	})}
	home := t.TempDir()
	t.Setenv("HOME", home)
	dest := filepath.Join(home, "nas", "m.gguf")
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte("data")))
	if e := FetchModel(context.Background(), "https://huggingface.co/repo/resolve/hash/m.gguf", dest, 4, sum); e != nil {
		t.Fatal(e)
	}
	if e := verifyModel(dest, Model{Name: "m", SizeBytes: 4, SHA256: sum}); e != nil {
		t.Fatal(e)
	}
	bad := filepath.Join(home, "nas", "bad.gguf")
	if e := FetchModel(context.Background(), "https://huggingface.co/repo/resolve/hash/m.gguf", bad, 4, strings.Repeat("0", 64)); e == nil {
		t.Fatal("fetch accepted bad master")
	}
	if _, e := os.Stat(bad); !os.IsNotExist(e) {
		t.Fatal("bad master published")
	}
}

func TestCacheInventoryDoesNotBlockHeartbeatDuringCopy(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "data")
	c.mu.Lock()
	defer c.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, _, e := c.Report([]Model{m}); done <- e }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(testWaitTimeout):
		t.Fatal("inventory blocked behind copy lock; worker heartbeat would stop")
	}
}

func TestCacheReuseEnforcesLoweredLimit(t *testing.T) {
	c, model := cacheFixture(t)
	a, b := model("a", "aaaa"), model("b", "bbbb")
	for _, m := range []Model{a, b} {
		if _, err := c.Prepare(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	c.Limit = 4 // operator restarts with a smaller allowance
	c.Protected = func(key string) bool { return key == cacheKey(b) }
	if _, err := c.Prepare(context.Background(), a); err == nil {
		t.Fatal("cached load ignored over-limit protected data")
	}
	c.Protected = nil
	if _, err := c.Prepare(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(c.Root, cacheKey(b))); !os.IsNotExist(err) {
		t.Fatal("cached load did not enforce smaller allowance", err)
	}
}

func TestCacheReportIncludesUnwrittenReservations(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "12345678")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.beforeChunk = func(context.Context) { once.Do(func() { close(entered); <-release }) }
	done := make(chan error, 1)
	go func() { _, err := c.Prepare(context.Background(), m); done <- err }()
	<-entered
	names, free, err := c.Report([]Model{m})
	close(release)
	if copyErr := <-done; copyErr != nil {
		t.Fatal(copyErr)
	}
	if err != nil || len(names) != 0 || free != 0 {
		t.Fatalf("unwritten copy advertised free space: %v %d %v", names, free, err)
	}
}

func TestCorruptProtectedCopyStopsAdvertisingVerifiedCache(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "good")
	path, err := c.Prepare(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	c.Protected = func(string) bool { return true }
	if err := os.WriteFile(path, []byte("evil"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Prepare(context.Background(), m); err == nil {
		t.Fatal("corrupt protected copy accepted")
	}
	if names, _, err := c.Report([]Model{m}); err != nil || len(names) != 0 {
		t.Fatal("corrupt copy still advertised verified", names, err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "evil" {
		t.Fatal("protected file was evicted", err)
	}
}
