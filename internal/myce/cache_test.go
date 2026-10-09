package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
func TestCacheInterruptedCopyCleanupChecksumAndDirect(t *testing.T) {
	c, model := cacheFixture(t)
	m := model("m", "data")
	partial := filepath.Join(c.Root, cacheKey(m)+".partial")
	if e := os.WriteFile(partial, []byte("da"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := c.init(); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(partial); !os.IsNotExist(e) {
		t.Fatal("partial survived restart")
	}
	m.SHA256 = strings.Repeat("0", 64)
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("bad checksum accepted")
	}
	entries, _ := os.ReadDir(c.Root)
	if len(entries) != 0 {
		t.Fatal("bad copy published")
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
		return 8 - used, e
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
	case <-time.After(time.Second):
		t.Fatal("inventory blocked behind copy lock; worker heartbeat would stop")
	}
}
