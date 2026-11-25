package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
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

	{
		m = model("m", "data")

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
