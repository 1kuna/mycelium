package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	"strings"
	"testing"
)

func bundleFixture(t *testing.T, c *ModelCache, name string) Model {
	t.Helper()
	m := Model{Name: name, Path: name, Engine: "vllm"}
	for _, item := range []struct{ path, data string }{{"config.json", "{}"}, {"weights/model.safetensors", "abcdef"}} {
		p := filepath.Join(c.NASRoot, name, item.path)
		if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(p, []byte(item.data), 0600); e != nil {
			t.Fatal(e)
		}
		m.Files = append(m.Files, ModelFile{Path: item.path, SizeBytes: int64(len(item.data)), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(item.data)))})
	}
	var e error
	m.SizeBytes, m.SHA256, e = ModelFilesIdentity(m.Files)
	if e != nil {
		t.Fatal(e)
	}
	return m
}

func TestModelBundlePrepareInventoryProtectionAndEviction(t *testing.T) {
	c := modelBundleCacheFixture(t)
	m := bundleFixture(t, c, "bundle")
	p, e := c.Prepare(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(p, m); e != nil {
		t.Fatal(e)
	}
	st, e := os.Stat(p)
	if e != nil || !st.IsDir() {
		t.Fatal("not local directory", e)
	}
	other := bundleFixture(t, c, "other")
	c.Protected = func(key string) bool { return key == cacheKey(m) }
	if _, e = c.Precopy(context.Background(), other, 1); e == nil {
		t.Fatal("protected bundle evicted")
	}
	c.Protected = nil
	if _, e = c.Prepare(context.Background(), other); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(p); !os.IsNotExist(e) {
		t.Fatal("bundle not evicted", e)
	}
	trash, e := filepath.Glob(filepath.Join(c.NASRoot, ".Trash", "myce-model-cache", "*"))
	if e != nil || len(trash) != 1 {
		t.Fatal(trash, e)
	}
	if e = verifyModel(trash[0], m); e != nil {
		t.Fatal("bundle recovery failed", e)
	}
}

func TestModelBundleResumeAndProgress(t *testing.T) {
	c := modelBundleCacheFixture(t)
	m := bundleFixture(t, c, "bundle")
	partial := filepath.Join(c.Root, cacheKey(m)+".partial")
	if e := os.MkdirAll(filepath.Join(partial, "weights"), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(partial, "config.json"), []byte("{}"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(partial, "weights/model.safetensors"), []byte("abc"), 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.beforeChunk = func(context.Context) { cancel() }
	if _, e := c.Precopy(ctx, m, 1); e == nil {
		t.Fatal("cancel ignored")
	}
	if n, e := cacheEntrySize(partial); e != nil || n != 5 {
		t.Fatal("partial lost", n, e)
	}
	c.beforeChunk = nil
	p, e := c.Prepare(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(p, m); e != nil {
		t.Fatal(e)
	}
}

func TestModelBundleRejectsCorruptionAndSymlinks(t *testing.T) {
	for _, mode := range []string{"hash", "symlink-file", "symlink-directory", "extra-local", "identity", "path-escape", "case-collision", "ancestor-collision"} {
		t.Run(mode, func(t *testing.T) {
			c := modelBundleCacheFixture(t)
			m := bundleFixture(t, c, "bundle")
			switch mode {
			case "path-escape":
				m.Files[0].Path = "../a"
			case "case-collision":
				m.Files = []ModelFile{{Path: "a", SizeBytes: 1, SHA256: strings.Repeat("a", 64)}, {Path: "A", SizeBytes: 1, SHA256: strings.Repeat("a", 64)}}
			case "ancestor-collision":
				m.Files = []ModelFile{{Path: "a", SizeBytes: 1, SHA256: strings.Repeat("a", 64)}, {Path: "a/b", SizeBytes: 1, SHA256: strings.Repeat("a", 64)}}
			case "identity":
				m.SizeBytes++
			case "hash":
				os.WriteFile(filepath.Join(c.NASRoot, m.Path, "config.json"), []byte("xx"), 0600)
			case "symlink-file":
				// Rename preserves the original fixture rather than deleting it.
				p := filepath.Join(c.NASRoot, m.Path, "config.json")
				if e := os.Rename(p, p+"-saved"); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(p+"-saved", p); e != nil {
					t.Fatal(e)
				}
			case "symlink-directory":
				p := filepath.Join(c.NASRoot, m.Path, "weights")
				if e := os.Rename(p, p+"-saved"); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(p+"-saved", p); e != nil {
					t.Fatal(e)
				}
			case "extra-local":
				p, e := c.Prepare(context.Background(), m)
				if e != nil {
					t.Fatal(e)
				}
				os.WriteFile(filepath.Join(p, "unlisted"), []byte("x"), 0600)
				c.Protected = func(string) bool { return true }
			}
			if _, e := c.Prepare(context.Background(), m); e == nil {
				t.Fatal("unsafe bundle accepted")
			}
			if names, _, e := c.Report([]Model{m}); e == nil && len(names) != 0 {
				t.Fatal("unsafe bundle advertised", names)
			}

		})
	}
}

func TestModelBundleDirectNASAndRecoveryCopy(t *testing.T) {
	c := modelBundleCacheFixture(t)
	m := bundleFixture(t, c, "bundle")
	m.DirectNAS = true
	p, e := c.Prepare(context.Background(), m)
	if e != nil || p != filepath.Join(c.NASRoot, m.Path) {
		t.Fatal(p, e)
	}
	recoveryRoot, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	recovery := filepath.Join(recoveryRoot, "recovery")
	if e = c.recoverModelDirectory(context.Background(), p, recovery); e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(recovery, m); e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(p, m); e != nil {
		t.Fatal("source changed", e)
	}
}

func modelBundleCacheFixture(t *testing.T) *ModelCache {
	t.Helper()
	c, _ := cacheFixture(t)
	var e error
	c.Root, e = filepath.EvalSymlinks(c.Root)
	if e != nil {
		t.Fatal(e)
	}
	c.NASRoot, e = filepath.EvalSymlinks(c.NASRoot)
	if e != nil {
		t.Fatal(e)
	}
	return c
}

func TestModelBundleActiveReservationAndActualResume(t *testing.T) {
	c := modelBundleCacheFixture(t)
	m := bundleFixture(t, c, "bundle")
	data := strings.Repeat("x", 2*1024*1024)
	if e := os.WriteFile(filepath.Join(c.NASRoot, m.Path, "weights/model.safetensors"), []byte(data), 0600); e != nil {
		t.Fatal(e)
	}
	m.Files[1].SizeBytes = int64(len(data))
	m.Files[1].SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(data)))
	var e error
	m.SizeBytes, m.SHA256, e = ModelFilesIdentity(m.Files)
	if e != nil {
		t.Fatal(e)
	}
	c.Limit = m.SizeBytes
	c.Free = func(string) (int64, error) { return m.SizeBytes, nil }
	ctx, cancel := context.WithCancel(context.Background())
	chunks := 0
	c.beforeChunk = func(context.Context) {
		chunks++
		if chunks == 1 {
			names, free, e := c.Report([]Model{m})
			if e != nil || len(names) != 0 || free != 0 {
				t.Errorf("reservation absent %v %d %v", names, free, e)
			}
		}
		partial := filepath.Join(c.Root, cacheKey(m)+".partial")
		if bytes, _ := cacheEntrySize(partial); bytes == 1024*1024+2 {
			cancel()
		}
	}
	if _, e = c.Precopy(ctx, m, 1); e == nil {
		t.Fatal("interruption ignored")
	}
	reports, _ := c.CopyReports()
	if len(reports) != 1 || reports[0].Bytes != 1024*1024+2 || reports[0].State != "paused" {
		t.Fatal(reports)
	}
	c.beforeChunk = nil
	if e := os.Rename(filepath.Join(c.NASRoot, m.Path), filepath.Join(c.NASRoot, m.Path+"-saved")); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("missing NAS accepted")
	}
	if n, e := cacheEntrySize(filepath.Join(c.Root, cacheKey(m)+".partial")); e != nil || n != 1024*1024+2 {
		t.Fatal("resume bytes lost", n, e)
	}
	if e := os.Rename(filepath.Join(c.NASRoot, m.Path+"-saved"), filepath.Join(c.NASRoot, m.Path)); e != nil {
		t.Fatal(e)
	}
	p, e := c.Prepare(context.Background(), m)
	if e != nil {
		t.Fatal(e)
	}
	if e = verifyModel(p, m); e != nil {
		t.Fatal(e)
	}
}

func TestModelBundleCorruptPrefixPreservedAndRetry(t *testing.T) {
	c := modelBundleCacheFixture(t)
	m := bundleFixture(t, c, "bundle")
	partial := filepath.Join(c.Root, cacheKey(m)+".partial")
	if e := os.MkdirAll(partial, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(partial, "config.json"), []byte("xx"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("corrupt prefix accepted")
	}
	trash, _ := filepath.Glob(filepath.Join(c.NASRoot, ".Trash", "myce-model-cache", "*"))
	if len(trash) != 1 {
		t.Fatal("corrupt prefix not recoverable", trash)
	}
	data, e := os.ReadFile(filepath.Join(trash[0], "config.json"))
	if e != nil || string(data) != "xx" {
		t.Fatal("corrupt evidence lost", e)
	}
	if _, e = c.Prepare(context.Background(), m); e != nil {
		t.Fatal("fresh retry failed", e)
	}
}
