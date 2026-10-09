package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

func TestModelFilesIdentityCanonicalAndRejectsUnsafe(t *testing.T) {
	hash := strings.Repeat("a", 64)
	files := []ModelFile{{Path: "z", SizeBytes: 2, SHA256: strings.ToUpper(hash)}, {Path: "a", SizeBytes: 1, SHA256: hash}}
	before := append([]ModelFile(nil), files...)
	size, sum, e := ModelFilesIdentity(files)
	if e != nil || size != 3 {
		t.Fatal(size, sum, e)
	}
	_, other, e := ModelFilesIdentity([]ModelFile{files[1], {Path: "z", SizeBytes: 2, SHA256: hash}})
	if e != nil || sum != other || !reflect.DeepEqual(before, files) {
		t.Fatal("identity not canonical or changed caller", e)
	}
	for _, p := range []string{"", ".", "..", "../a", "a/../b", "/a", "a//b", "a\\b"} {
		if _, _, e = ModelFilesIdentity([]ModelFile{{Path: p, SHA256: hash, SizeBytes: 1}}); e == nil {
			t.Fatal("unsafe path accepted", p)
		}
	}
	for _, bad := range [][]ModelFile{nil, {{Path: "a", SHA256: hash}, {Path: "a", SHA256: hash}}, {{Path: "a", SHA256: hash}, {Path: "A", SHA256: hash}}, {{Path: "a", SHA256: hash}, {Path: "a/b", SHA256: hash}}, {{Path: "a", SizeBytes: -1, SHA256: hash}}, {{Path: "a", SHA256: "bad"}}} {
		if _, _, e = ModelFilesIdentity(bad); e == nil {
			t.Fatal("invalid manifest accepted", bad)
		}
	}
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
	names, free, e := c.Report([]Model{m})
	if e != nil || free != 0 || !reflect.DeepEqual(names, []string{m.Name}) {
		t.Fatal(names, free, e)
	}
	restarted := &ModelCache{Root: c.Root, NASRoot: c.NASRoot, Limit: c.Limit, Clock: c.Clock, Free: c.Free}
	if e = restarted.init(); e != nil {
		t.Fatal(e)
	}
	restarted.ValidateInventory([]Model{m})
	names, _, e = restarted.Report([]Model{m})
	if e != nil || len(names) != 1 {
		t.Fatal(names, e)
	}
	other := bundleFixture(t, c, "other")
	c.Protected = func(key string) bool { return key == cacheKey(m) }
	if _, e = c.Precopy(context.Background(), other, 1); e == nil {
		t.Fatal("protected bundle evicted")
	}
	c.Protected = nil
	c.Usage = func(key string) int {
		if key == cacheKey(m) {
			return 2
		}
		return 0
	}
	if _, e = c.Precopy(context.Background(), other, 1); e == nil {
		t.Fatal("more used bundle evicted")
	}
	c.Usage = nil
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
	c.ValidateInventory([]Model{m})
	reports, _ := c.CopyReports()
	if len(reports) != 1 || reports[0].Bytes != 5 {
		t.Fatal(reports)
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
	reports, _ = c.CopyReports()
	if len(reports) != 1 || reports[0].State != "ready" || reports[0].Bytes != 8 {
		t.Fatal(reports)
	}
}

func TestModelBundleRejectsCorruptionAndSymlinks(t *testing.T) {
	for _, mode := range []string{"hash", "symlink-file", "symlink-directory", "extra-local", "identity"} {
		t.Run(mode, func(t *testing.T) {
			c := modelBundleCacheFixture(t)
			m := bundleFixture(t, c, "bundle")
			switch mode {
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
		if chunks == 4 {
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

func TestModelBundleMissingNASPreservesResumeBytes(t *testing.T) {
	c := modelBundleCacheFixture(t)
	m := bundleFixture(t, c, "bundle")
	partial := filepath.Join(c.Root, cacheKey(m)+".partial")
	if e := os.MkdirAll(partial, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(partial, "config.json"), []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Rename(filepath.Join(c.NASRoot, m.Path), filepath.Join(c.NASRoot, m.Path+"-saved")); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Prepare(context.Background(), m); e == nil {
		t.Fatal("missing NAS accepted")
	}
	if n, e := cacheEntrySize(partial); e != nil || n != 1 {
		t.Fatal("resume bytes lost", n, e)
	}
}
