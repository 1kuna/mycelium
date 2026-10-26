package myce

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sharedFixtureFile(t *testing.T, root, name, data string) FileRef {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0700); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
func sharedArchive(t *testing.T, root string) SharedInput {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	if err := tw.WriteHeader(&tar.Header{Name: "bin/run", Mode: 0755, Size: 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	ref := sharedFixtureFile(t, root, "runtime.tar", b.String())
	return SharedInput{NAS: "runtime.tar", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes, Archive: "tar", UnpackedBytes: 5}
}
func TestSharedInputsReuseTamperAndEviction(t *testing.T) {
	ctx := context.Background()
	nas := t.TempDir()
	s := sharedArchive(t, nas)
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}}
	t.Cleanup(func() {
		if err := removeSharedEntry(cache.cache.Root); err != nil {
			t.Error(err)
		}
	})
	path, release, err := cache.Acquire(ctx, s, nas)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(path, "bin/run"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0222 != 0 || info.Mode().Perm()&0100 == 0 {
		t.Fatal(info.Mode())
	}
	if err = os.Rename(filepath.Join(nas, "runtime.tar"), filepath.Join(nas, "offline")); err != nil {
		t.Fatal(err)
	}
	path2, release2, err := cache.Acquire(ctx, s, nas)
	if err != nil {
		t.Fatal(err)
	}
	if path != path2 {
		t.Fatal("attempt got a different tree")
	}
	release2()
	// Even disk pressure cannot evict an acquired entry.
	cache.cache.Limit = 1
	if err = cache.admit(ctx, "other", 10); err == nil {
		t.Fatal("evicted active input")
	}
	if _, err = os.Stat(path); err != nil {
		t.Fatal(err)
	}
	release()
	// Restart verifies payload hashes even when metadata is unchanged to callers.
	restarted := &SharedInputCache{cache: EnvironmentCache{Root: cache.cache.Root, Limit: 1 << 20}}
	p, done, err := restarted.Acquire(ctx, s, nas)
	if err != nil {
		t.Fatal(err)
	}
	done()
	file := filepath.Join(p, "bin/run")
	if err = os.Chmod(file, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(file, []byte("wrong"), 0500); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(file, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, done, err = restarted.Acquire(ctx, s, nas); err == nil {
		done()
		t.Fatal("tampered warm cache accepted")
	}
	cold := &SharedInputCache{cache: EnvironmentCache{Root: cache.cache.Root, Limit: 1 << 20}}
	if _, done, err = cold.Acquire(ctx, s, nas); err == nil {
		done()
		t.Fatal("tampered cache accepted after restart")
	}
	if err = cache.admit(ctx, "other", 10); err == nil {
		t.Fatal("over budget admission succeeded")
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("released entry was not evicted", err)
	}
}
func TestSharedInputsNASTreeAndArchiveConfinement(t *testing.T) {
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "tree/bin/tool", "tool")
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}}
	t.Cleanup(func() { _ = removeSharedEntry(cache.cache.Root) })
	s := SharedInput{NAS: "tree", Manifest: FileManifest{"bin/tool": ref}}
	path, done, err := cache.Acquire(context.Background(), s, nas)
	if err != nil {
		t.Fatal(err)
	}
	done()
	if got, err := os.ReadFile(filepath.Join(path, "bin/tool")); err != nil || string(got) != "tool" {
		t.Fatal(string(got), err)
	}
	manifest, e := json.Marshal(FileManifest{"bin/tool": ref})
	if e != nil {
		t.Fatal(e)
	}
	manifestRef := sharedFixtureFile(t, nas, "manifests/tree.json", string(manifest))
	byManifest := SharedInput{NAS: "tree", ManifestFile: "manifests/tree.json", SHA256: manifestRef.SHA256, SizeBytes: manifestRef.SizeBytes, UnpackedBytes: ref.SizeBytes}
	p, done, e := cache.Acquire(context.Background(), byManifest, nas)
	if e != nil {
		t.Fatal(e)
	}
	done()
	if e = os.Remove(filepath.Join(nas, "manifests/tree.json")); e != nil {
		t.Fatal(e)
	}
	p2, done, e := cache.Acquire(context.Background(), byManifest, nas)
	if e != nil {
		t.Fatal(e)
	}
	done()
	if p != p2 {
		t.Fatal("manifest tree not reused")
	}
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	_ = tw.WriteHeader(&tar.Header{Name: "../escape", Size: 1, Mode: 0600})
	_, _ = tw.Write([]byte("x"))
	_ = tw.Close()
	bad := sharedFixtureFile(t, nas, "bad.tar", b.String())
	s = SharedInput{NAS: "bad.tar", SHA256: bad.SHA256, SizeBytes: bad.SizeBytes, Archive: "tar", UnpackedBytes: 1}
	if _, done, err = cache.Acquire(context.Background(), s, nas); err == nil {
		done()
		t.Fatal("unsafe archive accepted")
	}
	s.NAS = "../bad.tar"
	if err = s.validate(); err == nil {
		t.Fatal("unsafe NAS accepted")
	}
	s = SharedInput{NAS: "tree", Manifest: FileManifest{"bin/tool": {SHA256: strings.Repeat("0", 64), SizeBytes: 4}}}
	if _, done, err = cache.Acquire(context.Background(), s, nas); err == nil {
		done()
		t.Fatal("wrong manifest accepted")
	}
}

func TestSharedInputsInterruptedStagingAndNASSizeGuard(t *testing.T) {
	ctx := context.Background()
	nas := t.TempDir()
	data := strings.Repeat("x", 64<<10)
	ref := sharedFixtureFile(t, nas, "input", data)
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 100 << 10}}
	t.Cleanup(func() { _ = removeSharedEntry(cache.cache.Root) })
	if err := cache.cache.init(); err != nil {
		t.Fatal(err)
	}
	input := SharedInput{NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	partial := filepath.Join(cache.cache.Root, input.key()+".partial")
	_ = sharedFixtureFile(t, partial, "interrupted", data)
	if _, done, err := cache.Acquire(ctx, input, nas); err != nil {
		t.Fatal("stale staging prevented recovery", err)
	} else {
		done()
	}
	// A stale source cannot write beyond the bytes reserved at admission.
	target := filepath.Join(t.TempDir(), "bounded")
	bad := ref
	bad.SizeBytes = 1
	if err := copySharedFile(ctx, filepath.Join(nas, "input"), target, bad); err == nil {
		t.Fatal("wrong NAS size accepted")
	}
	if info, err := os.Stat(target); err == nil && info.Size() > bad.SizeBytes {
		t.Fatal("copy exceeded its reservation", info.Size())
	}
}
