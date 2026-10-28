package main

import (
	"archive/tar"
	"debug/elf"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"mycelium/internal/myce"
)

// Transport installed tools as one immutable cached file; each attempt unpacks
// its private runtime without the shared archive cache's per-member syncs.
func adapterDogfoodRuntime(t *testing.T) myce.SharedInput {
	t.Helper()
	if runtime.GOOS != "linux" {
		return myce.SharedInput{}
	}
	path := filepath.Join(t.TempDir(), "runtime.tar")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tw := tar.NewWriter(f)
	add := func(name, source string) {
		t.Helper()
		in, err := os.Open(source)
		if err != nil {
			t.Fatal(err)
		}
		defer in.Close()
		info, err := in.Stat()
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("runtime member %s is not a regular file: %v", source, err)
		}
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(name), Mode: int64(info.Mode().Perm()), Size: info.Size(), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(tw, in); err != nil {
			t.Fatal(err)
		}
	}
	// Keep the real standard library and compiler/linker together rather than
	// maintaining a synthetic list of Go source imports for the tiny test.
	goroot := runtime.GOROOT()
	if err := filepath.WalkDir(goroot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(goroot, path)
		if err != nil {
			return err
		}
		add(filepath.Join("go", rel), path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Fatal(err)
	}
	uv, err = filepath.EvalSymlinks(uv)
	if err != nil {
		t.Fatal(err)
	}
	add("bin/uv", uv)
	// Resolve uv's ELF closure: the locked Linux gate currently needs its own
	// libzstd and libgcc_s. Static uv requires no additional libraries.
	system := []string{"/lib", "/lib64", "/usr/lib", "/usr/lib64"}
	for _, pattern := range []string{"/lib/*-linux-gnu", "/usr/lib/*-linux-gnu"} {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		system = append(system, paths...)
	}
	seen := map[string]string{}
	var libraries func(string)
	libraries = func(path string) {
		t.Helper()
		binary, err := elf.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer binary.Close()
		needed, err := binary.ImportedLibraries()
		if err != nil {
			t.Fatal(err)
		}
		var search []string
		for _, tag := range []elf.DynTag{elf.DT_RUNPATH, elf.DT_RPATH} {
			paths, err := binary.DynString(tag)
			if err != nil {
				t.Fatal(err)
			}
			for _, paths := range paths {
				for _, dir := range filepath.SplitList(paths) {
					search = append(search, strings.NewReplacer("${ORIGIN}", filepath.Dir(path), "$ORIGIN", filepath.Dir(path)).Replace(dir))
				}
			}
		}
		for _, name := range needed {
			var resolved string
			for _, dir := range append(search, system...) {
				candidate := filepath.Join(dir, name)
				if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
					resolved, err = filepath.EvalSymlinks(candidate)
					if err != nil {
						t.Fatal(err)
					}
					break
				}
			}
			if resolved == "" {
				t.Fatalf("cannot resolve runtime library %s for %s", name, path)
			}
			if strings.HasPrefix(resolved, "/usr/") || strings.HasPrefix(resolved, "/lib/") || strings.HasPrefix(resolved, "/lib64/") {
				continue
			}
			if previous, ok := seen[name]; ok {
				if previous != resolved {
					t.Fatalf("conflicting runtime library %s: %s and %s", name, previous, resolved)
				}
				continue
			}
			seen[name] = resolved
			add(filepath.Join("lib", name), resolved)
			libraries(resolved)
		}
	}
	libraries(uv)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return myce.SharedInput{Local: path}
}
