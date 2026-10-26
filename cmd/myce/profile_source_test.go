package main

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestProfileSourceExplicitAndStable(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "script"), []byte("hello"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(root, "script"), 0755); err != nil {
		t.Fatal(err)
	}
	a, err := freezeProfileSource(context.Background(), root, profileSource{Directory: "."}, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := freezeProfileSource(context.Background(), root, profileSource{Directory: "."}, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil || a != b {
		t.Fatal("unstable directory snapshot", a, b, err)
	}
	for _, args := range [][]string{{"init", "--quiet"}, {"add", "script"}, {"-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, out)
		}
	}
	gitRef, err := freezeProfileSource(context.Background(), root, profileSource{Git: "HEAD"}, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil {
		t.Fatal(err)
	}
	gitAgain, err := freezeProfileSource(context.Background(), root, profileSource{Git: "HEAD"}, filepath.Join(t.TempDir(), "source.tar"))
	if err != nil || gitAgain != gitRef {
		t.Fatal("unstable git tree snapshot", err)
	}
	receipts := filepath.Join(root, ".mycelium", "receipts")
	if err = os.MkdirAll(receipts, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(receipts, "submit.lock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = identifyProfileSource(context.Background(), root, profileSource{Git: "HEAD"}); err != nil {
		t.Fatal("own unignored receipts made HEAD dirty", err)
	}
	if err = os.WriteFile(filepath.Join(root, "script"), []byte("dirty"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err = identifyProfileSource(context.Background(), root, profileSource{Git: "HEAD"}); err == nil {
		t.Fatal("dirty HEAD silently archived")
	}
	if _, _, err = identifyProfileSource(context.Background(), root, profileSource{Git: "HEAD", AllowDirty: true}); err != nil {
		t.Fatal("explicit dirty opt-in refused", err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.tar")
	raw, err := os.Create(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(raw)
	if err = tw.WriteHeader(&tar.Header{Name: "./script", Mode: 0755, Size: 5, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(tw, "hello"); err != nil {
		t.Fatal(err)
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	raw.Close()
	canonical := filepath.Join(t.TempDir(), "canonical.tar")
	if _, err = freezeProfileSource(context.Background(), root, profileSource{Snapshot: snapshot}, canonical); err != nil {
		t.Fatal("normal tar ./ member rejected", err)
	}
	f, err := os.Open(canonical)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := tar.NewReader(f).Next()
	if err != nil || h.Name != "script" {
		t.Fatal("tar ./ name not normalized", h, err)
	}
	if err = os.Symlink(filepath.Join(root, "script"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err = freezeProfileSource(context.Background(), root, profileSource{Directory: "."}, filepath.Join(t.TempDir(), "source.tar")); err == nil {
		t.Fatal("followed undeclared symlink")
	}
}

func TestProfileSourceKeepsInTreeSymlinks(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "dir"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "script"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"top": "script", "dir/inner": "../script", "chain": "dir/inner"} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	git := func(dir string, args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, out)
		}
	}
	git(root, "init", "--quiet")
	git(root, "add", ".")
	git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "fixture")
	frozen := filepath.Join(t.TempDir(), "source.tar")
	if _, err := freezeProfileSource(context.Background(), root, profileSource{Git: "HEAD"}, frozen); err != nil {
		t.Fatal("in-tree git symlinks refused", err)
	}
	// Workers unpack with the system tar inside the attempt folder.
	out := t.TempDir()
	if b, err := exec.Command("tar", "-xf", frozen, "-C", out).CombinedOutput(); err != nil {
		t.Fatalf("tar: %v %s", err, b)
	}
	for _, name := range []string{"top", "dir/inner", "chain"} {
		if b, err := os.ReadFile(filepath.Join(out, name)); err != nil || string(b) != "hello" {
			t.Fatal("symlink not restored", name, string(b), err)
		}
		if info, err := os.Lstat(filepath.Join(out, name)); err != nil || info.Mode()&os.ModeSymlink == 0 {
			t.Fatal("symlink copied as file", name, err)
		}
	}
	if err := os.Symlink("../outside", filepath.Join(root, "up")); err != nil {
		t.Fatal(err)
	}
	git(root, "add", "up")
	git(root, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "escape")
	if _, err := freezeProfileSource(context.Background(), root, profileSource{Git: "HEAD"}, filepath.Join(t.TempDir(), "source.tar")); err == nil {
		t.Fatal("escaping git symlink accepted")
	}
	type entry struct{ name, link string }
	for label, entries := range map[string][]entry{
		"absolute":           {{"abs", "/etc/passwd"}},
		"escape via link":    {{"s", "."}, {"l", "s/.."}},
		"entry beneath link": {{"d", "sub"}, {"d/f", ""}},
		"loop":               {{"a", "b"}, {"b", "a"}},
	} {
		snapshot := filepath.Join(t.TempDir(), "snapshot.tar")
		raw, err := os.Create(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		tw := tar.NewWriter(raw)
		for _, e := range entries {
			h := &tar.Header{Name: e.name, Mode: 0777, Typeflag: tar.TypeSymlink, Linkname: e.link}
			if e.link == "" {
				h = &tar.Header{Name: e.name, Mode: 0644, Typeflag: tar.TypeReg}
			}
			if err = tw.WriteHeader(h); err != nil {
				t.Fatal(err)
			}
		}
		if err = tw.Close(); err != nil {
			t.Fatal(err)
		}
		raw.Close()
		if _, err = freezeProfileSource(context.Background(), root, profileSource{Snapshot: snapshot}, filepath.Join(t.TempDir(), "source.tar")); err == nil {
			t.Fatal("unsafe snapshot symlink accepted:", label)
		}
	}
}
