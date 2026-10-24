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
