package main

import (
	"context"
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
	if err = os.Symlink(filepath.Join(root, "script"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err = freezeProfileSource(context.Background(), root, profileSource{Directory: "."}, filepath.Join(t.TempDir(), "source.tar")); err == nil {
		t.Fatal("followed undeclared symlink")
	}
}
