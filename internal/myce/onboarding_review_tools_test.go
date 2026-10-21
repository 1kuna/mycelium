package myce

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReviewExistingToolsKeepPrecedence(t *testing.T) {
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "linux", Arch: "arm64", Platform: "cpu"})
	bin := filepath.Join(c.WorkerHome, "existing-bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"uv", "pixi"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("existing"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	c.Path = bin + ":/usr/bin"
	fetches := 0
	p := ToolBootstrapProbe{Fetch: func(context.Context, ToolRelease) (io.ReadCloser, error) { fetches++; return nil, io.EOF }, Command: func(_ context.Context, n string, _ ...string) ([]byte, error) {
		return []byte(filepath.Base(n) + " 9.0.0"), nil
	}}
	got, err := BootstrapEnvironmentTools(context.Background(), c, p)
	if err != nil || fetches != 0 || got.Path != c.Path {
		t.Fatalf("existing tools shadowed/replaced: downloads=%d PATH=%s err=%v", fetches, got.Path, err)
	}
}
func TestReviewUVArchiveInstallsUVX(t *testing.T) {
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "linux", Arch: "arm64", Platform: "cpu"})
	c.Path = "/fixture/empty"
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	for _, name := range []string{"uv", "uvx"} {
		data := []byte("fake " + name)
		if err := tw.WriteHeader(&tar.Header{Name: "release/" + name, Typeflag: tar.TypeReg, Mode: 0755, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		tw.Write(data)
	}
	tw.Close()
	gz.Close()
	sum := sha256.Sum256(archive.Bytes())
	r := ToolRelease{Name: "uv", Version: "0.8.22", SHA256: hex.EncodeToString(sum[:])}
	p := ToolBootstrapProbe{Fetch: func(context.Context, ToolRelease) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(archive.Bytes())), nil
	}, Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("uv 0.8.22"), nil }}
	got, err := bootstrapEnvironmentTools(context.Background(), c, p, []ToolRelease{r})
	if err != nil {
		t.Fatal(err)
	}
	var installed string
	for _, dir := range filepath.SplitList(got.Path) {
		if strings.Contains(dir, "uv-0.8.22") {
			installed = dir
		}
	}
	if b, err := os.ReadFile(filepath.Join(installed, "uvx")); err != nil || string(b) != "fake uvx" {
		t.Fatal("uvx missing from uv installation", err)
	}
	if !strings.HasPrefix(got.Path, c.Path+":") {
		t.Fatal("bootstrap changed service PATH precedence", got.Path)
	}
}
func TestReviewOfflineServiceDoesNotNeedTools(t *testing.T) {
	t.Setenv("PATH", os.Getenv("PATH"))
	c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "linux", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
	c.Path = filepath.Join(c.WorkerHome, ".local", "share", "mycelium", "tools", "uv-old") + ":/fixture/empty"
	p := MachineProbe{OS: "linux", Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) {
		t.Error("offline idle service ran a backend tool")
		return nil, os.ErrNotExist
	}}
	got, err := DetectServiceHardware(context.Background(), c, p, GPUAccessProbe{})
	if err != nil || got.Platform != "cpu" {
		t.Fatal(got, err)
	}
}
