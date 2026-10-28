package myce

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJobSourceExtractionAndInputCustody(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, ".myce-source.tar")
	f, err := os.Create(archive)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(f)
	for _, h := range []*tar.Header{{Name: "bin/run", Typeflag: tar.TypeReg, Mode: 0755, Size: 2}, {Name: "run", Typeflag: tar.TypeSymlink, Linkname: "bin/run"}} {
		if err = tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			_, err = tw.Write([]byte("ok"))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err = extractJobSource(archive, root); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "run"))
	if err != nil || string(b) != "ok" {
		t.Fatalf("source %q %v", b, err)
	}
	info, _ := os.Stat(filepath.Join(root, "bin/run"))
	if info.Mode()&0111 == 0 {
		t.Fatal("source executable bit lost")
	}
	if err = removeSourceInput(root, "run"); err != nil {
		t.Fatal(err)
	}
	if b, err = os.ReadFile(filepath.Join(root, "bin/run")); err != nil || string(b) != "ok" {
		t.Fatal("replacing link changed source target")
	}
	if err = os.Symlink("bin", filepath.Join(root, "dir-link")); err != nil {
		t.Fatal(err)
	}
	if err = removeSourceInput(root, "dir-link/run"); err == nil {
		t.Fatal("explicit input traversed source symlink")
	}
	if err = ValidateSourceNames(map[string]bool{"a": true, "b": true}, map[string]string{"a": ".", "b": "a/../escape"}); err == nil || !strings.Contains(err.Error(), "leaves") {
		t.Fatalf("escaped chained link: %v", err)
	}
}
