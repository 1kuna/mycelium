package myce

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func toolArchive(t *testing.T, name string) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	data := []byte("fake pinned " + name)
	if err := tw.WriteHeader(&tar.Header{Name: "release/" + name, Mode: 0755, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	tw.Write(data)
	tw.Close()
	gz.Close()
	return b.Bytes()
}
func TestOnboardingToolsRefuseTamperAndErrors(t *testing.T) {
	for _, mode := range []string{"checksum", "download", "symlink", "version"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := PlanInstall("worker", t.TempDir(), "", "/fixture/nas", "http://queue.invalid", "hypha", Detection{OS: "linux", Arch: "arm64", Platform: "cpu", MemoryMode: "fixed"})
			before := c
			b := toolArchive(t, "uv")
			sum := sha256.Sum256(b)
			r := ToolRelease{Name: "uv", Version: "0.8.22", SHA256: hex.EncodeToString(sum[:])}
			if mode == "checksum" {
				r.SHA256 = strings.Repeat("0", 64)
			}
			if mode == "symlink" {
				if err := os.Symlink(t.TempDir(), filepath.Join(c.WorkerHome, ".local")); err != nil {
					t.Fatal(err)
				}
			}
			p := ToolBootstrapProbe{Fetch: func(context.Context, ToolRelease) (io.ReadCloser, error) {
				if mode == "download" {
					return nil, fmt.Errorf("private-token")
				}
				return io.NopCloser(bytes.NewReader(b)), nil
			}, Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("private-token"), nil }}
			got, err := bootstrapEnvironmentTools(context.Background(), c, p, []ToolRelease{r})
			code := map[string]string{"checksum": "tool_checksum_mismatch", "download": "tool_download_failed", "symlink": "tool_install_failed", "version": "tool_version_invalid"}[mode]
			requirePreflightCode(t, err, code)
			if strings.Contains(err.Error(), "private-token") || !reflect.DeepEqual(got, before) {
				t.Fatal("error disclosed tool output or changed configuration", err)
			}
		})
	}
}
func TestOnboardingToolReleaseMatrix(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		for _, arch := range []string{"amd64", "arm64"} {
			releases, err := PinnedEnvironmentTools(osname, arch)
			if err != nil || len(releases) != 2 {
				t.Fatal(err)
			}
			for _, r := range releases {
				if len(r.SHA256) != 64 || !strings.HasPrefix(r.URL, "https://github.com/") || r.Version == "" {
					t.Fatal(r)
				}
			}
		}
	}
	_, err := PinnedEnvironmentTools("windows", "arm64")
	requirePreflightCode(t, err, "tool_platform_unsupported")
}
