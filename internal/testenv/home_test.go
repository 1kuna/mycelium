package testenv

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// #217: fleet go-test jobs inherit the worker service's 0077 umask.
func TestIsolateResetsFleetJobUmask(t *testing.T) {
	for _, key := range []string{"HOME", "TMPDIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "UV_CACHE_DIR", "PIXI_HOME", "GOMODCACHE", "GOCACHE", "UV_OFFLINE", "UV_PYTHON_DOWNLOADS", "GOTOOLCHAIN", "GOPROXY", "GOSUMDB", "GOTELEMETRY"} {
		t.Setenv(key, os.Getenv(key))
	}
	previous := syscall.Umask(0o077)
	t.Cleanup(func() { syscall.Umask(previous) })

	dir := t.TempDir()
	if err := Isolate(filepath.Join(dir, "root")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "public")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o644 {
		t.Fatalf("requested mode 644 became %o", st.Mode().Perm())
	}
}
