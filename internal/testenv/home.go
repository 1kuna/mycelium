// Package testenv confines test subprocess state and tool caches to owned roots.
package testenv

import (
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func Isolate(dir string) error {
	originalHome, _ := os.UserHomeDir()
	// A CLI build may read modules already fetched by the parent go test. Use
	// Go's local file proxy to populate an owned cache, never the network or HOME.
	proxy := "off"
	cache := os.Getenv("GOMODCACHE")
	relative, _ := filepath.Rel(originalHome, cache)
	if cache == "" || relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		if cache == "" {
			command := exec.Command("go", "env", "GOMODCACHE")
			command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOTELEMETRY=off")
			if output, err := command.Output(); err == nil {
				cache = strings.TrimSpace(string(output))
			}
		}
		if cache != "" {
			proxy = (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(cache, "cache", "download"))}).String()
			if err := os.Setenv("GOMODCACHE", filepath.Join(dir, "go-modules")); err != nil {
				return err
			}
		}
	}
	for key, value := range map[string]string{"UV_OFFLINE": "1", "UV_PYTHON_DOWNLOADS": "never", "GOTOOLCHAIN": "local", "GOPROXY": proxy, "GOSUMDB": "off", "GOTELEMETRY": "off"} {
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	// An explicitly supplied scratch build cache is reusable; a HOME cache isn't.
	if cache := os.Getenv("GOCACHE"); cache != "" {
		relative, err := filepath.Rel(originalHome, cache)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
			if err := os.Setenv("GOCACHE", filepath.Join(dir, "go-build")); err != nil {
				return err
			}
		}
	}
	for _, key := range []string{"HOME", "TMPDIR", "XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "UV_CACHE_DIR", "PIXI_HOME"} {
		path := filepath.Join(dir, key)
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
		if err := os.Setenv(key, path); err != nil {
			return err
		}
	}
	return nil
}

func Cleanup(dir string) error {
	// Go extracts module directories read-only. These copies belong exclusively
	// to this test root; make only their directories removable before cleanup.
	modules := filepath.Join(dir, "go-modules")
	if _, err := os.Stat(modules); err == nil {
		if err := filepath.WalkDir(modules, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return os.RemoveAll(dir)
}
