// Package testguard isolates test binaries from user storage and vendor tool downloads.
package testguard

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type transport struct{ base http.RoundTripper }

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Hostname() == "github.com" && (strings.HasPrefix(r.URL.Path, "/prefix-dev/pixi/") || strings.HasPrefix(r.URL.Path, "/astral-sh/uv/")) {
		panic("TEST GUARD: real environment tool download forbidden; inject ToolBootstrapProbe.Fetch")
	}
	return t.base.RoundTrip(r)
}

// Install runs before test helpers too. Children inherit the isolated home;
// only its owner removes it after every test and child has finished.
func Install() func() {
	cleanup := func() {}
	home := os.Getenv("MYCE_TEST_ISOLATED_HOME")
	if home == "" {
		var err error
		home, err = os.MkdirTemp("", "myce-test-home-")
		if err != nil {
			panic(err)
		}
		cleanup = func() { _ = os.RemoveAll(home) }
		if err = os.Setenv("MYCE_TEST_ISOLATED_HOME", home); err != nil {
			panic(err)
		}
	}
	for key, value := range map[string]string{"HOME": home, "USERPROFILE": home, "XDG_CONFIG_HOME": filepath.Join(home, ".config"), "XDG_CACHE_HOME": filepath.Join(home, ".cache"), "XDG_DATA_HOME": filepath.Join(home, ".local", "share")} {
		if err := os.Setenv(key, value); err != nil {
			panic(err)
		}
	}
	http.DefaultTransport = transport{http.DefaultTransport}
	return cleanup
}
