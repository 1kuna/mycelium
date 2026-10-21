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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// ToolRelease pins the vendor archive, not an installer script or PATH lookup.
type ToolRelease struct{ Name, Version, URL, SHA256 string }

var toolChecksums = map[string][2]string{
	"darwin/arm64": {"3f61099e261e449527141dbf125629fab33ad696468c8c90cebbac40185a306c", "f4e32ea91970d4e11739488817979a5f2c6ebbb9cedb0d6dea74b2b790b272dc"},
	"darwin/amd64": {"76638fdcfa91357858771551a1c88de1f7c3b270b33ab1866f8a0618d9e442d8", "9859588ba57f390b5c77d56b2654fab37bc00e10952da25e3efd6e3434557e5a"},
	"linux/arm64":  {"726b72a137fda33565143325f7d31c42cd30ff9ccdf067e00d124d37b4081cb2", "9f8d2113fe9dc01788a65f5c2acec34fa56b1193461a5c3e9a775d6d2d621bcb"},
	"linux/amd64":  {"741ff1f5742c5a4a25d2f829e8395355e43f7a5ae2ebc6368e9ae2df0efb69cf", "7aa3ec39aecceff9062fa2ed4d42cbaa0bdc25ddea727d048e061cf188d434f6"},
}

func PinnedEnvironmentTools(osname, arch string) ([]ToolRelease, error) {
	hashes, ok := toolChecksums[osname+"/"+arch]
	if !ok {
		return nil, preflightError("tool_platform_unsupported", "tools", "no pinned uv/pixi release for this OS/architecture")
	}
	machine := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[arch]
	uvTarget, pixiTarget := "apple-darwin", "apple-darwin"
	if osname == "linux" {
		uvTarget, pixiTarget = "unknown-linux-gnu", "unknown-linux-musl"
	}
	return []ToolRelease{
		{"uv", "0.8.22", "https://github.com/astral-sh/uv/releases/download/0.8.22/uv-" + machine + "-" + uvTarget + ".tar.gz", hashes[0]},
		{"pixi", "0.81.0", "https://github.com/prefix-dev/pixi/releases/download/v0.81.0/pixi-" + machine + "-" + pixiTarget + ".tar.gz", hashes[1]},
	}, nil
}

// ToolBootstrapProbe keeps network acquisition and version execution fakeable.
// Fetch returns the archive; callers never receive command output in errors.
type ToolBootstrapProbe struct {
	Fetch   func(context.Context, ToolRelease) (io.ReadCloser, error)
	Command func(context.Context, string, ...string) ([]byte, error)
}

func LocalToolBootstrapProbe(p MachineProbe) ToolBootstrapProbe {
	return ToolBootstrapProbe{Command: p.Command, Fetch: func(ctx context.Context, r ToolRelease) (io.ReadCloser, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.URL, nil)
		if err != nil {
			return nil, err
		}
		client := http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 5 || req.URL.Scheme != "https" || req.URL.User != nil {
				return fmt.Errorf("invalid tool redirect")
			}
			return nil
		}}
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if res.StatusCode != http.StatusOK {
			res.Body.Close()
			return nil, fmt.Errorf("download refused")
		}
		return res.Body, nil
	}}
}

// BootstrapEnvironmentTools installs only into this user's Mycelium tool tree.
// Existing suitable service tools and verified pinned executables are reused.
// User-managed uv/pixi are never overwritten; acquired paths are appended.
func BootstrapEnvironmentTools(ctx context.Context, c ServiceConfig, p ToolBootstrapProbe) (ServiceConfig, error) {
	releases, err := PinnedEnvironmentTools(c.Detection.OS, c.Detection.Arch)
	if err != nil {
		return c, err
	}
	if !filepath.IsAbs(c.WorkerHome) || p.Command == nil || p.Fetch == nil {
		return c, preflightError("tool_bootstrap_unavailable", "tools", "user home and tool probes are required")
	}
	return bootstrapEnvironmentTools(ctx, c, p, releases)
}
func bootstrapEnvironmentTools(ctx context.Context, c ServiceConfig, p ToolBootstrapProbe, releases []ToolRelease) (ServiceConfig, error) {
	dirs := filepath.SplitList(c.Path)
	for _, r := range releases {
		if suitableEnvironmentTool(ctx, c.Path, r.Name, p.Command) != "" {
			continue
		}
		dir := filepath.Join(c.WorkerHome, ".local", "share", "mycelium", "tools", r.Name+"-"+r.Version+"-"+c.Detection.OS+"-"+c.Detection.Arch)
		path := filepath.Join(dir, r.Name)
		if err := checkToolTree(c.WorkerHome, dir); err != nil {
			return c, err
		}
		if !verifiedTool(ctx, path, r, p.Command) {
			body, e := p.Fetch(ctx, r)
			if e != nil {
				return c, preflightError("tool_download_failed", r.Name, "cannot download the pinned vendor release")
			}
			data, e := io.ReadAll(io.LimitReader(body, 128<<20+1))
			closeErr := body.Close()
			if e != nil || closeErr != nil || len(data) > 128<<20 {
				return c, preflightError("tool_download_failed", r.Name, "pinned archive is incomplete or too large")
			}
			sum := sha256.Sum256(data)
			if hex.EncodeToString(sum[:]) != r.SHA256 {
				return c, preflightError("tool_checksum_mismatch", r.Name, "pinned vendor archive checksum differs; no executable published")
			}
			executable, e := toolExecutable(data, r.Name)
			if e != nil {
				return c, preflightError("tool_archive_invalid", r.Name, "pinned archive lacks one regular executable")
			}
			if e = os.MkdirAll(dir, 0700); e != nil {
				return c, preflightError("tool_install_failed", r.Name, "cannot create user-local tool directory")
			}
			if e = writeInstallFile(path, executable, 0700, installFiles{}, nil); e != nil {
				return c, preflightError("tool_install_failed", r.Name, "cannot publish user-local executable")
			}
			digest := sha256.Sum256(executable)
			if e = writeInstallFile(path+".sha256", []byte(hex.EncodeToString(digest[:])), 0600, installFiles{}, nil); e != nil {
				return c, e
			}
			if r.Name == "uv" {
				uvx, e := toolExecutable(data, "uvx")
				if e != nil && e.Error() != "missing executable" {
					return c, preflightError("tool_archive_invalid", "uvx", "invalid uvx executable in the pinned archive")
				}
				if e == nil {
					if e = writeInstallFile(filepath.Join(dir, "uvx"), uvx, 0700, installFiles{}, nil); e != nil {
						return c, preflightError("tool_install_failed", "uvx", "cannot publish user-local uvx")
					}
				}
			}
			if !verifiedTool(ctx, path, r, p.Command) {
				return c, preflightError("tool_version_invalid", r.Name, "pinned executable cannot run or reports the wrong version")
			}
		}
		dirs = append(dirs, dir)
	}
	// Preserve the complete configured service PATH and append only new tool paths.
	c.Path = strings.Join(dirs, string(os.PathListSeparator))
	return c, nil
}
func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func verifiedTool(ctx context.Context, path string, r ToolRelease, command func(context.Context, string, ...string) ([]byte, error)) bool {
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0111 == 0 {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(data)
	receipt, err := os.ReadFile(path + ".sha256")
	if err != nil || string(receipt) != hex.EncodeToString(sum[:]) {
		return false
	}
	out, err := command(ctx, path, "--version")
	fields := strings.Fields(string(out))
	return err == nil && len(fields) >= 2 && fields[0] == r.Name && fields[1] == r.Version
}
func checkToolTree(home, dir string) error {
	rel, err := filepath.Rel(home, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return preflightError("tool_install_failed", "tools", "tool directory must remain in the user home")
	}
	at := home
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		at = filepath.Join(at, part)
		st, e := os.Lstat(at)
		if e == nil && (!st.IsDir() || st.Mode()&os.ModeSymlink != 0) {
			return preflightError("tool_install_failed", "tools", "tool directory contains a symlink or non-directory")
		}
		if e != nil && !os.IsNotExist(e) {
			return preflightError("tool_install_failed", "tools", "tool directory is inaccessible")
		}
	}
	return nil
}
func toolExecutable(data []byte, name string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(io.LimitReader(gz, 512<<20))
	var binary []byte
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if filepath.Base(header.Name) != name {
			continue
		}
		if binary != nil || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > 128<<20 || !filepath.IsLocal(header.Name) {
			return nil, fmt.Errorf("invalid executable")
		}
		binary, err = io.ReadAll(io.LimitReader(tr, 128<<20+1))
		if err != nil {
			return nil, err
		}
	}
	if len(binary) == 0 {
		return nil, fmt.Errorf("missing executable")
	}
	return binary, nil
}

// suitableEnvironmentTool resolves only the saved service PATH. A suitable
// executable identifies itself with a numeric version via --version. Recipe
// locks define package versions; bootstrap pins apply only to acquired tools.
func suitableEnvironmentTool(ctx context.Context, path, name string, command func(context.Context, string, ...string) ([]byte, error)) string {
	if command == nil {
		return ""
	}
	for _, dir := range filepath.SplitList(path) {
		if !filepath.IsAbs(dir) {
			continue
		}
		executable := filepath.Join(dir, name)
		st, err := os.Stat(executable)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0111 == 0 {
			continue
		}
		b, err := command(ctx, executable, "--version")
		f := strings.Fields(string(b))
		if err == nil && len(f) >= 2 && f[0] == name {
			parts := strings.Split(f[1], ".")
			if len(parts) >= 2 {
				if _, e := strconv.ParseUint(parts[0], 10, 32); e == nil {
					if _, e = strconv.ParseUint(parts[1], 10, 32); e == nil {
						return executable
					}
				}
			}
		}
	}
	return ""
}

// acquireEnvironmentTool is lazy: the native environment build calls it only
// for the backend it is about to execute. Existing service PATH wins, including
// on upgrades. Acquired tools are invoked by absolute path, never prepended.
// EnvironmentToolSource injects lazy acquisition without replacing the build
// runner. Nil fields retain production defaults. Releases permits synthetic,
// checksum-verified archives in tests without mutating the production pins.
type EnvironmentToolSource struct {
	Home     func() (string, error)
	Probe    ToolBootstrapProbe
	Releases func(string, string) ([]ToolRelease, error)
}

func (cache *EnvironmentCache) acquireEnvironmentTool(ctx context.Context, name string) (string, error) {
	source := EnvironmentToolSource{}
	if cache.Tools != nil {
		source = *cache.Tools
	}
	if source.Home == nil {
		source.Home = os.UserHomeDir
	}
	if source.Probe.Command == nil {
		source.Probe.Command = LocalMachineProbe().Command
	}
	if source.Probe.Fetch == nil {
		source.Probe.Fetch = LocalToolBootstrapProbe(MachineProbe{}).Fetch
	}
	if source.Releases == nil {
		source.Releases = PinnedEnvironmentTools
	}
	p := source.Probe
	if path := suitableEnvironmentTool(ctx, os.Getenv("PATH"), name, p.Command); path != "" {
		return path, nil
	}
	home, err := source.Home()
	if err != nil {
		return "", err
	}
	c := ServiceConfig{WorkerHome: home, Path: os.Getenv("PATH"), Detection: Detection{OS: runtime.GOOS, Arch: runtime.GOARCH}}
	releases, err := source.Releases(c.Detection.OS, c.Detection.Arch)
	if err != nil {
		return "", err
	}
	for _, r := range releases {
		if r.Name == name {
			c, err = bootstrapEnvironmentTools(ctx, c, p, []ToolRelease{r})
			if err != nil {
				return "", err
			}
			if path := suitableEnvironmentTool(ctx, c.Path, name, p.Command); path != "" {
				return path, nil
			}
		}
	}
	return "", exec.ErrNotFound
}
