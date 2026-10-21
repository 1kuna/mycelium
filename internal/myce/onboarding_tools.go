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
	"path/filepath"
	"strings"
)

// ToolRelease pins the vendor archive, not an installer script or PATH lookup.
type ToolRelease struct{ Name, Version, URL, SHA256 string }

var toolChecksums = map[string][2]string{
	"darwin/arm64": {"3f61099e261e449527141dbf125629fab33ad696468c8c90cebbac40185a306c", "9e15f80e610633d954e41d1ae07b127607e9367e09d86679fb6ee0ba958bb1c7"},
	"darwin/amd64": {"76638fdcfa91357858771551a1c88de1f7c3b270b33ab1866f8a0618d9e442d8", "c96b1e3ff0e6357a3c9d99daa5bbca340e3fbb453a94a016ab537af5a7dae98b"},
	"linux/arm64":  {"726b72a137fda33565143325f7d31c42cd30ff9ccdf067e00d124d37b4081cb2", "a34a99e62f06eb8059f02865cb98697b4746625b62d0c0c0363324845c4cd400"},
	"linux/amd64":  {"741ff1f5742c5a4a25d2f829e8395355e43f7a5ae2ebc6368e9ae2df0efb69cf", "cb733205ae1a02986071bcbeff47c60460bfb92d1cd9565d40f4dea5448c86a5"},
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
		{"pixi", "0.55.0", "https://github.com/prefix-dev/pixi/releases/download/v0.55.0/pixi-" + machine + "-" + pixiTarget + ".tar.gz", hashes[1]},
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
// Already verified pinned executables are reused. System tools and existing
// user-managed uv/pixi are never overwritten; the saved service PATH is explicit.
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
	dirs := []string{}
	for _, r := range releases {
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
			if !verifiedTool(ctx, path, r, p.Command) {
				return c, preflightError("tool_version_invalid", r.Name, "pinned executable cannot run or reports the wrong version")
			}
		}
		dirs = append(dirs, dir)
	}
	// De-duplicate on rerun, preserving the remaining configured runtime PATH.
	for _, dir := range strings.Split(c.Path, string(os.PathListSeparator)) {
		if dir != "" && !containsString(dirs, dir) {
			dirs = append(dirs, dir)
		}
	}
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

func validateOnboardingToolEnvironment(ctx context.Context, c ServiceConfig, p MachineProbe) error {
	root := filepath.Join(c.WorkerHome, ".local", "share", "mycelium", "tools")
	managed := false
	for _, dir := range strings.Split(c.Path, string(os.PathListSeparator)) {
		if withinNAS(root, dir) {
			managed = true
		}
	}
	if !managed {
		return nil
	} // Existing installations have no bootstrap contract.
	releases, err := PinnedEnvironmentTools(c.Detection.OS, c.Detection.Arch)
	if err != nil {
		return err
	}
	for _, r := range releases {
		if p.Command == nil {
			return preflightError("service_tools_unavailable", r.Name, "service tool version runner missing")
		}
		b, e := p.Command(ctx, r.Name, "--version")
		fields := strings.Fields(string(b))
		if e != nil || len(fields) < 2 || fields[0] != r.Name || fields[1] != r.Version {
			return preflightError("service_tools_unavailable", r.Name, "service PATH cannot run the pinned environment tool; re-run onboarding")
		}
	}
	return nil
}
