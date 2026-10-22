package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// NASIdentity contains no machine-local paths or mount credentials. The
// fingerprint identifies the network server/share; checkpoints are share-relative.
type NASIdentity struct {
	Fingerprint    string `json:"fingerprint,omitempty"`
	CheckpointPath string `json:"checkpoint_path,omitempty"`
}

func (i NASIdentity) valid() bool {
	return validPin(i.Fingerprint) && filepath.IsLocal(i.CheckpointPath) && !strings.Contains(i.CheckpointPath, `\`)
}

func nasSourceFingerprint(source, kind string) string {
	var identity string
	switch kind {
	case "cifs", "smbfs", "smb3":
		if !strings.HasPrefix(source, "//") {
			return ""
		}
		u, err := url.Parse("smb:" + source)
		if err != nil || u.Hostname() == "" || u.RawQuery != "" || u.Fragment != "" {
			return ""
		}
		// Userinfo is deliberately excluded. SMB names are case insensitive;
		// mount aliases remain distinct unless the operator selects --nas.
		identity = "smb://" + strings.ToLower(u.Host) + strings.ToLower(strings.TrimSuffix(u.Path, "/"))
	case "nfs", "nfs4":
		host, share, ok := strings.Cut(source, ":/")
		if !ok || host == "" || strings.ContainsAny(host, "@\\") {
			return ""
		}
		identity = "nfs://" + strings.ToLower(host) + "/" + strings.TrimSuffix(share, "/")
	default:
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(identity)))
}

// DiscoverNASIdentity reads the OS mount inventory only. It never probes GPU,
// service state, credentials, or storage contents.
func DiscoverNASIdentity(ctx context.Context, osname, checkpoints string, command func(context.Context, string, ...string) ([]byte, error)) (NASIdentity, error) {
	bounded, cancel := context.WithTimeout(ctx, onboardingMountInventoryBudget)
	defer cancel()
	mounts, err := mountedNASInventory(bounded, osname, command)
	if err != nil {
		return NASIdentity{}, err
	}
	var chosen nasMount
	for _, m := range mounts {
		if m.fingerprint != "" && withinNAS(m.root, checkpoints) && len(m.root) > len(chosen.root) {
			chosen = m
		}
	}
	if chosen.root == "" {
		return NASIdentity{}, nil
	}
	rel, err := filepath.Rel(chosen.root, checkpoints)
	if err != nil {
		return NASIdentity{}, err
	}
	return NASIdentity{chosen.fingerprint, filepath.ToSlash(rel)}, nil
}

func (q *Queue) ConfigureOnboardingNAS(ctx context.Context, osname string, command func(context.Context, string, ...string) ([]byte, error)) error {
	identity, err := DiscoverNASIdentity(ctx, osname, q.root, command)
	if err != nil {
		return err
	}
	q.mu.Lock()
	q.onboardingNAS = identity
	q.mu.Unlock()
	return nil
}

func resolveClusterNAS(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), identity NASIdentity) (ServiceConfig, error) {
	if !identity.valid() {
		return c, preflightError("nas_identity_invalid", "storage", "queue NAS identity is invalid; select --nas PATH explicitly")
	}
	bounded, cancel := context.WithTimeout(ctx, onboardingMountInventoryBudget)
	defer cancel()
	mounts, err := mountedNASInventory(bounded, c.Detection.OS, command)
	if err != nil {
		return c, err
	}
	matches := map[string]bool{}
	for _, m := range mounts {
		if m.fingerprint == identity.Fingerprint {
			matches[m.root] = true
		}
	}
	if len(matches) == 0 {
		return c, preflightError("nas_identity_not_found", "storage", "no mounted NAS matches this cluster; mount its share through the OS or select --nas PATH")
	}
	if len(matches) > 1 {
		return c, preflightError("nas_identity_ambiguous", "storage", "more than one mounted NAS matches this cluster; select --nas PATH")
	}
	var root string
	for path := range matches {
		root = path
	}
	old := c.NAS
	for _, path := range []*string{&c.Checkpoints, &c.ModelRoot, &c.EnvironmentRoot} {
		if old != "" && withinNAS(old, *path) {
			rel, _ := filepath.Rel(old, *path)
			*path = filepath.Join(root, rel)
		}
	}
	if c.Checkpoints == filepath.Join(root, "mycelium", "checkpoints") {
		c.Checkpoints = filepath.Join(root, filepath.FromSlash(identity.CheckpointPath))
	}
	c.NAS = root
	return c, nil
}

// OS mount inventory is separate from hardware sensing. Tests install a fake
// command once at process setup, just like the worker's other host sources.
var onboardingMountCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = withoutMyceToken(os.Environ())
	return cmd.Output()
}

func OnboardingMountCommand() func(context.Context, string, ...string) ([]byte, error) {
	return onboardingMountCommand
}

func SetOnboardingMountCommand(command func(context.Context, string, ...string) ([]byte, error)) {
	onboardingMountCommand = command
}
