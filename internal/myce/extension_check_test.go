package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExtensionQualificationSmokeFailureClearsPriorSuccess(t *testing.T) {
	e := compositionExtension(t, false)
	e.Kinds = nil
	asset := []byte("small checked asset")
	hash := sha256.Sum256(asset)
	e.Assets = FileManifest{"model": {SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(asset))}}
	e.Variants[0].Check = []string{"sh", "-c", `test -z "$MYCE_TOKEN" && test -f "$1" && test ! -e "$MYCE_EXTENSION_CACHE/fail"`, "check", "{assets.model}"}
	e.Variants[0].Baselines = nil
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20, Fetch: func(_ context.Context, _ FileRef, dest string) error {
		if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
			return err
		}
		return os.WriteFile(dest, asset, 0600)
	}}
	devRoot := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(devRoot, "queue.db"), Checkpoints: filepath.Join(devRoot, "checkpoints"), FileStore: filepath.Join(devRoot, "files"), ExtensionRoot: root, HostID: "local-host", Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	assetFile := filepath.Join(devRoot, "asset")
	if err = os.WriteFile(assetFile, asset, 0600); err != nil {
		t.Fatal(err)
	}
	if err = d.Client().UploadFile(context.Background(), assetFile, e.Assets["model"]); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { snap, _ := d.Queue.Snapshot(); return len(snap.Machines) > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, root)
	if err != nil {
		t.Fatal(err)
	}
	reports, err := CheckExtension(ctx, relative, e, "fake", "0", "local-host", io.Discard, cache, d.Client())
	if err != nil || len(reports) != 1 || !reports[0].Qualified || !reports[0].Setup || reports[0].CheckID == "" {
		t.Fatal(reports, err)
	}
	m := Machine{HostID: "local-host", GPU: "fake", ExtensionFingerprint: reports[0].Fingerprint, CapabilityProfile: &CapabilityProfile{OS: e.Variants[0].Target.OS, Arch: e.Variants[0].Target.Arch}}
	if !currentExtensionQualification(e, reports[0], &m) {
		t.Fatal("qualification does not bind placement")
	}
	m.HostID = "other-host"
	if currentExtensionQualification(e, reports[0], &m) {
		t.Fatal("qualification crossed hosts")
	}
	if err = os.WriteFile(filepath.Join(extensionCache(root, e, "fake"), "fail"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	reports, err = CheckExtension(ctx, root, e, "fake", "0", "local-host", io.Discard, cache, d.Client())
	if err == nil || len(reports) != 1 || reports[0].Qualified {
		t.Fatal("failed check retained qualification", reports, err)
	}
	saved := readExtensionReports(root, e, "fake")
	if len(saved) != 1 || saved[0].Qualified {
		t.Fatal("failed check not persisted", saved)
	}
}
