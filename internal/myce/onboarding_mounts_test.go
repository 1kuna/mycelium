package myce

import (
	"context"
	"os"
	"strings"
	"testing"
)

// #227: global mount(8) can wait on an unrelated share for 51 seconds.
func TestOnboardingDarwinCachedInventoryProbesOnlySelectedShare(t *testing.T) {
	c, fake := onboardingNAS(t, "darwin")
	selected := c.NAS
	calls := 0
	command := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		calls++
		if name != cachedMountInventoryCommand {
			t.Fatalf("blocking global inventory requested: %s", name)
		}
		b, err := fake(ctx, name, args...)
		b = []byte(strings.ReplaceAll(string(b), "private-value on", "//server.invalid/share on"))
		return append(b, []byte("\n//unresponsive.invalid/other on /unresponsive-share (smbfs)\n")...), err
	}
	fs := onboardingStorageFS{lstat: func(path string) (os.FileInfo, error) {
		if !withinNAS(selected, path) {
			t.Fatalf("probed unrelated mount: %s", path)
		}
		return os.Lstat(path)
	}, mkdir: os.Mkdir, probe: func(path string) error {
		if !withinNAS(selected, path) {
			t.Fatalf("probed unrelated mount: %s", path)
		}
		return probeNASDirectory(path)
	}}
	if _, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("inventory calls = %d", calls)
	}
	identity, err := DiscoverNASIdentity(context.Background(), "darwin", c.Checkpoints, command)
	if err != nil || identity.Fingerprint != nasSourceFingerprint("//server.invalid/share", "smbfs") {
		t.Fatalf("selected identity lost: %+v %v", identity, err)
	}
}
