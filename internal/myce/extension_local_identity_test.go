package myce

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

func TestExtensionXPUFingerprintIgnoresEnumerationOrder(t *testing.T) {
	previous := extensionIdentityProbe
	t.Cleanup(func() { extensionIdentityProbe = previous })
	p := pciFixtureProbe(
		pciFixture{"0000:03:00.0", "0x8086", "8", "Intel GPU", "", true},
		pciFixture{"0000:07:00.0", "0x8086", "8", "Intel GPU", "", true},
	)
	reordered, unavailable := false, false
	driver := "driver:Intel GPU"
	kernel := "test-kernel\n"
	command := p.Command
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "uname" {
			return []byte(kernel), nil
		}
		if name == "sycl-ls" {
			if reordered {
				return []byte("[opencl:gpu][opencl:0] Intel GPU\n[opencl:cpu][opencl:1] CPU\n"), nil
			}
			return []byte("[opencl:cpu][opencl:0] CPU\n[opencl:gpu][opencl:1] Intel GPU\n"), nil
		}
		return command(ctx, name, args...)
	}
	p.CommandEnv = func(context.Context, map[string]string, string, ...string) ([]byte, error) {
		if unavailable {
			return nil, os.ErrPermission
		}
		rows := []map[string]string{
			{"identity": "uuid-a", "fingerprint": driver, "pci_address": "0000:03:00.0"},
			{"identity": "uuid-b", "fingerprint": driver, "pci_address": "0000:07:00.0"},
		}
		if reordered {
			rows[0], rows[1] = rows[1], rows[0]
		}
		return json.Marshal(rows)
	}
	extensionIdentityProbe = func() MachineProbe { return p }
	fingerprint := func(device string) string {
		t.Helper()
		fp, err := ExtensionFingerprint(context.Background(), "xpu", device)
		if err != nil || fp == "" {
			t.Fatalf("fingerprint: %q, %v", fp, err)
		}
		return fp
	}
	a, b := fingerprint("0"), fingerprint("1")
	reordered = true
	// Runtime ordinals change, but each physical device keeps its identity.
	if fingerprint("1") != a || fingerprint("0") != b {
		t.Fatal("enumeration order changed a physical device's fingerprint")
	}
	if a == b {
		t.Fatal("distinct physical GPUs share a fingerprint")
	}
	kernel = "new-test-kernel\n"
	if fingerprint("1") == a || fingerprint("0") == b {
		t.Fatal("kernel change retained a sibling's qualification identity")
	}
	kernel = "test-kernel\n"
	driver = "new-driver:Intel GPU"
	if fingerprint("1") == a {
		t.Fatal("driver change retained qualification identity")
	}
	unavailable = true
	if fp, err := ExtensionFingerprint(context.Background(), "xpu", "1"); err == nil || fp != "" {
		t.Fatalf("failed identity probe did not fail closed: %q, %v", fp, err)
	}
}
