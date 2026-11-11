package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"testing"
	"testing/fstest"
)

func TestAcceleratorFingerprintPreservesIdentity(t *testing.T) {
	for _, platform := range []string{"cpu", "fake", "metal", "cuda", "xpu"} {
		t.Run(platform, func(t *testing.T) {
			device := "0"
			expected := runtime.GOOS + "/" + runtime.GOARCH + "/" + platform + "/0"
			p := MachineProbe{Files: fstest.MapFS{"proc/cpuinfo": &fstest.MapFile{Data: []byte("model name : example\n")}}, Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("probe-result"), nil }}
			switch platform {
			case "cpu":
				expected = runtime.GOOS + "/" + runtime.GOARCH + "/cpu/"
				if runtime.GOOS == "darwin" {
					expected += "probe-resultprobe-result"
				} else {
					expected += cpuIdentity([]byte("model name : example\n")) + "probe-result"
				}
			case "fake":
				expected += "/fake-v1"
			case "metal", "cuda":
				expected += "probe-resultprobe-result"
			case "xpu":
				expected = runtime.GOOS + "/" + runtime.GOARCH + `/xpu/{"UUID":"physical-card","Driver":"driver"}probe-result`
			}
			got, err := extensionFingerprintWithInventory(t.Context(), platform, device, func() MachineProbe { return p }, func(context.Context) ([]MachineDevice, error) {
				return []MachineDevice{{ID: "0", Platform: "xpu", Identity: "physical-card", Fingerprint: "driver"}}, nil
			})
			sum := sha256.Sum256([]byte(expected))
			if err != nil || got != hex.EncodeToString(sum[:]) {
				t.Fatal("fingerprint bytes changed", got, err)
			}
		})
	}
}
