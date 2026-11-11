package myce

import (
	"errors"
	"reflect"
	"testing"
)

func TestAcceleratorNodesPhysicalMapping(t *testing.T) {
	for _, tc := range []struct {
		platform, path string
		nodes          []string
	}{
		{"cuda", "/proc/driver/nvidia/gpus/0000:03:00.0/information", []string{"/dev/nvidia2", "/dev/nvidiactl", "/dev/nvidia-uvm", "/dev/nvidia-uvm-tools"}},
		{"xpu", "/sys/class/drm/renderD*/device", []string{"/dev/dri/renderD129"}},
	} {
		t.Run(tc.platform, func(t *testing.T) {
			probe := acceleratorNodeProbe{
				readFile: func(path string) ([]byte, error) {
					if path != tc.path {
						t.Fatal(path)
					}
					return []byte("Device Minor: 2\n"), nil
				},
				glob: func(path string) ([]string, error) {
					if path != tc.path {
						t.Fatal(path)
					}
					return []string{"/sys/class/drm/renderD128/device", "/sys/class/drm/renderD129/device"}, nil
				},
				evalSymlinks: func(path string) (string, error) {
					if path == "/sys/class/drm/renderD129/device" {
						return "/sys/devices/0000:03:00.0", nil
					}
					return "/sys/devices/0000:04:00.0", nil
				},
			}
			nodes, err := backendFor(tc.platform).nativeNodes("0000:03:00.0", probe)
			if err != nil || !reflect.DeepEqual(nodes, tc.nodes) {
				t.Fatal(nodes, err)
			}
			probe.readFile = func(string) ([]byte, error) { return nil, errors.New("unavailable") }
			probe.glob = func(string) ([]string, error) { return nil, nil }
			if nodes, err = backendFor(tc.platform).nativeNodes("0000:03:00.0", probe); err == nil || len(nodes) > 0 {
				t.Fatal("unverified node exposed", nodes, err)
			}
		})
	}
	for _, platform := range []string{"cpu", "fake", "metal", "unknown"} {
		if backendFor(platform).nativeNodes != nil {
			t.Fatal("unexpected Linux device mapping", platform)
		}
	}
}
