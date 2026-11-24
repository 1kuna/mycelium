package myce

import (
	"maps"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestExecutionMemoryEnvelopeOptionalMembers(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 4
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "60"}}}
	if _, err := q.Submit(s); err != nil {
		t.Fatal("envelope-only native group refused:", err)
	}
}

func TestExecutionLockedLibraryPrecedence(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix dynamic loader contract")
	}
	key := "LD_LIBRARY_PATH"
	if runtime.GOOS == "darwin" {
		key = "DYLD_LIBRARY_PATH"
	}
	ambient := filepath.Join(t.TempDir(), "host-runtime")
	base := jobEnvironmentFrom([]string{"PATH=/usr/bin:/bin", "LD_LIBRARY_PATH=" + ambient, "ZE_AFFINITY_MASK=1", "CUDA_VISIBLE_DEVICES=2", "ONEAPI_DEVICE_SELECTOR=level_zero:0", "SYCL_DEVICE_FILTER=level_zero:gpu", "ZE_ENABLE_PCI_ID_DEVICE_ORDER=1"})
	// Plain native commands have no binding: preserve the install-time runtime.
	if base["LD_LIBRARY_PATH"] != ambient {
		t.Fatal("plain native job lost its ambient runtime", base)
	}
	base[key] = ambient // Darwin does not inherit ambient DYLD_* from the worker.
	for _, backend := range []string{"uv", "pixi"} {
		t.Run(backend, func(t *testing.T) {
			main, side := t.TempDir(), t.TempDir()
			prefix := filepath.Join(main, "venv")
			if backend == "pixi" {
				prefix = filepath.Join(main, "extension", ".pixi", "envs", "default")
				if err := saveWorkerJSON(main, "activation.json", map[string]string{"CONDA_PREFIX": prefix, "PATH": filepath.Join(prefix, "bin") + ":/usr/bin:/bin"}); err != nil {
					t.Fatal(err)
				}
			}
			env := maps.Clone(base)
			bindEnvironment(env, main)
			want := strings.Join([]string{filepath.Join(prefix, "lib"), filepath.Join(prefix, "lib64"), filepath.Join(main, "lib"), filepath.Join(main, "lib64"), ambient}, ":")
			if env[key] != want {
				t.Errorf("library order = %q, want %q", env[key], want)
			}
			// Every device-selection/control value survives binding unchanged.
			for _, k := range []string{"ZE_AFFINITY_MASK", "CUDA_VISIBLE_DEVICES", "ONEAPI_DEVICE_SELECTOR", "SYCL_DEVICE_FILTER", "ZE_ENABLE_PCI_ID_DEVICE_ORDER"} {
				if env[k] != base[k] {
					t.Errorf("binding changed %s", k)
				}
			}
			// Extra authored directories replace ambient paths, after the standard
			// locked dirs. Rebinding cannot duplicate its own managed prefix.
			extra := filepath.Join(main, "venv", "lib", "python3.12", "site-packages", "vendor", "lib")
			env[key] = extra
			bindEnvironment(env, main)
			if !strings.HasSuffix(env[key], ":"+extra) || strings.Contains(env[key], ambient) {
				t.Fatal("authored loader path was not retained", env[key])
			}
			before := env[key]
			bindEnvironment(env, main)
			if env[key] != before {
				t.Fatal("rebinding duplicated loader directories", env[key])
			}
			bindEnvironment(env, side)
			if strings.Contains(env[key], main) || !strings.HasPrefix(env[key], filepath.Join(side, "venv", "lib")+":") {
				t.Fatal("sidecar retained main runtime", env[key])
			}
		})
	}
}

// A pixi activation can include both package directories and an external runtime.
// Switching must remove the complete old activation, without losing sidecar env.
func TestExecutionLockedLibraryActivationSwitch(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix dynamic loader contract")
	}
	main, side, external, authored := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	key := "LD_LIBRARY_PATH"
	if runtime.GOOS == "darwin" {
		key = "DYLD_LIBRARY_PATH"
	}
	activation := map[string]string{
		"CONDA_PREFIX": filepath.Join(main, ".pixi", "envs", "default"),
		"PATH":         "/usr/bin:/bin",
		key:            filepath.Join(main, "vendor", "lib") + ":" + external,
	}
	if err := saveWorkerJSON(main, "activation.json", activation); err != nil {
		t.Fatal(err)
	}
	for _, override := range []bool{false, true} {
		env := map[string]string{"PATH": "/usr/bin:/bin"}
		bindEnvironment(env, main)
		if override {
			env[key] = authored
		}
		bindEnvironment(env, side)
		if strings.Contains(env[key], main) || strings.Contains(env[key], external) {
			t.Errorf("override=%t: previous pixi runtime survived: %q", override, env[key])
		}
		if override && !strings.HasSuffix(env[key], ":"+authored) {
			t.Errorf("sidecar-authored library path lost: %q", env[key])
		}
	}
}
