package myce

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestExecutionMemoryEnvelopeOptionalMembers(t *testing.T) {
	q, clock := fixture(t)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 4
	s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sleep", "60"}}}
	if _, err := q.Submit(s); err != nil {
		t.Fatal("envelope-only native group refused:", err)
	}
	clock.Advance(time.Second)
	s.Run = RunSpec{Docker: true, DockerNetwork: "private", Command: []string{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, DockerSidecars: []DockerSidecar{{Name: "helper", Command: []string{"docker", "run", "image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}}
	if err := s.validateDockerGroup(); err != nil {
		t.Fatal("envelope-only Docker group refused:", err)
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

// Load real shared objects with conflicting names. This proves loader selection,
// including the host-only driver fallback, rather than only inspecting strings.
func TestExecutionLockedLibraryResolution(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix dynamic loader contract")
	}
	cc, err := exec.LookPath("cc")
	if err != nil {
		t.Skip("shared-library proof needs a C compiler")
	}
	work, host, main, side := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	compile := func(output, source string, flags ...string) {
		t.Helper()
		file := filepath.Join(work, "input.c")
		if err := os.WriteFile(file, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		args := append(append([]string{}, flags...), file, "-o", output)
		if b, err := exec.Command(cc, args...).CombinedOutput(); err != nil {
			t.Fatalf("compile: %v: %s", err, b)
		}
	}
	shared := []string{"-shared", "-fPIC"}
	key, suffix := "LD_LIBRARY_PATH", ".so"
	if runtime.GOOS == "darwin" {
		shared = []string{"-dynamiclib"}
		key, suffix = "DYLD_LIBRARY_PATH", ".dylib"
	}
	for _, pair := range []struct{ root, label string }{{host, "ambient"}, {filepath.Join(main, "venv", "lib"), "main"}, {filepath.Join(side, "venv", "lib"), "side"}} {
		if err := os.MkdirAll(pair.root, 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"sycl", "ur_loader"} {
			compile(filepath.Join(pair.root, "libmyce_"+name+suffix), fmt.Sprintf(`const char *identity(void) { return "%s"; }`, pair.label), shared...)
		}
	}
	compile(filepath.Join(host, "libmyce_driver"+suffix), `const char *identity(void) { return "host-driver"; }`, shared...)
	program := filepath.Join(work, "resolve")
	compile(program, fmt.Sprintf(`#include <dlfcn.h>
#include <stdio.h>
int main(void) {
  const char *names[] = {"libmyce_sycl%s", "libmyce_ur_loader%s", "libmyce_driver%s"};
  for (int i = 0; i < 3; i++) {
    void *h = dlopen(names[i], RTLD_NOW | RTLD_LOCAL);
    if (!h) { fprintf(stderr, "%%s\n", dlerror()); return 1; }
    const char *(*identity)(void) = dlsym(h, "identity");
    if (!identity) return 2;
    puts(identity());
    dlclose(h);
  }
  return 0;
}`, suffix, suffix, suffix), "-ldl")
	env := jobBaseEnvironment()
	env[key] = host
	for _, step := range []struct{ root, want string }{{"", "ambient\nambient\nhost-driver\n"}, {main, "main\nmain\nhost-driver\n"}, {side, "side\nside\nhost-driver\n"}} {
		if step.root != "" {
			bindEnvironment(env, step.root)
		}
		var output bytes.Buffer
		var err error
		if step.root == "" {
			cmd := exec.Command(program)
			cmd.Env = cleanExtensionEnv(env)
			cmd.Stdout, cmd.Stderr = &output, &output
			err = cmd.Run()
		} else {
			// Include Mycelium's real protected-shell supervisor in the proof.
			err = runExtensionCommand(context.Background(), []string{program}, work, env, &output)
		}
		if err != nil || output.String() != step.want {
			t.Errorf("root %s: loader output %q, want %q: %v", step.root, output.String(), step.want, err)
		}
	}
}
