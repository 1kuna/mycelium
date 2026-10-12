package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func selectedDeviceEnv(platform, device string) map[string]string {
	if platform == "cuda" {
		return map[string]string{"CUDA_VISIBLE_DEVICES": device}
	}
	selector := "level_zero:0"
	if device == "0,1" {
		selector = "level_zero:0,1"
	}
	return map[string]string{"ZE_AFFINITY_MASK": device, "ONEAPI_DEVICE_SELECTOR": selector}
}

func TestDeviceSelectionEnvironment(t *testing.T) {
	for _, platform := range []string{"xpu", "cuda"} {
		for _, device := range []string{"0", "1", "0,1"} {
			t.Run(platform+"/"+device, func(t *testing.T) {
				m, err := WithModelDevice(Model{}, platform, device)
				if err != nil {
					t.Fatal(err)
				}
				for key, want := range selectedDeviceEnv(platform, device) {
					if m.DeviceEnv[key] != want {
						t.Fatalf("%s=%q, want %q", key, m.DeviceEnv[key], want)
					}
				}
				m.Engine, m.StartCommand, m.HealthURL = "server", []string{"echo"}, "http://127.0.0.1:{port}"
				m = withPlacementEnvironment(m, Machine{GPU: platform, Device: device})
				_, _, _, env, err := serverCommand(m, 7332)
				if err != nil {
					t.Fatal(err)
				}
				for key, want := range selectedDeviceEnv(platform, device) {
					if !slices.Contains(env, key+"="+want) {
						t.Fatalf("server missing %s=%s", key, want)
					}
				}
				if !slices.Contains(env, "MYCE_DEVICES="+device) {
					t.Fatal("server missing physical MYCE_DEVICES")
				}
			})
		}
	}
}

// The fake runtime applies affinity first, then selects the renumbered devices.
func TestDeviceSelectionDetection(t *testing.T) {
	root := t.TempDir()
	script := `#!/bin/sh
verbose=$1
mask=${ZE_AFFINITY_MASK:-0,1}
selector=${ONEAPI_DEVICE_SELECTOR#level_zero:}
oldIFS=$IFS
IFS=,
set -- $mask
IFS=$oldIFS
index=0
for physical in "$@"; do
 case ",${selector}," in
  ",,"|*",$index,"*)
   echo "[level_zero:gpu][level_zero:$index] Intel Arc Pro B70 physical=$physical"
   if [ "$verbose" = "--verbose" ]; then echo 'global_mem_size: 17179869184'; fi
   ;;
 esac
 index=$((index+1))
done
`
	if err := os.WriteFile(filepath.Join(root, "sycl-ls"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"nvidia-smi", "xpu-smi"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", root+":"+os.Getenv("PATH"))
	t.Setenv("ZE_AFFINITY_MASK", "1")
	t.Setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:1")
	probe := LocalMachineProbe()
	probe.OS, probe.Arch = "linux", "amd64"
	out, err := probe.Command(context.Background(), "sycl-ls")
	if err != nil || strings.TrimSpace(string(out)) != "" {
		t.Fatalf("fake must reject index 1 after a one-card mask: %q %v", out, err)
	}
	for _, device := range []string{"0", "1", "0,1"} {
		t.Run(device, func(t *testing.T) {
			t.Setenv("ZE_AFFINITY_MASK", "1")
			t.Setenv("ONEAPI_DEVICE_SELECTOR", "level_zero:1")
			d, err := probe.DetectDevice(context.Background(), device)
			if err != nil || d.Platform != "xpu" || d.MemoryGB != 16 || !strings.Contains(d.GPU, "physical="+strings.Split(device, ",")[0]) {
				t.Fatalf("install/join detection: %+v %v", d, err)
			}
			if os.Getenv("ZE_AFFINITY_MASK") != "1" || os.Getenv("ONEAPI_DEVICE_SELECTOR") != "level_zero:1" {
				t.Fatal("install probe changed global selectors")
			}
			c := ServiceConfig{Path: os.Getenv("PATH"), Device: device, Detection: Detection{OS: "linux", Platform: "xpu", MemoryMode: "fixed", MemoryGB: 16}}
			access := GPUAccessProbe{Glob: func(string) ([]string, error) { return []string{"/dev/dri/renderD129"}, nil }, Groups: func() ([]string, error) { return nil, nil }, Open: func(string) error { return nil }}
			d, err = DetectService(context.Background(), c, probe, access)
			if err != nil || d.Platform != "xpu" || d.MemoryGB != 16 {
				t.Fatalf("worker detection: %+v %v", d, err)
			}
		})
	}
}

func TestDeviceSelectionBatchEnvironment(t *testing.T) {
	for _, platform := range []string{"xpu", "cuda"} {
		for _, device := range []string{"0", "1", "0,1"} {
			t.Run(platform+"/"+device, func(t *testing.T) {
				w := recoveryWorker(t)
				w.Machine.GPU, w.Machine.Device = platform, device
				s, err := w.openStore()
				if err != nil {
					t.Fatal(err)
				}
				defer s.close()
				check := fmt.Sprintf(`test "$MYCE_DEVICES" = '%s'`, device)
				for key, want := range selectedDeviceEnv(platform, device) {
					check += fmt.Sprintf(` && test "$%s" = '%s'`, key, want)
				}
				p, err := w.start(s, recoveryJob(t, []string{"sh", "-c", check}))
				if err != nil {
					t.Fatal(err)
				}
				done := <-p.done
				if done.err != nil || !done.result.Success {
					t.Fatalf("batch env: %+v", done)
				}
			})
		}
	}
}

func TestDeviceSelectionDockerEnvironment(t *testing.T) {
	for _, platform := range []string{"xpu", "cuda"} {
		for _, device := range []string{"0", "1", "0,1"} {
			t.Run(platform+"/"+device, func(t *testing.T) {
				w, s, c, root := dockerFixture(t)
				w.Machine.GPU, w.Machine.Device = platform, device
				j := dockerJob()
				j.Checkpoint = filepath.Join(t.TempDir(), "checkpoint")
				// Preserve the full Intel inventory so the physical affinity mask is valid.
				j.Spec.Run.Command = append([]string{"docker", "run", "--device", "/dev/dri/renderD128", "--device", "/dev/dri/renderD129"}, j.Spec.Run.Command[2:]...)
				p, err := startDockerFixture(w, s, j)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(p.docker.close)
				await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
				data, err := os.ReadFile(filepath.Join(root, "calls"))
				if err != nil {
					t.Fatal(err)
				}
				p.stop(c.Now(), 0, true)
				<-p.done
				for key, want := range selectedDeviceEnv(platform, device) {
					if !strings.Contains(string(data), "--env "+key+"="+want+" ") {
						t.Fatalf("Docker missing %s=%s: %s", key, want, data)
					}
				}
				for _, want := range []string{"--env MYCE_DEVICES=" + device + " ", "--device /dev/dri/renderD128 ", "--device /dev/dri/renderD129 "} {
					if !strings.Contains(string(data), want) {
						t.Fatalf("Docker missing %s: %s", want, data)
					}
				}
			})
		}
	}
}

func TestDeviceSelectionDockerSidecarEnvironment(t *testing.T) {
	for _, device := range []string{"0", "1", "0,1"} {
		t.Run(device, func(t *testing.T) {
			w, s, c, root := dockerFixture(t)
			w.Machine.GPU, w.Machine.Device = "xpu", device
			j := dockerGroupJob(t)
			mapping := []string{"docker", "run", "--device", "/dev/dri/renderD128", "--device", "/dev/dri/renderD129"}
			j.Spec.Run.Command = append(mapping, j.Spec.Run.Command[2:]...)
			for i := range j.Spec.Run.DockerSidecars {
				j.Spec.Run.DockerSidecars[i].Command = append([]string(nil), j.Spec.Run.Command...)
			}
			p, err := startDockerFixture(w, s, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(p.docker.close)
			await(t, func() bool { _, err := os.Stat(filepath.Join(root, "running")); return err == nil })
			p.stop(c.Now(), 0, true)
			<-p.done
			data, err := os.ReadFile(filepath.Join(root, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, line := range strings.Split(string(data), "\n") {
				if !strings.HasPrefix(line, "create ") {
					continue
				}
				fields := strings.Fields(line)
				for _, name := range []string{"main", "sortformer", "naming"} {
					if !strings.Contains(line, "--network-alias "+name+" ") {
						continue
					}
					if seen[name] {
						t.Fatalf("duplicate create for %s", name)
					}
					seen[name] = true
					wantEnv := selectedDeviceEnv("xpu", device)
					wantEnv["MYCE_DEVICES"] = device
					for key, want := range wantEnv {
						if !slices.Contains(fields, key+"="+want) {
							t.Fatalf("%s missing %s=%s", name, key, want)
						}
					}
					for _, path := range []string{"/dev/dri/renderD128", "/dev/dri/renderD129"} {
						if !strings.Contains(line, "--device "+path+" ") {
							t.Fatalf("%s missing device mapping %s", name, path)
						}
					}
				}
			}
			if len(seen) != 3 {
				t.Fatalf("validated %d containers, want 3", len(seen))
			}
		})
	}
}
