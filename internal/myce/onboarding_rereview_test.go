package myce

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRereviewHealthyNASPerOperationBudget(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probe: func(path string) error {
		time.Sleep(120 * time.Millisecond)
		return probeNASDirectory(path)
	}}
	start := time.Now()
	got, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
	if err != nil || !got.RequireMountedNAS {
		t.Fatalf("healthy NAS refused after %s: %v", time.Since(start), err)
	}
	if time.Since(start) < 350*time.Millisecond {
		t.Fatal("fixture did not exceed old total budget")
	}
}

func TestRereviewCancelledNAS(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := PrepareOnboardingStorage(ctx, c, false, command)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Ctrl-C must retain cancellation: %v", err)
	}
	if strings.Contains(err.Error(), "nas_unresponsive") {
		t.Fatal("cancelled check classified as NAS failure", err)
	}
}

func TestRereviewTimedOutIOIsNotReused(t *testing.T) {
	n := checkpointIO[bool]{timeout: 10 * time.Millisecond}
	release := make(chan struct{})
	_, err := n.runContext(context.Background(), "same", func() (bool, error) { <-release; return true, nil })
	if !errors.Is(err, errCheckpointIOTimeout) {
		t.Fatal(err)
	}
	n.mu.Lock()
	done := n.pending.done
	n.mu.Unlock()
	close(release)
	<-done
	want := errors.New("fresh check")
	value, err := n.run("same", func() (bool, error) { return false, want })
	if value || !errors.Is(err, want) {
		t.Fatalf("served stale successful result: %v %v", value, err)
	}
}

func TestRereviewASPEEDWithFailingNvidiaSMI(t *testing.T) {
	p := reviewPCIProbe("0x1a03", "0x2000", "0")
	command := p.Command
	var notes []string
	p.Info = func(s string) { notes = append(notes, s) }
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "nvidia-smi" {
			return nil, &exec.ExitError{ProcessState: exitNine(t)}
		}
		return command(ctx, name, args...)
	}
	got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
	if err != nil || got.Platform != "cpu" {
		t.Fatalf("GPU-less host refused: %+v %v", got, err)
	}
	_, err = p.Detect(context.Background())
	if !errors.Is(err, ErrNoGPU) {
		t.Fatalf("join detection disagrees: %v", err)
	}
	if len(notes) == 0 {
		t.Fatal("missing CPU information note")
	}
}
func exitNine(t *testing.T) *os.ProcessState {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", "exit 9")
	_ = cmd.Run()
	return cmd.ProcessState
}

func TestRereviewIntegratedIntelRuntime(t *testing.T) {
	for _, runtimeState := range []string{"working", "missing", "broken"} {
		t.Run(runtimeState, func(t *testing.T) {
			p := reviewPCIProbe("0x8086", "0x9a49", "0")
			glob := p.Glob
			p.Glob = func(pattern string) ([]string, error) {
				if strings.Contains(pattern, "renderD") {
					return []string{"/dev/dri/renderD128"}, nil
				}
				return glob(pattern)
			}
			command := p.Command
			p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name == "sycl-ls" && runtimeState == "working" {
					return []byte("[level_zero:gpu:0] Intel UHD Integrated Graphics"), nil
				}
				if name == "sycl-ls" && runtimeState == "broken" {
					return nil, errors.New("broken runtime")
				}
				return command(ctx, name, args...)
			}
			p.CommandEnv = func(ctx context.Context, _ map[string]string, name string, args ...string) ([]byte, error) {
				if name == "python3" {
					if runtimeState == "missing" {
						return nil, exec.ErrNotFound
					}
					if runtimeState == "broken" {
						return nil, errors.New("broken runtime")
					}
					return []byte(`[{"identity":"intel-integrated-uuid","fingerprint":"driver:Intel UHD"}]`), nil
				}
				return p.Command(ctx, name, args...)
			}
			got, err := PreflightHardware(context.Background(), ServiceConfig{}, p)
			detection, detectErr := p.Detect(context.Background())
			switch runtimeState {
			case "working":
				if err != nil || got.Platform != "xpu" || len(got.Devices) != 1 || got.MemoryMode != "unified" {
					t.Fatalf("working iGPU excluded: %+v %v", got, err)
				}
				if detectErr != nil || detection.Platform != got.Platform || detection.MemoryMode != got.MemoryMode {
					t.Fatalf("join/install disagree: %+v %v", detection, detectErr)
				}
			case "missing":
				if err != nil || got.Platform != "cpu" || !errors.Is(detectErr, ErrNoGPU) {
					t.Fatalf("missing iGPU runtime: %+v %v / %v", got, err, detectErr)
				}
			case "broken":
				requirePreflightCode(t, err, "gpu_driver_unavailable")
				requirePreflightCode(t, detectErr, "gpu_driver_unavailable")
			}
		})
	}
}

func TestRereviewPixiPin(t *testing.T) {
	for _, osname := range []string{"linux", "darwin"} {
		for _, arch := range []string{"amd64", "arm64"} {
			releases, err := PinnedEnvironmentTools(osname, arch)
			if err != nil {
				t.Fatal(err)
			}
			pixi := releases[1]
			if pixi.Version != "0.81.0" || !strings.Contains(pixi.URL, "/v0.81.0/") || filepath.Base(pixi.URL) == "" {
				t.Fatal("incompatible pixi pin", pixi)
			}
		}
	}
}

func TestRereviewLazyFakeToolDownload(t *testing.T) {
	builds := fakePixi(t)
	script, err := os.ReadFile(filepath.Join(filepath.Dir(builds), "pixi"))
	if err != nil {
		t.Fatal(err)
	}
	// The backend is absent from PATH and HOME differs from the injected home.
	t.Setenv("PATH", "/usr/bin:/bin")
	home := t.TempDir()
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "pixi", Typeflag: tar.TypeReg, Mode: 0700, Size: int64(len(script))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(script); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(archive.Bytes())
	downloads, probes, homes := 0, 0, 0
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20, Tools: &EnvironmentToolSource{
		Home: func() (string, error) { homes++; return home, nil },
		Releases: func(osname, arch string) ([]ToolRelease, error) {
			releases, err := PinnedEnvironmentTools(osname, arch)
			if err != nil {
				return nil, err
			}
			releases[1].SHA256 = hex.EncodeToString(sum[:])
			return releases, nil
		},
		Probe: ToolBootstrapProbe{
			Fetch: func(_ context.Context, r ToolRelease) (io.ReadCloser, error) {
				downloads++
				if r.Name != "pixi" || r.Version != "0.81.0" {
					t.Errorf("unexpected download: %+v", r)
				}
				return io.NopCloser(bytes.NewReader(archive.Bytes())), nil
			},
			Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				probes++
				if !withinNAS(home, name) {
					t.Errorf("version probe escaped injected home: %s", name)
				}
				return LocalMachineProbe().Command(ctx, name, args...)
			},
		},
	}}
	recipe := pixiRecipe(t, "locked")
	root, release, err := c.Acquire(context.Background(), recipe)
	if err != nil {
		t.Fatal(err)
	}
	release()
	activation := environmentActivation(root)
	if activation["PIXI_ACTIVE"] != "yes" || activation["CONDA_PREFIX"] == "" {
		t.Fatal("lazy build activation missing", activation)
	}
	log, err := os.ReadFile(builds)
	if err != nil || string(log) != "install\n" {
		t.Fatalf("fake tool did not install: %q %v", log, err)
	}
	// A fresh recipe enters the lazy path again; the verified downloaded tool is reused.
	recipe = pixiRecipe(t, "another-lock")
	_, release, err = c.Acquire(context.Background(), recipe)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if downloads != 1 || homes < 2 || probes < 3 {
		t.Fatalf("lazy dependencies bypassed: downloads=%d homes=%d probes=%d", downloads, homes, probes)
	}
	if _, err := os.Stat(filepath.Join(os.Getenv("HOME"), ".local", "share", "mycelium", "tools")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("lazy install bypassed injected home", err)
	}
}

func TestRereviewToolDownloadGuard(t *testing.T) {
	const helper = "MYCE_TEST_TOOL_DOWNLOAD_GUARD"
	if os.Getenv(helper) == "1" {
		releases, _ := PinnedEnvironmentTools("darwin", "arm64")
		_, _ = LocalToolBootstrapProbe(LocalMachineProbe()).Fetch(context.Background(), releases[1])
		t.Fatal("real download bypassed test guard")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRereviewToolDownloadGuard$")
	cmd.Env = append(os.Environ(), helper+"=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !bytes.Contains(out, []byte("TEST GUARD: real environment tool download forbidden")) {
		t.Fatalf("guard not enforced: %v %s", err, out)
	}
	home, err := os.UserHomeDir()
	if err != nil || home != os.Getenv("MYCE_TEST_ISOLATED_HOME") {
		t.Fatal("HOME isolation absent", err)
	}
}

func TestRereviewCancelledIOIsNotReused(t *testing.T) {
	n := checkpointIO[bool]{timeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := n.runContext(ctx, "same", func() (bool, error) { close(entered); <-release; return true, nil })
		result <- err
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	n.mu.Lock()
	done := n.pending.done
	n.mu.Unlock()
	close(release)
	<-done
	want := errors.New("fresh cancelled check")
	value, err := n.run("same", func() (bool, error) { return false, want })
	if value || !errors.Is(err, want) {
		t.Fatalf("cancelled result reused: %v %v", value, err)
	}
}

func TestRereviewNASTotalBudget(t *testing.T) {
	c, command := onboardingNAS(t, "linux")
	st, err := os.Stat(c.NAS)
	if err != nil {
		t.Fatal(err)
	}
	// Every operation is healthy, but a very deep root would exceed the cap.
	c.Checkpoints = filepath.Join(c.NAS, strings.Repeat("part/", 60), "checkpoints")
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		fs := onboardingStorageFS{lstat: func(string) (os.FileInfo, error) { time.Sleep(100 * time.Millisecond); return st, nil }, mkdir: func(string, os.FileMode) error { return nil }, probe: func(string) error { return nil }}
		_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
		requirePreflightCode(t, err, "nas_unresponsive")
		if elapsed := time.Since(start); elapsed != onboardingStorageBudget {
			t.Fatalf("whole-check cap=%s, want %s", elapsed, onboardingStorageBudget)
		}
		// Drain and detach the bubble-owned channel before other tests use the
		// process-wide guard. The syscall itself is allowed to outlive refusal.
		onboardingNASIO.mu.Lock()
		pending := onboardingNASIO.pending
		onboardingNASIO.mu.Unlock()
		if pending != nil {
			<-pending.done
		}
		onboardingNASIO.mu.Lock()
		onboardingNASIO.pending = nil
		onboardingNASIO.mu.Unlock()
	})
}

func TestRereviewCancellationDuringMountInventory(t *testing.T) {
	c, _ := onboardingNAS(t, "linux")
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	result := make(chan error, 1)
	command := func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	go func() { _, err := PrepareOnboardingStorage(ctx, c, false, command); result <- err }()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("mount-inventory cancellation misclassified: %v", err)
	}
}

func TestRereviewMountInventorySharesTotalBudget(t *testing.T) {
	c, _ := onboardingNAS(t, "linux")
	synctest.Test(t, func(t *testing.T) {
		// The parent is longer than the documented whole-check budget.
		ctx, cancel := context.WithTimeout(context.Background(), 2*onboardingStorageBudget)
		defer cancel()
		start := time.Now()
		command := func(ctx context.Context, _ string, _ ...string) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }
		_, err := PrepareOnboardingStorage(ctx, c, false, command)
		requirePreflightCode(t, err, "nas_unresponsive")
		if elapsed := time.Since(start); elapsed != onboardingStorageBudget {
			t.Fatalf("mount inventory exceeded total budget: %s", elapsed)
		}
	})
}
