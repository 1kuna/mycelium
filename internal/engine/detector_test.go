package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"mycelium/internal/domain"
)

func TestDetectorReportsHostAndEngines(t *testing.T) {
	detector := Detector{
		GOOS:   "linux",
		GOARCH: "arm64",
		Hardware: fakeHardwareDetector{node: domain.Node{
			ID:               "spark",
			OS:               "linux",
			OOMSeverity:      domain.OOMCatastrophic,
			DiskTotalMB:      1000,
			DiskFreeMB:       500,
			DiskMinFreeRatio: domain.DefaultDiskMinFreeRatio,
			Accelerators: []domain.Accelerator{{
				Index:             0,
				Vendor:            "nvidia",
				Kind:              "gb10",
				VRAMTotalMB:       131072,
				ComputeCapability: "9.0",
				ArchFamily:        "GB10",
			}},
		}},
		Command: fakeEngineCommand(map[string][]byte{
			"which llama-server":              []byte("/usr/bin/llama-server\n"),
			"which vllm":                      []byte("/opt/vllm/bin/vllm\n"),
			"which docker":                    []byte("/usr/bin/docker\n"),
			"which uv":                        []byte("/usr/bin/uv\n"),
			"/usr/bin/llama-server --version": []byte("llama 1.0\n"),
			"/opt/vllm/bin/vllm --version":    []byte("vllm 1.0\n"),
		}),
	}
	host, err := detector.DetectHost(context.Background(), domain.Node{ID: "spark"})
	if err != nil {
		t.Fatalf("DetectHost: %v", err)
	}
	if host.Platform != "linux/arm64" || host.OOMSeverity != domain.OOMCatastrophic || host.TotalMemoryMB != 131072 || host.ContainerRuntime != "docker" {
		t.Fatalf("host = %+v", host)
	}
	if !reflect.DeepEqual(host.PackageManagers, []string{"docker", "uv"}) {
		t.Fatalf("package managers = %+v", host.PackageManagers)
	}
	if host.DriverFacts["nvidia.compute_capability"] != "9.0" {
		t.Fatalf("driver facts = %+v", host.DriverFacts)
	}
	profiles, err := detector.DetectEngines(context.Background(), host)
	if err != nil {
		t.Fatalf("DetectEngines: %v", err)
	}
	vllmProfile := profileByBackend(profiles, domain.BackendVLLM)
	if !vllmProfile.Ready || !reflect.DeepEqual(vllmProfile.Args, []string{"--gpu-memory-utilization", "0.85"}) || vllmProfile.Safety.VLLMGPUUtilization != SparkSafeVLLMGPUUtilization {
		t.Fatalf("vllm profile = %+v", vllmProfile)
	}
	mlxProfile := profileByBackend(profiles, domain.BackendMLX)
	if mlxProfile.Ready || !strings.Contains(mlxProfile.UnreadyReason, "unsupported platform") {
		t.Fatalf("mlx profile = %+v", mlxProfile)
	}
}

func TestConfiguredEngineDetectionAndProbeFailures(t *testing.T) {
	host := domain.HostFacts{Platform: "linux/arm64", OOMSeverity: domain.OOMCatastrophic}
	detector := Detector{Command: fakeEngineCommand(map[string][]byte{
		"which vllm":                   []byte("/opt/vllm/bin/vllm\n"),
		"/opt/vllm/bin/vllm --version": []byte("vllm 1.0\n"),
	})}
	profile := detector.DetectConfiguredEngine(context.Background(), host, Config{Backend: domain.BackendVLLM})
	if !profile.Ready || profile.BinaryPath != "/opt/vllm/bin/vllm" || !reflect.DeepEqual(profile.Args, []string{"--gpu-memory-utilization", "0.85"}) {
		t.Fatalf("configured vllm = %+v", profile)
	}
	profile = detector.DetectConfiguredEngine(context.Background(), host, Config{Backend: domain.BackendVLLM, BackendBinary: "/bad/vllm"})
	if profile.Ready || !strings.Contains(profile.UnreadyReason, "probe failed") {
		t.Fatalf("bad configured vllm = %+v", profile)
	}
	profile = detector.DetectConfiguredEngine(context.Background(), host, Config{})
	if profile.Ready || !strings.Contains(profile.UnreadyReason, "backend") {
		t.Fatalf("missing backend = %+v", profile)
	}
}

func TestDetectorHelpProbeAllowsUnknownVersion(t *testing.T) {
	detector := Detector{Command: fakeEngineCommand(map[string][]byte{
		"which llama-server":           []byte("/usr/bin/llama-server\n"),
		"/usr/bin/llama-server --help": []byte("usage\n"),
	})}
	profile := detector.detectProfile(context.Background(), domain.HostFacts{Platform: "darwin/arm64"}, domain.BackendLlamaCpp, "llama.cpp", "llama-server", nil, []string{"darwin/arm64"}, detector.now())
	if !profile.Ready || profile.Version != "unknown" {
		t.Fatalf("help profile = %+v", profile)
	}
}

func TestDetectorHelperBranches(t *testing.T) {
	detector := NewDetector()
	if detector.GOOS == "" || detector.GOARCH == "" || detector.Command == nil || detector.Hardware == nil {
		t.Fatalf("NewDetector = %+v", detector)
	}
	errDetector := Detector{Hardware: fakeHardwareDetector{err: errors.New("hardware")}}
	if _, err := errDetector.DetectHost(context.Background(), domain.Node{}); err == nil || !strings.Contains(err.Error(), "hardware") {
		t.Fatalf("DetectHost err = %v", err)
	}
	podmanDetector := Detector{Command: fakeEngineCommand(map[string][]byte{"which podman": []byte("/usr/bin/podman\n")})}
	if got := podmanDetector.detectContainerRuntime(context.Background()); got != "podman" {
		t.Fatalf("container runtime = %s", got)
	}
	if _, err := podmanDetector.resolveBinary(context.Background(), ""); err == nil {
		t.Fatal("empty binary accepted")
	}
	if got, err := (Detector{}).command(context.Background(), "definitely-not-a-real-mycelium-command"); err == nil || got != nil {
		t.Fatalf("default command = %q %v", got, err)
	}
	for _, backend := range []domain.Backend{domain.BackendLlamaCpp, domain.BackendMLX, domain.BackendVLLM, domain.BackendCustom, domain.Backend("unknown")} {
		_ = supportedPlatformsForBackend(backend, "linux/amd64")
		_ = defaultBinary(backend)
	}
	if platformAllowed("linux/arm64", []string{"linux/amd64"}) {
		t.Fatal("disallowed platform accepted")
	}
	for _, args := range [][]string{
		{"--gpu-memory-utilization=0.85"},
		{"--gpu-memory-utilization", "0.85"},
	} {
		if !hasVLLMUtil(args) {
			t.Fatalf("vllm util not detected in %+v", args)
		}
	}
	if hasVLLMUtil([]string{"--gpu-memory-utilization"}) {
		t.Fatal("missing vllm util value accepted")
	}
}

type fakeHardwareDetector struct {
	node domain.Node
	err  error
}

func (f fakeHardwareDetector) Detect(context.Context, domain.Node) (domain.Node, error) {
	if f.err != nil {
		return domain.Node{}, f.err
	}
	return f.node, nil
}

func fakeEngineCommand(outputs map[string][]byte) func(context.Context, string, ...string) ([]byte, error) {
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		key := strings.TrimSpace(name + " " + strings.Join(args, " "))
		if out, ok := outputs[key]; ok {
			return out, nil
		}
		return nil, errors.New("not found")
	}
}

func profileByBackend(profiles []domain.EngineProfile, backend domain.Backend) domain.EngineProfile {
	for _, profile := range profiles {
		if profile.Backend == backend {
			return profile
		}
	}
	return domain.EngineProfile{}
}
