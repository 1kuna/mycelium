//go:build smoke

package smoke

import (
	"os"
	"strings"
	"testing"

	"mycelium/internal/backends/mlx"
	"mycelium/internal/backends/vllm"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
)

func TestLocalMLXConformance(t *testing.T) {
	binary := os.Getenv("MYCELIUM_MLX_BINARY")
	model := os.Getenv("MYCELIUM_MLX_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_MLX_BINARY and MYCELIUM_MLX_MODEL for MLX smoke")
	}
	preset := fixtures.MakePreset(fixtures.WithModelRef(model), fixtures.WithContextLength(2048))
	preset.Backend = domain.BackendMLX
	contract.RunBackendAdapterConformanceAt(t, "mlx",
		func() ports.BackendAdapter { return mlx.NewAdapter(binary) },
		preset,
		freeAddr(t))
}

func TestLocalVLLMConformance(t *testing.T) {
	binary := os.Getenv("MYCELIUM_VLLM_BINARY")
	model := os.Getenv("MYCELIUM_VLLM_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_VLLM_BINARY and MYCELIUM_VLLM_MODEL for vLLM smoke")
	}
	preset := fixtures.MakePreset(fixtures.WithModelRef(model), fixtures.WithContextLength(2048))
	preset.Backend = domain.BackendVLLM
	if rawArgs := os.Getenv("MYCELIUM_VLLM_LAUNCH_ARGS"); rawArgs != "" {
		preset.LaunchArgs = append(preset.LaunchArgs, strings.Fields(rawArgs)...)
	}
	contract.RunBackendAdapterConformanceAt(t, "vllm",
		func() ports.BackendAdapter { return vllm.NewAdapter(binary) },
		preset,
		freeAddr(t))
}
