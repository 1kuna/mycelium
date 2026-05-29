package mlx

import "testing"

func TestNewAdapterNamesMLX(t *testing.T) {
	adapter := NewAdapter("mlx-lm")
	if adapter.Name() != "mlx" {
		t.Fatalf("name = %s", adapter.Name())
	}
	configured := NewAdapterWithConfig(Config{BinaryPath: "mlx_lm.server"})
	if configured.Name() != "mlx" {
		t.Fatalf("configured name = %s", configured.Name())
	}
}
