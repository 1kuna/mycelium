package mlx

import "testing"

func TestNewAdapterNamesMLX(t *testing.T) {
	adapter := NewAdapter("mlx-lm")
	if adapter.Name() != "mlx" {
		t.Fatalf("name = %s", adapter.Name())
	}
}
