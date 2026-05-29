//go:build smoke

package smoke

import (
	"net"
	"os"
	"testing"
	"time"

	"mycelium/internal/backends/llamacpp"
	"mycelium/internal/clock"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
)

func TestLlamaCppConformance(t *testing.T) {
	binary := os.Getenv("MYCELIUM_LLAMA_CPP_BINARY")
	model := os.Getenv("MYCELIUM_LLAMA_CPP_MODEL")
	if binary == "" || model == "" {
		t.Skip("set MYCELIUM_LLAMA_CPP_BINARY and MYCELIUM_LLAMA_CPP_MODEL for llama.cpp smoke")
	}
	addr := freeAddr(t)
	preset := fixtures.MakePreset(fixtures.WithModelRef(model), fixtures.WithContextLength(2048))
	contract.RunBackendAdapterConformanceAt(t, "llamacpp",
		func() ports.BackendAdapter {
			return llamacpp.NewAdapter(llamacpp.Config{
				BinaryPath: binary,
				Args: []string{
					"--host", "{host}",
					"--port", "{port}",
					"-m", "{model}",
					"-c", "{ctx}",
				},
				Clock:        clock.System{},
				PollInterval: 250 * time.Millisecond,
			})
		},
		preset,
		addr)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}
