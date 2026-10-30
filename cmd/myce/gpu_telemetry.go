package main

import (
	"context"
	"errors"
	"flag"
	"io"

	"mycelium/internal/myce"
)

func gpuTelemetryClientCommand(ctx context.Context, args []string, out io.Writer) error {
	f := flag.NewFlagSet("myce gpu-telemetry-client", flag.ContinueOnError)
	socket := f.String("socket", myce.DefaultGPUTelemetryBrokerSocket, "operator-owned read-only telemetry broker Unix socket")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("usage: myce gpu-telemetry-client [--socket PATH]")
	}
	return myce.GPUTelemetryBrokerClient(ctx, *socket, out)
}
