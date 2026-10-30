package myce

import (
	"context"
	"errors"
	"io"
	"net"
)

// DefaultGPUTelemetryBrokerSocket belongs to an independently administered,
// root-owned read-only broker. The worker and client never elevate privileges.
const DefaultGPUTelemetryBrokerSocket = "/run/mycelium/gpu-telemetry.sock"

// GPUTelemetryBrokerClient requests one fresh snapshot. No request bytes, paths,
// PIDs or credentials are sent: EOF is the only request. The broker responds
// with one JSON object and closes; the worker validates its timestamp/counters.
func GPUTelemetryBrokerClient(ctx context.Context, socket string, out io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, attemptMemorySamplePeriod)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return errors.New("GPU telemetry broker unavailable")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return errors.New("GPU telemetry broker deadline unavailable")
	}
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		return errors.New("GPU telemetry broker request failed")
	}
	const maxOutput = 64 << 10
	b, err := io.ReadAll(io.LimitReader(conn, maxOutput+1))
	if err != nil || ctx.Err() != nil || len(b) == 0 || len(b) > maxOutput {
		return errors.New("GPU telemetry broker reply incomplete")
	}
	// Buffer until EOF/limit/deadline succeeds. Partial replies never reach the
	// helper parser and errors never include private broker output or paths.
	n, err := out.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}
