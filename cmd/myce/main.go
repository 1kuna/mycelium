package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"mycelium/cmd/internal/controlcli"
)

func main() {
	os.Exit(runMain(os.Args[1:], os.Stderr))
}

func runMain(args []string, stderr io.Writer) int {
	ctx, stop := signalContext(context.Background())
	defer stop()
	return mainExitCode(ctx, args, stderr)
}

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

func mainExitCode(ctx context.Context, args []string, stderr io.Writer) int {
	if err := run(ctx, args); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return controlcli.Run(ctx, args)
}
