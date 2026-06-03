package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(mainExitCode(ctx, os.Args[1:], os.Stderr))
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
	if len(args) == 0 {
		return runPeer(ctx, nil)
	}

	switch args[0] {
	case "bootstrap":
		return runBootstrap(ctx, args[1:])
	case "config":
		return runConfig(ctx, args[1:])
	case "service":
		return runService(ctx, args[1:])
	case "run":
		return runPeer(ctx, args[1:])
	case "ctl":
		return runControl(ctx, args[1:])
	case "server", "node":
		return fmt.Errorf("mycelium %s was removed by the peer-native spec; use mycelium run with compute configured", args[0])
	default:
		if len(args[0]) > 0 && args[0][0] == '-' {
			return runPeer(ctx, args)
		}
		return fmt.Errorf("unknown command %q", args[0])
	}
}
