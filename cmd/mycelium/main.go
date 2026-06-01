package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return runPeer(ctx, nil)
	}

	switch args[0] {
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
