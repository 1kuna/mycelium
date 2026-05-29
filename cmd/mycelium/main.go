package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mycelium <server|node|myce>")
	}

	switch args[0] {
	case "server":
		return runServer(ctx, args[1:])
	case "node":
		return runNode(ctx, args[1:])
	case "myce":
		return runControl(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
