package main

import (
	"context"
	"fmt"
	"os"

	"mycelium/cmd/internal/controlcli"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	return controlcli.Run(ctx, args)
}
