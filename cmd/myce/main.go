package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"mycelium/cmd/internal/controlcli"
)

func main() {
	os.Exit(mainExitCode(context.Background(), os.Args[1:], os.Stderr))
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
