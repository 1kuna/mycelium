// myce-host-bridge is the narrow Windows companion of the WSL myce worker.
package main

import (
	"context"
	"errors"
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
	if len(args) != 1 {
		return errors.New("usage: myce-host-bridge install|serve|uninstall")
	}
	return platformRun(ctx, args[0])
}
