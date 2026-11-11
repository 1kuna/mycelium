package main

import (
	"fmt"
	"io"

	"mycelium/internal/myce"
)

// printCompactStatus renders the default summary through the same projection
// that default `--json` encodes.
func printCompactStatus(w io.Writer, s myce.Snapshot, full bool) {
	for _, line := range myce.NewCompactStatus(s).Lines(full) {
		fmt.Fprintln(w, line)
	}
}
