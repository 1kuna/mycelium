package main

import (
	"fmt"
	"io"
	"strings"

	"mycelium/internal/myce"
)

func printCancellationScope(w io.Writer, j *myce.Job) {
	if c := j.Cancellation; c != nil && c.Release != nil {
		fmt.Fprintf(w, "  cancellation will_not_run=%t release=%s\n", c.WillNotRun, *c.Release)
	}
	if scope := j.CancellationScope; scope != nil {
		for _, p := range scope.Pieces {
			fmt.Fprintf(w, "  piece %s materialized=%t accepted=%t wave=%s will_not_run=%t release=%s dependents=%s\n", p.Key, p.Materialized, p.Accepted, p.WaveID, p.WillNotRun, p.Release, strings.Join(p.Dependents, ","))
		}
		for _, m := range scope.Members {
			fmt.Fprintf(w, "  member %s state=%s will_not_run=%t release=%s\n", m.JobID, m.State, m.WillNotRun, m.Release)
		}
	}
}
