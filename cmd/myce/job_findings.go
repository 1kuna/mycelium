package main

import (
	"fmt"
	"io"

	"mycelium/internal/myce"
)

// printJobFinding writes a terminal job's one-line finding to the progress
// stream, so agents read the cause and action without fetching diagnostics.
// Stdout's "ID state" contract for wait is untouched.
func printJobFinding(w io.Writer, j *myce.Job) error {
	if j == nil || j.Finding == nil {
		return nil
	}
	_, err := fmt.Fprintf(w, "%s %s\n", j.ID, j.Finding.Text())
	return err
}
