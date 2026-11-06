package main

import (
	"fmt"
	"io"

	"mycelium/internal/myce"
)

func printJobFindings(out io.Writer, findings []myce.JobFinding) {
	if text := myce.JobFindingsText(findings); text != "" {
		fmt.Fprintln(out, text)
	}
}
