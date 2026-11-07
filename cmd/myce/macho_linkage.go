package main

import (
	"context"
	"path/filepath"

	"mycelium/internal/myce"
)

// Keep payload linkage local to the read-only report. Do not upload binaries or
// absolute submitter paths to the queue merely to ask for portability advice.
func inspectSourceLinkage(root string, source profileSource, found *submitterFindings) {
	if source.Directory == "" {
		found.notes = append(found.notes, "Mach-O linkage: selected Git/snapshot source not inspected locally; staged Darwin payloads are checked before launch. Working-tree bytes are not evidence for the selected source.")
		return
	}
	directory := source.Directory
	if !filepath.IsAbs(directory) {
		directory = filepath.Join(root, directory)
	}
	findings, err := myce.InspectMachOLinkage(context.Background(), directory, "")
	found.linkage = append(found.linkage, findings...)
	if err != nil {
		found.notes = append(found.notes, "Mach-O linkage inspection incomplete: selected source metadata or runtime candidates unreadable; worker prelaunch rechecks the staged payload.")
	}
}
