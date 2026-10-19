package myce

import "path/filepath"

// QueueStateRoots enumerates the queue startup roots before any writes. The
// implicit upload spool is used by Open even when a separate spool is configured.
// Defaults are resolved here so install and startup cannot omit an implicit root.
func QueueStateRoots(database, host, uploads, files, joining string, discovery bool) []string {
	if database == "" {
		return []string{host, uploads, files, joining}
	}
	if uploads == "" {
		uploads = database + ".uploads"
	}
	if files == "" {
		files = database + ".files"
	}
	if discovery && joining == "" {
		joining = database + ".joining"
	}
	return []string{filepath.Dir(database), host, database + ".uploads", uploads, files, joining}
}
