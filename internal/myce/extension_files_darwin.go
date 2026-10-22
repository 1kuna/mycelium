package myce

import (
	"golang.org/x/sys/unix"
	"os"
)

// Keep the filesystem operations injectable for SMB's unsupported hard links.
type outputPublishFS struct {
	link            func(string, string) error
	renameExclusive func(string, string) error
}

func publishJobOutput(source, destination string) error {
	return publishDarwinOutput(source, destination, outputPublishFS{
		link:            os.Link,
		renameExclusive: func(from, to string) error { return unix.RenamexNp(from, to, unix.RENAME_EXCL) },
	})
}

func publishDarwinOutput(source, destination string, fs outputPublishFS) error {
	return fs.renameExclusive(source, destination)
}
