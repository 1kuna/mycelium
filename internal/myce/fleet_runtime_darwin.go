package myce

import (
	"context"
	"debug/macho"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func openFleetRunningImage() (*os.File, error) {
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

func verifyFleetRunningImage(file *os.File) error {
	image, err := macho.NewFile(file)
	if err != nil || image.Type != macho.TypeExec {
		return errors.New("opened image is not a Mach-O executable")
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("running executable vnode unavailable")
	}
	// Darwin has no /proc/self/exe. lsof exposes the kernel's mapped text
	// vnodes. Match the retained descriptor, so a swap before opening fails
	// closed and a swap afterwards still hashes the original mapped image.
	// The one-time 5s observation budget bounds a broken OS tool's startup
	// cost; missing evidence withholds fleet support while join can proceed.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/usr/sbin/lsof", "-b", "-a", "-p", strconv.Itoa(os.Getpid()), "-d", "txt", "-F", "fDi")
	cmd.Env = withoutMyceToken(os.Environ())
	output, err := cmd.Output()
	if err != nil {
		return errors.New("running executable mappings unavailable")
	}
	var device uint64
	text := false
	for _, line := range strings.Split(string(output), "\n") {
		if len(line) < 2 {
			continue
		}
		switch line[0] {
		case 'f':
			text, device = line[1:] == "txt", 0
		case 'D':
			device, _ = strconv.ParseUint(line[1:], 0, 64)
		case 'i':
			inode, err := strconv.ParseUint(line[1:], 10, 64)
			if err == nil && text && device == uint64(uint32(stat.Dev)) && inode == stat.Ino {
				return nil
			}
		}
	}
	return errors.New("opened executable is not the running image")
}
