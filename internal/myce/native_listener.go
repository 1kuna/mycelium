package myce

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var errNativeForeignListener = errors.New("native readiness listener belongs to another process")

// A 2xx response alone could be another job's server after the reservation is
// released. Verify every listener belongs to this attempt's group; unknown
// ownership fails preparation. No port registry or application nonce protocol.
func verifyNativeListener(port string, group int) error {
	members, err := workerGroupMembers(group)
	if err != nil {
		return err
	}
	owned := map[int]bool{}
	for _, pid := range members {
		owned[pid] = true
	}
	if runtime.GOOS == "linux" {
		return verifyLinuxNativeListener(port, owned)
	}
	if runtime.GOOS != "darwin" {
		return errors.New("native listener ownership unsupported on this OS")
	}
	out, err := exec.Command("lsof", "-nP", "-a", "-iTCP:"+port, "-sTCP:LISTEN", "-Fpn").Output()
	if err != nil {
		return fmt.Errorf("native listener ownership: %w", err)
	}
	found := false
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "p") {
			pid, e := strconv.Atoi(line[1:])
			if e != nil || !owned[pid] {
				return errNativeForeignListener
			}
			found = true
		}
		if strings.HasPrefix(line, "n") && line != "n127.0.0.1:"+port {
			return errors.New("native sidecar must listen on 127.0.0.1")
		}
	}
	if !found {
		return errors.New("native readiness listener ownership unconfirmed")
	}
	return nil
}
func verifyLinuxNativeListener(port string, owned map[int]bool) error {
	number, _ := strconv.Atoi(port)
	suffix := fmt.Sprintf(":%04X", number)
	inodes := map[string]bool{}
	for _, file := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" || !strings.HasSuffix(fields[1], suffix) {
				continue
			}
			if fields[1] != "0100007F"+suffix {
				return errors.New("native sidecar must listen on 127.0.0.1")
			}
			inodes["socket:["+fields[9]+"]"] = false
		}
	}
	if len(inodes) == 0 {
		return errors.New("native readiness listener ownership unconfirmed")
	}
	for pid := range owned {
		paths, err := filepath.Glob(fmt.Sprintf("/proc/%d/fd/*", pid))
		if err != nil {
			return err
		}
		for _, path := range paths {
			target, err := os.Readlink(path)
			if err == nil {
				if _, ok := inodes[target]; ok {
					inodes[target] = true
				}
			}
		}
	}
	for _, found := range inodes {
		if !found {
			return errNativeForeignListener
		}
	}
	return nil
}
