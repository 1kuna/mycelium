package myce

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var errNativeForeignListener = errors.New("native readiness listener belongs to another process")

// A 2xx response alone could be another job's server after the reservation is
// released. Linux verifies every listener belongs to this attempt's group;
// Darwin checks owned sockets, then dials to distinguish foreign from absent.
// Unknown ownership fails preparation. No port registry or application nonce.
func verifyNativeListener(port string, group int) error {
	return verifyNativeListenerContext(context.Background(), port, group)
}

func verifyNativeListenerContext(ctx context.Context, port string, group int) error {
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
	return verifyNativeListenerBounded(ctx, func() error { return verifyDarwinNativeListener(port, owned) })
}

func verifyNativeListenerBounded(ctx context.Context, probe func() error) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	// A kernel call can remain uninterruptible. Readiness must not wait for
	// it; the buffered channel lets the probe finish asynchronously afterward.
	go func() { done <- probe() }()
	select {
	case err := <-done:
		if ctx.Err() != nil {
			return runtimeErrorFor("readiness", ctx.Err())
		}
		return err
	case <-ctx.Done():
		go func() {
			select {
			case <-done:
			case <-time.After(time.Second):
				log.Print("native listener probe still blocked one second after cancellation; awaiting completion asynchronously")
			}
		}()
		return runtimeErrorFor("readiness", ctx.Err())
	}
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
