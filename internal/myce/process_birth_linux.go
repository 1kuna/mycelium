package myce

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func processBirth(pid int) (string, error) {
	return processBirthUsing(pid, os.ReadFile)
}

func processBirthUsing(pid int, readFile func(string) ([]byte, error)) (string, error) {
	b, err := readFile("/proc/" + strconv.Itoa(pid) + "/stat")
	// Linux can return ESRCH when the process vanishes after opening stat.
	// Callers use the same absence contract as a missing file or zombie;
	// all other inspection errors must keep cleanup custody.
	if errors.Is(err, syscall.ESRCH) {
		return "", os.ErrNotExist
	}
	if err != nil {
		return "", err
	}
	i := strings.LastIndex(string(b), ") ")
	if i < 0 {
		return "", errors.New("invalid process identity")
	}
	f := strings.Fields(string(b)[i+2:])
	if len(f) < 20 {
		return "", errors.New("invalid process identity")
	}
	if f[0] == "Z" {
		return "", os.ErrNotExist
	}
	boot, err := readFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(boot)) + ":" + f[19], nil
}
