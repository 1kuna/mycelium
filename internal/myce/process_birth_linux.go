package myce

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

func processBirth(pid int) (string, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
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
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(boot)) + ":" + f[19], nil
}
