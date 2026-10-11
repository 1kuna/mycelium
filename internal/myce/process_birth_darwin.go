package myce

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
)

func processBirth(pid int) (string, error) {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	if len(processes) == 0 {
		return "", os.ErrNotExist
	}
	p := processes[0]
	if p.Proc.P_pid != int32(pid) || p.Proc.P_stat == 5 {
		return "", os.ErrNotExist
	}
	return fmt.Sprintf("%d:%d", p.Proc.P_starttime.Sec, p.Proc.P_starttime.Usec), nil
}
