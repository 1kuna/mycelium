//go:build darwin

package myce

import "golang.org/x/sys/unix"

// One kernel process-table read, without forking ps: periodic custody
// refreshes stay cheap (a ps tree inventory measured 32ms).
func init() { processInventory = kernelProcessInventory }

func kernelProcessInventory() ([]processRow, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return psProcessInventory()
	}
	rows := make([]processRow, 0, len(procs))
	for _, p := range procs {
		rows = append(rows, processRow{pid: int(p.Proc.P_pid), ppid: int(p.Eproc.Ppid), pgid: int(p.Eproc.Pgid), zombie: p.Proc.P_stat == 5})
	}
	return rows, nil
}
