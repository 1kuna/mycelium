//go:build darwin && cgo

package myce

/*
#include <libproc.h>
#include <sys/proc_info.h>
#include <mach/mach.h>
#include <errno.h>

// Resident bytes and birth of one process from a single kernel call, or -errno.
static int myce_task_resident(int pid, unsigned long long *rss, long long *sec, long long *usec) {
    struct proc_taskallinfo info;
    errno = 0;
    int n = proc_pidinfo(pid, PROC_PIDTASKALLINFO, 0, &info, sizeof(info));
    if (n != (int)sizeof(info)) return errno ? -errno : -ESRCH;
    *rss = info.ptinfo.pti_resident_size;
    *sec = (long long)info.pbsd.pbi_start_tvsec;
    *usec = (long long)info.pbsd.pbi_start_tvusec;
    return 0;
}

// Physical footprint and resident size of one process, then its birth, or
// -errno. The footprint includes Metal buffers (IOKit/graphics memory charged
// to the task) and compressed pages, which the resident size omits. Birth is
// read second, so a successor under a reused PID fails the caller's fence.
static int myce_task_footprint(int pid, unsigned long long *footprint, unsigned long long *rss, long long *sec, long long *usec) {
    struct rusage_info_v4 ri;
    errno = 0;
    if (proc_pid_rusage(pid, RUSAGE_INFO_V4, (rusage_info_t *)&ri) != 0) return errno ? -errno : -ESRCH;
    struct proc_bsdinfo bsd;
    errno = 0;
    int n = proc_pidinfo(pid, PROC_PIDTBSDINFO, 0, &bsd, sizeof(bsd));
    if (n != (int)sizeof(bsd)) return errno ? -errno : -ESRCH;
    *footprint = ri.ri_phys_footprint;
    *rss = ri.ri_resident_size;
    *sec = (long long)bsd.pbi_start_tvsec;
    *usec = (long long)bsd.pbi_start_tvusec;
    return 0;
}

// The counters vm_stat prints, without spawning it. vm_stat reports
// "Pages free" as free_count - speculative_count.
static int myce_vm_counts(unsigned long long *page, unsigned long long *free_pages,
                          unsigned long long *inactive, unsigned long long *speculative,
                          unsigned long long *swapouts) {
    static mach_port_t host = MACH_PORT_NULL;
    if (host == MACH_PORT_NULL) host = mach_host_self();
    vm_size_t size = 0;
    if (host_page_size(host, &size) != KERN_SUCCESS) return -1;
    vm_statistics64_data_t vm;
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    if (host_statistics64(host, HOST_VM_INFO64, (host_info64_t)&vm, &count) != KERN_SUCCESS) return -1;
    *page = size;
    *free_pages = vm.free_count - vm.speculative_count;
    *inactive = vm.inactive_count;
    *speculative = vm.speculative_count;
    *swapouts = vm.swapouts;
    return 0;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// In-process probes replace two ps and two exec'd probes per sample (about
// 70 ms measured on an M-series Mac) with one process-table read and one
// libproc call per owned process (about 1 ms). That cost is what allows the
// guard to sample at the cgroup sampler's cadence: the measured reaction time
// was three 1 s samples, so overshoot was the job's growth rate times ~3 s.
const processMemorySamplePeriod = attemptMemorySamplePeriod

// ok=false means this sample needs the ps fallback (process table unreadable,
// or an owned process this user cannot inspect, such as a sandbox account).
func darwinFastOwnedResident(roots map[int]string) (uint64, int, error, bool) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return 0, 0, nil, false
	}
	return ownedResidentFromTable(roots, darwinProcRows(procs), func(pid int) (int, error) { return unix.Getsid(pid) }, libprocResident, processBirth)
}

// The same owned tree as darwinFastOwnedResident, summing phys_footprint and
// RSS. Measured on M4 Macs (#1044), private and shared MTLBuffers filled by a
// GPU blit raise the footprint 1:1 while RSS sees none of them. There is no ps
// fallback: a process this user cannot inspect makes the sample unavailable,
// and Metal work then keeps its envelope.
func darwinFastOwnedFootprint(roots map[int]string) (footprint, resident uint64, members int, err error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return 0, 0, 0, err
	}
	total, members, err, ok := ownedPairFromTable(roots, darwinProcRows(procs), func(pid int) (int, error) { return unix.Getsid(pid) }, libprocFootprint, processBirth)
	if err == nil && !ok {
		err = errors.New("footprint member not inspectable")
	}
	return total[0], total[1], members, err
}

func darwinProcRows(procs []unix.KinfoProc) []darwinProcRow {
	rows := make([]darwinProcRow, 0, len(procs))
	for _, p := range procs {
		rows = append(rows, darwinProcRow{pid: int(p.Proc.P_pid), ppid: int(p.Eproc.Ppid), pgid: int(p.Eproc.Pgid),
			birth: fmt.Sprintf("%d:%d", p.Proc.P_starttime.Sec, p.Proc.P_starttime.Usec), zombie: p.Proc.P_stat == 5})
	}
	return rows
}

func libprocFootprint(pid int) ([2]uint64, string, error) {
	var footprint, rss C.ulonglong
	var sec, usec C.longlong
	if rc := C.myce_task_footprint(C.int(pid), &footprint, &rss, &sec, &usec); rc < 0 {
		return [2]uint64{}, "", syscall.Errno(-rc)
	}
	return [2]uint64{uint64(footprint), uint64(rss)}, fmt.Sprintf("%d:%d", int64(sec), int64(usec)), nil
}

func libprocResident(pid int) (uint64, string, error) {
	var rss C.ulonglong
	var sec, usec C.longlong
	if rc := C.myce_task_resident(C.int(pid), &rss, &sec, &usec); rc < 0 {
		return 0, "", syscall.Errno(-rc)
	}
	return uint64(rss), fmt.Sprintf("%d:%d", int64(sec), int64(usec)), nil
}

func darwinFastPressure() (*gpuHostState, error, bool) {
	level, err := unix.SysctlUint32("kern.memorystatus_vm_pressure_level")
	if err != nil {
		return nil, nil, false
	}
	if level != 1 && level != 2 && level != 4 {
		return nil, errors.New("memory pressure level unavailable"), true
	}
	var page, free, inactive, speculative, swapouts C.ulonglong
	if C.myce_vm_counts(&page, &free, &inactive, &speculative, &swapouts) != 0 || page == 0 {
		return nil, nil, false
	}
	// Same availability as parseVMStat: free + inactive + speculative pages.
	available := float64(uint64(free)+uint64(inactive)+uint64(speculative)) * float64(page) / (1 << 30)
	return &gpuHostState{available: available, swapOut: uint64(swapouts), level: int(level)}, nil, true
}

func darwinOwnedFootprintProbe() func(map[int]string) (uint64, uint64, int, error) {
	return func(roots map[int]string) (uint64, uint64, int, error) {
		probeguard.Check()
		return darwinFastOwnedFootprint(roots)
	}
}
