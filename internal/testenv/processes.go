package testenv

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// CheckProcesses must run before removing a suite's fixture. It catches both
// unjoined descendants and reparented workers whose argv still names the root,
// including processes holding files beneath already removed test directories.
func CheckProcesses(root string) error {
	cmd := exec.Command("ps", "-axo", "pid=,ppid=,stat=,args=")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("inspect test children: %w", err)
	}
	type process struct {
		pid, parent int
		args, state string
	}
	var processes []process
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		pid, e1 := strconv.Atoi(fields[0])
		parent, e2 := strconv.Atoi(fields[1])
		if e1 != nil || e2 != nil {
			return fmt.Errorf("invalid process inventory")
		}
		if pid != os.Getpid() && pid != cmd.Process.Pid {
			processes = append(processes, process{pid, parent, strings.Join(fields[3:], " "), fields[2]})
		}
	}
	owned := map[int]bool{os.Getpid(): true}
	for changed := true; changed; {
		changed = false
		for _, p := range processes {
			if !owned[p.pid] && (owned[p.parent] || strings.Contains(p.args, root+string(os.PathSeparator))) {
				owned[p.pid], changed = true, true
			}
		}
	}
	var remaining []process
	for _, p := range processes {
		if owned[p.pid] {
			remaining = append(remaining, p)
		}
	}
	if len(remaining) != 0 {
		return fmt.Errorf("test left child processes running or unjoined: %+v", remaining)
	}
	return nil
}
