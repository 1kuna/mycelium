package myce

import (
	"errors"
	"os"
	"syscall"
)

// The private fd closes on successful exec; failures are returned before any start receipt.
func CommandExec(args []string) error {
	ready := os.NewFile(3, "command-exec")
	if ready == nil {
		return errors.New("command exec receipt pipe unavailable")
	}
	defer ready.Close()
	syscall.CloseOnExec(3)
	err := errors.New("command exec requires an executable")
	if len(args) > 0 {
		err = syscall.Exec(args[0], args, os.Environ())
	}
	_, _ = ready.WriteString("command exec failed: " + err.Error())
	return err
}
