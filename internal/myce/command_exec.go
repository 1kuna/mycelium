package myce

import (
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
)

// Private pipes fence execution behind a durable queue hand-off and report exec errors.
func CommandExec(args []string) error {
	ready := os.NewFile(3, "command-exec")
	if ready == nil {
		return errors.New("command exec receipt pipe unavailable")
	}
	defer ready.Close()
	syscall.CloseOnExec(3)
	// The worker confirms the queue hand-off only once this helper reaches the gate.
	approval := os.NewFile(4, "command-start-gate")
	defer approval.Close()
	syscall.CloseOnExec(4)
	if _, err := ready.WriteString("R"); err != nil {
		return err
	}
	var answer [1]byte
	if _, err := io.ReadFull(approval, answer[:]); err != nil || answer[0] != 'S' {
		return errors.New("command start hand-off refused")
	}
	approval.Close()
	err := errors.New("command exec requires an executable")
	if len(args) > 0 {
		err = syscall.Exec(args[0], args, os.Environ())
	}
	_, _ = ready.WriteString("command exec failed: " + err.Error())
	return err
}

const execFormatMessage = "program was built for another architecture; name its required arch in targets, or declare portable: true if this runs on any arch"

// Only an exec/launch error, never captured stderr, can supply this explanation.
// It changes the failure message, not placement or future submissions.
func execFormatFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOEXEC) {
		return true
	}
	text := strings.ToLower(err.Error())
	for _, diagnostic := range []string{"exec format error", "bad cpu type in executable"} {
		if text == diagnostic || strings.HasSuffix(text, ": "+diagnostic) {
			return true
		}
	}
	return false
}
