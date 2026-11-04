package myce

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestProcessBirthDisappearsDuringStatRead(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.ESRCH, syscall.EACCES} {
		t.Run(errno.Error(), func(t *testing.T) {
			readErr := &os.PathError{Op: "read", Path: "/proc/42/stat", Err: errno}
			birth, err := processBirthUsing(42, func(path string) ([]byte, error) {
				if path != "/proc/42/stat" {
					t.Fatalf("read boot identity after failed process observation: %s", path)
				}
				return nil, readErr
			})
			if birth != "" {
				t.Fatalf("failed read manufactured process identity: %q", birth)
			}
			if errno == syscall.ESRCH {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("disappeared process must use absence contract: %v", err)
				}
			} else if !errors.Is(err, syscall.EACCES) || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("permission refusal must retain custody: %v", err)
			}
		})
	}
}
