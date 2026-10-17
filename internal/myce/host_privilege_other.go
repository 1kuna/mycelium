//go:build !linux

package myce

import "os/exec"

func setHostWorkerCredential(_ *exec.Cmd, _ WorkerAccount) error { return ErrHostPrivilegeUnsupported }
