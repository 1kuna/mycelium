package myce

import (
	"os/exec"
	"syscall"
)

func setHostWorkerCredential(cmd *exec.Cmd, a WorkerAccount) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: a.UID, Gid: a.GID, Groups: append([]uint32(nil), a.Groups...)}}
	return nil
}
