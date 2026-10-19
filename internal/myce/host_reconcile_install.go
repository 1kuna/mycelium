package myce

import (
	"os"
	"path/filepath"
)

func (c ServiceConfig) HostReconcileServiceText() string {
	dir := c.HostStateDir
	if dir == "" {
		dir, _ = HostStateDirectory()
	}
	return "[Unit]\nDescription=Mycelium host reconciliation\nAfter=network-online.target\n\n[Service]\nType=oneshot\nExecStart=:" + unitQuote(c.Binary) + " host reconcile --local --config " + unitQuote(c.Config) + " --state-dir " + unitQuote(dir) + " --timeout 30s\nTimeoutStartSec=35s\nUMask=0077\n"
}

const hostReconcileTimerText = "[Unit]\nDescription=Converge Mycelium host intent with its queues\n\n[Timer]\nOnBootSec=5s\nOnUnitInactiveSec=5s\nUnit=myce-host-reconcile.service\n\n[Install]\nWantedBy=timers.target\n"

func atomicServiceText(path, body string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".reconcile-unit-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.WriteString(body)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncWorkerDir(filepath.Dir(path))
}
