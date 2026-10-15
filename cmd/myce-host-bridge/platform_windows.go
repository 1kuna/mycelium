//go:build windows

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows"
	"mycelium/internal/hostbridge"
)

func platformRun(ctx context.Context, action string) error {
	if windows.GetCurrentProcessToken().IsElevated() {
		return errors.New("run the helper as a regular user, not elevated")
	}
	root, err := hostbridge.InstallRoot()
	if err != nil {
		return err
	}
	source, err := os.Executable()
	if err != nil {
		return err
	}
	switch action {
	case "install":
		if !hostbridge.InteractiveInstallAllowed() {
			return errors.New("install from the covered interactive desktop session")
		}
		if err = hostbridge.Install(root, source, hostbridge.WindowsInstallHooks()); err != nil {
			return err
		}
		fmt.Println("Installed for this user at next logon. Config: %LOCALAPPDATA%\\MyceliumHostBridge\\bridge.json. Start the installed helper with serve to test now.")
		return nil
	case "uninstall":
		return hostbridge.Uninstall(ctx, root, source, hostbridge.WindowsInstallHooks())
	case "serve":
		return serve(ctx, filepath.Join(root, "bridge.json"))
	default:
		return errors.New("usage: myce-host-bridge install|serve|uninstall")
	}
}
func serve(ctx context.Context, path string) error {
	c, err := hostbridge.ReadConfig(path)
	if err != nil {
		return err
	}
	handler, err := hostbridge.Handler(c, hostbridge.WindowsSource())
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp4", c.Address)
	if err != nil {
		return err
	}
	server := http.Server{Handler: handler, ReadHeaderTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 4096}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	defer server.Close()
	for {
		select {
		case err := <-done:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-ctx.Done():
			return nil
		case <-tick.C:
			current, err := hostbridge.ReadConfig(path)
			if err != nil || current != c {
				return nil
			} // uninstall or credential rotation revokes this helper
		}
	}
}
