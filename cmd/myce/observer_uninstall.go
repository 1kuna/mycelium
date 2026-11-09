package main

import (
	"context"
	"encoding/xml"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"mycelium/internal/myce"
)

func observerServicePath(platform, home string) string {
	if platform == "darwin" {
		return filepath.Join(home, "Library", "LaunchAgents", observerServiceLabel+".plist")
	}
	config := os.Getenv("XDG_CONFIG_HOME")
	if config == "" {
		config = filepath.Join(home, ".config")
	}
	return filepath.Join(config, "systemd", "user", "myce-observer.service")
}

// Read the registry actually configured in the installed definition, including
// explicit --root installs; never infer an empty registry from another HOME.
func observerDefinitionRoot(platform string, data []byte) (string, error) {
	var root string
	if platform == "darwin" {
		var plist struct {
			Arguments []string `xml:"dict>array>string"`
		}
		if err := xml.Unmarshal(data, &plist); err != nil {
			return "", err
		}
		for i, arg := range plist.Arguments {
			if arg == "--root" && i+1 < len(plist.Arguments) {
				root = plist.Arguments[i+1]
			}
		}
	} else {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "ExecStart=") {
				if _, value, ok := strings.Cut(line, `"--root" `); ok {
					decoded, err := strconv.Unquote(value)
					if err != nil {
						return "", err
					}
					root = strings.ReplaceAll(decoded, "%%", "%")
				}
			}
		}
	}
	if !filepath.IsAbs(root) {
		return "", errors.New("observer_registry_unconfirmed: installed definition has no absolute registry")
	}
	return root, nil
}

func observerHasArmedRegistrations(root string) (bool, error) {
	entries, err := filepath.Glob(filepath.Join(root, "*.json"))
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		var reg sharedProfileRegistration
		if err := readProfileJSON(entry, &reg); err != nil {
			return false, err
		}
		if !filepath.IsAbs(reg.Directory) {
			return false, errors.New("observer_registrations_unconfirmed")
		}
		var receipt profileReceipt
		err := readProfileJSON(filepath.Join(reg.Directory, "receipt.json"), &receipt)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false, err
		}
		if receipt.Observation == "armed" {
			return true, nil
		}
	}
	return false, nil
}

func uninstallObserverService(ctx context.Context, platform, home, expectedRoot string, command observerServiceRunner) error {
	path := observerServicePath(platform, home)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	root, err := observerDefinitionRoot(platform, data)
	if err != nil {
		return err
	}
	if expectedRoot != "" && filepath.Clean(expectedRoot) != filepath.Clean(root) {
		return errors.New("observer_registry_mismatch")
	}
	// Registration writers use this lock. Socket retirement is nonblocking on
	// it, so the native manager can stop an idle owner without lock inversion.
	startup, err := sharedProfileLock(ctx, filepath.Join(root, "startup.lock"))
	if err != nil {
		return err
	}
	defer startup.Close()
	armed, err := observerHasArmedRegistrations(root)
	if err != nil {
		return err
	}
	if armed {
		return errors.New("observer_registrations_armed: retain supervision until observations finish")
	}
	if platform == "darwin" {
		err = myce.StopUserService(ctx, platform, path, observerServiceLabel, func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return nil, command(ctx, name, args...)
		})
	} else if platform == "linux" {
		err = command(ctx, "systemctl", "--user", "disable", "--now", "myce-observer.service")
	} else {
		return errors.New("observer_supervision_unavailable: unsupported manager")
	}
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if platform == "linux" {
		if err := command(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
			return err
		}
	}
	if err := os.Remove(observerClientBinary(home)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil // Registry, receipts and diagnostic history are retained.
}

func retireUnusedObserver(ctx context.Context, c myce.ServiceConfig, home string, command observerServiceRunner) error {
	// Installed role definitions already enumerate the account's roles, including
	// instance units/custom installation roots. No separate membership registry.
	for _, dir := range []string{filepath.Dir(c.Service), filepath.Dir(observerServicePath(c.Detection.OS, home))} {
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			name := entry.Name()
			if c.Detection.OS == "darwin" && (name == "local.mycelium.queue.plist" || name == "local.mycelium.worker.plist") || c.Detection.OS == "linux" && (name == "myce-queue.service" || myce.ValidWorkerUnit(name)) {
				return nil
			}
		}
	}
	err := uninstallObserverService(ctx, c.Detection.OS, home, "", command)
	if err != nil && strings.HasPrefix(err.Error(), "observer_registrations_armed:") {
		return nil
	}
	return err
}
