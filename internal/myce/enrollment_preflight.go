package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// PreflightEnrollmentInstall runs the installer's prerequisite and service
// hardware checks before enrollment acquires a drain. The existing service and
// its files stay in place; the manager detects with the candidate binary and a
// private config. Environment tools remain lazy until a recipe requires them.
func PreflightEnrollmentInstall(ctx context.Context, c ServiceConfig, executable string, command func(context.Context, string, ...string) ([]byte, error)) (result error) {
	if err := c.ValidateInstall(); err != nil {
		return err
	}
	roots, err := c.StateRoots()
	if err != nil {
		return err
	}
	if err := CheckStateRoots(roots...); err != nil {
		return err
	}
	if err := prepareInstallNAS(ctx, c, command); err != nil {
		return err
	}
	if c.Detection.OS == "linux" {
		if err := validateInstallDropIns(ctx, c, command); err != nil {
			return err
		}
	}
	if c.AllowDeviceless {
		return nil
	}
	dir, err := os.MkdirTemp("", "myce-onboarding-preflight-")
	if err != nil {
		return err
	}
	defer func() {
		if result != nil && strings.Contains(result.Error(), "service_probe_cleanup_failed") {
			return // Keep the config while a transient manager job may still use it.
		}
		result = errors.Join(result, os.RemoveAll(dir))
	}()
	staged := c
	staged.Config, staged.Binary = filepath.Join(dir, "config.json"), executable
	data, err := json.Marshal(staged)
	if err != nil {
		return err
	}
	if err := os.WriteFile(staged.Config, data, 0600); err != nil {
		return err
	}
	if c.Detection.OS == "linux" {
		data, err := os.ReadFile(c.GPUEnvironmentFile())
		if errors.Is(err, os.ErrNotExist) {
			data, err = []byte(gpuRuntimeEnvironment(os.Environ())), nil
		}
		if err != nil {
			return err
		}
		if err := os.WriteFile(staged.GPUEnvironmentFile(), data, 0600); err != nil {
			return err
		}
	}
	return ValidateWorkerServiceEnvironment(ctx, staged, RealClock{}, command)
}
