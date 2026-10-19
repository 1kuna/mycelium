package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Pixi owns the mixed conda/PyPI solve. Workers never solve or install pixi.
func (c *EnvironmentCache) installPixi(ctx context.Context, r EnvironmentRecipe, dir string, env []string, out io.Writer, run func(context.Context, []string, string, []string, io.Writer) error) error {
	name := r.Environment
	if name == "" {
		name = "default"
	}
	if !extensionName.MatchString(name) {
		return errors.New("invalid pixi environment name")
	}
	args := []string{"pixi", "install", "--locked", "--environment", name, "--no-config", "--manifest-path", filepath.Join(dir, "pixi.toml")}
	if err := run(ctx, args, dir, env, out); err != nil {
		return err
	}
	var activation bytes.Buffer
	args = []string{"pixi", "shell-hook", "--json", "--locked", "--environment", name, "--no-config", "--manifest-path", filepath.Join(dir, "pixi.toml")}
	if err := run(ctx, args, dir, env, &activation); err != nil {
		fmt.Fprintln(out, activation.String())
		return err
	}
	var result struct {
		EnvironmentVariables map[string]string `json:"environment_variables"`
	}
	if err := json.Unmarshal(activation.Bytes(), &result); err != nil {
		return fmt.Errorf("pixi activation: %w", err)
	}
	if result.EnvironmentVariables["PATH"] == "" {
		return errors.New("pixi activation missing PATH")
	}
	// Activation cannot overwrite attempt, auth, port or worker device ownership.
	for k := range result.EnvironmentVariables {
		if reservedActivation(k) {
			delete(result.EnvironmentVariables, k)
		}
	}
	return saveWorkerJSON(dir, "activation.json", result.EnvironmentVariables)
}
func reservedActivation(k string) bool {
	return strings.HasPrefix(k, "MYCE_") || k == "CUDA_VISIBLE_DEVICES" || k == "ZE_AFFINITY_MASK" || k == "ONEAPI_DEVICE_SELECTOR"
}
func environmentActivation(root string) map[string]string {
	var env map[string]string
	b, _ := os.ReadFile(filepath.Join(root, "activation.json"))
	_ = json.Unmarshal(b, &env)
	return env
}
func environmentBins(root string) []string {
	if env := environmentActivation(root); env != nil {
		return filepath.SplitList(env["PATH"])
	}
	return []string{filepath.Join(root, "venv", "bin"), filepath.Join(root, "bin")}
}
func environmentPython(root string, target ExecutionTarget) string {
	if env := environmentActivation(root); env != nil {
		if target.OS == "windows" {
			return filepath.Join(env["CONDA_PREFIX"], "python.exe")
		}
		return filepath.Join(env["CONDA_PREFIX"], "bin", "python")
	}
	if target.OS == "windows" {
		return filepath.Join(root, "venv", "Scripts", "python.exe")
	}
	return filepath.Join(root, "venv", "bin", "python")
}
