package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ErrWorkerUnitMigrationRequired prevents an install rerun from activating a
// containment migration, including one already staged by daemon-reload.
var ErrWorkerUnitMigrationRequired = errors.New("worker_unit_migration_required")

// InstallUnitPlan compares the manager's effective properties and the complete
// generated definition. It never reloads the manager or changes installed files.
func InstallUnitPlan(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) (string, error) {
	if c.Detection.OS != "linux" || c.Role != "worker" {
		return "", nil
	}
	if _, err := os.Stat(c.Service); errors.Is(err, os.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	if command == nil {
		return "", errors.New("worker_unit_observation_unavailable")
	}
	b, err := command(ctx, "systemctl", "--user", "show", filepath.Base(c.Service))
	if err != nil {
		return "", fmt.Errorf("worker_unit_observation_unavailable: %w", err)
	}
	active := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok {
			active[k] = v
		}
	}
	// Unit sources retain properties without a one-to-one D-Bus representation
	// (ExecStart, EnvironmentFile, install dependencies), including operator drop-ins.
	before := map[string]string{}
	after := map[string]string{}
	apply := func(values map[string]string, text string) {
		section := ""
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "[") {
				section = line
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok || strings.HasPrefix(k, "#") || strings.HasPrefix(k, ";") {
				continue
			}
			key := section + " " + k
			if k == "EnvironmentFile" || k == "Environment" {
				if v != "" && values[key] != "" {
					v = values[key] + "\n" + v
				}
			}
			values[key] = v
		}
	}
	path := active["FragmentPath"]
	if path != c.Service || active["ActiveState"] == "" || active["Slice"] == "" || active["OOMScoreAdjust"] == "" {
		return "", errors.New("worker_unit_observation_unavailable: incomplete unit identity or containment properties")
	}
	old, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	apply(before, string(old))
	apply(after, c.ServiceText())
	for _, path := range strings.Fields(active["DropInPaths"]) {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		apply(before, string(data))
		apply(after, string(data))
	}
	changes := map[string]string{}
	for k, v := range before {
		if next, ok := after[k]; !ok || v != next {
			changes[k] = unitDefinitionChange(k, v, next)
		}
	}
	for k, v := range after {
		if _, ok := before[k]; !ok {
			changes[k] = unitDefinitionChange(k, "", v)
		}
	}
	// Prefer effective manager values for scalar properties. This also catches
	// inherited defaults and a loaded definition differing from the files on disk.
	for key, want := range after {
		if !strings.HasPrefix(key, "[Service] ") {
			continue
		}
		property := strings.TrimPrefix(key, "[Service] ")
		switch property {
		case "ExecStart", "Environment", "EnvironmentFile":
			continue
		case "Delegate":
			property = "DelegateControllers"
		case "RestartSec":
			property = "RestartUSec"
			want += "s"
		case "TimeoutStopSec":
			property = "TimeoutStopUSec"
			if !strings.HasSuffix(want, "s") {
				want += "s"
			}
		case "TimeoutStartSec":
			property = "TimeoutStartUSec"
		}
		if got, ok := active[property]; ok {
			equal := got == want
			if property == "DelegateControllers" {
				a, b := strings.Fields(got), strings.Fields(want)
				sort.Strings(a)
				sort.Strings(b)
				equal = strings.Join(a, " ") == strings.Join(b, " ")
			}
			if strings.HasSuffix(property, "USec") {
				a, ea := systemdTimeout(got)
				b, eb := systemdTimeout(want)
				equal = equal || ea == nil && eb == nil && a == b
			}
			if !equal {
				changes[key] = fmt.Sprintf("%s: %q -> %q", key, got, want)
			} else {
				delete(changes, key)
			}
		}
	}
	keys := make([]string, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := []string{}
	for _, key := range keys {
		lines = append(lines, changes[key])
	}
	var refusal error
	slice, score, group := active["Slice"], active["OOMScoreAdjust"], active["ControlGroup"]
	if active["ActiveState"] == "active" && group == "" {
		return strings.Join(lines, "\n"), errors.New("worker_unit_observation_unavailable: incomplete containment properties")
	}
	var placementErr error
	if group != "" {
		_, placementErr = jointMemoryRoot(group, c.workerSlice())
	}
	if slice != c.workerSlice() || score != "0" || placementErr != nil {
		lines = append(lines, fmt.Sprintf("Active ControlGroup: %q; generated Slice=%s OOMScoreAdjust=0", group, c.workerSlice()))
		refusal = fmt.Errorf("%w: stage with myce migrate worker-unit --config INSTALLED_WORKER_CONFIG; activate through the next normal myce fleet upgrade to a newer approved release (MY-55)", ErrWorkerUnitMigrationRequired)
	}
	if len(lines) == 0 {
		lines = append(lines, "none")
	}
	return strings.Join(lines, "\n"), refusal
}

func unitDefinitionChange(key, before, after string) string {
	if key == "[Service] Environment" {
		return key + ": changed (values redacted)"
	}
	return fmt.Sprintf("%s: %q -> %q", key, before, after)
}
