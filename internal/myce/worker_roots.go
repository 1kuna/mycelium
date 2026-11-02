package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"

	"mycelium/internal/probeguard"
)

// Role names identify affected configured roots without publishing local paths
// or mount source credentials. An empty list means all observed roots are up.
type UnavailableRoot struct {
	Root   string `json:"root"`
	Reason string `json:"reason"`
}

type workerRootMonitor struct {
	mu       sync.Mutex
	expected map[string]string
}

// Inspect cached OS mount metadata, not the NAS filesystem. Installed NAS is an
// explicit expectation even when the worker boots with a surviving mountpoint
// directory. Independently configured roots learn network backing when seen.
func (w *Worker) sampleStorageRoots(ctx context.Context, checkpoint string) []UnavailableRoot {
	if w.rootMonitor == nil {
		w.rootMonitor = &workerRootMonitor{expected: map[string]string{}}
	}
	w.rootMonitor.mu.Lock()
	defer w.rootMonitor.mu.Unlock()
	roots := []struct{ name, path string }{
		{"job-files", w.JobFileRoot}, {"checkpoint", checkpoint},
		{"model", w.ModelRoot}, {"model-cache", w.CacheRoot},
		{"environment-source", w.EnvironmentSourceRoot}, {"environment", w.EnvironmentRoot},
		{"file-cache", w.FileCacheRoot}, {"data", w.StateDir}, {"extension", w.ExtensionRoot},
	}
	nas := ""
	if w.ServiceConfig != nil {
		nas = w.ServiceConfig.NAS
	}
	probe := w.MountRoots
	if probe == nil {
		probe = workerMountRoots
	}
	if nas != "" {
		roots = append(roots, struct{ name, path string }{"nas", nas})
	}
	mounts, err := probe(ctx)
	missing := []UnavailableRoot{}
	for _, root := range roots {
		if root.path == "" {
			continue
		}
		path := filepath.Clean(root.path)
		expected := w.rootMonitor.expected[root.name]
		if nas != "" && withinNAS(nas, path) {
			expected = filepath.Clean(nas)
		}
		for _, mount := range mounts {
			if withinNAS(mount, path) && (expected == "" || len(mount) > len(expected)) {
				expected = mount
			}
		}
		if expected == "" {
			continue
		}
		w.rootMonitor.expected[root.name] = expected
		mounted := slices.ContainsFunc(mounts, func(mount string) bool { return withinNAS(mount, expected) })
		if err != nil || !mounted {
			reason := "configured NAS mount missing"
			if err != nil {
				reason = "configured NAS mount inventory unavailable"
			}
			missing = append(missing, UnavailableRoot{Root: root.name, Reason: reason})
		}
	}
	return missing
}

// Uses existing frozen dependency identities and cache receipts. A missing NAS
// is not a host-wide disable: a local prepared environment or staged immutable
// input remains usable unless its own cache root is affected.
func storageRootReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	for _, root := range m.UnavailableRoots {
		needs := false
		switch root.Root {
		case "data", "extension":
			needs = true
		case "checkpoint":
			needs = needsCheckpoint(j)
			for _, input := range j.Spec.SharedInputs {
				needs = needs || input.NAS != "" && !slices.Contains(m.CachedSharedInputs, input.key())
			}
		case "job-files":
			needs = len(jobInputPaths(j)) > 0 || len(j.Spec.Outputs) > 0 || len(j.OutputFiles) > 0 || j.Spec.OutputDelivery == asyncNAS
		case "model":
			needs = len(j.Spec.ModelSnapshots) > 0 || j.Spec.Needs.Model != ""
		case "model-cache":
			needs = len(j.Spec.ModelSnapshots) > 0 || j.Spec.Needs.Model != ""
		case "file-cache":
			needs = len(jobInputFiles(j)) > 0 || len(j.Spec.SharedInputs) > 0 || j.ResumeFiles != nil
		case "environment-source":
			needs = j.Spec.Environment.Key() != "" && !environmentReady(j, m)
		case "environment":
			needs = j.Spec.Environment.Key() != "" || len(j.EnvironmentRecipes) > 0 || j.Spec.Extension != "" || len(j.Extensions) > 0
		}
		if needs {
			return fmt.Sprintf("%s: %s unavailable", root.Reason, root.Root)
		}
	}
	return ""
}

func storageRootReasonForModel(j *Job, m *Machine, model Model) string {
	copy := *m
	copy.UnavailableRoots = nil
	localModel := !model.DirectNAS && (model.FileStore || model.LocalSource != "" || slices.Contains(m.CachedModels, model.Name) || slices.Contains(m.Models, model.Name))
	for _, root := range m.UnavailableRoots {
		if root.Root == "model" && localModel {
			continue
		}
		if root.Root == "model-cache" && (model.DirectNAS || slices.Contains(m.Models, model.Name)) {
			continue
		}
		copy.UnavailableRoots = append(copy.UnavailableRoots, root)
	}
	return storageRootReason(j, &copy)
}

func (q *Queue) storageRootReason(j *Job, m *Machine) string {
	if m == nil {
		return ""
	}
	if model, ok := q.model(j.Spec.Needs.Model); ok {
		return storageRootReasonForModel(j, m, model)
	}
	return storageRootReason(j, m)
}

func (w Worker) storageRootReason(j *Job) string {
	for _, model := range w.ModelConfig.Models {
		if model.Name == j.Spec.Needs.Model {
			return storageRootReasonForModel(j, &w.Machine, model)
		}
	}
	return storageRootReason(j, &w.Machine)
}

var workerMountRoots = localWorkerMountRoots

func localWorkerMountRoots(ctx context.Context) ([]string, error) {
	probeguard.Check()
	return workerMountedNASRoots(ctx, runtime.GOOS, func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if runtime.GOOS == "linux" {
			return os.ReadFile("/proc/mounts")
		}
		return cachedMountInventory(ctx)
	})
}

func workerMountedNASRoots(ctx context.Context, osname string, command func(context.Context, string, ...string) ([]byte, error)) ([]string, error) {
	mounts, err := mountedNASInventory(ctx, osname, command)
	roots := []string{}
	for _, mount := range mounts {
		if mount.kind != "autofs" {
			roots = append(roots, mount.root)
		}
	}
	return roots, err
}
