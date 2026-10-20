package myce

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Existing shared storage is the transport in slice 1. Only relative names go
// over the API. Each hypha supplies its own local mount with --job-file-root.
func confinedPath(root, relative string) (string, error) {
	if root == "" || !safeRelative(relative) {
		return "", errors.New("job files require a root and clean relative names")
	}
	base, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	base, err = filepath.Abs(base)
	if err != nil {
		return "", err
	}
	p, err := filepath.EvalSymlinks(filepath.Join(base, relative))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(base, p)
	if err != nil || !safeRelative(rel) {
		return "", errors.New("job file escapes shared root")
	}
	return p, nil
}
func copyJobFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("job input/output must be a regular file")
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, f)
	syncErr := out.Sync()
	return errors.Join(copyErr, syncErr, out.Close())
}
func (w Worker) prepareExtensionFiles(j *Job, stateDir string) error {
	if !j.Spec.extensionJob() && len(j.Spec.Files) == 0 {
		return nil
	}
	dir := filepath.Join(stateDir, "job-files", j.ID, strconv.Itoa(j.Attempt))
	if info, err := os.Lstat(dir); err == nil {
		if !info.IsDir() {
			return errors.New("attempt input folder is not a directory")
		}
		j.Spec.Run.Workdir = dir
		return nil // Atomic publication makes retries idempotent.
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return err
	}
	staged, err := os.MkdirTemp(filepath.Dir(dir), ".inputs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staged)
	for name, ref := range j.Spec.Files {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := w.fileCache.copyTo(ctx, ref, filepath.Join(staged, name))
		cancel()
		if err != nil {
			return err
		}
	}
	for name, source := range j.Spec.Inputs {
		p, err := confinedPath(w.JobFileRoot, source)
		if err != nil {
			return err
		}
		if err := copyJobFile(p, filepath.Join(staged, name)); err != nil {
			return err
		}
	}
	if err := syncModelDirectory(staged); err != nil {
		return err
	}
	if err := os.Rename(staged, dir); err != nil {
		return err
	}

	if err := syncWorkerDir(filepath.Dir(dir)); err != nil {
		return err
	}
	j.Spec.Run.Workdir = dir
	return nil
}
func outputRelative(j Job, name, prefix string) string {
	return filepath.Join(prefix, j.ID, strconv.Itoa(j.Attempt), name)
}
func (w Worker) publishExtensionFiles(j Job) error {
	if j.FileTransport || !j.Spec.extensionJob() || len(j.Spec.Outputs) == 0 {
		return nil
	}
	root, err := filepath.EvalSymlinks(w.JobFileRoot)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	for name, prefix := range j.Spec.Outputs {
		src, err := confinedPath(j.Spec.Run.Workdir, name)
		if err != nil {
			return err
		}
		relative := outputRelative(j, name, prefix)
		dest := filepath.Join(root, relative)
		// Refuse every existing symlink in the destination ancestry. Attempt-specific
		// outputs never overwrite another execution's results, including late ones.
		parent := filepath.Dir(dest)
		for p := parent; p != root; p = filepath.Dir(p) {
			if !strings.HasPrefix(p, root+string(filepath.Separator)) {
				return errors.New("output escapes root")
			}
			if info, e := os.Lstat(p); e == nil && !info.IsDir() {
				return errors.New("output parent is not a directory")
			} else if e != nil && !os.IsNotExist(e) {
				return e
			}
		}
		if err := os.MkdirAll(parent, 0700); err != nil {
			return err
		}
		tmp, err := os.MkdirTemp(parent, ".output-")
		if err != nil {
			return err
		}
		staged := filepath.Join(tmp, "file")
		err = copyJobFile(src, staged)
		if err == nil {
			err = os.Link(staged, dest)
		} // Atomic no-overwrite publication.
		_ = os.RemoveAll(tmp)
		if err != nil {
			return fmt.Errorf("attempt output publication failed: %w", err)
		}
	}
	return nil
}
func validateExtensionJob(s Spec) error {
	if err := validateExtensionSelector(s); err != nil {
		return err
	}
	if c := s.ExtensionCheck; c != nil {
		if s.Extension == "" || s.ExtensionDigest == "" || len(s.AllowedMachines) != 1 || !filepath.IsAbs(c.Directory) || c.Work == "" || s.Kind != "" || s.Program != nil || len(s.Args)+len(s.Inputs)+len(s.Outputs)+len(s.Files) != 0 || s.Resumable {
			return errors.New("extension checks require an exact extension, one local worker, a check work type and an absolute scratch directory")
		}
	}
	if s.Kind != "" && s.Program != nil {
		return errors.New("choose kind or program, not both")
	}
	if s.Kind != "" && !extensionName.MatchString(s.Kind) {
		return errors.New("invalid kind of work")
	}
	if s.Program != nil {
		if !extensionName.MatchString(s.Program.Type) || !safeRelative(s.Program.File) {
			return errors.New("program requires type and an input file name")
		}
		_, nas := s.Inputs[s.Program.File]
		_, stored := s.Files[s.Program.File]
		if !nas && !stored {
			return errors.New("program file must name an input")
		}
		if len(s.Args) > 0 {
			return errors.New("program arguments belong in program.args")
		}
	}
	if s.Execution != nil || (s.Kind == "" && s.Program == nil && len(s.Run.Env) > 0) || len(s.Run.Sidecars) > 0 || len(s.Run.Command) > 0 || s.Run.Workdir != "" || s.Run.Docker || len(s.Run.DockerSidecars) > 0 || s.Run.DockerNetwork != "" || s.Run.DockerMemoryGB != 0 || s.Run.DockerMemorySwap != nil || s.Environment.Key() != "" || s.Needs.MemoryGB != 0 || s.Needs.HostMemoryGB != 0 || s.Needs.Model != "" || s.Needs.Platform.Any || len(s.Needs.Platform.Required)+len(s.Needs.Platform.Preferred) > 0 {
		return errors.New("kind/program jobs use extension-owned commands, environments and hardware reservations; use size_hint")
	}
	if !validMemoryValue(s.SizeHint.MemoryGB) || !validMemoryValue(s.SizeHint.HostMemoryGB) {
		return errors.New("size hints must be finite and nonnegative")
	}
	names := []string{}
	for name := range s.Files {
		names = append(names, name)
	}
	for name, p := range s.Inputs {
		if !safeRelative(name) || !safeRelative(p) {
			return errors.New("inputs require clean relative local names and shared paths")
		}
		names = append(names, name)
	}
	for name, p := range s.Outputs {
		if !safeRelative(name) || !safeRelative(p) {
			return errors.New("outputs require clean relative local names and shared prefixes")
		}
		names = append(names, name)
	}
	for i, a := range names {
		for k, b := range names {
			if i != k && (a == b || strings.HasPrefix(a, b+"/")) {
				return errors.New("job file names overlap")
			}
		}
	}
	return nil
}
