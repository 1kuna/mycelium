package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	if len(jobInputPaths(j)) > 0 && w.JobFileRoot == "" {
		return errors.New("job files require a root and clean relative names")
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
	var staged string
	var err error
	// Worker retention may remove an empty job folder between these calls once.
	for range 2 {
		if err = os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
			return err
		}
		if staged, err = os.MkdirTemp(filepath.Dir(dir), ".inputs-"); !errors.Is(err, os.ErrNotExist) {
			break
		}
	}
	if err != nil {
		return err
	}
	defer os.RemoveAll(staged)
	if !j.Spec.Run.Docker && j.Extension != nil {
		var files map[string][]byte
		for _, extension := range j.Extensions {
			if extension.Digest == j.Extension.Digest {
				files = extension.Files
				break
			}
		}
		if len(files) == 0 {
			return errors.New("frozen native extension source missing")
		}
		for name, data := range files {
			if !safeRelative(name) {
				return errors.New("invalid frozen extension path")
			}
			path := filepath.Join(staged, ".myce-extension", name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				return err
			}
		}
	}
	if j.Spec.PrepareSource {
		ref, ok := jobInputFiles(j)[".myce-source.tar"]
		if !ok {
			return errors.New("missing frozen source archive")
		}
		ctx, cancel := context.WithCancel(context.Background())
		err = w.fileCache.copyTo(ctx, ref, filepath.Join(staged, ".myce-source.tar"))
		cancel()
		if err != nil {
			return err
		}
		if err = extractJobSource(filepath.Join(staged, ".myce-source.tar"), staged); err != nil {
			return err
		}
	}
	for name, ref := range jobInputFiles(j) {
		if j.Spec.PrepareSource && name == ".myce-source.tar" {
			continue
		}
		if j.Spec.PrepareSource {
			// Explicit inputs replace source files only after archive extraction.
			if err := removeSourceInput(staged, name); err != nil {
				return err
			}
		}
		err := w.fileCache.copyTo(context.Background(), ref, filepath.Join(staged, name))
		if err != nil {
			return err
		}
	}
	for name, source := range jobInputPaths(j) {
		p, err := confinedPath(w.JobFileRoot, source)
		if err != nil {
			return err
		}
		if err := copyJobFile(p, filepath.Join(staged, name)); err != nil {
			return err
		}
	}
	if j.Batch != nil && j.Batch.Run {
		data, err := json.Marshal(batchManifest(j))
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(staged, ".myce-batch.json"), data, 0600); err != nil {
			return err
		}
		for name := range j.Spec.Outputs {
			if err = os.MkdirAll(filepath.Dir(filepath.Join(staged, name)), 0700); err != nil {
				return err
			}
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
func (w Worker) publishExtensionFiles(j Job, attemptWork string) error {
	if check := j.Spec.ExtensionCheck; check != nil && !j.Spec.Run.Docker && j.Spec.Run.Env["MYCE_CHECK_OUTPUT"] != "" {
		// Only the worker publishes the bounded check result into the CLI's
		// scratch folder. Payloads never receive write access to shared caches.
		actual, err := readExtensionCheckOutputAt(attemptWork, filepath.Join(".myce-check", "actual"))
		if err != nil {
			return err
		}
		f, err := os.CreateTemp(check.Directory, ".actual-")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		_, writeErr := f.Write(actual)
		if err := errors.Join(writeErr, f.Close()); err != nil {
			return err
		}
		if err := os.Rename(f.Name(), filepath.Join(check.Directory, "actual")); err != nil {
			return err
		}
	}
	if err := w.validateExtensionCheckOutput(j); err != nil {
		return err
	}
	if j.storedFileResults() || (!j.Spec.extensionJob() && j.Spec.After == nil) || len(j.Spec.Outputs) == 0 {
		return nil
	}
	if w.JobFileRoot == "" {
		return errors.New("job files require a root and clean relative names")
	}
	root, err := filepath.EvalSymlinks(w.JobFileRoot)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	return publishJobOutputs(j, root)
}

// Non-Darwin workers retain hard-link publication. Darwin installs its SMB
// directory publisher during package initialization.
var publishJobOutputs = publishLinkedJobOutputs

func publishLinkedJobOutputs(j Job, root string) error {
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
			rel, err := filepath.Rel(root, p)
			if err != nil || !filepath.IsLocal(rel) {
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
	if s.Execution != nil || (s.Kind == "" && s.Program == nil && len(s.Run.Env) > 0) || len(s.Run.Sidecars) > 0 || len(s.Run.Command) > 0 || s.Run.Workdir != "" || s.Run.Docker || len(s.Run.DockerSidecars) > 0 || s.Run.DockerNetwork != "" || s.Run.DockerMemoryGB != 0 || s.Run.DockerMemorySwap != nil || s.Environment.Key() != "" || s.Needs.CPUCores != 0 || s.Needs.MemoryGB != 0 || s.Needs.HostMemoryGB != 0 || s.Needs.Model != "" || s.Needs.Platform.Any || len(s.Needs.Platform.Required)+len(s.Needs.Platform.Preferred) > 0 {
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

// Baseline correctness belongs to the attempt result, so status and qualification
// agree even when the command exits zero with incorrect output.
func (w Worker) validateExtensionCheckOutput(j Job) error {
	check := j.Spec.ExtensionCheck
	if check == nil || j.Extension == nil {
		return nil
	}
	for _, e := range j.Extensions {
		if e.Digest != j.Extension.Digest {
			continue
		}
		view, ok := e.forMachine(&w.Machine)
		if !ok {
			return errors.New("check target unavailable")
		}
		b, ok := view.baseline(check.Work, w.Machine.GPU)
		if !ok {
			return nil
		}
		actual, err := readExtensionCheckOutput(check.Directory)
		if err != nil {
			return err
		}
		return compareExtensionOutput(actual, e.Files[b.Output], b)
	}
	return errors.New("frozen check extension missing")
}

func readExtensionCheckOutput(directory string) ([]byte, error) {
	return readExtensionCheckOutputAt(directory, "actual")
}

// Root traversal rejects escaping ancestor/leaf symlinks even during replacement.
// Nonblocking open lets us reject FIFOs before any read can wait for a writer.
func readExtensionCheckOutputAt(directory, relative string) ([]byte, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	f, err := root.OpenFile(relative, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.Join(err, errors.New("check output must be a regular file beneath its attempt directory"))
	}
	actual, err := io.ReadAll(io.LimitReader(f, extensionMaxBytes+1))
	err = errors.Join(err, f.Close())
	if err != nil {
		return nil, err
	}
	if len(actual) > extensionMaxBytes {
		return nil, errors.New("check output too large")
	}
	return actual, nil
}
