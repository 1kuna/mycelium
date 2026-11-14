package myce

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
)

// Output declarations (#1341, MY-11). "name" is one regular file, "dir/" is
// every regular file beneath dir, and a name containing * ? or [ is a
// path.Match glob (one segment per *); a matched folder contributes every file
// beneath it. A result is still a flat FileManifest of individual
// workdir-relative files, so content checks, downloads, retention and attempt
// fences treat each file exactly as before.
const (
	outputFile = iota
	outputFolder
	outputGlob
)

var errOutputNoMatch = errors.New("output folder or glob matched no files")

// The limit refusal keeps the queue's explanation in the local chain while the
// queue-visible finalization class stays a fixed rule (see finalizationErrors).
type outputLimitError struct{ cause error }

func (e outputLimitError) Error() string { return "outputs exceed file-store limits" }

func outputKind(decl string) (int, string) {
	if base, ok := strings.CutSuffix(decl, "/"); ok {
		return outputFolder, base
	}
	if strings.ContainsAny(decl, "*?[") {
		return outputGlob, decl
	}
	return outputFile, decl
}

func validOutputDeclaration(decl string) bool {
	kind, base := outputKind(decl)
	if !safeRelative(base) {
		return false
	}
	if kind == outputGlob {
		_, err := path.Match(base, "")
		return err == nil
	}
	return true
}

// ValidOutputDeclaration lets clients (adapter entries) accept the same folder
// and glob declarations.
func ValidOutputDeclaration(decl string) bool { return validOutputDeclaration(decl) }

// OutputDeclared reports whether a result file name belongs to one of the
// declarations, so result recovery accepts files a folder or glob expands to.
func OutputDeclared(decls []string, name string) bool {
	return slices.ContainsFunc(decls, func(decl string) bool { return outputCovers(decl, name) })
}

// ExpandOutputs resolves declarations against dir exactly as a worker resolves
// them against its workdir and returns the sorted file names.
func ExpandOutputs(dir string, decls []string) ([]string, error) {
	outputs := map[string]string{}
	for _, decl := range decls {
		outputs[decl] = ""
	}
	files, err := collectOutputFiles(dir, outputs, nil)
	if err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(files)), nil
}

func outputPatterns(outputs map[string]string) bool {
	for decl := range outputs {
		if kind, _ := outputKind(decl); kind != outputFile {
			return true
		}
	}
	return false
}

func outputCovers(decl, name string) bool {
	kind, base := outputKind(decl)
	switch kind {
	case outputFolder:
		return strings.HasPrefix(name, base+"/")
	case outputGlob:
		for p := name; p != "." && p != "/"; p = path.Dir(p) {
			if ok, _ := path.Match(base, p); ok {
				return true
			}
		}
		return false
	}
	return name == decl
}

// outputDeclarationFor names the declaration that owns a result file: an exact
// file declaration first, otherwise the first covering declaration in sorted
// order, so NAS prefixes stay deterministic when declarations overlap.
func outputDeclarationFor(outputs map[string]string, name string) (string, bool) {
	if kind, _ := outputKind(name); kind == outputFile {
		if _, ok := outputs[name]; ok {
			return name, true
		}
	}
	for _, decl := range slices.Sorted(maps.Keys(outputs)) {
		if outputCovers(decl, name) {
			return decl, true
		}
	}
	return "", false
}

// validateOutputManifest is pure string matching, safe under q.mu: every file
// must belong to a declaration and every declaration must contribute a file.
// With only file declarations this is the former exact-name bijection.
func validateOutputManifest(outputs map[string]string, refs FileManifest) error {
	decls := slices.Sorted(maps.Keys(outputs))
	covered := map[string]bool{}
	for name := range refs {
		owned := false
		for _, decl := range decls {
			if outputCovers(decl, name) {
				covered[decl], owned = true, true
			}
		}
		if !owned {
			return errors.New("complete output manifest required")
		}
	}
	for _, decl := range decls {
		if !covered[decl] {
			return errors.New("missing output")
		}
	}
	return nil
}

// Worker-owned attempt entries (result spool, its stage, progress files) are
// never payload output, even under a "*" glob.
func workerOwnedOutput(name string) bool {
	first, _, _ := strings.Cut(name, "/")
	return strings.HasPrefix(first, ".myce-") || strings.HasPrefix(first, ".result-spool-")
}

// collectOutputFiles resolves declarations against the attempt workdir and
// returns workdir-relative names mapped to confined source paths. Folders and
// glob matches are walked without following symlinks: a symlink or special
// file inside one is refused rather than silently skipped. Patterns skip
// declared input files and worker-owned entries. The error names the
// declaration and finalization step.
func collectOutputFiles(workdir string, outputs map[string]string, inputs FileManifest) (map[string]string, error) {
	files := map[string]string{}
	var root *os.Root
	defer func() {
		if root != nil {
			root.Close()
		}
	}()
	for _, decl := range slices.Sorted(maps.Keys(outputs)) {
		kind, base := outputKind(decl)
		if kind == outputFile {
			src, err := confinedPath(workdir, decl)
			if err != nil {
				return nil, finalizationErrorFor("output_path", decl, err)
			}
			files[decl] = src
			continue
		}
		if root == nil {
			var err error
			if root, err = os.OpenRoot(workdir); err != nil {
				return nil, finalizationErrorFor("output_path", decl, err)
			}
		}
		matches := []string{base}
		if kind == outputGlob {
			var err error
			if matches, err = fs.Glob(root.FS(), base); err != nil {
				return nil, finalizationErrorFor("output_path", decl, err)
			}
		}
		found := 0
		for _, match := range matches {
			if workerOwnedOutput(match) {
				continue
			}
			info, err := root.Lstat(match)
			if err != nil {
				return nil, finalizationErrorFor("output_path", decl, err)
			}
			if info.Mode()&fs.ModeSymlink != 0 {
				return nil, finalizationErrorFor("output_validation", decl, errors.New("file must be regular"))
			}
			if kind == outputFolder && !info.IsDir() {
				return nil, finalizationErrorFor("output_validation", decl, errors.New("output folder must be a directory"))
			}
			err = fs.WalkDir(root.FS(), match, func(name string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				if !d.Type().IsRegular() {
					return errors.New("file must be regular")
				}
				if _, input := inputs[name]; input || workerOwnedOutput(name) {
					return nil
				}
				src, err := confinedPath(workdir, name)
				if err != nil {
					return err
				}
				files[name] = src
				found++
				return nil
			})
			if err != nil {
				return nil, finalizationErrorFor("output_validation", decl, err)
			}
		}
		if found == 0 {
			return nil, finalizationErrorFor("output_path", decl, errOutputNoMatch)
		}
	}
	return files, nil
}

// fileStoreLimitRefusal reports a preflight refusal that only owner action can
// clear: the store allowance or project quota after reclaim, or a manifest
// larger than the queue's measured request capacity (the file-count bound).
func fileStoreLimitRefusal(err error) bool {
	var he *HTTPError
	return fileStoreSpaceRefusal(err) || errors.As(err, &he) && he.Code == "request_too_large"
}

// publishOutputFiles identifies and uploads a stored-file job's outputs.
// terminal marks a contract failure (missing, unsafe, unmatched, or more than
// the store admits); other errors keep the local files for a retry. Pattern
// manifests are preflighted whole, so an oversized folder or glob is refused
// once before any upload instead of failing part-way.
func (w Worker) publishOutputFiles(ctx context.Context, j Job, step, outputName *string) (FileManifest, bool, error) {
	done := w.Client.publication.begin(w.Client.clock(), "filesystem/output_path")
	*step, *outputName = "output_path", ""
	sources, err := collectOutputFiles(j.Spec.Run.Workdir, j.Spec.Outputs, jobInputFiles(&j))
	done()
	if err != nil {
		return nil, true, err
	}
	names := slices.Sorted(maps.Keys(sources))
	refs := FileManifest{}
	for _, name := range names {
		decl, _ := outputDeclarationFor(j.Spec.Outputs, name)
		*step, *outputName = "output_validation", decl
		done := w.Client.publication.begin(w.Client.clock(), "filesystem/output_validation")
		ref, err := IdentifyFile(sources[name])
		done()
		if err != nil {
			return nil, true, err
		}
		refs[name] = ref
	}
	if outputPatterns(j.Spec.Outputs) {
		*step, *outputName = "output_preflight", ""
		if err := w.Client.PreflightFileStore(ctx, refs); err != nil {
			if fileStoreLimitRefusal(err) {
				log.Printf("job %s outputs refused by the file store (%d files): %v", ShortID(j.ID), len(refs), err)
				return nil, true, outputLimitError{err}
			}
			return nil, false, err
		}
	}
	for _, name := range names {
		decl, _ := outputDeclarationFor(j.Spec.Outputs, name)
		*step, *outputName = "output_upload", decl
		if err := w.Client.UploadFile(ctx, sources[name], refs[name]); err != nil {
			return nil, false, err
		}
	}
	return refs, false, nil
}

// Folder and glob declarations need one execution that collects from its own
// workdir into the file store. Fan-out keys, after-stage name mapping, NAS
// extension publication and the storage protocol address outputs by exact name.
func validateOutputPatternSpec(s Spec) error {
	if s.FanOut != nil || s.After != nil || s.OutputDestination != nil || s.StorageProtocol != "" || s.extensionJob() {
		return errors.New("folder and glob outputs require a plain command without fan_out, after, output_destination or storage_protocol")
	}
	return nil
}
