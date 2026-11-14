package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var errRequiredDeliveryOutput = errors.New("required output missing or invalid")

func deliverySpoolPath(j Job, workerRoot string) string {
	return filepath.Join(attemptStorageRoot(j, workerRoot), ".myce-result-spool")
}

// Retained until exact queue acceptance, including upload/capacity failures.
// Manifest publication follows file sync and directory sync; restart always
// verifies the saved snapshot rather than re-reading potentially changed work.
func captureDeliverySpool(j Job, workerRoot string, observers ...*storageObserver) (string, FileManifest, error) {
	if !filepath.IsAbs(j.Spec.Run.Workdir) || !filepath.IsAbs(workerRoot) {
		return "", nil, errors.New("result spool requires a prepared absolute workdir")
	}
	dest := deliverySpoolPath(j, workerRoot)
	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", nil, err
	}
	// The managed attempt directory may be new when the command uses a custom
	// workdir. Persist its ancestors before publishing a durable spool below it.
	root, err := os.OpenRoot(workerRoot)
	if err != nil {
		return "", nil, err
	}
	relative, err := filepath.Rel(workerRoot, parent)
	if err == nil {
		err = syncDeliveryAncestors(root, relative)
	}
	if err = errors.Join(err, root.Close()); err != nil {
		return "", nil, err
	}
	if data, err := os.ReadFile(filepath.Join(dest, "manifest.json")); err == nil {
		var receipt deliveryReceipt
		if json.Unmarshal(data, &receipt) != nil || receipt.Execution != (Execution{ID: j.ID, Attempt: j.Attempt}) || receipt.Machine != j.Machine || receipt.Session != j.Session || receipt.Policy != "local-spool/1" || validateOutputManifest(j.Spec.Outputs, receipt.Files) != nil {
			return "", nil, errors.New("conflicting local result spool")
		}
		if err = receipt.Files.validate(); err != nil {
			return "", nil, err
		}
		for name, ref := range receipt.Files {
			decl, _ := outputDeclarationFor(j.Spec.Outputs, name)
			if err = verifyFile(context.Background(), filepath.Join(dest, "data", name), ref); err != nil {
				return "", nil, finalizationErrorFor("output_spool", decl, err)
			}
			f, e := os.Open(filepath.Join(dest, "data", name))
			if e != nil {
				return "", nil, finalizationErrorFor("output_spool", decl, e)
			}
			if e = errors.Join(f.Sync(), f.Close()); e != nil {
				return "", nil, finalizationErrorFor("output_spool", decl, e)
			}
		}
		if err = syncModelDirectory(dest); err != nil {
			return "", nil, err
		}
		if err = syncWorkerDir(parent); err != nil {
			return "", nil, err
		}
		return filepath.Join(dest, "data"), receipt.Files, nil
	} else if !os.IsNotExist(err) {
		return "", nil, err
	}
	// Estimate from actual local files before allocating a second copy. The worker
	// admission hint already budgets work + spool; this handles underestimated output.
	sources, err := collectOutputFiles(j.Spec.Run.Workdir, j.Spec.Outputs, jobInputFiles(&j))
	if err != nil {
		return "", nil, errors.Join(errRequiredDeliveryOutput, err)
	}
	var bytes uint64
	for name, src := range sources {
		decl, _ := outputDeclarationFor(j.Spec.Outputs, name)
		st, err := os.Stat(src)
		if err = regularOutputError(st, err); err != nil {
			return "", nil, requiredOutputError("output_validation", decl, err)
		}
		bytes = addDiskBytes(bytes, uint64(st.Size()))
	}
	disk, err := workerDiskProbe(parent)
	if err != nil {
		return "", nil, err
	}
	floor := diskFreeFloor(disk.TotalBytes)
	if disk.TotalBytes == 0 || disk.FreeBytes < floor || bytes > disk.FreeBytes-floor {
		return "", nil, fmt.Errorf("local result spool capacity: need=%d bytes; work files retained", bytes)
	}
	stage, err := os.MkdirTemp(parent, ".result-spool-")
	if err != nil {
		return "", nil, err
	}
	defer func() {
		for _, observer := range observers {
			observer.sample()
		}
		_ = os.RemoveAll(stage)
	}()
	files := FileManifest{}
	for name, src := range sources {
		path := filepath.Join(stage, "data", name)
		decl, _ := outputDeclarationFor(j.Spec.Outputs, name)
		if err = copyJobFile(src, path); err != nil {
			return "", nil, finalizationErrorFor("output_spool", decl, err)
		}
		ref, e := IdentifyFile(path)
		if e != nil {
			return "", nil, finalizationErrorFor("output_spool", decl, e)
		}
		files[name] = ref
	}
	receipt := deliveryReceipt{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Machine: j.Machine, Session: j.Session, Policy: "local-spool/1", Files: files}
	if err = saveWorkerJSON(stage, "manifest.json", receipt); err != nil {
		return "", nil, err
	}
	if err = syncModelDirectory(stage); err != nil {
		return "", nil, err
	}
	if err = os.Rename(stage, dest); err != nil {
		return "", nil, err
	}
	if err = syncWorkerDir(parent); err != nil {
		return "", nil, err
	}
	return filepath.Join(dest, "data"), files, nil
}
