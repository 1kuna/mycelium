package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"mycelium/internal/myce"
)

func recoverResultCommand(ctx context.Context, args []string, errOut io.Writer) error {
	f := flag.NewFlagSet("result recover", flag.ContinueOnError)
	f.SetOutput(errOut)
	authority := installedClientFlags(f)
	root := f.String("files-from", "", "directory containing declared output names (local or mounted NAS)")
	key := f.String("key", "", "idempotency key; reuse only for identical evidence")
	by := f.String("by", "", "requester label (operator assertion, not authentication)")
	reason := f.String("reason", "", "why these bytes are being recovered")
	source := f.String("source", "upload:local-files", "provenance: upload:LABEL or nas:RELATIVE_PATH; no absolute host paths")
	attempt := f.Int("attempt", 0, "require this failed attempt (default: current failed attempt)")
	var paths []string
	f.Func("file", "output-name=local-path; repeat to select or override files", func(s string) error { paths = append(paths, s); return nil })
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		args = append(append([]string{}, args[1:]...), args[0])
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	kind, location, ok := strings.Cut(*source, ":")
	if f.NArg() != 1 || *key == "" || *by == "" || strings.TrimSpace(*reason) == "" || !ok || (kind != "upload" && kind != "nas") || location == "" || (*root == "" && len(paths) == 0) {
		return errors.New("usage: myce result recover JOB --key KEY --by REQUESTER --reason REASON [--source upload:LABEL|nas:RELATIVE_PATH] [--attempt N] --files-from DIR | --file NAME=PATH ...")
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	fail := func(err error) error { return &ResultError{Code: "result_recovery_failed", Err: err, token: c.Token} }
	var plan myce.RecoveryPlan
	if err = c.Call(ctx, "GET", "/api/jobs/"+f.Arg(0)+"/recovery-plan", nil, &plan); err != nil {
		return fail(err)
	}
	if *attempt != 0 && *attempt != plan.Attempt {
		return fail(errors.New("failed attempt no longer matches --attempt"))
	}
	selected := map[string]string{}
	var sourceRoot *os.Root
	if len(paths) == 0 {
		sourceRoot, err = os.OpenRoot(*root)
		if err != nil {
			return fail(err)
		}
		defer sourceRoot.Close()
		for _, name := range plan.Outputs {
			selected[name] = filepath.FromSlash(name)
		}
	} else {
		for _, pair := range paths {
			name, path, ok := strings.Cut(pair, "=")
			if !ok || name == "" || path == "" || selected[name] != "" {
				return fail(errors.New("--file requires unique NAME=PATH mappings"))
			}
			selected[name] = path
		}
	}
	if len(selected) == 0 {
		return fail(errors.New("job has no declared outputs"))
	}
	p := myce.RecoveryRequest{Execution: plan.Execution, Machine: plan.Machine, Session: plan.Session, Key: *key, RequestedBy: *by, Reason: *reason, Source: myce.RecoverySource{Kind: kind, Location: location}, Files: myce.FileManifest{}}
	sources := map[string]*os.File{}
	for name, path := range selected {
		if name == "." || !fs.ValidPath(name) || strings.ContainsAny(name, "\\:") {
			return fail(errors.New("output names must be clean relative paths"))
		}
		declared := false
		for _, output := range plan.Outputs {
			if name == output {
				declared = true
			}
		}
		if !declared {
			return fail(errors.New("--file must name a declared output"))
		}
		lstat, open := os.Lstat, os.Open
		if sourceRoot != nil {
			lstat, open = sourceRoot.Lstat, sourceRoot.Open
		}
		info, err := lstat(path)
		if err != nil {
			return fail(err)
		}
		if !info.Mode().IsRegular() {
			return fail(errors.New("recovery sources must be regular files, not symlinks or special files"))
		}
		file, err := open(path)
		if err != nil {
			return fail(err)
		}
		defer file.Close()
		sources[name] = file
		ref, err := myce.IdentifyOpenFile(ctx, file)
		if err != nil {
			return fail(err)
		}
		p.Files[name] = ref
	}
	if err = c.PreflightFileStore(ctx, p.Files); err != nil {
		return fail(err)
	}
	for name, file := range sources {
		if err = c.UploadJobInputFile(ctx, file, p.Files[name]); err != nil {
			return fail(err)
		}
	}
	r, err := c.RecoverResult(ctx, p)
	if err != nil {
		return fail(err)
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
