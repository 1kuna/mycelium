package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"mycelium/internal/myce"
)

func portabilityCommand(ctx context.Context, args []string, out io.Writer) error {
	f := flag.NewFlagSet("portability", flag.ContinueOnError)
	adapter := f.String("adapter", "", "project adapter YAML (omitted: inventory only)")
	entry := f.String("entry", "", "entry name (default: adapter default)")
	extension := f.String("extension", "", "registered extension name@version")
	project := f.String("project", "", "project policy for registered extension admission")
	asJSON := f.Bool("json", false, "structured portability report")
	config := f.String("config", "", "installed config")
	joining := f.String("joining-file", env("MYCE_JOINING_FILE", ""), "private joining file")
	queue := f.String("queue", env("MYCE_QUEUE", ""), "queue address")
	token := f.String("token", env("MYCE_TOKEN", ""), "queue token (prefer installed authority)")
	f.Lookup("token").DefValue = ""
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *entry != "" && *adapter == "" || *extension != "" && *adapter != "" {
		return errors.New("usage: myce portability [--adapter FILE [--entry NAME] | --extension NAME@VERSION] [--json]")
	}
	request := myce.PortabilityRequest{Extension: *extension, Project: *project}
	var placementAdvice []string
	if *adapter != "" {
		// Metadata read only: no source archive, Git operation, registration or setup.
		data, err := os.ReadFile(*adapter)
		if err != nil {
			return errors.New("cannot read adapter")
		}
		a, err := decodeAdapter(data)
		if err != nil {
			return errors.New("invalid adapter; run myce adapter check for local diagnostics")
		}
		name := *entry
		if name == "" {
			name = a.Default
		}
		p, _, err := adapterProfile(a, name)
		if err != nil {
			return errors.New("cannot select adapter entry")
		}
		request.Project = p.Project
		request.RequirementReasons = a.Entries[name].RequirementReasons
		placementAdvice = p.placementAdvisories()
		if p.Native != nil {
			request.Recipe = p.Native
			root, err := os.Getwd()
			if err != nil {
				return err
			}
			if p.Source.Directory != "" {
				if filepath.IsAbs(p.Source.Directory) {
					root = p.Source.Directory
				} else {
					root = filepath.Join(root, p.Source.Directory)
				}
			}
			// Bundle only known lock/manifest and explicit build input files.
			// Payloads are never executed by an advice read. A 1 MiB metadata budget matches
			// the existing extension-description contract and bounds API transfer.
			budget := int64(1 << 20)
			p.Native.Files = map[string][]byte{}
			for _, v := range p.Native.Variants {
				for _, r := range v.Environments {
					inputs := []string{r.LockFile, path.Join(path.Dir(r.LockFile), "uv.lock"), path.Join(path.Dir(r.LockFile), "pyproject.toml"), path.Join(path.Dir(r.LockFile), "pixi.lock"), path.Join(path.Dir(r.LockFile), "pixi.toml")}
					for _, arg := range r.Build {
						if file, ok := strings.CutPrefix(arg, "{extension}/"); ok {
							inputs = append(inputs, file)
						}
					}
					for _, file := range inputs {
						if !profileRelative(file) || strings.Contains(file, "\\") || p.Native.Files[file] != nil {
							continue
						}
						// Lstat every component: advice must not follow project symlinks outside
						// the selected directory to collect private local files.
						current := root
						safe := true
						for _, part := range strings.Split(filepath.FromSlash(file), string(filepath.Separator)) {
							current = filepath.Join(current, part)
							info, e := os.Lstat(current)
							if e != nil || info.Mode()&os.ModeSymlink != 0 {
								safe = false
								break
							}
						}
						if !safe {
							continue
						}
						info, err := os.Stat(current)
						if err != nil || !info.Mode().IsRegular() || info.Size() > budget {
							continue
						}
						input, err := os.Open(current)
						if err != nil {
							continue
						}
						b, err := io.ReadAll(io.LimitReader(input, budget+1))
						input.Close()
						if err != nil || int64(len(b)) > budget {
							continue
						}
						budget -= int64(len(b))
						p.Native.Files[file] = b
					}
				}
			}
		} else {
			request.Spec = &p.Spec
		}
	}
	client, err := runClient(*config, *joining, *queue, *token)
	if err != nil {
		return err
	}
	var report myce.PortabilityReport
	if err = client.Call(ctx, "POST", "/api/portability", request, &report); err != nil {
		return err
	}
	// Local pins live outside a native recipe. Also support queues predating
	// these notes without requiring a fleet upgrade for agent guidance.
	for _, advice := range placementAdvice {
		if !slices.Contains(report.Notes, advice) {
			report.Notes = append(report.Notes, advice)
		}
	}
	return myce.WritePortability(out, report, *asJSON)
}
