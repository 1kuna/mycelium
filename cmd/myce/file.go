package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
)

func fileCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: myce file put [flags] FILE | get [flags] SHA256 DEST")
	}
	f := flag.NewFlagSet("file "+args[0], flag.ContinueOnError)
	authority := installedClientFlags(f)
	size := f.Int64("size-bytes", -1, "expected file byte count for get")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	c, err := authority.client()
	if err != nil {
		return err
	}
	if c.HTTP != nil {
		defer c.HTTP.CloseIdleConnections()
	}
	ctx := context.Background()
	switch args[0] {
	case "put":
		if f.NArg() != 1 {
			return errors.New("put requires one file")
		}
		ref, err := myce.IdentifyFile(f.Arg(0))
		if err != nil {
			return err
		}
		if err = c.UploadFile(ctx, f.Arg(0), ref); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(ref)
	case "get":
		if f.NArg() != 2 || *size < 0 {
			return errors.New("get requires --size-bytes, SHA256 and a new destination")
		}
		dest := f.Arg(1)
		if _, err := os.Lstat(dest); err == nil {
			return errors.New("destination exists")
		} else if !os.IsNotExist(err) {
			return err
		}
		partial := dest + ".myce-partial"
		if err := c.DownloadFile(ctx, myce.FileRef{SHA256: f.Arg(0), SizeBytes: *size}, partial); err != nil {
			return err
		}
		if err := os.Link(partial, dest); err != nil {
			return err
		}
		if err := os.Remove(partial); err != nil {
			return err
		}
		dir, err := os.Open(filepath.Dir(dest))
		if err != nil {
			return err
		}
		return errors.Join(dir.Sync(), dir.Close())
	default:
		return fmt.Errorf("unknown file command %q", args[0])
	}
}

// Upload only the explicitly declared input mappings. The transmitted description
// contains relative job names and content identities, never local source paths.
func uploadJobInputs(ctx context.Context, c myce.Client, s *myce.Spec, root string) error {
	if len(s.Files) > 0 {
		return errors.New("files-from cannot be combined with files")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return err
	}
	files := myce.FileManifest{}
	for name, source := range s.Inputs {
		if filepath.IsAbs(source) {
			return errors.New("input source must be relative")
		}
		p, err := filepath.EvalSymlinks(filepath.Join(resolved, source))
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(resolved, p)
		if err != nil || relative == ".." || len(relative) > 3 && relative[:3] == "../" {
			return errors.New("input escapes files-from folder")
		}
		ref, err := myce.IdentifyFile(p)
		if err != nil {
			return err
		}
		if err = c.UploadFile(ctx, p, ref); err != nil {
			return err
		}
		files[name] = ref
	}
	s.Files = files
	s.Inputs = nil
	return nil
}
