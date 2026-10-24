package main

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"mycelium/internal/myce"
)

type profileSource struct {
	Git       string `json:"git,omitempty" yaml:"git,omitempty"`
	Directory string `json:"directory,omitempty" yaml:"directory,omitempty"`
	Snapshot  string `json:"snapshot,omitempty" yaml:"snapshot,omitempty"`
}

func profileRelative(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.HasPrefix(name, "../") && !filepath.IsAbs(name) && filepath.Clean(name) == name && !strings.ContainsAny(name, "\\\x00\r\n:")
}

// Only explicit source bytes travel. Tar is a standard transport, not a second
// file store. Canonical headers make directory/tree snapshots content-stable.
// Reject links/devices rather than following them outside the declared source.
func freezeProfileSource(ctx context.Context, root string, source profileSource, dest string) (ref myce.FileRef, err error) {
	modes := 0
	for _, s := range []string{source.Git, source.Directory, source.Snapshot} {
		if s != "" {
			modes++
		}
	}
	if modes != 1 {
		return ref, errors.New("source requires exactly one of git, directory or snapshot (uncompressed tar)")
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return ref, err
	}
	tw := tar.NewWriter(out)
	write := func(name string, mode int64, size int64, r io.Reader) error {
		if !profileRelative(name) || name == ".myce-source.tar" || name == ".myce-inputs" || strings.HasPrefix(name, ".myce-inputs/") {
			return errors.New("source requires clean relative file names")
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: mode & 0777, Size: size, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err := io.CopyN(tw, r, size)
		return err
	}
	var cmd *exec.Cmd
	var input io.ReadCloser
	if source.Directory != "" {
		directory := filepath.Join(root, source.Directory)
		if filepath.IsAbs(source.Directory) {
			directory = source.Directory
		}
		err = filepath.WalkDir(directory, func(path string, entry os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if e = ctx.Err(); e != nil {
				return e
			}
			name, e := filepath.Rel(directory, path)
			if e != nil {
				return e
			}
			if name == ".git" || name == filepath.Join(".mycelium", "receipts") {
				if entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if entry.IsDir() {
				return nil
			}
			info, e := entry.Info()
			if e != nil {
				return e
			}
			if !info.Mode().IsRegular() {
				return errors.New("source must contain only regular files and directories")
			}
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			defer f.Close()
			return write(filepath.ToSlash(name), int64(info.Mode().Perm()), info.Size(), f)
		})
	} else {
		if source.Git != "" {
			tree, resolveErr := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--verify", "--end-of-options", source.Git+"^{tree}").Output()
			if resolveErr != nil {
				tw.Close()
				out.Close()
				return ref, resolveErr
			}
			cmd = exec.CommandContext(ctx, "git", "-C", root, "archive", "--format=tar", strings.TrimSpace(string(tree)))
			input, err = cmd.StdoutPipe()
			if err == nil {
				err = cmd.Start()
			}
		} else {
			path := source.Snapshot
			if !filepath.IsAbs(path) {
				path = filepath.Join(root, path)
			}
			input, err = os.Open(path)
		}
		if err == nil {
			tr := tar.NewReader(input)
			for {
				h, e := tr.Next()
				if e == io.EOF {
					break
				}
				if e != nil {
					err = e
					break
				}
				if h.Typeflag == tar.TypeDir || h.Typeflag == tar.TypeXGlobalHeader {
					continue
				}
				if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA {
					err = errors.New("source snapshot cannot contain links or special files")
					break
				}
				if err = write(h.Name, h.Mode, h.Size, tr); err != nil {
					break
				}
			}
			if err == nil {
				_, err = io.Copy(io.Discard, input)
			}
			input.Close()
		}
		if cmd != nil && cmd.Process != nil {
			err = errors.Join(err, cmd.Wait())
		}
	}
	err = errors.Join(err, tw.Close(), out.Sync(), out.Close())
	if err != nil {
		return ref, err
	}
	return myce.IdentifyFile(dest)
}

func shellArg(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func shellCommand(argv []string) string {
	quoted := make([]string, len(argv))
	for i, s := range argv {
		quoted[i] = shellArg(s)
	}
	return strings.Join(quoted, " ")
}
