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
	AllowDirty bool   `json:"allow_dirty,omitempty" yaml:"allow_dirty,omitempty"`
	Git        string `json:"git,omitempty" yaml:"git,omitempty"`
	Directory  string `json:"directory,omitempty" yaml:"directory,omitempty"`
	Snapshot   string `json:"snapshot,omitempty" yaml:"snapshot,omitempty"`
}

func profileRelative(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.HasPrefix(name, "../") && !filepath.IsAbs(name) && filepath.Clean(name) == name && !strings.ContainsAny(name, "\\\x00\r\n:")
}

// Only explicit source bytes travel. Tar is a standard transport, not a second
// file store. Canonical headers make directory/tree snapshots content-stable.
// Git and tar sources keep relative symlinks that resolve inside the source, as
// a checkout would; directory walks, hardlinks and devices are still refused.
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
	names, links := map[string]bool{}, map[string]string{}
	header := func(name string) (string, error) {
		name = strings.TrimPrefix(name, "./")
		if !profileRelative(name) || name == ".myce-source.tar" || name == ".myce-inputs" || strings.HasPrefix(name, ".myce-inputs/") {
			return "", errors.New("source requires clean relative file names")
		}
		if names[name] {
			return "", errors.New("source repeats a file name")
		}
		names[name] = true
		return name, nil
	}
	write := func(name string, mode int64, size int64, r io.Reader) error {
		name, err := header(name)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: mode & 0777, Size: size, Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		_, err = io.CopyN(tw, r, size)
		return err
	}
	link := func(name, target string) error {
		name, err := header(name)
		if err != nil {
			return err
		}
		if target == "" || filepath.IsAbs(target) || strings.ContainsAny(target, "\\\x00\r\n") {
			return errors.New("source symlink target must be relative")
		}
		links[name] = target
		return tw.WriteHeader(&tar.Header{Name: name, Mode: 0777, Linkname: target, Typeflag: tar.TypeSymlink})
	}
	var cmd *exec.Cmd
	var input io.ReadCloser
	if source.Directory != "" {
		err = walkProfileDirectory(ctx, root, source.Directory, func(name, path string, info os.FileInfo) error {
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			defer f.Close()
			return write(name, int64(info.Mode().Perm()), info.Size(), f)
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
				if h.Typeflag == tar.TypeSymlink {
					if err = link(h.Name, h.Linkname); err != nil {
						break
					}
					continue
				}
				if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA {
					err = errors.New("source snapshot cannot contain hardlinks or special files")
					break
				}
				if err = write(h.Name, h.Mode, h.Size, tr); err != nil {
					break
				}
			}
			if err == nil {
				err = checkProfileLinks(names, links)
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

// Every symlink must resolve inside the source, following other source links,
// and no entry may sit beneath a symlink, so extraction cannot write elsewhere.
func checkProfileLinks(names map[string]bool, links map[string]string) error {
	for name := range names {
		for dir := filepath.Dir(name); dir != "."; dir = filepath.Dir(dir) {
			if _, ok := links[dir]; ok {
				return errors.New("source entry " + name + " is beneath a symlink")
			}
		}
	}
	var resolve func(dir []string, target string, hops int) ([]string, bool)
	resolve = func(dir []string, target string, hops int) ([]string, bool) {
		if hops > 40 {
			return nil, false
		}
		path := append([]string(nil), dir...)
		for _, part := range strings.Split(target, "/") {
			switch part {
			case "", ".":
			case "..":
				if len(path) == 0 {
					return nil, false
				}
				path = path[:len(path)-1]
			default:
				path = append(path, part)
				if next, ok := links[strings.Join(path, "/")]; ok {
					var inside bool
					if path, inside = resolve(path[:len(path)-1], next, hops+1); !inside {
						return nil, false
					}
				}
			}
		}
		return path, true
	}
	for name, target := range links {
		parts := strings.Split(name, "/")
		if _, inside := resolve(parts[:len(parts)-1], target, 0); !inside {
			return errors.New("source symlink " + name + " leaves the source tree")
		}
	}
	return nil
}

// Resolve and hash in place before allocating staging space. Existing keys
// never need a second archive or another copy of their upload bytes.
func identifyProfileSource(ctx context.Context, root string, source profileSource) (profileSource, string, error) {
	modes := 0
	for _, value := range []string{source.Git, source.Directory, source.Snapshot} {
		if value != "" {
			modes++
		}
	}
	if modes != 1 {
		return source, "", errors.New("declare exactly one source")
	}
	if source.Git != "" {
		if source.Git == "HEAD" && !source.AllowDirty {
			dirty, err := exec.CommandContext(ctx, "git", "-C", root, "status", "--porcelain", "--untracked-files=normal", "--", ".", ":(exclude).mycelium/receipts").Output()
			if err != nil {
				return source, "", err
			}
			if len(dirty) != 0 {
				return source, "", errors.New("git HEAD source is dirty; commit it or explicitly allow_dirty")
			}
		}
		tree, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--verify", "--end-of-options", source.Git+"^{tree}").Output()
		source.Git = strings.TrimSpace(string(tree))
		return source, "git:" + source.Git, err
	}
	if source.Snapshot != "" {
		path := source.Snapshot
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		ref, err := myce.IdentifyFile(path)
		return source, "snapshot:" + profileHash(ref), err
	}
	type entry struct {
		Ref  myce.FileRef
		Mode uint32
	}
	files := map[string]entry{}
	err := walkProfileDirectory(ctx, root, source.Directory, func(name, path string, info os.FileInfo) error {
		ref, err := myce.IdentifyFile(path)
		if err != nil {
			return err
		}
		files[name] = entry{ref, uint32(info.Mode().Perm())}
		return nil
	})
	return source, "directory:" + profileHash(files), err
}

func walkProfileDirectory(ctx context.Context, root, directory string, visit func(string, string, os.FileInfo) error) error {
	if !filepath.IsAbs(directory) {
		directory = filepath.Join(root, directory)
	}
	return filepath.WalkDir(directory, func(path string, item os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		name, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if name == ".git" || name == filepath.Join(".mycelium", "receipts") {
			if item.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if item.IsDir() {
			return nil
		}
		info, err := item.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("source requires regular files")
		}
		return visit(filepath.ToSlash(name), path, info)
	})
}

func shellArg(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func shellCommand(argv []string) string {
	quoted := make([]string, len(argv))
	for i, s := range argv {
		quoted[i] = shellArg(s)
	}
	return strings.Join(quoted, " ")
}
