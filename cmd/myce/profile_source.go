package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"mycelium/internal/myce"
)

type profileSource struct {
	AdapterFile string   `json:"-" yaml:"-"`
	Paths       []string `json:"paths,omitempty" yaml:"paths,omitempty"`
	AllowDirty  bool     `json:"allow_dirty,omitempty" yaml:"allow_dirty,omitempty"`
	Git         string   `json:"git,omitempty" yaml:"git,omitempty"`
	Directory   string   `json:"directory,omitempty" yaml:"directory,omitempty"`
	Snapshot    string   `json:"snapshot,omitempty" yaml:"snapshot,omitempty"`
}

func profileInputName(name string) bool {
	return profileRelative(name) && name != ".myce-source.tar" && name != ".myce-inputs" && !strings.HasPrefix(name, ".myce-inputs/")
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
	if modes != 1 || source.Paths != nil && (len(source.Paths) == 0 || source.Git == "") {
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
	if source.Git != "" && len(source.Paths) > 0 {
		var entries []gitProfileEntry
		entries, err = scopedGitEntries(ctx, root, source)
		if err == nil {
			err = archiveGitEntries(ctx, root, entries, write, link)
		}
	} else if source.Directory != "" {
		err = walkProfileDirectory(ctx, root, source.Directory, func(name, path string, info os.FileInfo) error {
			if source.AdapterFile != "" && filepath.Clean(path) == filepath.Clean(filepath.Join(root, source.AdapterFile)) {
				return nil
			}
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
				if source.AdapterFile != "" && strings.TrimPrefix(h.Name, "./") == source.AdapterFile {
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
				_, err = io.Copy(io.Discard, input)
			}
			input.Close()
		}
		if cmd != nil && cmd.Process != nil {
			err = errors.Join(err, cmd.Wait())
		}
	}
	if err == nil {
		err = checkProfileLinks(names, links)
	}
	err = errors.Join(err, tw.Close(), out.Sync(), out.Close())
	if err != nil {
		return ref, err
	}
	return myce.IdentifyFile(dest)
}

func checkProfileLinks(names map[string]bool, links map[string]string) error {
	return myce.ValidateSourceNames(names, links)
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
	if modes != 1 || source.Paths != nil && (len(source.Paths) == 0 || source.Git == "") {
		return source, "", errors.New("declare exactly one source")
	}
	if source.Git != "" {
		if source.Git == "HEAD" && !source.AllowDirty {
			paths := []string{"."}
			if len(source.Paths) > 0 {
				var err error
				paths, err = cleanProfilePaths(source.Paths)
				if err != nil {
					return source, "", err
				}
				for i := range paths {
					paths[i] = ":(literal)" + paths[i]
				}
			}
			command := append([]string{"-C", root, "status", "--porcelain", "--untracked-files=normal", "--"}, paths...)
			command = append(command, ":(exclude).mycelium/receipts")
			if source.AdapterFile != "" {
				command = append(command, ":(exclude,literal)"+source.AdapterFile)
			}
			dirty, err := exec.CommandContext(ctx, "git", command...).Output()
			if err != nil {
				return source, "", err
			}
			if len(dirty) != 0 {
				return source, "", errors.New("git HEAD source is dirty; commit it or explicitly allow_dirty")
			}
		}
		tree, err := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "--verify", "--end-of-options", source.Git+"^{tree}").Output()
		source.Git = strings.TrimSpace(string(tree))
		if err != nil {
			return source, "", err
		}
		if len(source.Paths) > 0 || source.AdapterFile != "" {
			source.Paths, err = cleanProfilePaths(source.Paths)
			if err != nil {
				return source, "", err
			}
			entries, err := scopedGitEntries(ctx, root, source)
			return source, "git-paths:" + profileHash(entries), err
		}
		return source, "git:" + source.Git, nil
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
		if source.AdapterFile != "" && filepath.Clean(path) == filepath.Clean(filepath.Join(root, source.AdapterFile)) {
			return nil
		}
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

// Literal repo-relative files/directories only, never Git pathspec expressions.
func cleanProfilePaths(paths []string) ([]string, error) {
	if paths == nil {
		return nil, nil
	}
	result := make([]string, len(paths))
	for i, name := range paths {
		name = strings.TrimSuffix(name, "/")
		if !profileRelative(name) {
			return nil, errors.New("source.paths requires clean relative paths")
		}
		result[i] = name
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

type gitProfileEntry struct {
	Name, Mode, Object string
}

func scopedGitEntries(ctx context.Context, root string, source profileSource) ([]gitProfileEntry, error) {
	paths, err := cleanProfilePaths(source.Paths)
	if err != nil {
		return nil, err
	}
	args := append([]string{"-C", root, "--literal-pathspecs", "ls-tree", "-r", "-z", "--full-tree", source.Git, "--"}, paths...)
	data, err := exec.CommandContext(ctx, "git", args...).Output()
	if err != nil {
		return nil, err
	}
	var entries []gitProfileEntry
	for _, row := range bytes.Split(data, []byte{0}) {
		if len(row) == 0 {
			continue
		}
		header, name, ok := strings.Cut(string(row), "\t")
		fields := strings.Fields(header)
		if name == source.AdapterFile {
			continue
		}
		if !ok || len(fields) != 3 || fields[1] != "blob" || (fields[0] != "100644" && fields[0] != "100755" && !(source.AdapterFile != "" && fields[0] == "120000")) || !profileInputName(name) {
			return nil, errors.New("source.paths requires regular files")
		}
		entries = append(entries, gitProfileEntry{name, fields[0], fields[2]})
	}
	for _, path := range paths {
		found := false
		for _, entry := range entries {
			if entry.Name == path || strings.HasPrefix(entry.Name, path+"/") {
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("source path missing from committed tree")
		}
	}
	return entries, nil
}

// Read selected Git blobs directly: archive attributes outside source.paths
// cannot alter selected bytes, and export-subst cannot inject a whole-tree ID.
func archiveGitEntries(ctx context.Context, root string, entries []gitProfileEntry, write func(string, int64, int64, io.Reader) error, links ...func(string, string) error) error {
	cmd := exec.CommandContext(ctx, "git", "-C", root, "cat-file", "--batch")
	in, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		in.Close()
		return err
	}
	if err = cmd.Start(); err != nil {
		in.Close()
		return err
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	defer in.Close()
	defer out.Close()
	reader := bufio.NewReader(out)
	for _, entry := range entries {
		if _, err = fmt.Fprintln(in, entry.Object); err != nil {
			return err
		}
		header, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(header)
		if len(fields) != 3 || fields[0] != entry.Object || fields[1] != "blob" {
			return errors.New("unexpected Git blob")
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil || size < 0 {
			return errors.New("invalid Git blob size")
		}
		mode, _ := strconv.ParseInt(entry.Mode, 8, 64)
		if entry.Mode == "120000" && len(links) > 0 {
			b, e := io.ReadAll(io.LimitReader(reader, size))
			if e != nil {
				return e
			}
			err = links[0](entry.Name, string(b))
		} else {
			err = write(entry.Name, mode, size, reader)
		}
		if err != nil {
			return err
		}
		if newline, err := reader.ReadByte(); err != nil || newline != '\n' {
			return errors.New("invalid Git blob boundary")
		}
	}
	in.Close()
	return cmd.Wait()
}
