package myce

import (
	"archive/tar"
	"errors"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Walk each ancestry before replacing a source file. An explicit input must
// never follow an archived link, including a link to another source directory.
func removeSourceInput(root, name string) error {
	if !safeRelative(name) {
		return errors.New("invalid source input name")
	}
	for p := filepath.Dir(name); p != "."; p = filepath.Dir(p) {
		info, err := os.Lstat(filepath.Join(root, p))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("input ancestry is not a directory")
		}
	}
	p := filepath.Join(root, name)
	info, err := os.Lstat(p)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() {
		return errors.New("input overlaps a source directory")
	}
	return os.Remove(p)
}

func extractJobSource(archive, root string) error {
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	links := map[string]string{}
	seen := map[string]bool{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := h.Name
		if !safeRelative(name) || name == ".myce-source.tar" || name == ".myce-inputs" || strings.HasPrefix(name, ".myce-inputs/") || seen[name] {
			return errors.New("invalid source archive name")
		}
		seen[name] = true
		for p := filepath.Dir(name); p != "."; p = filepath.Dir(p) {
			if _, ok := links[p]; ok {
				return errors.New("source traverses a symlink")
			}
		}
		dest := filepath.Join(root, name)
		switch h.Typeflag {
		case tar.TypeReg:
			if err = os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
				return err
			}
			out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(h.Mode)&0777)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(out, tr, h.Size)
			if err = errors.Join(copyErr, out.Close()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			target := h.Linkname
			resolved := filepath.Clean(filepath.Join(filepath.Dir(name), target))
			if target == "" || filepath.IsAbs(target) || strings.ContainsAny(target, "\\\x00\r\n") || !(resolved == "." || safeRelative(resolved)) {
				return errors.New("source symlink escapes archive")
			}
			links[name] = target
		default:
			return errors.New("source archive contains unsupported file type")
		}
	}
	if err = ValidateSourceNames(seen, links); err != nil {
		return err
	}
	// Publish links after regular files, so no archive member can traverse one.
	for name, target := range links {
		dest := filepath.Join(root, name)
		if err = os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
			return err
		}
		if err = os.Symlink(target, dest); err != nil {
			return err
		}
	}
	return nil
}

// Every source uses portable, nonreserved names. Symlinks must resolve inside
// the source, with no entries beneath them, so extraction cannot write elsewhere.
func ValidateSourceNames(names map[string]bool, links map[string]string) error {
	// Sources can move from a case-sensitive submitter to macOS. Resolve the
	// same aliases there, including decomposed Unicode, before trusting a path.
	key := func(name string) string { return norm.NFD.String(cases.Fold().String(name)) }
	portableNames, portableLinks := map[string]bool{}, map[string]string{}
	for name := range names {
		k := key(name)
		if portableNames[k] || k == ".myce-source.tar" || k == ".myce-inputs" || strings.HasPrefix(k, ".myce-inputs/") {
			return errors.New("source contains filesystem aliases or reserved names")
		}
		portableNames[k] = true
	}
	for name, target := range links {
		portableLinks[key(name)] = key(target)
	}
	names, links = portableNames, portableLinks
	for name := range names {
		for dir := filepath.Dir(name); dir != "."; dir = filepath.Dir(dir) {
			if _, ok := links[dir]; ok {
				return errors.New("source entry " + name + " is beneath a symlink")
			}
		}
	}
	var resolve func(dir []string, target string, hops *int) ([]string, bool)
	resolve = func(dir []string, target string, hops *int) ([]string, bool) {
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
					// Count every expansion, including siblings. A depth-only
					// limit permits exponential work and paths the OS cannot open.
					*hops++
					if *hops > 40 {
						return nil, false
					}
					var inside bool
					if path, inside = resolve(path[:len(path)-1], next, hops); !inside {
						return nil, false
					}
				}
			}
		}
		return path, true
	}
	for name, target := range links {
		parts := strings.Split(name, "/")
		hops := 1
		if _, inside := resolve(parts[:len(parts)-1], target, &hops); !inside {
			return errors.New("source symlink " + name + " leaves the source tree")
		}
	}
	return nil
}
