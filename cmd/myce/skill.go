package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/mod/semver"
	"mycelium/internal/myce"
	"mycelium/skills"
)

// The consumer skill ships inside the binary, so the guide an agent loads is
// the one for the release it actually runs. A marker records what myce wrote;
// only an unedited copy myce wrote is ever replaced without --replace.
const skillMarker = ".myce-skill.json"

type skillMarkerFile struct {
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}

type skillState string

const (
	skillMissing   skillState = "missing"
	skillCurrent   skillState = "current"
	skillStale     skillState = "stale"     // written by myce for another release or content
	skillModified  skillState = "modified"  // written by myce, then edited
	skillSymlink   skillState = "symlink"   // e.g. a link into a source checkout
	skillUnmanaged skillState = "unmanaged" // a copy myce did not write
)

type skillInspection struct {
	Dir       string     `json:"dir"`
	State     skillState `json:"state"`
	Installed string     `json:"installed_version,omitempty"`
	Binary    string     `json:"binary_version"`
	Target    string     `json:"symlink_target,omitempty"`
}

// embeddedSkill returns the skill's files keyed by slash path relative to the
// skill directory, and their digest.
func embeddedSkill() (map[string][]byte, string, error) {
	files := map[string][]byte{}
	err := fs.WalkDir(skills.Mycelium, "mycelium", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := skills.Mycelium.ReadFile(p)
		if err != nil {
			return err
		}
		files[strings.TrimPrefix(p, "mycelium/")] = data
		return nil
	})
	return files, skillDigest(files), err
}

func skillDigest(files map[string][]byte) string {
	h := sha256.New()
	for _, name := range slices.Sorted(maps.Keys(files)) {
		fmt.Fprintf(h, "%s\x00%d\x00", name, len(files[name]))
		h.Write(files[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func readSkillDir(dir string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if rel == skillMarker {
			return nil
		}
		data, err := os.ReadFile(p)
		files[filepath.ToSlash(rel)] = data
		return err
	})
	return files, err
}

func readSkillMarker(dir string) (skillMarkerFile, error) {
	var m skillMarkerFile
	data, err := os.ReadFile(filepath.Join(dir, skillMarker))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil || m.Version == "" || m.SHA256 == "" {
		return m, fmt.Errorf("unreadable skill marker in %s", dir)
	}
	return m, nil
}

func inspectSkill(dir, version, embeddedDigest string) (skillInspection, error) {
	r := skillInspection{Dir: dir, Binary: version}
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		r.State = skillMissing
		return r, nil
	}
	if err != nil {
		return r, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		r.State = skillSymlink
		r.Target, _ = os.Readlink(dir)
		return r, nil
	}
	m, err := readSkillMarker(dir)
	if !info.IsDir() || err != nil {
		r.State = skillUnmanaged
		return r, nil
	}
	r.Installed = m.Version
	files, err := readSkillDir(dir)
	if err != nil {
		return r, err
	}
	switch {
	case skillDigest(files) != m.SHA256:
		r.State = skillModified
	case m.Version == version && m.SHA256 == embeddedDigest:
		r.State = skillCurrent
	default:
		r.State = skillStale
	}
	return r, nil
}

// defaultSkillDirs lists the mycelium skill folder of each agent harness whose
// home exists: Claude Code (~/.claude), the shared agents folder (~/.agents)
// and Codex ($CODEX_HOME, default ~/.codex).
func defaultSkillDirs(home, codexHome string) []string {
	if codexHome == "" {
		codexHome = filepath.Join(home, ".codex")
	}
	var dirs []string
	for _, root := range []string{filepath.Join(home, ".claude"), filepath.Join(home, ".agents"), codexHome} {
		if info, err := os.Stat(root); err == nil && info.IsDir() {
			dir := filepath.Join(root, "skills", "mycelium")
			if !slices.Contains(dirs, dir) {
				dirs = append(dirs, dir)
			}
		}
	}
	return dirs
}

type skillInstaller struct {
	Version    string
	BackupRoot string // where --replace moves copies myce did not write
	Out        io.Writer
}

// writeSkill replaces dir with the embedded skill. The new copy is staged
// beside dir and swapped in by rename; a concurrent writer that wins the swap
// leaves a current copy, which counts as success.
func (s skillInstaller) writeSkill(dir string, files map[string][]byte, digest string) error {
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(parent, ".mycelium-skill-new-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for name, data := range files {
		p := filepath.Join(tmp, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			return err
		}
	}
	marker, _ := json.Marshal(skillMarkerFile{Version: s.Version, SHA256: digest})
	if err := os.WriteFile(filepath.Join(tmp, skillMarker), append(marker, '\n'), 0o644); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		return err
	}
	var old string
	if _, err := os.Lstat(dir); err == nil {
		holder, err := os.MkdirTemp(parent, ".mycelium-skill-old-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(holder)
		old = filepath.Join(holder, "mycelium")
		if err := os.Rename(dir, old); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Rename(tmp, dir); err != nil {
		if r, ierr := inspectSkill(dir, s.Version, digest); ierr == nil && r.State == skillCurrent {
			return nil
		}
		if old != "" {
			_ = os.Rename(old, dir)
		}
		return err
	}
	return nil
}

// backup moves a copy myce did not write out of the skills folder, so the
// harness cannot load two skills named mycelium.
func (s skillInstaller) backup(dir string) (string, error) {
	if err := os.MkdirAll(s.BackupRoot, 0o755); err != nil {
		return "", err
	}
	holder, err := os.MkdirTemp(s.BackupRoot, "mycelium-")
	if err != nil {
		return "", err
	}
	dest := filepath.Join(holder, filepath.Base(dir))
	if err := os.Rename(dir, dest); err != nil {
		os.Remove(holder)
		return "", fmt.Errorf("move %s aside: %w; remove it yourself and rerun", dir, err)
	}
	return dest, nil
}

// install writes the skill into each dir. Without replace, a symlink, an edited
// copy or a copy myce did not write is kept and reported; with replace, a
// symlink is removed (never its target) and a directory is moved to BackupRoot.
func (s skillInstaller) install(dirs []string, replace, dry bool) error {
	files, digest, err := embeddedSkill()
	if err != nil {
		return err
	}
	var failed []string
	for _, dir := range dirs {
		r, err := inspectSkill(dir, s.Version, digest)
		if err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", dir, err))
			continue
		}
		kept := r.State == skillSymlink || r.State == skillModified || r.State == skillUnmanaged
		switch {
		case r.State == skillCurrent:
			fmt.Fprintf(s.Out, "%s: current (%s)\n", dir, s.Version)
			continue
		case kept && !replace:
			fmt.Fprintf(s.Out, "%s: kept %s; rerun with --replace to install this release's copy\n", dir, describeSkill(r))
			continue
		case dry:
			fmt.Fprintf(s.Out, "%s: would install %s (now %s)\n", dir, s.Version, describeSkill(r))
			continue
		case r.State == skillSymlink:
			if err := os.Remove(dir); err != nil {
				failed = append(failed, fmt.Sprintf("%s: %v", dir, err))
				continue
			}
			fmt.Fprintf(s.Out, "%s: removed symlink to %s\n", dir, r.Target)
		case kept:
			moved, err := s.backup(dir)
			if err != nil {
				failed = append(failed, err.Error())
				continue
			}
			fmt.Fprintf(s.Out, "%s: moved previous %s copy to %s\n", dir, r.State, moved)
		}
		if err := s.writeSkill(dir, files, digest); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", dir, err))
			continue
		}
		fmt.Fprintf(s.Out, "%s: installed %s\n", dir, s.Version)
	}
	if len(failed) > 0 {
		return errors.New("skill install failed: " + strings.Join(failed, "; "))
	}
	return nil
}

// refresh brings every unedited copy myce wrote to this binary's release. It
// runs before release-build commands, reads only small marker files when
// nothing changed, and never fails the command it precedes.
func (s skillInstaller) refresh(dirs []string) {
	if !semver.IsValid(s.Version) {
		return // development builds never overwrite a release's guide
	}
	var files map[string][]byte
	var digest string
	for _, dir := range dirs {
		m, err := readSkillMarker(dir)
		if err != nil || m.Version == s.Version {
			continue
		}
		if files == nil {
			if files, digest, err = embeddedSkill(); err != nil {
				return
			}
		}
		if r, err := inspectSkill(dir, s.Version, digest); err != nil || r.State != skillStale {
			continue
		}
		if err := s.writeSkill(dir, files, digest); err != nil {
			fmt.Fprintf(s.Out, "myce: could not refresh agent skill %s: %v\n", dir, err)
			continue
		}
		fmt.Fprintf(s.Out, "myce: refreshed agent skill %s from %s to %s\n", dir, m.Version, s.Version)
	}
}

func describeSkill(r skillInspection) string {
	switch r.State {
	case skillSymlink:
		return "symlink to " + r.Target
	case skillStale, skillModified:
		return fmt.Sprintf("%s %s copy", r.State, r.Installed)
	}
	return string(r.State)
}

func skillBackupRoot() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "mycelium", "skill-backups")
}

func localSkillInstaller(out io.Writer) skillInstaller {
	return skillInstaller{Version: myce.BuildVersion, BackupRoot: skillBackupRoot(), Out: out}
}

func localSkillDirs() []string {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	return defaultSkillDirs(home, os.Getenv("CODEX_HOME"))
}

const skillUsage = `usage: myce skill install [--replace] [--dry-run] [DIR...]
       myce skill status [--json] [DIR...]
       myce skill show [FILE]
DIR is a skill folder such as ~/.claude/skills/mycelium. Without DIR, myce uses
the mycelium folder under each agent home that exists: ~/.claude/skills,
~/.agents/skills and $CODEX_HOME/skills (default ~/.codex/skills).`

func skillCommand(args []string, stdout io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		if len(args) == 0 {
			return &exitError{2, errors.New(skillUsage)}
		}
		fmt.Fprintln(stdout, skillUsage)
		return nil
	}
	if args[0] == "show" {
		name := "SKILL.md"
		if len(args) == 2 {
			name = args[1]
		} else if len(args) > 2 {
			return &exitError{2, errors.New(skillUsage)}
		}
		data, err := skills.Mycelium.ReadFile("mycelium/" + strings.TrimPrefix(name, "./"))
		if err != nil {
			return fmt.Errorf("no skill file %q", name)
		}
		_, err = stdout.Write(data)
		return err
	}
	if args[0] != "install" && args[0] != "status" {
		return &exitError{2, fmt.Errorf("unknown skill command %q\n%s", args[0], skillUsage)}
	}
	f := flag.NewFlagSet("skill "+args[0], flag.ContinueOnError)
	f.SetOutput(stdout)
	replace := f.Bool("replace", false, "also replace a symlink, an edited copy or a copy myce did not write (directories are moved to the user cache, symlink targets are untouched)")
	dry := f.Bool("dry-run", false, "print what would change")
	asJSON := f.Bool("json", false, "status as JSON")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	dirs := f.Args()
	for i, d := range dirs {
		abs, err := filepath.Abs(d)
		if err != nil {
			return err
		}
		dirs[i] = abs
	}
	if len(dirs) == 0 {
		dirs = localSkillDirs()
	}
	if len(dirs) == 0 {
		return errors.New("no agent home found (~/.claude, ~/.agents, ~/.codex); pass the skill folder, e.g. myce skill install ~/.claude/skills/mycelium")
	}
	s := localSkillInstaller(stdout)
	if args[0] == "install" {
		return s.install(dirs, *replace, *dry)
	}
	_, digest, err := embeddedSkill()
	if err != nil {
		return err
	}
	var all []skillInspection
	for _, dir := range dirs {
		r, err := inspectSkill(dir, s.Version, digest)
		if err != nil {
			return err
		}
		all = append(all, r)
	}
	if *asJSON {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		enc.SetIndent("", "  ")
		if err := enc.Encode(all); err != nil {
			return err
		}
		_, err := stdout.Write(b.Bytes())
		return err
	}
	for _, r := range all {
		fmt.Fprintf(stdout, "%s: %s (binary %s)\n", r.Dir, describeSkill(r), r.Binary)
	}
	return nil
}
