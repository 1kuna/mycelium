package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// npm environments prepare node_modules once per frozen package-lock.json and
// package.json (plus an optional pixi.lock/pixi.toml Node layer) and reuse it.
// npm owns the solve; workers only run `npm ci`.

// npmNodeLayer is the optional Pixi workspace beside package-lock.json that
// provides Node. Both files or neither.
var npmNodeLayer = []string{"pixi.lock", "pixi.toml"}

// npmToolCaches are tools' standard download-cache variables. Post-install
// downloads (headless browsers) land in the prepared prefix and jobs find them.
var npmToolCaches = map[string]string{
	"PUPPETEER_CACHE_DIR":      "puppeteer",
	"PLAYWRIGHT_BROWSERS_PATH": "ms-playwright",
	"CYPRESS_CACHE_FOLDER":     "Cypress",
}

// NPMLockError is a frozen package-lock.json that npm ci would refuse.
type NPMLockError struct{ Reason string }

func (e *NPMLockError) Error() string { return "package-lock.json: " + e.Reason }

// readNPMNodeLayer adds the optional Pixi Node layer beside an npm lock.
func readNPMNodeLayer(files map[string][]byte, resolved, authored, side, base string) error {
	found := map[string][]byte{}
	var missing string
	for _, n := range npmNodeLayer {
		b, e := os.ReadFile(filepath.Join(filepath.Dir(resolved), n))
		if errors.Is(e, os.ErrNotExist) {
			missing = n
			continue
		}
		if e == nil && len(b) > maxEnvironmentInput {
			e = errEnvironmentInputTooLarge
		}
		if e != nil {
			return PathReadError("environment.lock_file", side, authored, base, n, e)
		}
		found[n] = b
	}
	if len(found) == 0 {
		return nil
	}
	if missing != "" {
		return fmt.Errorf("npm Node layer needs both pixi.toml and pixi.lock: %w", PathReadError("environment.lock_file", side, authored, base, missing, os.ErrNotExist))
	}
	maps.Copy(files, found)
	return nil
}

// uploadedNPMNodeLayer counts the optional Pixi pair among uploaded files.
func uploadedNPMNodeLayer(files, uploaded map[string][]byte) (int, error) {
	n := 0
	for _, name := range npmNodeLayer {
		if b, ok := uploaded[name]; ok {
			if len(b) > maxEnvironmentInput {
				return 0, fmt.Errorf("uploaded %s: %w", name, errEnvironmentInputTooLarge)
			}
			files[name] = b
			n++
		}
	}
	if n == 1 {
		return 0, errors.New("npm Node layer needs both pixi.toml and pixi.lock")
	}
	return n, nil
}

// checkNPMLock applies npm ci's own refusals at resolve time: a parseable lock
// whose root entry matches package.json's dependency declarations.
func checkNPMLock(files map[string][]byte) error {
	var lock struct {
		LockfileVersion int                        `json:"lockfileVersion"`
		Packages        map[string]json.RawMessage `json:"packages"`
	}
	if err := json.Unmarshal(files["package-lock.json"], &lock); err != nil {
		return &NPMLockError{Reason: "not valid JSON: " + err.Error()}
	}
	if lock.LockfileVersion < 1 || lock.LockfileVersion > 3 {
		return &NPMLockError{Reason: fmt.Sprintf("unsupported lockfileVersion %d (npm writes 1-3)", lock.LockfileVersion)}
	}
	var manifest map[string]json.RawMessage
	if err := json.Unmarshal(files["package.json"], &manifest); err != nil {
		return &NPMLockError{Reason: "package.json is not valid JSON: " + err.Error()}
	}
	if lock.LockfileVersion == 1 {
		return nil // v1 has no root entry; npm ci checks it on the worker.
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(lock.Packages[""], &root); err != nil || root == nil {
		return &NPMLockError{Reason: `missing the root "packages[""]" entry`}
	}
	for _, field := range []string{"dependencies", "devDependencies", "optionalDependencies", "peerDependencies"} {
		var want, got map[string]string
		_ = json.Unmarshal(manifest[field], &want)
		_ = json.Unmarshal(root[field], &got)
		if !maps.Equal(want, got) {
			return &NPMLockError{Reason: field + " is out of sync with package.json; run `npm install` and commit package-lock.json"}
		}
	}
	return nil
}

// npmBuildRecord is the host Node identity of a prepared tree without a Pixi
// layer. Pixi-layered trees pin Node through pixi.lock in the recipe key.
type npmBuildRecord struct {
	Node string `json:"node"`
	OS   string `json:"os"`
	Arch string `json:"arch"`
	Host bool   `json:"host_node"`
}

// nodeVersion asks the node on env's PATH (Pixi's or the worker's). It is
// an instant probe, so it runs outside build custody; c.Run is the test seam.
func (c *EnvironmentCache) nodeVersion(ctx context.Context, env []string, dir string) (string, error) {
	var out bytes.Buffer
	var err error
	if c.Run != nil {
		err = c.Run(ctx, []string{"node", "--version"}, dir, env, &out, io.Discard)
	} else {
		cmd := exec.CommandContext(ctx, "sh", "-c", "exec node --version")
		cmd.Dir, cmd.Env, cmd.Stdout = dir, env, &out
		err = cmd.Run()
	}
	if err != nil {
		return "", fmt.Errorf("node --version: %w (install Node on this worker or add pixi.toml and pixi.lock providing nodejs beside package-lock.json)", err)
	}
	return strings.TrimSpace(out.String()), nil
}

func (c *EnvironmentCache) installNPM(ctx context.Context, r EnvironmentRecipe, dir string, env []string, out io.Writer, run func(context.Context, []string, string, []string, io.Writer, io.Writer) error) error {
	layered := len(r.Files["pixi.lock"]) > 0
	if layered {
		node := r
		node.Environment = ""
		if err := c.installPixi(ctx, node, dir, env, out, run); err != nil {
			return fmt.Errorf("npm Node layer: %w", err)
		}
	}
	build := map[string]string{}
	for _, entry := range env {
		k, v, _ := strings.Cut(entry, "=")
		build[k] = v
	}
	activation := environmentActivation(dir)
	if activation == nil {
		activation = map[string]string{"PATH": build["PATH"]}
	}
	maps.Copy(build, activation)
	for k, sub := range npmToolCaches {
		activation[k] = filepath.Join(dir, "tool-cache", sub)
		build[k] = activation[k]
	}
	build["npm_config_update_notifier"] = "false"
	if err := os.MkdirAll(c.downloadCache("npm"), 0700); err != nil {
		return err
	}
	list := environmentList(build)
	if err := run(ctx, []string{"npm", "ci", "--cache", c.downloadCache("npm"), "--prefer-offline", "--no-audit", "--no-fund"}, dir, list, out, out); err != nil {
		return err
	}
	node, err := c.nodeVersion(ctx, list, dir)
	if err != nil {
		return err
	}
	if err := saveWorkerJSON(dir, "npm.json", npmBuildRecord{Node: node, OS: runtime.GOOS, Arch: runtime.GOARCH, Host: !layered}); err != nil {
		return err
	}
	modules := filepath.Join(dir, "node_modules")
	for _, p := range []string{modules, filepath.Join(dir, "tool-cache")} {
		if err := os.MkdirAll(p, 0700); err != nil {
			return err
		}
	}
	activation["PATH"] = filepath.Join(modules, ".bin") + string(os.PathListSeparator) + activation["PATH"]
	activation["NODE_PATH"] = modules
	for k := range activation {
		if reservedActivation(k) {
			delete(activation, k)
		}
	}
	if err := saveWorkerJSON(dir, "activation.json", activation); err != nil {
		return err
	}
	// Jobs read the prepared tree; they never modify it.
	return errors.Join(readOnlyTree(modules), readOnlyTree(filepath.Join(dir, "tool-cache")))
}

// npmStale reports a host-Node tree prepared by another Node or platform.
func (c *EnvironmentCache) npmStale(ctx context.Context, key string) (bool, string) {
	var record npmBuildRecord
	b, err := os.ReadFile(filepath.Join(c.Root, key, "npm.json"))
	if err != nil || json.Unmarshal(b, &record) != nil || !record.Host {
		return false, ""
	}
	env := environmentList(buildEnvironmentFrom(os.Environ()))
	node, err := c.nodeVersion(ctx, env, c.Root)
	if err != nil {
		return false, ""
	}
	current := npmBuildRecord{Node: node, OS: runtime.GOOS, Arch: runtime.GOARCH, Host: true}
	if current == record {
		return false, ""
	}
	return true, fmt.Sprintf("prepared with node %s on %s/%s; this worker has node %s on %s/%s", record.Node, record.OS, record.Arch, current.Node, current.OS, current.Arch)
}

// retireStaleNPM retires a ready host-Node tree whose Node changed, so the
// normal build path prepares it again. An in-use tree is left for its users.
func (c *EnvironmentCache) retireStaleNPM(ctx context.Context, r EnvironmentRecipe) error {
	if r.Backend != "npm" || !c.ready(r.Key) {
		return nil
	}
	stale, reason := c.npmStale(ctx, r.Key)
	if !stale {
		return nil
	}
	gate, err := c.lock(ctx, "admission", syscall.LOCK_EX)
	if err != nil {
		return err
	}
	defer unlockEnvironment(gate)
	use, err := c.tryLock(r.Key + ".use")
	if err != nil {
		return err
	}
	if use == nil {
		return fmt.Errorf("npm environment %s: %s; another attempt is using it, retry later", r.Key, reason)
	}
	defer unlockEnvironment(use)
	build, err := c.tryLock(r.Key + ".build")
	if err != nil || build == nil {
		return errors.Join(err, fmt.Errorf("npm environment %s is being prepared, retry later", r.Key))
	}
	defer unlockEnvironment(build)
	if err := c.trash(filepath.Join(c.Root, r.Key)); err != nil {
		return err
	}
	if f, err := os.OpenFile(filepath.Join(c.Root, r.Key+".build.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600); err == nil {
		fmt.Fprintf(f, "npm environment retired: %s; preparing again\n", reason)
		_ = f.Close()
	}
	return nil
}

// readOnlyTree removes write bits from every file and directory under root.
// Symlinks are skipped; their targets are inside the tree or outside custody.
func readOnlyTree(root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.Type()&fs.ModeSymlink != 0 {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.Chmod(p, info.Mode().Perm()&^0222)
	})
}

// removeEnvironmentTree removes a prefix, restoring owner write bits on
// directories a read-only npm tree carries so their entries can be unlinked.
func removeEnvironmentTree(root string) error {
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			if info, e := d.Info(); e == nil && info.Mode().Perm()&0200 == 0 {
				_ = os.Chmod(p, info.Mode().Perm()|0700)
			}
		}
		return nil
	})
	return os.RemoveAll(root)
}

// linkNodeModules gives a job's workdir the prepared tree, because Node's ESM
// resolution ignores NODE_PATH. A job's own node_modules is left alone.
func linkNodeModules(workdir, root string) error {
	modules := filepath.Join(root, "node_modules")
	if workdir == "" {
		return nil
	}
	if _, err := os.Stat(filepath.Join(root, "npm.json")); err != nil {
		return nil
	}
	link := filepath.Join(workdir, "node_modules")
	if _, err := os.Lstat(link); err == nil || !errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return os.Symlink(modules, link)
}
