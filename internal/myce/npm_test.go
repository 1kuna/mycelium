package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func writeNPMProject(t *testing.T, dir string, files map[string]string) string {
	t.Helper()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(dir, "package-lock.json")
}

// Resolve gives npm ci's own refusals before any worker prepares the tree.
func TestEnvironmentInlineNPMLockRefusals(t *testing.T) {
	const lock = `{"lockfileVersion":3,"packages":{"":{"dependencies":{"a":"^1.0.0"}}}}`
	const manifest = `{"dependencies":{"a":"^1.0.0"}}`
	for name, tc := range map[string]struct {
		files     map[string]string
		lockName  string
		field     string
		companion string
		problem   string
	}{
		"missing lock":         {files: map[string]string{"package.json": manifest}, field: "environment.lock_file"},
		"missing package.json": {files: map[string]string{"package-lock.json": lock}, field: "environment.lock_file", companion: "package.json"},
		"wrong lock name":      {files: map[string]string{"package.json": manifest}, lockName: "package.json", field: "environment", problem: "npm requires package-lock.json"},
		"invalid JSON":         {files: map[string]string{"package-lock.json": "{", "package.json": manifest}, field: "environment", problem: "not valid JSON"},
		"old format":           {files: map[string]string{"package-lock.json": `{"lockfileVersion":9}`, "package.json": manifest}, field: "environment", problem: "unsupported lockfileVersion 9"},
		"out of sync":          {files: map[string]string{"package-lock.json": lock, "package.json": `{"dependencies":{"a":"^2.0.0"}}`}, field: "environment", problem: "dependencies is out of sync"},
		"half node layer":      {files: map[string]string{"package-lock.json": lock, "package.json": manifest, "pixi.toml": "[workspace]\n"}, field: "environment.lock_file", companion: "pixi.lock"},
	} {
		t.Run(name, func(t *testing.T) {
			q, _ := fixture(t)
			dir := t.TempDir()
			writeNPMProject(t, dir, tc.files)
			lockName := tc.lockName
			if lockName == "" {
				lockName = "package-lock.json"
			}
			s := cpuResolveSpec()
			s.Environment = Environment{Backend: "npm", LockFile: filepath.Join(dir, lockName), Platforms: []string{"cpu"}}
			c := Client{URL: testServerForEnvironments(t, q), Token: "test"}
			var report ResolveReport
			if err := c.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &report); err != nil {
				t.Fatal(err)
			}
			f := resolveFinding(report, tc.field)
			if f == nil || !strings.Contains(f.Problem, tc.problem) {
				t.Fatalf("resolve findings: %+v", report.Findings)
			}
			if tc.companion != "" && (f.PathDiagnostic == nil || f.PathDiagnostic.Companion != tc.companion) {
				t.Fatalf("companion diagnostic: %+v", f.PathDiagnostic)
			}
			if strings.Contains(tc.problem, "sync") && !strings.Contains(f.Fix, "npm install") {
				t.Fatalf("fix: %q", f.Fix)
			}
			_, err := c.Submit(context.Background(), s)
			var apiErr *HTTPError
			if !errors.As(err, &apiErr) {
				t.Fatalf("expected API refusal: %v", err)
			}
		})
	}
}

// Uploaded inputs (myce run --profile) accept the optional Pixi pair whole.
func TestEnvironmentInlineNPMUploadedNodeLayer(t *testing.T) {
	base := map[string][]byte{"package-lock.json": []byte(`{"lockfileVersion":3,"packages":{"":{}}}`), "package.json": []byte(`{}`)}
	layered := map[string][]byte{"pixi.lock": []byte("version: 6\n"), "pixi.toml": []byte("[workspace]\n")}
	for k, v := range base {
		layered[k] = v
	}
	half := map[string][]byte{"pixi.lock": []byte("version: 6\n")}
	for k, v := range base {
		half[k] = v
	}
	for name, tc := range map[string]struct {
		files map[string][]byte
		ok    bool
	}{"plain": {base, true}, "layered": {layered, true}, "half": {half, false}} {
		r, err := snapshotEnvironmentFiles(EnvironmentRecipe{Backend: "npm", LockFile: "package-lock.json"}, "cpu", "queue-working-directory", "", tc.files)
		if (err == nil) != tc.ok || tc.ok && len(r.Files) != len(tc.files) {
			t.Fatalf("%s: %v %v", name, len(r.Files), err)
		}
	}
}

// One lock digest prepares once per worker: the second job runs no install and
// downloads nothing, reads the post-install "browser" from the read-only tree,
// and imports the prepared package from ESM. A changed lock prepares again.
func TestNPMEnvironmentPreparedOnceWithDev(t *testing.T) {
	for _, tool := range []string{"node", "npm"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not installed", tool)
		}
	}
	state := t.TempDir()
	project := t.TempDir()
	counter := filepath.Join(t.TempDir(), "installs")
	t.Cleanup(func() { _ = removeEnvironmentTree(state) })
	write := func(version string) string {
		postinstall := strings.Join([]string{
			"echo run >> " + counter,
			`mkdir -p "$PUPPETEER_CACHE_DIR" node_modules/fakepkg`,
			`echo "browser-$$" > "$PUPPETEER_CACHE_DIR/browser"`,
			`echo 'export default 42;' > node_modules/fakepkg/index.mjs`,
			`echo '{"name":"fakepkg","exports":"./index.mjs"}' > node_modules/fakepkg/package.json`,
		}, " && ")
		pkg, _ := json.Marshal(map[string]any{"name": "npm-env-test", "version": version, "scripts": map[string]string{"postinstall": postinstall}})
		lock, _ := json.Marshal(map[string]any{"name": "npm-env-test", "version": version, "lockfileVersion": 3, "requires": true,
			"packages": map[string]any{"": map[string]any{"name": "npm-env-test", "version": version, "hasInstallScript": true}}})
		return writeNPMProject(t, project, map[string]string{"package.json": string(pkg), "package-lock.json": string(lock)})
	}
	d, err := StartDev(DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(state, "queue.db"), Checkpoints: filepath.Join(state, "checkpoints"), FileStore: filepath.Join(state, "files"), Token: "test", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	run := func(lock string) string {
		t.Helper()
		s := cpuSpec(1, .1)
		s.Environment = Environment{Backend: "npm", LockFile: lock, Platforms: []string{"cpu"}}
		s.Run.Command = []string{"sh", "-c", strings.Join([]string{
			`cat "$PUPPETEER_CACHE_DIR/browser"`,
			`echo 'import v from "fakepkg"; console.log("esm-" + v)' > probe.mjs`,
			`node probe.mjs`,
			`(cd / && node -e 'require.resolve("fakepkg")')`, // NODE_PATH, outside the workdir link
			`if touch "$MYCE_ENVIRONMENT_DIR/node_modules/write-probe" 2>/dev/null; then echo writable; else echo read-only; fi`,
		}, " && ")}
		j, err := d.Client().Submit(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		got, err := d.Client().Wait(ctx, j.ID)
		logs, _ := d.Queue.Logs(j.ID, 0)
		if err != nil || got.State != "succeeded" {
			t.Fatalf("npm job: %+v %v\n%v", got, err, logs)
		}
		out := fmt.Sprint(logs)
		if !strings.Contains(out, "esm-42") || !strings.Contains(out, "read-only") || strings.Contains(out, "writable") {
			t.Fatalf("job output: %s", out)
		}
		// The first job's log also carries the build log's postinstall echo.
		return regexp.MustCompile(`browser-[0-9]+`).FindString(out)
	}
	installs := func() int {
		b, _ := os.ReadFile(counter)
		return strings.Count(string(b), "run")
	}
	lock := write("1.0.0")
	first := run(lock)
	if installs() != 1 {
		t.Fatalf("first job installs = %d", installs())
	}
	if second := run(lock); second != first || installs() != 1 {
		t.Fatalf("second job reinstalled: %s vs %s, installs %d", second, first, installs())
	}
	if third := run(write("1.0.1")); third == first || installs() != 2 {
		t.Fatalf("changed lock reused tree: %s, installs %d", third, installs())
	}
}

// Host Node is part of a tree's identity: a changed node retires and prepares
// it again (the read-only retired tree still purges). A Pixi layer pins Node
// through pixi.lock in the key, so its tree is reused.
func TestNPMEnvironmentNodeIdentity(t *testing.T) {
	for _, layered := range []bool{false, true} {
		t.Run(fmt.Sprintf("pixi=%v", layered), func(t *testing.T) {
			node, installs := "v20.0.0", 0
			clock := &fakeClock{now: time.Now()}
			root := t.TempDir()
			t.Cleanup(func() { _ = removeEnvironmentTree(root) })
			c := &EnvironmentCache{Root: root, Limit: 1 << 30, Clock: clock}
			c.Run = func(_ context.Context, argv []string, dir string, env []string, stdout, _ io.Writer) error {
				vars := map[string]string{}
				for _, e := range env {
					k, v, _ := strings.Cut(e, "=")
					vars[k] = v
				}
				switch {
				case argv[0] == "pixi" && argv[1] == "shell-hook":
					_, err := fmt.Fprint(stdout, `{"environment_variables":{"PATH":"/pixi/bin:/usr/bin","CONDA_PREFIX":"/pixi"}}`)
					return err
				case argv[0] == "pixi":
					return nil
				case argv[0] == "node":
					_, err := fmt.Fprintln(stdout, node)
					return err
				case argv[0] == "npm":
					installs++
					if argv[1] != "ci" || argv[3] != c.downloadCache("npm") || vars["PUPPETEER_CACHE_DIR"] != filepath.Join(dir, "tool-cache", "puppeteer") {
						return fmt.Errorf("npm invocation: %v", argv)
					}
					if layered != strings.HasPrefix(vars["PATH"], "/pixi/bin") {
						return fmt.Errorf("npm PATH: %s", vars["PATH"])
					}
					if err := os.MkdirAll(filepath.Join(dir, "node_modules", "pkg"), 0700); err != nil {
						return err
					}
					return os.WriteFile(filepath.Join(dir, "node_modules", "pkg", "index.js"), nil, 0600)
				}
				return fmt.Errorf("unexpected command %v", argv)
			}
			files := map[string][]byte{"package-lock.json": []byte(`{"lockfileVersion":3,"packages":{"":{}}}`), "package.json": []byte(`{}`)}
			if layered {
				files["pixi.lock"], files["pixi.toml"] = []byte("version: 6\n"), []byte("[workspace]\n")
			}
			r, err := snapshotEnvironmentFiles(EnvironmentRecipe{Backend: "npm", LockFile: "package-lock.json"}, "cpu", "queue-working-directory", "", files)
			if err != nil {
				t.Fatal(err)
			}
			acquire := func() string {
				t.Helper()
				path, release, err := c.Acquire(context.Background(), r)
				if err != nil {
					t.Fatal(err)
				}
				release()
				return path
			}
			path := acquire()
			activation := environmentActivation(path)
			modules := filepath.Join(path, "node_modules")
			if !strings.HasPrefix(activation["PATH"], filepath.Join(modules, ".bin")+string(os.PathListSeparator)) || activation["NODE_PATH"] != modules || activation["PLAYWRIGHT_BROWSERS_PATH"] == "" {
				t.Fatalf("activation: %v", activation)
			}
			for _, p := range []string{modules, filepath.Join(modules, "pkg", "index.js"), filepath.Join(path, "tool-cache")} {
				if st, err := os.Stat(p); err != nil || st.Mode().Perm()&0222 != 0 {
					t.Fatalf("%s writable: %v %v", p, st.Mode(), err)
				}
			}
			acquire()
			node = "v22.0.0"
			acquire()
			want := 2
			if layered {
				want = 1
			}
			if installs != want {
				t.Fatalf("installs = %d, want %d", installs, want)
			}
			clock.Advance(25 * time.Hour)
			c.purgeReleased()
			if trash, err := os.ReadDir(filepath.Join(c.Root, ".Trash")); err != nil || len(trash) != 0 {
				t.Fatalf("retired read-only tree not purged: %v %v", trash, err)
			}
		})
	}
}
