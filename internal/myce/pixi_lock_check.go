package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// Workers prepare Pixi recipes with `pixi install --locked`, which refuses a
// lock that the workers' Pixi considers out of date with its manifest (for
// example a lock written by an older Pixi with `__unix=0` where 0.81 writes
// `__unix=0=0`). The queue runs the same up-to-date check when it reads the
// lock, so the consumer gets the refusal at submission or resolve instead of
// after placement. `shell-hook --locked --no-install` evaluates the identical
// satisfiability check without installing or solving.

// PixiLockError is a lock the workers' Pixi refuses as out of date.
type PixiLockError struct {
	Environment string
	Reasons     []string
}

func (e *PixiLockError) Error() string {
	reasons := "pixi reported: lock file not up-to-date with the workspace"
	if len(e.Reasons) > 0 {
		reasons = strings.Join(e.Reasons, "; ")
	}
	return fmt.Sprintf("pixi.lock is not up to date with pixi.toml for the workers' Pixi %s (environment %s): %s. Fix: run `pixi lock` with Pixi >= %s and resubmit", workerPixiVersion(), e.Environment, reasons, workerPixiVersion())
}

func workerPixiVersion() string {
	releases, err := PinnedEnvironmentTools(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		releases, _ = PinnedEnvironmentTools("linux", "amd64")
	}
	for _, r := range releases {
		if r.Name == "pixi" {
			return r.Version
		}
	}
	return ""
}

// pixiLockCheckTimeout bounds a hung tool on the submission path. The check
// is local and normally takes milliseconds; on timeout the worker still checks.
const pixiLockCheckTimeout = 30 * time.Second

// Results are cached by input digest so fan-out submissions of one lock run
// one subprocess. The bound only caps memory; a full cache is reset.
const pixiLockCacheEntries = 256

var pixiLockResults = struct {
	sync.Mutex
	m map[string]error
}{m: map[string]error{}}

// workerPixi finds the exact pinned Pixi the workers bootstrap: the managed
// tool tree first, then PATH. It never downloads on the submission path.
func workerPixi(ctx context.Context) string {
	want := "pixi " + workerPixiVersion()
	var candidates []string
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".local", "share", "mycelium", "tools", "pixi-"+workerPixiVersion()+"-"+runtime.GOOS+"-"+runtime.GOARCH, "pixi"))
	}
	if p, err := exec.LookPath("pixi"); err == nil {
		candidates = append(candidates, p)
	}
	for _, candidate := range candidates {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		out, err := exec.CommandContext(ctx, candidate, "--version").Output()
		cancel()
		if err == nil && strings.TrimSpace(string(out)) == want {
			return candidate
		}
	}
	return ""
}

// checkPixiLock runs the workers' up-to-date check on frozen inputs. files
// maps relative names to bytes; manifest is the pixi.toml name within files.
// It returns *PixiLockError only for Pixi's out-of-date verdict. An absent
// pinned Pixi, a timeout or any other tool failure skips the check.
func checkPixiLock(ctx context.Context, files map[string][]byte, manifest, environment string) error {
	if environment == "" {
		environment = "default"
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for name := range files {
			if !yield(name) {
				return
			}
		}
	})
	digest := sha256.New()
	_ = json.NewEncoder(digest).Encode([]any{manifest, environment, names})
	for _, name := range names {
		sum := sha256.Sum256(files[name])
		digest.Write(sum[:])
	}
	key := hex.EncodeToString(digest.Sum(nil))
	pixiLockResults.Lock()
	result, cached := pixiLockResults.m[key]
	pixiLockResults.Unlock()
	if cached {
		return result
	}
	result, conclusive := runPixiLockCheck(ctx, files, manifest, environment)
	if conclusive {
		pixiLockResults.Lock()
		if len(pixiLockResults.m) >= pixiLockCacheEntries {
			pixiLockResults.m = map[string]error{}
		}
		pixiLockResults.m[key] = result
		pixiLockResults.Unlock()
	}
	return result
}

func runPixiLockCheck(ctx context.Context, files map[string][]byte, manifest, environment string) (error, bool) {
	pixi := workerPixi(ctx)
	if pixi == "" {
		return nil, false
	}
	dir, err := os.MkdirTemp("", "myce-pixi-check-")
	if err != nil {
		return nil, false
	}
	defer os.RemoveAll(dir)
	for name, data := range files {
		if !safeRelative(name) {
			return nil, false
		}
		target := filepath.Join(dir, "workspace", filepath.FromSlash(name))
		if os.MkdirAll(filepath.Dir(target), 0700) != nil || os.WriteFile(target, data, 0600) != nil {
			return nil, false
		}
	}
	ctx, cancel := context.WithTimeout(ctx, pixiLockCheckTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, pixi, "shell-hook", "--json", "--locked", "--no-install", "--environment", environment, "--no-config", "--manifest-path", filepath.Join(dir, "workspace", filepath.FromSlash(manifest)), "-v")
	cmd.Dir = dir
	// Hermetic: no queue credentials, private caches or user configuration.
	cmd.Env = []string{"HOME=" + dir, "PATH=" + os.Getenv("PATH"), "PIXI_CACHE_DIR=" + filepath.Join(dir, "cache"), "PIXI_NO_PROGRESS=true", "NO_COLOR=1", "TMPDIR=" + dir}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()
	if err == nil {
		return nil, true
	}
	text := out.String()
	if ctx.Err() != nil || !strings.Contains(text, "lock file not up-to-date") {
		return nil, false
	}
	lockErr := &PixiLockError{Environment: environment}
	for _, line := range strings.Split(text, "\n") {
		_, reason, ok := strings.Cut(line, "lock_file::outdated: ")
		if !ok {
			continue
		}
		// Temporary workspace paths are not the consumer's paths.
		reason = strings.ReplaceAll(strings.TrimSpace(reason), filepath.Join(dir, "workspace")+string(filepath.Separator), "")
		if len(lockErr.Reasons) < 8 && len(reason) <= 400 {
			lockErr.Reasons = append(lockErr.Reasons, reason)
		}
	}
	return lockErr, true
}

// checkInlinePixiLock checks an inline recipe's frozen pixi.lock/pixi.toml.
func checkInlinePixiLock(ctx context.Context, r EnvironmentRecipe) error {
	if r.Backend != "pixi" || len(r.Files["pixi.lock"]) == 0 {
		return nil
	}
	return checkPixiLock(ctx, map[string][]byte{"pixi.lock": r.Files["pixi.lock"], "pixi.toml": r.Files["pixi.toml"]}, "pixi.toml", r.Environment)
}

// checkExtensionPixiLocks checks bundled Pixi recipes, with the full bundled
// folder so manifest-relative references resolve as on the worker.
func checkExtensionPixiLocks(ctx context.Context, e Extension) error {
	for _, v := range e.Variants {
		for _, name := range slices.Sorted(func(yield func(string) bool) {
			for name := range v.Environments {
				if !yield(name) {
					return
				}
			}
		}) {
			r := v.Environments[name]
			if r.Backend != "pixi" {
				continue
			}
			files := map[string][]byte{}
			for file, data := range r.Files {
				if rel, ok := strings.CutPrefix(file, "extension/"); ok {
					files[rel] = data
				}
			}
			manifest := path.Join(path.Dir(r.LockFile), "pixi.toml")
			if len(files[manifest]) == 0 {
				continue
			}
			if err := checkPixiLock(ctx, files, manifest, r.Environment); err != nil {
				return fmt.Errorf("environment %s: %w", name, err)
			}
		}
	}
	return nil
}

func isPixiLockError(err error) bool {
	var lockErr *PixiLockError
	return errors.As(err, &lockErr)
}
