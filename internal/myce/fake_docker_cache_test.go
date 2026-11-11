package myce

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestMain isolates HOME and the Go build cache before tests run, so an
// in-test `go test -c` used to compile the whole dependency graph cold (40-60 s
// per test process). Snapshot the invoking environment while it still names the
// caller's warm caches; the fake Docker build and its key use it.
var fakeDockerBuildEnv = os.Environ()

const (
	fakeDockerCacheDir  = "mycelium-fake-docker"
	fakeDockerCacheKeep = 6 // Concurrent worktrees/branches; each entry is ~60 MB.
)

// buildFakeDockerCLI places the external Docker simulator at out. It reuses a
// binary keyed by the content of every input: main-module sources, dependency
// module versions, Go version, target and build settings. The fake is always
// built without race instrumentation or vet.
func buildFakeDockerCLI(out string) ([]byte, error) {
	env := fakeDockerGoEnv(fakeDockerBuildEnv)
	key, cacheRoot, err := fakeDockerKey(env, ".")
	if err != nil {
		// The cache is an optimisation: never let it change the build result.
		return fakeDockerCompile(env, out)
	}
	dir := filepath.Join(cacheRoot, fakeDockerCacheDir)
	cached, output, err := installFakeDocker(dir, key, func(target string) ([]byte, error) { return fakeDockerCompile(env, target) })
	if err != nil {
		return output, err
	}
	if err = copyExecutable(cached, out); err != nil {
		// Pruned by another process between install and copy: build directly.
		return fakeDockerCompile(env, out)
	}
	return output, nil
}

func fakeDockerCompile(env []string, out string) ([]byte, error) {
	command := exec.Command("go", "test", "-c", "-vet=off", "-race=false", "-o", out, ".")
	command.Env = env
	return command.CombinedOutput()
}

func fakeDockerGoEnv(base []string) []string {
	env := slices.Clone(base)
	env = append(env, "GOTOOLCHAIN=local", "GOTELEMETRY=off")
	// The caller's go test already resolved every module; never reach the network.
	// An isolated child already points at a local file proxy; keep that.
	if proxy := envValue(env, "GOPROXY"); !strings.HasPrefix(proxy, "file:") {
		env = append(env, "GOPROXY=off")
	}
	return env
}

func envValue(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if value, ok := strings.CutPrefix(env[i], key+"="); ok {
			return value
		}
	}
	return ""
}

// fakeDockerKey hashes the inputs of `go test -c -race=false` for the package in
// dir and returns the caller's Go build cache, where the binaries live.
func fakeDockerKey(env []string, dir string) (string, string, error) {
	settings := exec.Command("go", "env", "-json", "GOCACHE", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOEXPERIMENT", "GOAMD64", "GOARM64", "CC", "CGO_CFLAGS", "CGO_LDFLAGS")
	settings.Env, settings.Dir = env, dir
	raw, err := settings.Output()
	if err != nil {
		return "", "", err
	}
	var goEnv map[string]string
	if err = json.Unmarshal(raw, &goEnv); err != nil {
		return "", "", err
	}
	if goEnv["GOCACHE"] == "" || goEnv["GOCACHE"] == "off" {
		return "", "", errors.New("no Go build cache")
	}
	list := exec.Command("go", "list", "-deps", "-test", "-json=ImportPath,Standard,Dir,Module,GoFiles,CgoFiles,CFiles,CXXFiles,HFiles,SFiles,SysoFiles,EmbedFiles,TestGoFiles,XTestGoFiles,TestEmbedFiles,XTestEmbedFiles", ".")
	list.Env, list.Dir = env, dir
	raw, err = list.Output()
	if err != nil {
		return "", "", err
	}
	hash := sha256.New()
	fmt.Fprintf(hash, "fake-docker v1 race=false vet=off\n")
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for name := range goEnv {
			if name != "GOCACHE" && !yield(name) {
				return
			}
		}
	}) {
		fmt.Fprintf(hash, "env %s=%q\n", name, goEnv[name])
	}
	type module struct {
		Path, Version, Dir string
		Main               bool
		Replace            *struct{ Path, Version, Dir string }
	}
	type pkg struct {
		ImportPath, Dir string
		Standard        bool
		Module          *module
		GoFiles, CgoFiles, CFiles, CXXFiles, HFiles, SFiles, SysoFiles,
		EmbedFiles, TestGoFiles, XTestGoFiles, TestEmbedFiles, XTestEmbedFiles []string
	}
	files := map[string]bool{}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var p pkg
		if err = decoder.Decode(&p); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return "", "", err
		}
		fmt.Fprintf(hash, "package %s\n", p.ImportPath)
		if p.Standard || strings.HasSuffix(p.ImportPath, ".test") {
			continue // GOVERSION covers std; the generated test main follows the sources.
		}
		// A released module version is immutable (go.sum); hash local sources.
		if m := p.Module; m != nil && !m.Main && (m.Replace == nil || m.Replace.Version != "") {
			if m.Replace != nil {
				m = &module{Path: m.Replace.Path, Version: m.Replace.Version}
			}
			fmt.Fprintf(hash, "module %s@%s\n", m.Path, m.Version)
			continue
		}
		for _, group := range [][]string{p.GoFiles, p.CgoFiles, p.CFiles, p.CXXFiles, p.HFiles, p.SFiles, p.SysoFiles, p.EmbedFiles, p.TestGoFiles, p.XTestGoFiles, p.TestEmbedFiles, p.XTestEmbedFiles} {
			for _, name := range group {
				files[filepath.Join(p.Dir, name)] = true
			}
		}
	}
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for name := range files {
			if !yield(name) {
				return
			}
		}
	}) {
		data, err := os.ReadFile(name)
		if err != nil {
			return "", "", err
		}
		sum := sha256.Sum256(data)
		fmt.Fprintf(hash, "file %s %x\n", name, sum)
	}
	return hex.EncodeToString(hash.Sum(nil)), goEnv["GOCACHE"], nil
}

// installFakeDocker returns dir/key, building it when absent. Builders write a
// private temporary directory and rename atomically, so concurrent processes
// building one key each end with a complete binary at the same path.
func installFakeDocker(dir, key string, build func(string) ([]byte, error)) (string, []byte, error) {
	cached := filepath.Join(dir, key)
	if info, err := os.Stat(cached); err == nil && info.Mode().IsRegular() {
		now := time.Now() // mtime orders pruning by last use
		_ = os.Chtimes(cached, now, now)
		return cached, nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, err
	}
	work, err := os.MkdirTemp(dir, ".build-")
	if err != nil {
		return "", nil, err
	}
	defer os.RemoveAll(work)
	output, err := build(filepath.Join(work, "fake-docker.test"))
	if err != nil {
		return "", output, err
	}
	if err = os.Rename(filepath.Join(work, "fake-docker.test"), cached); err != nil {
		return "", output, err
	}
	pruneFakeDocker(dir, key)
	return cached, output, nil
}

// pruneFakeDocker keeps the most recently used binaries. A running or copying
// process keeps its open inode; a later lookup that misses simply rebuilds.
func pruneFakeDocker(dir, keep string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type entry struct {
		name string
		used int64
	}
	var binaries []entry
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() || len(e.Name()) != sha256.Size*2 || e.Name() == keep {
			continue
		}
		binaries = append(binaries, entry{e.Name(), info.ModTime().UnixNano()})
	}
	slices.SortFunc(binaries, func(a, b entry) int { return cmp.Compare(b.used, a.used) })
	for i, b := range binaries {
		if i >= fakeDockerCacheKeep-1 {
			_ = os.Remove(filepath.Join(dir, b.name))
		}
	}
}

func copyExecutable(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err = io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// A change to any source input must select a different binary; unchanged
// inputs must reuse it.
func TestFakeDockerKeyTracksInputs(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.test/fake\n\ngo 1.22\n")
	write("fake.go", "package fake\n")
	write("fake_test.go", "package fake\n")
	env := fakeDockerGoEnv(fakeDockerBuildEnv)
	key := func(env []string) string {
		t.Helper()
		k, cache, err := fakeDockerKey(env, dir)
		if err != nil || cache == "" {
			t.Fatalf("key: %q %q %v", k, cache, err)
		}
		return k
	}
	first := key(env)
	if again := key(env); again != first {
		t.Fatalf("unchanged inputs changed key: %s != %s", again, first)
	}
	write("fake_test.go", "package fake\n\n// changed\n")
	changed := key(env)
	if changed == first {
		t.Fatal("changed test source reused the cached binary")
	}
	other := "amd64"
	if runtime.GOARCH == "amd64" {
		other = "arm64"
	}
	if key(append(slices.Clone(env), "GOARCH="+other)) == changed {
		t.Fatal("changed target architecture reused the cached binary")
	}
}

// Concurrent processes building one key each end with a complete binary.
func TestFakeDockerInstallConcurrentBuilders(t *testing.T) {
	dir := t.TempDir()
	key := strings.Repeat("a", sha256.Size*2)
	start := make(chan struct{})
	var missed sync.WaitGroup
	missed.Add(4)
	type result struct {
		path string
		err  error
	}
	results := make(chan result, 4)
	for range 4 {
		go func() {
			path, _, err := installFakeDocker(dir, key, func(out string) ([]byte, error) {
				missed.Done()
				<-start // Every builder has missed the cache before any renames.
				return nil, os.WriteFile(out, []byte("complete binary"), 0o755)
			})
			results <- result{path, err}
		}()
	}
	missed.Wait()
	close(start)
	for range 4 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if data, err := os.ReadFile(r.path); err != nil || string(data) != "complete binary" {
			t.Fatalf("builder saw %q %v", data, err)
		}
	}
	if leftovers, _ := filepath.Glob(filepath.Join(dir, ".build-*")); len(leftovers) != 0 {
		t.Fatalf("temporary build directories left behind: %v", leftovers)
	}
}
