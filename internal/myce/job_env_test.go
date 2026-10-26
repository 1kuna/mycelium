package myce

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// A worker started inside a login session carries agent sockets, desktop buses
// and its own queue authority. None of them are job inputs (#219).
var workerSessionEnv = map[string]string{
	"SSH_AUTH_SOCK":            "/tmp/ssh-agent.sock",
	"SSH_AGENT_PID":            "4242",
	"DISPLAY":                  ":0",
	"DBUS_SESSION_BUS_ADDRESS": "unix:path=/run/user/1000/bus",
	"XDG_RUNTIME_DIR":          "/run/user/1000",
	"MYCE_QUEUE":               "http://fleet.invalid:7331",
	"MYCE_JOINING_FILE":        "/private/joining.json",
	"MYCE_TOKEN":               "must-not-inherit",
}

func setWorkerSession(t *testing.T) {
	t.Helper()
	for k, v := range workerSessionEnv {
		t.Setenv(k, v)
	}
}

func parseEnvDump(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			env[k] = v
		}
	}
	return env
}

func requireJobEnv(t *testing.T, path string, env map[string]string, want map[string]string, ignore ...string) {
	t.Helper()
	for k := range workerSessionEnv {
		if _, ok := env[k]; ok && !slices.Contains(ignore, k) {
			t.Errorf("%s inherited worker %s", path, k)
		}
	}
	if !strings.Contains(env["PATH"], os.Getenv("PATH")) || env["HOME"] != os.Getenv("HOME") {
		t.Errorf("%s lost PATH or HOME: %q %q", path, env["PATH"], env["HOME"])
	}
	for k, v := range want {
		if got, ok := env[k]; !ok || v != "*" && got != v {
			t.Errorf("%s %s=%q, want %q", path, k, got, v)
		}
	}
}

func TestJobEnvironmentAllowlistBatchCommand(t *testing.T) {
	setWorkerSession(t)
	s := nativeSpec(t)
	s.Run.Sidecars = nil
	s.Run.Command = []string{"sh", "-c", `env > "$NATIVE_TEST_DIR/plain.env"`}
	s.Run.Env["RUN_ENV"] = "kept"
	r := nativeStart(t, s, "fake")
	if result := nativeFinish(t, r); !result.Success {
		t.Fatal(result)
	}
	env := parseEnvDump(t, filepath.Join(r.dir, "plain.env"))
	requireJobEnv(t, "batch", env, map[string]string{"RUN_ENV": "kept", "MYCE_JOB_ID": r.j.ID, "MYCE_ATTEMPT": "1", "MYCE_CHECKPOINT_DIR": "*", "MYCE_PROCESS_ID": "*", "MYCE_PLATFORM": "cpu-fake", "MYCE_MACHINE": "native"})
}

func TestJobEnvironmentAllowlistNativeGroup(t *testing.T) {
	setWorkerSession(t)
	s := nativeSpec(t)
	// Shell members report exactly what the worker passed; the Go helper re-isolates HOME.
	s.Run.Command = []string{"sh", "-c", `while [ ! -e "$NATIVE_TEST_DIR/side.env" ]; do sleep 0.05; done; env > "$NATIVE_TEST_DIR/main.env"`}
	s.Run.Env["RUN_ENV"] = "kept"
	// An authored name on main must not let helpers fall back to the worker's value.
	s.Run.Env["DISPLAY"] = ":authored"
	s.Run.Sidecars[0].Command = []string{"sh", "-c", `env > "$NATIVE_TEST_DIR/side.env.partial" && mv "$NATIVE_TEST_DIR/side.env.partial" "$NATIVE_TEST_DIR/side.env"; exec sleep 60`}
	s.Run.Sidecars[0].Readiness = nil
	r := nativeStart(t, s, "fake")
	if result := nativeFinish(t, r); !result.Success {
		t.Fatal(result)
	}
	main, side := parseEnvDump(t, filepath.Join(r.dir, "main.env")), parseEnvDump(t, filepath.Join(r.dir, "side.env"))
	requireJobEnv(t, "main", main, map[string]string{"RUN_ENV": "kept", "DISPLAY": ":authored", "MYCE_JOB_ID": r.j.ID, "MYCE_ATTEMPT": "1", "MYCE_CHECKPOINT_DIR": "*", "MYCE_SIDECAR_HELPER_URL": "*"}, "DISPLAY")
	requireJobEnv(t, "sidecar", side, map[string]string{"NATIVE_TEST_MODE": "side", "MYCE_JOB_ID": r.j.ID, "MYCE_SIDECAR_PORT": "*"})
}

func TestJobEnvironmentAllowlistExtensionCommand(t *testing.T) {
	setWorkerSession(t)
	root, dir := t.TempDir(), t.TempDir()
	env, err := extensionEnvironment(root, fakeExtension(t), "fake", "")
	if err != nil {
		t.Fatal(err)
	}
	// Setup, check, benchmark and extension runs all start through this runner.
	out := filepath.Join(dir, "extension.env")
	if err := runExtensionCommand(context.Background(), []string{"sh", "-c", `env > "$1"`, "dump", out}, dir, env, io.Discard); err != nil {
		t.Fatal(err)
	}
	requireJobEnv(t, "extension", parseEnvDump(t, out), map[string]string{"MYCE_EXTENSION_DIR": env["MYCE_EXTENSION_DIR"], "MYCE_PLATFORM": "fake", "MYCE_PROCESS_ID": "*"})
}

func TestJobEnvironmentAllowlistEnvironmentBuild(t *testing.T) {
	setWorkerSession(t)
	t.Setenv("UV_LINK_MODE", "copy")
	t.Setenv("UV_INDEX_URL", "https://pypi.mirror.invalid/simple")
	// uv's credential settings and URL settings with userinfo stay with the
	// operator; recipe build code is arbitrary (#219).
	credentials := map[string]string{
		"UV_EXTRA_INDEX_URL":        "https://builder:index-secret@private.invalid/simple",
		"UV_INDEX_PRIVATE_USERNAME": "builder",
		"UV_INDEX_PRIVATE_PASSWORD": "index-secret",
		"UV_PUBLISH_USERNAME":       "publisher",
		"UV_PUBLISH_PASSWORD":       "publish-secret",
		"UV_PUBLISH_TOKEN":          "pypi-token",
		"UV_KEYRING_PROVIDER":       "subprocess",
		"UV_GITHUB_TOKEN":           "gh-token",
		"UV_PYTHON_INSTALL_MIRROR":  "https://mirror:secret@python.invalid",
	}
	for k, v := range credentials {
		t.Setenv(k, v)
	}
	r, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", `env > "$1/build.env"`, "build", "{root}"}}, "fake")
	if err != nil {
		t.Fatal(err)
	}
	c := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}
	path, release, err := c.Acquire(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	env := parseEnvDump(t, filepath.Join(path, "build.env"))
	requireJobEnv(t, "build", env, map[string]string{"UV_LINK_MODE": "copy", "UV_INDEX_URL": "https://pypi.mirror.invalid/simple", "MYCE_ENVIRONMENT_DIR": "*"})
	for k := range credentials {
		if _, ok := env[k]; ok {
			t.Errorf("build inherited worker %s", k)
		}
	}
}

func TestJobEnvironmentProxyAndCAVariables(t *testing.T) {
	setWorkerSession(t)
	kept := map[string]string{
		"HTTPS_PROXY":         "http://proxy.invalid:3128",
		"https_proxy":         "http://proxy.invalid:3128",
		"NO_PROXY":            "localhost,.internal",
		"no_proxy":            "localhost,.internal",
		"SSL_CERT_FILE":       "/etc/ssl/corp.pem",
		"SSL_CERT_DIR":        "/etc/ssl/certs",
		"REQUESTS_CA_BUNDLE":  "/etc/ssl/corp.pem",
		"CURL_CA_BUNDLE":      "/etc/ssl/corp.pem",
		"NODE_EXTRA_CA_CERTS": "/etc/ssl/corp.pem",
	}
	dropped := map[string]string{
		"HTTP_PROXY": "http://proxyuser:proxy-secret@proxy.invalid:3128",
		"http_proxy": "//proxyuser:proxy-secret@proxy.invalid:3128",
		"all_proxy":  "proxyuser:proxy-secret@proxy.invalid:1080",
	}
	for k, v := range kept {
		t.Setenv(k, v)
	}
	for k, v := range dropped {
		t.Setenv(k, v)
	}
	droppedCredentialLogged.Clear()
	var logs strings.Builder
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	s := nativeSpec(t)
	s.Run.Sidecars = nil
	s.Run.Command = []string{"sh", "-c", `env > "$NATIVE_TEST_DIR/proxy.env"`}
	r := nativeStart(t, s, "fake")
	if result := nativeFinish(t, r); !result.Success {
		t.Fatal(result)
	}
	jobBaseEnvironment()
	env := parseEnvDump(t, filepath.Join(r.dir, "proxy.env"))
	requireJobEnv(t, "batch", env, kept)
	for k := range dropped {
		if _, ok := env[k]; ok {
			t.Errorf("batch inherited credentialed %s", k)
		}
		if n := strings.Count(logs.String(), "dropping worker "+k+" "); n != 1 {
			t.Errorf("%s drop logged %d times, want once: %s", k, n, logs.String())
		}
	}
	if strings.Contains(logs.String(), "proxy-secret") || strings.Contains(logs.String(), "proxyuser") {
		t.Errorf("drop log exposes credentials: %s", logs.String())
	}
}

func TestJobEnvironmentAllowlistModelServer(t *testing.T) {
	setWorkerSession(t)
	m := serverTestModel(t, "ready")
	m.Env = map[string]string{"MODEL_ENV": "kept"}
	_, _, _, list, err := serverCommand(m, 1234)
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{}
	for _, entry := range list {
		k, v, _ := strings.Cut(entry, "=")
		env[k] = v
	}
	requireJobEnv(t, "server", env, map[string]string{"MODEL_ENV": "kept", "CUDA_VISIBLE_DEVICES": "2"})
}

func TestJobEnvironmentAllowlistDockerContainer(t *testing.T) {
	w, s, c, root := dockerFixture(t)
	setWorkerSession(t)
	j := dockerJob()
	j.Spec.Run.Env = map[string]string{"RUN_ENV": "kept"}
	j.Checkpoint = filepath.Join(t.TempDir(), "checkpoints")
	p, err := startDockerFixture(w, s, j)
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { _, e := os.Stat(filepath.Join(root, "running")); return e == nil })
	data, _ := os.ReadFile(filepath.Join(root, "calls"))
	if !strings.Contains(string(data), "--env RUN_ENV=kept") || !strings.Contains(string(data), "--env MYCE_JOB_ID="+j.ID) {
		t.Fatalf("container env missing: %s", data)
	}
	// Containers only receive explicit --env values, never the CLI's environment.
	for k := range workerSessionEnv {
		if strings.Contains(string(data), k) {
			t.Errorf("container create mentions worker %s", k)
		}
	}
	p.stop(c.Now(), time.Second, false)
	c.advance(time.Second)
	p.enforceGrace(c.Now())
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish")
	}
}
