package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const maxGenericRequestBytes int64 = 8 << 30

var errModelRequestTooLarge = errors.New("generic model request exceeds 8 GiB disk-spool limit")

// PrepareModelEnvironment holds the cache use lock for the complete server life.
func PrepareModelEnvironment(ctx context.Context, m Model, platform string, catalog EnvironmentCatalog, cache *EnvironmentCache) (Model, func(), error) {
	if m.Environment == "" {
		return m, func() {}, nil
	}
	r, ok := catalog.Environments[m.Environment][platform]
	if !ok || cache == nil {
		return m, nil, fmt.Errorf("model %s environment %s/%s unavailable", m.Name, m.Environment, platform)
	}
	root, release, err := cache.Acquire(ctx, r)
	if err != nil {
		return m, nil, err
	}
	m.EnvironmentPath = root
	return m, release, nil
}

func validateServer(m Model) error {
	if strings.ContainsAny(m.Name, "/\\?#") || m.Name == "." || m.Name == ".." {
		return errors.New("model name must be one URL path segment")
	}
	if m.ReadyTimeout < 0 || m.StopTimeout < 0 {
		return errors.New("server timeouts must be positive")
	}
	if m.Engine == "server" && (len(m.StartCommand) == 0 || m.StartCommand[0] == "" || m.HealthURL == "") {
		return errors.New("server requires start_command and health_url")
	}
	if m.Engine == "vllm" {
		if m.VLLM == nil || m.VLLM.ServedName == "" || m.VLLM.MaxContext <= 0 || m.VLLM.KVCacheDType == "" || !(m.VLLM.GPUMemoryUtilization > 0 && m.VLLM.GPUMemoryUtilization <= 1) {
			return errors.New("vllm requires served_name, max_context, kv_cache_dtype and gpu_memory_utilization in (0,1]")
		}
		for _, a := range m.Args {
			for _, reserved := range []string{"--host", "--port", "--model", "--served-model-name", "--max-model-len", "--kv-cache-dtype", "--gpu-memory-utilization", "--download-dir"} {
				if a == reserved || strings.HasPrefix(a, reserved+"=") {
					return fmt.Errorf("vllm args cannot override %s", reserved)
				}
			}
		}
	}
	for k := range m.DeviceEnv {
		if k != "CUDA_VISIBLE_DEVICES" && k != "ZE_AFFINITY_MASK" && k != "ONEAPI_DEVICE_SELECTOR" {
			return fmt.Errorf("unsupported device environment %s", k)
		}
	}
	if m.HealthURL != "" {
		u, e := url.Parse(strings.NewReplacer("{port}", "1234").Replace(m.HealthURL))
		if e != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") || u.User != nil {
			return errors.New("health_url must be a loopback HTTP URL")
		}
	}
	return nil
}

func serverCommand(m Model, port int) ([]string, string, []string, []string, error) {
	replace := strings.NewReplacer("{port}", strconv.Itoa(port), "{model}", m.Path, "{model_path}", m.Path, "{environment}", m.EnvironmentPath, "{device}", m.Device)
	expand := func(args []string) []string {
		out := make([]string, len(args))
		for i, a := range args {
			out[i] = replace.Replace(a)
		}
		return out
	}
	var argv []string
	health := "http://127.0.0.1:" + strconv.Itoa(port) + "/health"
	switch m.Engine {
	case "server":
		argv = expand(m.StartCommand)
		health = replace.Replace(m.HealthURL)
	case "vllm":
		if m.VLLM == nil {
			return nil, "", nil, nil, errors.New("vllm preset missing")
		}
		v := m.VLLM
		argv = []string{"vllm", "serve", m.Path, "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--served-model-name", v.ServedName, "--max-model-len", strconv.Itoa(v.MaxContext), "--kv-cache-dtype", v.KVCacheDType, "--gpu-memory-utilization", strconv.FormatFloat(v.GPUMemoryUtilization, 'g', -1, 64)}
		if v.ServedName != m.Name {
			argv = append(argv[:9], append([]string{m.Name}, argv[9:]...)...)
		}
		argv = append(argv, m.Args...)
	default:
		argv = []string{"llama-server", "--model", m.Path, "--alias", m.Name, "--host", "127.0.0.1", "--port", strconv.Itoa(port), "--n-gpu-layers", "99"}
		argv = append(argv, m.Args...)
	}
	if len(argv) == 0 {
		return nil, "", nil, nil, errors.New("server command missing")
	}
	if err := validateModelEnv(m); err != nil {
		return nil, "", nil, nil, err
	}
	env := map[string]string{}
	for _, entry := range os.Environ() {
		k, v, ok := strings.Cut(entry, "=")
		if ok && k != "MYCE_TOKEN" {
			env[k] = v
		}
	}
	for k, v := range m.Env {
		env[k] = v
	}
	secrets, err := readModelSecrets(m.secretRoot, m.SecretEnv, m.fleetToken)
	if err != nil {
		return nil, "", nil, nil, err
	}
	for k, v := range secrets {
		env[k] = v
	}
	if m.Device != "" {
		// Explicit per-process bindings override inherited selectors for every backend.
		delete(env, "CUDA_VISIBLE_DEVICES")
		delete(env, "ONEAPI_DEVICE_SELECTOR")
		delete(env, "ZE_AFFINITY_MASK")
	}
	if m.EnvironmentPath != "" {
		bindEnvironment(env, m.EnvironmentPath)
		if !strings.Contains(argv[0], "/") {
			for _, dir := range environmentBins(m.EnvironmentPath) {
				path := filepath.Join(dir, argv[0])
				if st, e := os.Stat(path); e == nil && st.Mode()&0111 != 0 {
					argv[0] = path
					break
				}
			}
		}
	}
	for k, v := range m.DeviceEnv {
		env[k] = replace.Replace(v)
	}
	// Frameworks must never download a missing component during load.
	env["HF_HUB_OFFLINE"] = "1"
	env["TRANSFORMERS_OFFLINE"] = "1"
	out := []string{}
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	stop := expand(m.StopCommand)
	if len(stop) > 0 && !strings.Contains(stop[0], "/") && m.EnvironmentPath != "" {
		for _, dir := range environmentBins(m.EnvironmentPath) {
			path := filepath.Join(dir, stop[0])
			if st, e := os.Stat(path); e == nil && st.Mode()&0111 != 0 {
				stop[0] = path
				break
			}
		}
	}
	return argv, health, stop, out, nil
}

// Remove routing credentials and hop-by-hop fields at each proxy boundary.
func proxyHeaders(h http.Header) http.Header {
	out := h.Clone()
	for _, v := range h.Values("Connection") {
		for _, key := range strings.Split(v, ",") {
			out.Del(strings.TrimSpace(key))
		}
	}
	for _, k := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade", "Authorization", "X-Myce-Session", "X-Myce-Project", "X-Myce-Priority", "X-Myce-Deadline", "X-Myce-Engine-Attempt", "X-Myce-Engine-Failed", "X-Myce-Engine-Rejected"} {
		out.Del(k)
	}
	return out
}
func copyProxyHeaders(dst, src http.Header) {
	for k, values := range proxyHeaders(src) {
		dst[k] = append([]string(nil), values...)
	}
}
func (l *LlamaEngine) CallRequest(ctx context.Context, method, path string, body []byte, headers http.Header) (*http.Response, error) {
	return l.Request(ctx, method, path, headers, bytes.NewReader(body), int64(len(body)))
}

type CallRequestEngine interface {
	CallRequest(context.Context, string, string, []byte, http.Header) (*http.Response, error)
}

// StreamRequestEngine accepts request bodies without first materializing them in
// worker memory. contentLength is preserved when known so media servers that do
// not accept chunked uploads see the original request contract.
type StreamRequestEngine interface {
	StreamRequest(context.Context, string, string, io.Reader, int64, http.Header) (*http.Response, error)
}

func (l *LlamaEngine) StreamRequest(ctx context.Context, method, path string, body io.Reader, contentLength int64, headers http.Header) (*http.Response, error) {
	return l.Request(ctx, method, path, headers, body, contentLength)
}

func modelFitsMachine(model Model, m *Machine) bool {
	return !m.Leaving && modelFenceAllows(model, m) && contains(model.Platforms, m.GPU) && (model.Device == "" || model.Device == m.Device)
}

// WithModelDevice applies a per-process selector; global environment is untouched.
func WithModelDevice(m Model, platform, device string) (Model, error) {
	if device == "" && platform != "cpu" {
		return m, nil
	}
	if m.Device != "" && m.Device != device {
		return m, fmt.Errorf("model %s device %s differs from worker %s", m.Name, m.Device, device)
	}
	m.Device = device
	env := map[string]string{}
	for k, v := range m.DeviceEnv {
		env[k] = v
	}
	m.DeviceEnv = env
	// Never carry a selector for another accelerator family into this process.
	// A mixed CUDA/XPU host must not expose an unselected physical device merely
	// because the catalog contained a stale or irrelevant selector.
	delete(env, "CUDA_VISIBLE_DEVICES")
	delete(env, "ONEAPI_DEVICE_SELECTOR")
	delete(env, "ZE_AFFINITY_MASK")
	switch platform {
	case "cuda":
		env["CUDA_VISIBLE_DEVICES"] = device
	case "xpu":
		// Level Zero renumbers the affinity-masked physical devices from zero.
		indices := strings.Split(device, ",")
		for i := range indices {
			indices[i] = strconv.Itoa(i)
		}
		env["ONEAPI_DEVICE_SELECTOR"] = "level_zero:" + strings.Join(indices, ",")
		env["ZE_AFFINITY_MASK"] = device
	case "cpu":
		env["CUDA_VISIBLE_DEVICES"] = ""
	case "fake":
	case "metal":
		if device != "0" {
			return m, errors.New("Metal supports only device 0")
		}
	default:
		return m, errors.New("unknown device platform")
	}
	return m, nil
}

var modelHTTPClient = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
