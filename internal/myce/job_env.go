package myce

import (
	"log"
	"os"
	"slices"
	"strings"
	"sync"
)

// Job processes start from this allowlist of the worker's own environment,
// never the whole of it: a service may run inside a login session whose agent
// sockets and bus addresses are credentials (#219). Mycelium then layers the
// spec's run.env, recipe activation, device/placement selectors and MYCE_* job
// variables on top. Everything else, including MYCE_TOKEN, MYCE_QUEUE and
// MYCE_JOINING_FILE, is dropped.
var jobBaseVariables = []string{"PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "TMPDIR"}

// Proxy and CA settings let dependency downloads work on proxied or custom-CA
// hosts. Any value containing @ is treated as credentialed and dropped.
var jobNetworkVariables = strings.Fields("HTTPS_PROXY https_proxy HTTP_PROXY http_proxy NO_PROXY no_proxy ALL_PROXY all_proxy SSL_CERT_FILE SSL_CERT_DIR REQUESTS_CA_BUNDLE CURL_CA_BUNDLE NODE_EXTRA_CA_CERTS")

// Environment builds also keep these operator uv settings. Each is a cache,
// concurrency, TLS or index-selection knob; uv's credential variables
// (UV_INDEX_*_PASSWORD/USERNAME, UV_PUBLISH_*, UV_KEYRING_PROVIDER,
// UV_GITHUB_TOKEN) are never forwarded, and URL settings containing @ are
// dropped. Mycelium owns the cache, paths and retry knobs.
var buildUVVariables = strings.Fields("UV_LINK_MODE UV_CONCURRENT_BUILDS UV_CONCURRENT_DOWNLOADS UV_CONCURRENT_INSTALLS UV_CONCURRENT_CACHE_READS UV_COMPILE_BYTECODE UV_NATIVE_TLS UV_SYSTEM_CERTS UV_INDEX_STRATEGY UV_INDEX UV_DEFAULT_INDEX UV_INDEX_URL UV_EXTRA_INDEX_URL UV_FIND_LINKS UV_PYTHON_INSTALL_MIRROR UV_TORCH_BACKEND")

var droppedCredentialLogged sync.Map

func jobBaseEnvironment() map[string]string {
	return jobEnvironmentFrom(os.Environ())
}

func jobEnvironmentFrom(environ []string) map[string]string {
	env := map[string]string{}
	for _, entry := range environ {
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if slices.Contains(jobNetworkVariables, k) {
			keepWithoutCredentials(env, k, v)
		} else if slices.Contains(jobBaseVariables, k) || strings.HasPrefix(k, "LC_") || slices.Contains(gpuRuntimeVariables, k) {
			env[k] = v
		}
	}
	return env
}

// buildEnvironmentFrom is the job base plus the operator's safe uv settings.
func buildEnvironmentFrom(environ []string) map[string]string {
	env := jobEnvironmentFrom(environ)
	for _, entry := range environ {
		if k, v, ok := strings.Cut(entry, "="); ok && slices.Contains(buildUVVariables, k) {
			keepWithoutCredentials(env, k, v)
		}
	}
	return env
}

// keepWithoutCredentials treats any @ in v as credentials and drops k,
// logging once per name without the value. This deliberately avoids parsing
// proxy and uv URL formats, including scheme-relative and named URLs.
func keepWithoutCredentials(env map[string]string, k, v string) {
	if !strings.Contains(v, "@") {
		env[k] = v
		return
	}
	if _, seen := droppedCredentialLogged.LoadOrStore(k, true); !seen {
		log.Printf("job environment: dropping worker %s because its URL contains credentials; pass it through run.env if a job needs it", k)
	}
}
