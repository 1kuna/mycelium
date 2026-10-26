package myce

import (
	"os"
	"slices"
	"strings"
)

// Job processes start from this allowlist of the worker's own environment,
// never the whole of it: a service may run inside a login session whose agent
// sockets and bus addresses are credentials (#219). Mycelium then layers the
// spec's run.env, recipe activation, device/placement selectors and MYCE_* job
// variables on top. Everything else, including MYCE_TOKEN, MYCE_QUEUE and
// MYCE_JOINING_FILE, is dropped.
var jobBaseVariables = []string{"PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "TMPDIR"}

func jobBaseEnvironment() map[string]string {
	return jobEnvironmentFrom(os.Environ())
}

func jobEnvironmentFrom(environ []string) map[string]string {
	env := map[string]string{}
	for _, entry := range environ {
		k, v, ok := strings.Cut(entry, "=")
		if ok && (slices.Contains(jobBaseVariables, k) || strings.HasPrefix(k, "LC_") || slices.Contains(gpuRuntimeVariables, k)) {
			env[k] = v
		}
	}
	return env
}
