package main

import "os"

// The shared owner is user infrastructure, not part of its starter's job.
// An allowlist also excludes future CI tracking variables and unrelated secrets.
// Notifiers retain their separately saved caller environment/absolute executable.
func observerEnvironment() []string {
	var env []string
	for _, key := range []string{"PATH", "LANG", "LANGUAGE", "LC_ALL", "LC_CTYPE", "LC_COLLATE", "LC_MESSAGES", "LC_MONETARY", "LC_NUMERIC", "LC_TIME", "HOME", "USER", "LOGNAME", "XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	if _, ok := os.LookupEnv("PATH"); !ok {
		env = append(env, "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
	}
	// These are explicit internal custody contracts, never job identity. Preserve
	// them across upgrade handoff; a managed attempt must not escape cleanup.
	for _, key := range []string{"MYCE_DEV_SESSION_ROOT", "MYCE_PROCESS_SESSION"} {
		if value := os.Getenv(key); value != "" {
			env = append(env, key+"="+value)
		}
	}
	return env
}
