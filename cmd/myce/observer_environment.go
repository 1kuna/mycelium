package main

import (
	"os"
	"strings"
)

// Only configuration locations and wake bindings belong in durable notify_env.
// Notifiers load credentials from private configuration, never this snapshot.
var notifierEnvironmentVariables = map[string]string{
	"PATH":             "Notifier scripts and the harness CLI resolve child executables.",
	"HOME":             "Notifiers and the harness CLI locate private user configuration.",
	"LANG":             "Notifier text uses the caller's default locale.",
	"LANGUAGE":         "Notifier messages use the caller's language preference.",
	"LC_ALL":           "Notifier text honors the caller's locale override.",
	"LC_CTYPE":         "Notifier tools preserve character encoding.",
	"LC_COLLATE":       "Notifier tools preserve text collation.",
	"LC_MESSAGES":      "Notifier tools preserve message language.",
	"LC_MONETARY":      "Notifier tools preserve monetary formatting.",
	"LC_NUMERIC":       "Notifier tools preserve numeric formatting.",
	"LC_TIME":          "Notifier tools preserve time formatting.",
	"XDG_CONFIG_HOME":  "Notifier hooks locate private receiver configuration.",
	"XDG_CACHE_HOME":   "The harness CLI locates its compile cache.",
	"SSL_CERT_FILE":    "HTTPS notifier hooks locate the caller's trusted CA file.",
	"SSL_CERT_DIR":     "HTTPS notifier hooks locate the caller's trusted CA directory.",
	"T3CODE_THREAD_ID": "detectHarnessWake selects the submitting thread.",
	"T3CODE_CLI_PATH":  "harnessWakeCommand selects the absolute wake executable.",
	"T3CODE_HOME":      "The default harness CLI selects its running server's state directory.",
}

func filterNotifierEnvironment(source []string) []string {
	var env []string
	for _, entry := range source {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || secretEnvironmentVariable(key) {
			continue
		}
		if _, allowed := notifierEnvironmentVariables[key]; allowed {
			env = append(env, entry)
		}
	}
	return env
}

// Secret names are forbidden even if a future allowlist entry includes one.
func secretEnvironmentVariable(key string) bool {
	key = strings.ToUpper(key)
	for _, suffix := range []string{"_TOKEN", "_KEY", "_SECRET", "_PASSWORD", "_CREDENTIAL", "_CREDENTIALS"} {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return key == "MYCE_TOKEN"
}

// The shared owner is user infrastructure, not part of its starter's job.
// An allowlist also excludes future CI tracking variables and unrelated secrets.
// Notifiers retain their separately filtered caller environment/absolute executable.
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
