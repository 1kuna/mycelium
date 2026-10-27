package myce

import (
	"os"
	"path/filepath"
	"strings"
)

// Frozen extension source is small and immutable. Each native attempt gets a
// private copy so relative check commands cannot use the shared source as cwd.
// Prepared software and assets remain shared, read-only runtime dependencies.
func privateNativeExtension(j *Job, env map[string]string, work, cwd string) (string, error) {
	if j.Spec.Run.Docker || j.Extension == nil {
		return cwd, nil
	}
	private := filepath.Join(work, ".myce-extension")
	rules := []string{env["MYCE_EXTENSION_DIR"], private}
	if check := j.Spec.ExtensionCheck; check != nil {
		rules = append(rules, check.Directory, work)
	}
	r := strings.NewReplacer(rules...)
	expandExecutionReference(&j.Spec, r)
	for key, value := range env {
		env[key] = r.Replace(value)
	}
	if check := j.Spec.ExtensionCheck; check != nil && env["MYCE_CHECK_OUTPUT"] != "" {
		dir := filepath.Join(work, ".myce-check")
		if err := os.MkdirAll(dir, 0700); err != nil {
			return "", err
		}
		env["MYCE_CHECK_OUTPUT"] = filepath.Join(dir, "actual")
		if j.Spec.Run.Env == nil {
			j.Spec.Run.Env = map[string]string{}
		}
		j.Spec.Run.Env["MYCE_CHECK_OUTPUT"] = env["MYCE_CHECK_OUTPUT"]
	}
	return r.Replace(cwd), nil
}
