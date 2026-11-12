package myce

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Tools are executable basenames, not packages, version expressions or commands.
// Versioned requirements use versioned executables (python3.12) and locked recipes.
func validToolName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._+-", c)) {
			return false
		}
	}
	return true
}

func validateTools(s Spec) error {
	for _, name := range s.Needs.Tools {
		if !validToolName(name) {
			return reject("job_tools_invalid", fmt.Errorf("needs.tools requires executable basenames such as python3.12, node, npm or git"))
		}
	}
	if len(s.Needs.Tools) > 0 && s.Needs.Model != "" {
		return reject("job_tools_invalid", fmt.Errorf("needs.tools currently describes native command prerequisites; model runtimes own their tools"))
	}
	return nil
}

func missingTools(j *Job, m *Machine) []string {
	if len(j.Spec.Needs.Tools) == 0 {
		return nil
	}
	inventory := m.ToolInventory
	if m.toolInventory != nil {
		inventory = m.toolInventory.tools
	}
	key := environmentKey(j, m)
	// A cold managed recipe must be prepared before its executable inventory can
	// be known. Preparation is allowed, but dispatch rechecks the prepared key.
	if inventory != nil && key != "" && !slices.Contains(m.Environments, key) && m.EnvironmentBuilder {
		return nil
	}
	var missing []string
	for _, tool := range j.Spec.Needs.Tools {
		if !slices.Contains(inventory[key], tool) {
			missing = append(missing, tool)
		}
	}
	return missing
}
func toolsFit(j *Job, m *Machine) bool { return len(missingTools(j, m)) == 0 }
func missingToolsReason(host string, missing []string) string {
	return fmt.Sprintf("tools_missing: missing tools [%s] on %s; declare a locked environment providing them or use a worker that reports them", strings.Join(missing, ", "), host)
}
func toolMissingError(host string, missing []string) error {
	return &runtimeError{diagnostic: RuntimeCause{Operation: "launch", Class: "tools_missing", Host: host, MissingTools: missing}}
}

func executableTools(path string) []string {
	var names []string
	for _, dir := range filepath.SplitList(path) {
		// Relative PATH entries depend on the eventual job workdir and cannot be
		// advertised as host-wide software.
		if !filepath.IsAbs(dir) {
			continue
		}
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if !validToolName(entry.Name()) {
				continue
			}
			info, err := os.Stat(filepath.Join(dir, entry.Name()))
			if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				names = append(names, entry.Name())
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}
func missingExecutableTools(tools []string, path string) []string {
	if len(tools) == 0 {
		return nil
	}
	var missing []string
	for _, tool := range tools {
		found := false
		for _, dir := range filepath.SplitList(path) {
			if !filepath.IsAbs(dir) {
				continue
			}
			info, err := os.Stat(filepath.Join(dir, tool))
			if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, tool)
		}
	}
	return missing
}
func preparedToolInventory(cache map[string][]string, ready []string, root string) map[string][]string {
	out := map[string][]string{"": cache[""]}
	for _, key := range ready {
		names, ok := cache[key]
		if !ok {
			env := jobBaseEnvironment()
			bindEnvironment(env, filepath.Join(root, key))
			names = executableTools(env["PATH"])
			cache[key] = names
		}
		out[key] = names
	}
	return out
}
