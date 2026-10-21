package myce

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Fleet files deliberately accept only literal values, never shell expansion.
func ReadFleetEnvironment(path string) (token, queue string, err error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return "", "", errors.New("fleet EnvironmentFile must be a private regular file (0600)")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		line := strings.TrimSpace(scan.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || (key != "MYCE_TOKEN" && key != "MYCE_QUEUE") {
			return "", "", errors.New("fleet EnvironmentFile accepts only MYCE_TOKEN and MYCE_QUEUE literal assignments")
		}
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "'") {
			if len(value) < 2 || !strings.HasSuffix(value, "'") || strings.Contains(value[1:len(value)-1], "'") {
				return "", "", errors.New("invalid fleet EnvironmentFile quoting")
			}
			value = value[1 : len(value)-1]
		} else if strings.HasPrefix(value, `"`) {
			value, err = strconv.Unquote(value)
			if err != nil {
				return "", "", errors.New("invalid fleet EnvironmentFile quoting")
			}
		} else if strings.ContainsAny(value, " \t'\"\\") {
			return "", "", errors.New("quote fleet EnvironmentFile values containing spaces")
		}
		if value == "" || strings.ContainsAny(value, "\x00\n\r") {
			return "", "", errors.New("fleet EnvironmentFile values must be nonempty single-line literals")
		}
		if key == "MYCE_TOKEN" {
			token = value
		} else {
			queue = value
		}
	}
	if err = scan.Err(); err != nil {
		return "", "", errors.New("cannot read fleet EnvironmentFile")
	}
	if token == "" || queue == "" {
		return "", "", errors.New("fleet EnvironmentFile requires MYCE_TOKEN and MYCE_QUEUE")
	}
	u, err := url.Parse(queue)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", errors.New("MYCE_QUEUE must be an HTTP URL without credentials, query or fragment")
	}
	return token, queue, nil
}

func prepareInstallNAS(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	if c.RequireMountedNAS || c.Role == "worker" && c.Detection.Platform != "fake" {
		_, err := PrepareOnboardingStorage(ctx, c, false, command)
		return err
	}
	// Refuse symlinks so creation and probes cannot escape the Mycelium namespace.
	for _, dir := range []string{c.NAS, filepath.Join(c.NAS, "mycelium"), filepath.Join(c.NAS, "mycelium", "checkpoints"), filepath.Join(c.NAS, "mycelium", "models"), filepath.Join(c.NAS, "mycelium", "environments")} {
		st, err := os.Lstat(dir)
		if errors.Is(err, os.ErrNotExist) {
			if err = os.Mkdir(dir, 0700); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("NAS namespace must contain real directories, not symlinks")
		}
	}
	for _, name := range []string{"checkpoints", "models", "environments"} {
		dir := filepath.Join(c.NAS, "mycelium", name)
		f, err := os.CreateTemp(dir, ".myce-install-probe-")
		if err != nil {
			return err
		}
		path := f.Name()
		renamed := path + ".renamed"
		_, writeErr := f.WriteString("myce NAS write and rename probe\n")
		err = errors.Join(writeErr, f.Sync(), f.Close())
		if err != nil {
			_ = os.Remove(path)
			return err
		}
		if err = os.Rename(path, renamed); err != nil {
			_ = os.Remove(path)
			return err
		}
		if err = os.Remove(renamed); err != nil {
			return err
		}
	}
	return nil
}

func InstallConfigDiff(old, new ServiceConfig) string {
	old.Force, new.Force = false, false
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(new)
	var before, after map[string]json.RawMessage
	json.Unmarshal(a, &before)
	json.Unmarshal(b, &after)
	keys := map[string]bool{}
	for key := range before {
		keys[key] = true
	}
	for key := range after {
		keys[key] = true
	}
	fields := make([]string, 0, len(keys))
	for key := range keys {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	out := []string{}
	for _, key := range fields {
		if string(before[key]) != string(after[key]) {
			out = append(out, key+": changed")
		}
	}
	return strings.Join(out, "\n")
}

func (c ServiceConfig) ValidateInstall() error {
	if _, err := deviceAllowance(c.Device); err != nil {
		return err
	}
	for _, setting := range InstallSettings() {
		if setting.validate != nil {
			if err := setting.validate(c); err != nil {
				name := setting.Flag
				if name == "" {
					name = setting.Field
				}
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	return nil
}

func validateInstallQuota(value string) error {
	quota, err := strconv.ParseFloat(strings.TrimSuffix(value, "%"), 64)
	if err != nil || !strings.HasSuffix(value, "%") || quota <= 0 || math.IsNaN(quota) || math.IsInf(quota, 0) {
		return errors.New("cpu-quota must be a positive percentage")
	}
	return nil
}
func validateInstallLimit(value string, memory bool) error {
	if value == "infinity" {
		return nil
	}
	digits := value
	if memory {
		digits = strings.TrimRight(value, "KMGTPE")
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || n == 0 || (memory && len(value)-len(digits) > 1) {
		return errors.New("limit must be positive, with an optional memory suffix, or infinity")
	}
	return nil
}
