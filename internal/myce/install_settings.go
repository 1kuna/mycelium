package myce

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

// InstallSetting is the sole mapping from installer flags to saved fields.
// Empty Flag means the field is installer-owned or has no CLI override.
// Validation and path normalization are attached here, never to flag presence.
type InstallSetting struct {
	Flag, Field, Usage string
	bind               func(*flag.FlagSet, *ServiceConfig)
	copy               func(*ServiceConfig, ServiceConfig)
	validate           func(ServiceConfig) error
	normalize          func(*ServiceConfig) error
	fresh              func(*ServiceConfig, ServiceConfig) error
}

func installSetting[T any](name, field, usage string, at func(*ServiceConfig) *T, valid func(T) error) InstallSetting {
	return InstallSetting{Flag: name, Field: field, Usage: usage,
		bind: func(f *flag.FlagSet, c *ServiceConfig) {
			switch p := any(at(c)).(type) {
			case *string:
				f.StringVar(p, name, *p, usage)
			case *bool:
				f.BoolVar(p, name, *p, usage)
			case *int:
				f.IntVar(p, name, *p, usage)
			case *int64:
				f.Int64Var(p, name, *p, usage)
			case *float64:
				f.Float64Var(p, name, *p, usage)
			case *time.Duration:
				f.DurationVar(p, name, *p, usage)
			case **time.Duration:
				value := DefaultContainerRetention
				if *p != nil {
					value = **p
				}
				*p = &value
				f.DurationVar(*p, name, value, usage)
			default:
				panic("unsupported installer flag type")
			}
		},
		copy:     func(dst *ServiceConfig, src ServiceConfig) { *at(dst) = *at(&src) },
		validate: func(c ServiceConfig) error { return valid(*at(&c)) },
	}
}
func installPath(name, field, usage string, at func(*ServiceConfig) *string, required bool) InstallSetting {
	s := installSetting(name, field, usage, at, func(p string) error {
		if required && p == "" {
			return errors.New("path must not be empty")
		}
		if p == "/usr/local/libexec/myce" || strings.HasPrefix(p, "/usr/local/libexec/myce/") {
			return errors.New("worker installer cannot write the protected root binary tree")
		}
		if p != "" && (!filepath.IsAbs(p) || strings.ContainsAny(p, "\x00\n\r")) {
			return errors.New("installed paths must be absolute and single-line")
		}
		return nil
	})
	s.normalize = func(c *ServiceConfig) error {
		p := at(c)
		var err error
		*p, err = installAbsolute(*p)
		return err
	}
	return s
}
func installAny[T any](T) error { return nil }
func installNonnegative(v float64) error {
	if !validMemoryValue(v) {
		return errors.New("must be finite and nonnegative")
	}
	return nil
}
func installBytes(v int64) error {
	if v <= 0 {
		return errors.New("byte limit must be positive")
	}
	return nil
}
func installPositiveDuration(v time.Duration) error {
	if v <= 0 {
		return errors.New("duration must be positive")
	}
	return nil
}
func installNonnegativeDuration(v time.Duration) error {
	if v < 0 {
		return errors.New("duration must be nonnegative")
	}
	return nil
}
func installName(v string) error {
	if v == "" || strings.ContainsAny(v, "\x00\n\r") {
		return errors.New("must be nonempty and single-line")
	}
	return nil
}

// InstallSettings includes aliases as separate rows. The overlay rejects two
// explicit aliases for one field, so table order can never define precedence.
func InstallSettings() []InstallSetting {
	return []InstallSetting{
		installSetting("daily-use", "DailyUse", "wait for owner inactivity and safe power/heat", func(c *ServiceConfig) *bool { return &c.DailyUse }, installAny[bool]),
		installContext(installSetting("laptop", "Laptop", "require external power", func(c *ServiceConfig) *bool { return &c.Laptop }, installAny[bool]), func(c ServiceConfig) error {
			if c.Laptop && c.Desktop {
				return errors.New("--desktop and --laptop are mutually exclusive")
			}
			return nil
		}),
		installSetting("desktop", "Desktop", "declare a daily-use desktop", func(c *ServiceConfig) *bool { return &c.Desktop }, installAny[bool]),
		installSetting("enrollment", "Enrollment", "record exact-session enrollment and runner smoke", func(c *ServiceConfig) *bool { return &c.Enrollment }, installAny[bool]),
		installSetting("allow-deviceless", "AllowDeviceless", "allow CPU/fake test worker", func(c *ServiceConfig) *bool { return &c.AllowDeviceless }, installAny[bool]),
		installSetting("device", "Device", "physical GPU selector", func(c *ServiceConfig) *string { return &c.Device }, installAny[string]),
		installSetting("docker-host", "DockerEndpoint", "opt in to managed Docker at this endpoint (empty disables Docker)", func(c *ServiceConfig) *string { return &c.DockerEndpoint }, installAny[string]),

		installPath("joining-file", "JoiningFile", "private joining invitation", func(c *ServiceConfig) *string { return &c.JoiningFile }, false),
		installNASSetting(),
		installSetting("queue", "Queue", "queue address", func(c *ServiceConfig) *string { return &c.Queue }, func(v string) error {
			u, e := url.Parse(v)
			if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return errors.New("must be an HTTP URL")
			}
			return nil
		}),
		installSetting("listen", "Listen", "queue bind address", func(c *ServiceConfig) *string { return &c.Listen }, installName),
		installSetting("name", "Name", "stable worker name", func(c *ServiceConfig) *string { return &c.Name }, installName),
		installContext(installSetting("gpu", "Detection.Platform", "GPU platform override", func(c *ServiceConfig) *string { return &c.Detection.Platform }, installAny[string]), func(c ServiceConfig) error {
			if c.Role == "worker" && !validGPU(c.Detection.Platform) {
				return errors.New("worker GPU platform required")
			}
			return nil
		}),
		installContext(installSetting("memory-mode", "Detection.MemoryMode", "GPU memory mode", func(c *ServiceConfig) *string { return &c.Detection.MemoryMode }, installAny[string]), func(c ServiceConfig) error {
			if c.Role == "worker" && c.Detection.MemoryMode != "" {
				_, err := NewMemorySourceWithMode(c.Detection.Platform, c.Detection.MemoryGB, c.Detection.ReserveGB, c.Detection.MemoryMode)
				return err
			}
			return nil
		}),
		installSetting("memory-gb", "Detection.MemoryGB", "GPU memory budget", func(c *ServiceConfig) *float64 { return &c.Detection.MemoryGB }, installNonnegative),
		installSetting("reserve-memory-gb", "Detection.ReserveGB", "GPU memory reserve", func(c *ServiceConfig) *float64 { return &c.Detection.ReserveGB }, installNonnegative),
		installSetting("reserve-cpu-cores", "CPUReserveCores", "logical cores reserved for the owner", func(c *ServiceConfig) *int { return &c.CPUReserveCores }, installNonnegativeInt),
		installSetting("cpu-budget-cores", "CPUBudgetCores", "initial queue host CPU allowance (zero uses detected capacity minus reserve)", func(c *ServiceConfig) *int { return &c.CPUBudgetCores }, installNonnegativeInt),
		installContext(installSetting("host-memory-budget-gb", "HostMemoryBudgetGB", "initial queue host RAM allowance", func(c *ServiceConfig) *float64 { return &c.HostMemoryBudgetGB }, installNonnegative), func(c ServiceConfig) error {
			_, err := MemoryCeilingConfig(c.HostMemoryBudgetGB, c.MemoryCeilingSlackGB, c.MemorySwapMaxBytes)
			return err
		}),
		installSetting("memory-ceiling-slack-gb", "MemoryCeilingSlackGB", "extra GiB per declared RAM ceiling", func(c *ServiceConfig) *float64 { return &c.MemoryCeilingSlackGB }, installNonnegative),
		installSetting("memory-swap-max-bytes", "MemorySwapMaxBytes", "swap allowance per ceiling", func(c *ServiceConfig) *int64 { return &c.MemorySwapMaxBytes }, func(v int64) error {
			if v < 0 {
				return errors.New("must be nonnegative")
			}
			return nil
		}),
		installSetting("host-id", "HostID", "physical host identity", func(c *ServiceConfig) *string { return &c.HostID }, installName),
		installSetting("reserve-host-memory-gb", "ReserveHostMemoryGB", "host RAM reserve", func(c *ServiceConfig) *float64 { return &c.ReserveHostMemoryGB }, installNonnegative),
		installStateSetting(),
		installPath("checkpoint-root", "Checkpoints", "worker checkpoint root", func(c *ServiceConfig) *string { return &c.Checkpoints }, false),
		installPath("checkpoints", "Checkpoints", "queue checkpoint root (alias of checkpoint-root)", func(c *ServiceConfig) *string { return &c.Checkpoints }, false),
		installPath("model-root", "ModelRoot", "NAS model root", func(c *ServiceConfig) *string { return &c.ModelRoot }, false),
		installPath("environment-root", "EnvironmentRoot", "NAS environment root", func(c *ServiceConfig) *string { return &c.EnvironmentRoot }, false),
		installPath("model-cache", "ModelCache", "local model cache", func(c *ServiceConfig) *string { return &c.ModelCache }, false),
		installSetting("model-cache-bytes", "ModelCacheBytes", "local model cache bytes", func(c *ServiceConfig) *int64 { return &c.ModelCacheBytes }, installBytes),
		installPath("environment-cache", "EnvironmentCache", "local environment cache", func(c *ServiceConfig) *string { return &c.EnvironmentCache }, false),
		installSetting("environment-cache-bytes", "EnvironmentCacheBytes", "local environment cache bytes", func(c *ServiceConfig) *int64 { return &c.EnvironmentCacheBytes }, installBytes),
		installSetting("heartbeat", "Heartbeat", "worker check-in interval", func(c *ServiceConfig) *time.Duration { return &c.Heartbeat }, installPositiveDuration),

		installContext(installSetting("require-memory-ceilings", "RequireMemoryCeilings", "require tested cgroup v2 ceilings", func(c *ServiceConfig) *bool { return &c.RequireMemoryCeilings }, installAny[bool]), func(c ServiceConfig) error {
			if c.RequireMemoryCeilings && c.Role != "worker" {
				return errors.New("requires a worker")
			}
			return nil
		}),
		installSetting("require-mounted-nas", "RequireMountedNAS", "require NAS mount in /proc/mounts", func(c *ServiceConfig) *bool { return &c.RequireMountedNAS }, installAny[bool]),
		installPath("db", "DB", "queue SQLite path", func(c *ServiceConfig) *string { return &c.DB }, false),
		installPath("project-config", "ProjectConfig", "project policy YAML", func(c *ServiceConfig) *string { return &c.ProjectConfig }, false),
		installPath("host-config", "HostConfig", "host policy YAML", func(c *ServiceConfig) *string { return &c.HostConfig }, false),
		installSetting("container-retention", "ContainerRetention", "acknowledged stopped container retention", func(c *ServiceConfig) **time.Duration { return &c.ContainerRetention }, func(v *time.Duration) error {
			if v != nil {
				return installNonnegativeDuration(*v)
			}
			return nil
		}),
		installSetting("log-retention", "LogRetention", "completed log retention", func(c *ServiceConfig) *time.Duration { return &c.LogRetention }, installPositiveDuration),
		installPath("upload-spool-dir", "UploadSpoolDir", "local upload spool", func(c *ServiceConfig) *string { return &c.UploadSpoolDir }, false),
		installSetting("upload-spool-bytes", "UploadSpoolBytes", "total upload spool bytes", func(c *ServiceConfig) *int64 { return &c.UploadSpoolBytes }, installBytes),
		installSetting("restart-grace", "RestartGrace", "queue restart recovery grace", func(c *ServiceConfig) *time.Duration { return &c.RestartGrace }, installNonnegativeDuration),
		installSetting("production-infrastructure-retries", "ProductionInfrastructureRetries", "maximum infrastructure requeues per production job", func(c *ServiceConfig) *int { return &c.ProductionInfrastructureRetries }, func(n int) error {
			if n < 0 {
				return errors.New("must be nonnegative")
			}
			return nil
		}),
		installSetting("production-loss-timeout", "ProductionLossTimeout", "production quiet timeout", func(c *ServiceConfig) *time.Duration { return &c.ProductionLossTimeout }, installPositiveDuration),
		installSetting("deadline-loss-timeout", "DeadlineLossTimeout", "deadline quiet timeout", func(c *ServiceConfig) *time.Duration { return &c.DeadlineLossTimeout }, installPositiveDuration),
		installSetting("experiment-loss-timeout", "ExperimentLossTimeout", "experiment quiet timeout", func(c *ServiceConfig) *time.Duration { return &c.ExperimentLossTimeout }, installPositiveDuration),
		installEnvironmentFileSetting(),
		installContext(installSetting("private-devices", "PrivateDevices", "queue private device namespace", func(c *ServiceConfig) *bool { return &c.PrivateDevices }, installAny[bool]), func(c ServiceConfig) error {
			if c.Detection.OS == "linux" && c.Role == "queue" && c.PrivateDevices {
				return errors.New("PrivateDevices is unsupported for portable systemd user queues; use --private-devices=false")
			}
			return nil
		}),
		installSetting("no-new-privileges", "NoNewPrivileges", "queue disallows privilege escalation", func(c *ServiceConfig) *bool { return &c.NoNewPrivileges }, installAny[bool]),
		installSetting("nice", "Nice", "queue scheduling priority", func(c *ServiceConfig) *int { return &c.Nice }, func(v int) error {
			if v < -20 || v > 19 {
				return errors.New("must be between -20 and 19")
			}
			return nil
		}),
		installSetting("cpu-quota", "CPUQuota", "queue systemd CPUQuota", func(c *ServiceConfig) *string { return &c.CPUQuota }, validateInstallQuota),
		installSetting("memory-max", "MemoryMax", "queue systemd MemoryMax", func(c *ServiceConfig) *string { return &c.MemoryMax }, func(v string) error { return validateInstallLimit(v, true) }),
		installSetting("tasks-max", "TasksMax", "queue systemd TasksMax", func(c *ServiceConfig) *string { return &c.TasksMax }, func(v string) error { return validateInstallLimit(v, false) }),
		installPath("model-config", "ModelCatalog", "NAS model catalog", func(c *ServiceConfig) *string { return &c.ModelCatalog }, false),
		installEnvironmentCatalogSetting(),
		// No CLI overrides: preserved verbatim, with identity enforced separately.
		{Field: "MachineDevices", validate: func(c ServiceConfig) error { return ValidateDeviceSettings(c.MachineDevices) }}, {Field: "FromWorkerConfigs"},
		{Field: "Version", validate: func(c ServiceConfig) error {
			if c.Version < 0 || c.Version > 1 {
				return errors.New("unsupported installed config version")
			}
			return nil
		}}, {Field: "WorkerUser"}, {Field: "WorkerHome"},
		{Field: "LocalDisableReceipts", validate: func(c ServiceConfig) error {
			if c.LocalDisableReceipts {
				return ErrLegacyLocalDisableReceipts
			}
			return nil
		}}, {Field: "OnboardingBenchmark"}, {Field: "Force"},
		{Field: "Path", validate: func(c ServiceConfig) error {
			if strings.ContainsAny(c.Path, "\x00\n\r\"\\") {
				return errors.New("PATH cannot contain newlines, quotes or backslashes for service capture")
			}
			return nil
		}}, {Field: "Role", validate: func(c ServiceConfig) error {
			if c.Role != "worker" && c.Role != "queue" {
				return errors.New("install requires worker or queue")
			}
			return nil
		}}, {Field: "Detection.OS"}, {Field: "Detection.Arch"}, {Field: "Detection.GPU"},
		installPath("", "Binary", "", func(c *ServiceConfig) *string { return &c.Binary }, true),
		installPath("", "Credentials", "", func(c *ServiceConfig) *string { return &c.Credentials }, true),
		installPath("", "Service", "", func(c *ServiceConfig) *string { return &c.Service }, true),
		installContext(installPath("config", "Config", "installed config path", func(c *ServiceConfig) *string { return &c.Config }, true), func(c ServiceConfig) error { return validateInstallDirective(c, c.GPUEnvironmentFile(), true) }),
	}
}

func RegisterInstallFlags(f *flag.FlagSet, values *ServiceConfig) {
	for _, s := range InstallSettings() {
		if s.Flag != "" {
			s.bind(f, values)
		}
	}
}

// OverlayInstallFlags applies only Visit's explicit flags, even empty/zero/false.
// The base is either the saved config or a fresh plan; it is never default-filled
// after overlay. On a usage error the returned config still contains the input.
func OverlayInstallFlags(f *flag.FlagSet, base, values ServiceConfig) (ServiceConfig, error) {
	explicit := map[string]bool{}
	f.Visit(func(v *flag.Flag) { explicit[v.Name] = true })
	fields := map[string]string{}
	for _, s := range InstallSettings() {
		if s.Flag == "" || !explicit[s.Flag] {
			continue
		}
		if previous := fields[s.Field]; previous != "" {
			return base, fmt.Errorf("usage: --%s and --%s select the same setting", previous, s.Flag)
		}
		fields[s.Field] = s.Flag
		s.copy(&base, values)
	}
	for _, s := range InstallSettings() {
		if s.Flag != "" && explicit[s.Flag] && s.normalize != nil {
			if err := s.normalize(&base); err != nil {
				return base, fmt.Errorf("usage: --%s: %w", s.Flag, err)
			}
		}
	}
	if err := base.ValidateInstall(); err != nil {
		return base, fmt.Errorf("usage: %w", err)
	}
	return base, nil
}

// Derived defaults belong to their setting and run only for a fresh plan.
func installStateSetting() InstallSetting {
	s := installPath("state-dir", "State", "local worker spool", func(c *ServiceConfig) *string { return &c.State }, true)
	s.fresh = func(c *ServiceConfig, v ServiceConfig) error {
		state, err := installAbsolute(v.State)
		if err != nil {
			return err
		}
		if state != "" {
			c.ModelCache = filepath.Join(state, "model-cache")
			c.EnvironmentCache = filepath.Join(state, "environment-cache")
			c.DB = filepath.Join(state, "myce.db")
			c.UploadSpoolDir = filepath.Join(state, "uploads")
		}
		return nil
	}
	return s
}
func installEnvironmentFileSetting() InstallSetting {
	s := installContext(installPath("environment-file", "EnvironmentFile", "private fleet EnvironmentFile reference", func(c *ServiceConfig) *string { return &c.EnvironmentFile }, false), func(c ServiceConfig) error { return validateInstallDirective(c, c.EnvironmentFile, false) })
	normalize := s.normalize
	s.normalize = func(c *ServiceConfig) error {
		// An omitted empty reference uses credentials.json. Explicitly clearing
		// the reference cannot provision those credentials on an upgrade.
		if c.EnvironmentFile == "" {
			return errors.New("path must not be empty; omit --environment-file to preserve the credential source")
		}
		return normalize(c)
	}
	return s
}
func installEnvironmentCatalogSetting() InstallSetting {
	s := installPath("environment-config", "EnvironmentCatalog", "NAS environment catalog", func(c *ServiceConfig) *string { return &c.EnvironmentCatalog }, false)
	s.fresh = func(c *ServiceConfig, v ServiceConfig) error {
		catalog, err := installAbsolute(v.EnvironmentCatalog)
		if err != nil {
			return err
		}
		if catalog != "" {
			c.EnvironmentRoot = filepath.Dir(catalog)
		}
		return nil
	}
	return s
}
func FreshInstallDefaults(c *ServiceConfig, values ServiceConfig) error {
	for _, s := range InstallSettings() {
		if s.fresh != nil {
			if err := s.fresh(c, values); err != nil {
				return err
			}
		}
	}
	return nil
}

func installContext(s InstallSetting, valid func(ServiceConfig) error) InstallSetting {
	fieldValid := s.validate
	s.validate = func(c ServiceConfig) error {
		if fieldValid != nil {
			if err := fieldValid(c); err != nil {
				return err
			}
		}
		return valid(c)
	}
	return s
}
func validateInstallDirective(c ServiceConfig, path string, workerOnly bool) error {
	if c.Detection.OS != "linux" || workerOnly && c.Role != "worker" {
		return nil
	}
	if strings.ContainsAny(path, "'\"\\") || strings.IndexFunc(path, unicode.IsSpace) >= 0 {
		return errors.New("systemd directive paths cannot contain whitespace, quotes or backslashes")
	}
	return nil
}

func installNASSetting() InstallSetting {
	s := installPath("nas", "NAS", "local NAS mount root", func(c *ServiceConfig) *string { return &c.NAS }, true)
	s.fresh = func(c *ServiceConfig, v ServiceConfig) error {
		nas, err := installAbsolute(v.NAS)
		if err != nil {
			return err
		}
		c.Checkpoints = filepath.Join(nas, "mycelium", "checkpoints")
		c.ModelRoot = filepath.Join(nas, "mycelium", "models")
		c.EnvironmentRoot = filepath.Join(nas, "mycelium", "environments")
		return nil
	}
	return s
}
func installAbsolute(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	return filepath.Abs(path)
}

func installNonnegativeInt(v int) error {
	if v < 0 {
		return errors.New("must be nonnegative")
	}
	return nil
}
