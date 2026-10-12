package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Detection struct {
	OS         string  `json:"os"`
	Arch       string  `json:"arch"`
	Platform   string  `json:"platform"`
	MemoryMode string  `json:"memory_mode"`
	MemoryGB   float64 `json:"memory_gb"`
	ReserveGB  float64 `json:"reserve_memory_gb"`
	GPU        string  `json:"gpu"`
}
type MachineProbe struct {
	OS, Arch string
	Command  func(context.Context, string, ...string) ([]byte, error)
}

func LocalMachineProbe() MachineProbe {
	return MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH, Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, n, args...).Output()
	}}
}
func (p MachineProbe) Detect(ctx context.Context) (Detection, error) {
	return p.DetectDevice(ctx, "")
}

// DetectDevice shares setup detection while allowing foreground workers to
// inspect a selected CUDA card instead of requiring a single-card host.
func (p MachineProbe) DetectDevice(ctx context.Context, device string) (Detection, error) {
	d := Detection{OS: p.OS, Arch: p.Arch, MemoryMode: "auto"}
	if p.OS != "linux" && p.OS != "darwin" {
		return d, fmt.Errorf("unsupported OS %s", p.OS)
	}
	if p.Arch != "amd64" && p.Arch != "arm64" {
		return d, fmt.Errorf("unsupported architecture %s", p.Arch)
	}
	if p.Command == nil {
		return d, errors.New("GPU detection command runner missing")
	}
	if p.OS == "darwin" && p.Arch == "arm64" {
		b, e := p.Command(ctx, "system_profiler", "SPDisplaysDataType")
		if e != nil || !strings.Contains(string(b), "Metal") {
			return d, errors.New("Apple GPU/Metal not visible")
		}
		d.Platform = "metal"
		d.GPU = "Apple Silicon"
		d.MemoryMode = "unified"
		d.ReserveGB = 4
		return d, nil
	}
	args := []string{"--query-gpu=name,memory.total", "--format=csv,noheader,nounits"}
	if device != "" {
		args = append(args, "--id="+device)
	}
	b, e := p.Command(ctx, "nvidia-smi", args...)
	if e == nil && strings.TrimSpace(string(b)) != "" {
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(lines) != 1 {
			return d, errors.New("automatic setup requires exactly one GPU")
		}
		name, memory, ok := strings.Cut(lines[0], ",")
		if !ok {
			return d, errors.New("invalid nvidia-smi inventory")
		}
		d.Platform = "cuda"
		d.GPU = strings.TrimSpace(name)
		d.ReserveGB = 2
		n, e := parseCUDAFreeMemory([]byte(strings.TrimSpace(memory)))
		if errors.Is(e, errUnifiedMemory) || strings.Contains(strings.ToUpper(name), "GB10") {
			d.MemoryMode = "unified"
			d.ReserveGB = 16
			return d, nil
		}
		if e != nil || n <= 0 {
			return d, errors.New("invalid CUDA memory inventory")
		}
		d.MemoryGB = n
		d.MemoryMode = "dedicated"
		return d, nil
	}
	b, e = p.Command(ctx, "sycl-ls")
	if e != nil {
		b, e = p.Command(ctx, "xpu-smi", "discovery")
	}
	intelGPU := ""
	if e == nil {
		for _, line := range strings.Split(string(b), "\n") {
			low := strings.ToLower(line)
			if strings.Contains(low, "gpu") && strings.Contains(low, "intel") {
				intelGPU = line
				break
			}
		}
	}
	if intelGPU != "" {
		// Integrated XPU uses OS memory. Discrete XPU needs a measured allowance:
		// do not silently substitute host RAM for its device memory.
		low := strings.ToLower(intelGPU)
		if !strings.Contains(low, "integrated") && !strings.Contains(low, "uhd") && !strings.Contains(low, "iris") {
			verbose, err := p.Command(ctx, "sycl-ls", "--verbose")
			if err == nil {
				budget := uint64(0)
				for _, line := range strings.Split(string(verbose), "\n") {
					if !strings.Contains(line, "global_mem_size") && !strings.Contains(strings.ToLower(line), "global memory size") {
						continue
					}
					_, value, ok := strings.Cut(line, ":")
					if !ok {
						continue
					}
					fields := strings.Fields(value)
					if len(fields) == 0 {
						continue
					}
					bytes, err := strconv.ParseUint(fields[0], 10, 64)
					if err == nil && bytes > 0 {
						if budget == 0 || bytes < budget {
							budget = bytes
						}
					}

				}
				if budget > 0 {
					d.Platform = "xpu"
					d.GPU = strings.TrimSpace(intelGPU)
					d.MemoryMode = "fixed"
					d.MemoryGB = float64(budget) / (1024 * 1024 * 1024)
					d.ReserveGB = min(2, d.MemoryGB/4)
					return d, nil
				}
			}
			return d, errors.New("discrete Intel GPU memory unavailable: specify --gpu xpu --memory-mode fixed --memory-gb with an explicit device budget")
		}
		d.Platform = "xpu"
		d.GPU = strings.TrimSpace(intelGPU)
		d.MemoryMode = "unified"
		d.ReserveGB = 8
		return d, nil
	}
	return d, errors.New("no supported GPU visible (nvidia-smi, sycl-ls/xpu-smi, or Apple Silicon Metal)")
}

type ServiceConfig struct {
	Device                string    `json:"device,omitempty"`
	HostStateDir          string    `json:"host_state_dir,omitempty"`
	Path                  string    `json:"path"`
	EnvironmentRoot       string    `json:"environment_root"`
	Role                  string    `json:"role"`
	Detection             Detection `json:"detection"`
	Name                  string    `json:"name"`
	Queue                 string    `json:"queue"`
	Listen                string    `json:"listen"`
	NAS                   string    `json:"nas"`
	Checkpoints           string    `json:"checkpoint_root"`
	ModelRoot             string    `json:"model_root"`
	ModelCatalog          string    `json:"model_catalog,omitempty"`
	EnvironmentCatalog    string    `json:"environment_catalog,omitempty"`
	State                 string    `json:"state_dir"`
	ModelCache            string    `json:"model_cache"`
	EnvironmentCache      string    `json:"environment_cache"`
	ModelCacheBytes       int64     `json:"model_cache_bytes"`
	EnvironmentCacheBytes int64     `json:"environment_cache_bytes"`
	Binary                string    `json:"binary"`
	Credentials           string    `json:"credentials"`
	Service               string    `json:"service"`
	Config                string    `json:"config"`
}

func PlanInstall(role, home, root, nas, queue, name string, d Detection) (ServiceConfig, error) {
	c := ServiceConfig{Role: role, Detection: d, Name: name, Queue: queue, Listen: "127.0.0.1:7331", NAS: nas}
	if role != "worker" && role != "queue" {
		return c, errors.New("install requires worker or queue")
	}
	if d.OS != "linux" && d.OS != "darwin" {
		return c, errors.New("only Linux systemd user and macOS launchd supported")
	}
	if role == "worker" && !validGPU(d.Platform) {
		return c, errors.New("worker GPU detection required")
	}
	u, e := url.Parse(queue)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return c, errors.New("queue must be an HTTP URL")
	}
	if !filepath.IsAbs(home) || !filepath.IsAbs(nas) {
		return c, errors.New("home and NAS root must be absolute paths")
	}
	if root == "" {
		root = filepath.Join(home, ".local", "share", "mycelium")
	}
	if !filepath.IsAbs(root) {
		return c, errors.New("install root must be absolute")
	}
	c.Config = filepath.Join(root, role, "config.json")
	c.Binary = filepath.Join(root, role, "myce")
	c.Credentials = filepath.Join(root, role, "credentials.json")
	c.State = filepath.Join(root, role, "state")
	c.ModelCache = filepath.Join(c.State, "model-cache")
	c.EnvironmentCache = filepath.Join(c.State, "environment-cache")
	c.ModelCacheBytes = 100 * 1024 * 1024 * 1024
	c.EnvironmentCacheBytes = 20 * 1024 * 1024 * 1024
	c.Checkpoints = filepath.Join(nas, "mycelium", "checkpoints")
	c.ModelRoot = filepath.Join(nas, "models")
	c.EnvironmentRoot = filepath.Join(nas, "mycelium", "environments")
	c.Path = filepath.Join(home, ".local", "bin") + ":" + filepath.Join(home, ".cargo", "bin") + ":/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
	if d.OS == "linux" {
		c.Service = filepath.Join(home, ".config", "systemd", "user", "myce-"+role+".service")
	} else {
		c.Service = filepath.Join(home, "Library", "LaunchAgents", "local.mycelium."+role+".plist")
	}
	return c, nil
}
func (c *ServiceConfig) WorkerInstance(instance string) error {
	unit := "myce-worker@" + instance + ".service"
	if c.Role != "worker" || c.Detection.OS != "linux" || !ValidWorkerUnit(unit) {
		return errors.New("instance requires Linux worker and letters, digits, dash or underscore")
	}
	old := filepath.Dir(c.Config)
	root := filepath.Join(filepath.Dir(old), "worker-"+instance)
	for _, p := range []*string{&c.Config, &c.Binary, &c.Credentials, &c.State, &c.ModelCache, &c.EnvironmentCache} {
		rel, err := filepath.Rel(old, *p)
		if err != nil {
			return err
		}
		*p = filepath.Join(root, rel)
	}
	c.Service = filepath.Join(filepath.Dir(c.Service), unit)
	return nil
}
func unitQuote(v string) string { v = strings.ReplaceAll(v, "%", "%%"); return strconv.Quote(v) }

// Condition paths consume the whole value, not ExecStart's quoted argument syntax.
func unitConditionPath(v string) string {
	return strings.NewReplacer("%", "%%", "\n", "\\x0a", "\r", "\\x0d").Replace(v)
}
func (c ServiceConfig) ServiceText() string {
	if c.Detection.OS == "linux" {
		condition := ""
		stopSettings := ""
		if c.Role == "worker" {
			condition = "ConditionPathExists=!%h/.local/share/myce/DISABLED\n"
			if c.HostStateDir != "" {
				condition = "ConditionPathExists=!" + unitConditionPath(filepath.Join(c.HostStateDir, "DISABLED")) + "\n"
			}
			if ValidWorkerUnit(filepath.Base(c.Service)) {
				dir := c.HostStateDir
				if dir == "" {
					condition += "ConditionPathExists=!%h/.local/share/myce/units/" + filepath.Base(c.Service) + "/DISABLED\n"
				} else {
					scoped, _ := WorkerUnitDirectory(dir, filepath.Base(c.Service))
					condition += "ConditionPathExists=!" + unitConditionPath(filepath.Join(scoped, "DISABLED")) + "\n"
				}
			}
			stopSettings = "KillMode=control-group\nSendSIGKILL=yes\nTimeoutStopSec=10s\n"
		}
		return "[Unit]\n" + condition + "Description=Mycelium " + c.Role + "\nAfter=network-online.target\n\n[Service]\nExecStart=" + unitQuote(c.Binary) + " service --config " + unitQuote(c.Config) + "\n" + stopSettings + "Restart=on-failure\nRestartSec=3\nUMask=0077\n\n[Install]\nWantedBy=default.target\n"
	}
	vals := []string{c.Binary, "service", "--config", c.Config}
	args := ""
	for _, v := range vals {
		args += "<string>" + html.EscapeString(v) + "</string>"
	}
	return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>local.mycelium." + c.Role + "</string><key>ProgramArguments</key><array>" + args + "</array><key>RunAtLoad</key><true/><key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict><key>StandardOutPath</key><string>" + html.EscapeString(filepath.Join(filepath.Dir(c.Config), "service.log")) + "</string><key>StandardErrorPath</key><string>" + html.EscapeString(filepath.Join(filepath.Dir(c.Config), "service.log")) + "</string></dict></plist>\n"
}

// Install is only called by an explicit install invocation. Tests use plans or
// inject a no-op service runner and temp home; no test installs a boot service.
func Install(ctx context.Context, c ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error)) error {
	if token == "" {
		return errors.New("set MYCE_TOKEN before installing")
	}
	if command == nil {
		return errors.New("service runner unavailable")
	}
	if c.Role == "worker" && c.Detection.OS == "linux" {
		if _, e := os.Lstat(c.Config); e == nil {
			existing, e := ReadServiceConfig(c.Config)
			if e != nil {
				return e
			}
			if existing.Role != "worker" || existing.Detection.OS != "linux" || existing.Config != c.Config || existing.Service != c.Service {
				return errors.New("existing worker configuration does not match upgrade")
			}
			st, e := os.Lstat(c.Service)
			if e != nil {
				return e
			}
			if !st.Mode().IsRegular() {
				return errors.New("worker unit must be a regular file")
			}
			if e = saveServiceText(existing); e != nil {
				return e
			}
			_, e = command(ctx, "systemctl", "--user", "daemon-reload")
			return e
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	for _, p := range []string{c.Config, c.Binary, c.Credentials, c.Service} {
		if _, e := os.Lstat(p); e == nil {
			return fmt.Errorf("refusing to overwrite %s; uninstall the existing role first", p)
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	for _, p := range []string{filepath.Dir(c.Config), filepath.Dir(c.Service), c.State} {
		if e := os.MkdirAll(p, 0700); e != nil {
			return e
		}
	}
	b, e := os.ReadFile(executable)
	if e != nil {
		return e
	}
	if e = os.WriteFile(c.Binary, b, 0700); e != nil {
		return e
	}
	if e = saveWorkerJSON(filepath.Dir(c.Config), filepath.Base(c.Config), c); e != nil {
		return e
	}
	if e = saveWorkerJSON(filepath.Dir(c.Credentials), filepath.Base(c.Credentials), map[string]string{"token": token}); e != nil {
		return e
	}
	if e = os.WriteFile(c.Service, []byte(c.ServiceText()), 0600); e != nil {
		return e
	}
	if c.Detection.OS == "linux" {
		if _, e = command(ctx, "systemctl", "--user", "daemon-reload"); e != nil {
			return e
		}
		_, e = command(ctx, "systemctl", "--user", "enable", "--now", filepath.Base(c.Service))
	} else {
		_, e = command(ctx, "launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), c.Service)
	}
	return e
}
func ReadServiceConfig(path string) (ServiceConfig, error) {
	var c ServiceConfig
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	return c, e
}
func Uninstall(ctx context.Context, c ServiceConfig, dry bool, command func(context.Context, string, ...string) ([]byte, error)) error {
	if dry {
		return nil
	}
	if command == nil {
		return errors.New("service runner unavailable")
	}
	var e error
	if c.Detection.OS == "linux" {
		_, e = command(ctx, "systemctl", "--user", "disable", "--now", filepath.Base(c.Service))
	} else {
		_, e = command(ctx, "launchctl", "bootout", "gui/"+strconv.Itoa(os.Getuid()), c.Service)
	}
	if e != nil {
		return fmt.Errorf("stop service before removing setup: %w", e)
	}
	// Preserve binary, private credentials, spools, cache and config together.
	trash := filepath.Join(filepath.Dir(filepath.Dir(c.Config)), ".Trash")
	if e = os.MkdirAll(trash, 0700); e != nil {
		return e
	}
	if e = os.Rename(c.Service, filepath.Join(trash, ID()+"-"+filepath.Base(c.Service))); e != nil {
		return e
	}
	dst := filepath.Join(trash, ID()+"-"+c.Role)
	if e = os.Rename(filepath.Dir(c.Config), dst); e != nil {
		return e
	}
	if _, e = os.Stat(filepath.Join(dst, "config.json")); e != nil {
		return e
	}
	if c.Detection.OS == "linux" {
		_, e = command(ctx, "systemctl", "--user", "daemon-reload")
	}
	return e
}

type DoctorCheck struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}
type DoctorProbe struct {
	Machine MachineProbe
	NAS     func(context.Context, string) error
	Disk    func(string) (int64, error)
	Queue   func(context.Context, ServiceConfig) error
}

func LocalDoctorProbe(token string) DoctorProbe {
	return LocalDoctorProbeWithMachine(token, LocalMachineProbe())
}

func LocalDoctorProbeWithMachine(token string, machine MachineProbe) DoctorProbe {
	return DoctorProbe{Machine: machine, NAS: func(ctx context.Context, path string) error {
		if machine.Command == nil {
			return errors.New("command probe missing")
		}
		b, e := machine.Command(ctx, "mount")
		if e != nil {
			return e
		}
		mounted := false
		for _, line := range strings.Split(string(b), "\n") {
			_, rest, ok := strings.Cut(line, " on ")
			if !ok {
				continue
			}
			mount, _, ok := strings.Cut(rest, " type ")
			if !ok {
				mount, _, ok = strings.Cut(rest, " (")
			}
			if ok && (path == mount || strings.HasPrefix(path, mount+string(filepath.Separator))) && (strings.Contains(line, "nfs") || strings.Contains(line, "smbfs") || strings.Contains(line, "cifs")) {
				mounted = true
			}
		}
		if !mounted {
			return errors.New("NAS path is not on a visible NFS/SMB mount")
		}
		f, e := os.CreateTemp(path, ".myce-doctor-")
		if e != nil {
			return e
		}
		name := f.Name()
		e = f.Sync()
		e = errors.Join(e, f.Close())
		trash := filepath.Join(path, ".Trash")
		if err := os.MkdirAll(trash, 0700); err != nil {
			return err
		}
		return errors.Join(e, os.Rename(name, filepath.Join(trash, filepath.Base(name))))
	}, Disk: func(path string) (int64, error) {
		for {
			var st syscall.Statfs_t
			e := syscall.Statfs(path, &st)
			if e == nil {
				return int64(st.Bavail) * int64(st.Bsize), nil
			}
			parent := filepath.Dir(path)
			if parent == path {
				return 0, e
			}
			path = parent
		}
	}, Queue: func(ctx context.Context, c ServiceConfig) error {
		return (Client{URL: c.Queue, Token: token}).Call(ctx, "GET", "/api/status", nil, &Snapshot{})
	}}
}
func Doctor(ctx context.Context, c ServiceConfig, p DoctorProbe) []DoctorCheck {
	out := []DoctorCheck{}
	add := func(name string, e error, detail string) {
		if e != nil {
			detail = e.Error()
		}
		out = append(out, DoctorCheck{Name: name, OK: e == nil, Detail: detail})
	}
	if c.Role == "worker" {
		d, e := p.Machine.DetectDevice(ctx, c.Device)
		if e == nil && d.Platform != c.Detection.Platform {
			e = fmt.Errorf("detected %s; configured %s", d.Platform, c.Detection.Platform)
		}
		add("GPU visible", e, d.Platform)
	}
	if p.NAS == nil {
		add("NAS mounted and writable", errors.New("NAS probe missing"), "")
	} else {
		add("NAS mounted and writable", p.NAS(ctx, c.NAS), c.NAS)
	}
	if p.Queue == nil {
		add("queue reachable", errors.New("queue probe missing"), "")
	} else {
		add("queue reachable", p.Queue(ctx, c), c.Queue)
	}
	if p.Machine.Command == nil {
		add("uv present", errors.New("command probe missing"), "")
	} else {
		b, e := p.Machine.Command(ctx, "uv", "--version")
		add("uv present", e, strings.TrimSpace(string(b)))
	}
	if c.Detection.OS == "linux" {
		var e error
		detail := "enabled; user service starts at boot and survives logout"
		if p.Machine.Command == nil {
			e = errors.New("command probe missing")
		} else {
			uid := strconv.Itoa(os.Getuid())
			b, err := p.Machine.Command(ctx, "loginctl", "show-user", uid, "--property=Linger", "--value")
			switch {
			case err != nil:
				e = fmt.Errorf("cannot inspect user lingering: %w", err)
			case strings.TrimSpace(string(b)) == "yes":
			case strings.TrimSpace(string(b)) == "no":
				e = fmt.Errorf("disabled; starts only at login and may stop at logout; an administrator must run loginctl enable-linger %s for boot without login", uid)
			default:
				e = fmt.Errorf("cannot inspect user lingering: unexpected loginctl result %q", strings.TrimSpace(string(b)))
			}
		}
		add("Linux user lingering", e, detail)
	}
	if p.Disk == nil {
		add("disk space", errors.New("disk probe missing"), "")
	} else {
		free, e := p.Disk(c.State)
		if e == nil && free < 1024*1024*1024 {
			e = fmt.Errorf("only %d bytes free; need at least 1 GiB", free)
		}
		add("disk space", e, fmt.Sprintf("%d bytes free", free))
	}
	return out
}
func ReadServiceToken(c ServiceConfig) (string, error) {
	st, e := os.Stat(c.Credentials)
	if e != nil {
		return "", e
	}
	if st.Mode().Perm()&0077 != 0 {
		return "", errors.New("credentials file must be private (0600)")
	}
	b, e := os.ReadFile(c.Credentials)
	if e != nil {
		return "", e
	}
	var secret struct {
		Token string `json:"token"`
	}
	if e = json.Unmarshal(b, &secret); e != nil {
		return "", e
	}
	if secret.Token == "" {
		return "", errors.New("missing service token")
	}
	return secret.Token, nil
}
func (c ServiceConfig) Args() []string {
	args := []string{}
	if c.Role == "queue" {
		args = []string{"queue", "--listen", c.Listen, "--db", filepath.Join(c.State, "myce.db"), "--checkpoints", c.Checkpoints}
	} else {
		args = []string{"join", "--name", c.Name, "--queue", c.Queue, "--gpu", c.Detection.Platform, "--memory-mode", c.Detection.MemoryMode, "--memory-gb", strconv.FormatFloat(c.Detection.MemoryGB, 'f', -1, 64), "--reserve-memory-gb", strconv.FormatFloat(c.Detection.ReserveGB, 'f', -1, 64), "--state-dir", c.State, "--checkpoint-root", c.Checkpoints, "--model-root", c.ModelRoot, "--model-cache", c.ModelCache, "--model-cache-bytes", strconv.FormatInt(c.ModelCacheBytes, 10), "--environment-root", c.EnvironmentRoot, "--environment-cache", c.EnvironmentCache, "--environment-cache-bytes", strconv.FormatInt(c.EnvironmentCacheBytes, 10)}
	}
	if c.Role == "worker" {
		if c.HostStateDir != "" {
			args = append(args, "--host-state-dir", c.HostStateDir)
		}
		if c.Device != "" {
			args = append(args, "--device", c.Device)
		}
	}
	if c.ModelCatalog != "" {
		args = append(args, "--model-config", c.ModelCatalog)
	}
	if c.EnvironmentCatalog != "" {
		args = append(args, "--environment-config", c.EnvironmentCatalog)
	}
	return args
}
func SetupTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 15*time.Second)
}

func saveServiceText(c ServiceConfig) error {
	f, e := os.CreateTemp(filepath.Dir(c.Service), ".myce-unit-")
	if e != nil {
		return e
	}
	if e = f.Chmod(0600); e != nil {
		f.Close()
		return e
	}
	_, e = f.WriteString(c.ServiceText())
	e = errors.Join(e, f.Sync(), f.Close())
	if e != nil {
		return e
	}
	trash := filepath.Join(filepath.Dir(c.Service), ".Trash")
	if e = os.MkdirAll(trash, 0700); e != nil {
		return e
	}
	backup := filepath.Join(trash, ID()+"-"+filepath.Base(c.Service))
	if e = os.Rename(c.Service, backup); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), c.Service); e != nil {
		return errors.Join(e, os.Rename(backup, c.Service))
	}
	return syncWorkerDir(filepath.Dir(c.Service))
}
