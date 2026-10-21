package myce

import (
	"bytes"
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

var ErrNoGPU = errors.New("no supported GPU visible")

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
	Info     func(string) // Optional informational diagnostics; never inventory output.
	Command  func(context.Context, string, ...string) ([]byte, error)
	// CommandEnv runs a probe with per-process overrides; nil keeps injected Command fixtures.
	CommandEnv func(context.Context, map[string]string, string, ...string) ([]byte, error)
	Glob       func(string) ([]string, error)
	ReadFile   func(string) ([]byte, error)
}

func LocalMachineProbe() MachineProbe {
	run := func(ctx context.Context, env map[string]string, n string, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, n, args...)
		cmd.Env = withoutMyceToken(os.Environ())
		for key, value := range env {
			if key == "MYCE_TOKEN" {
				continue
			}
			cmd.Env = append(cmd.Env, key+"="+value)
		}
		var stdout, stderr boundedProbeOutput
		stdout.limit, stderr.limit = 1<<20, 4096
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			exit.Stderr = stderr.Bytes()
		}
		return stdout.Bytes(), err
	}
	return MachineProbe{Info: func(line string) { fmt.Fprintln(os.Stderr, line) }, OS: runtime.GOOS, Arch: runtime.GOARCH, Glob: filepath.Glob, ReadFile: os.ReadFile, CommandEnv: run, Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
		return run(ctx, nil, n, args...)
	}}
}
func (p MachineProbe) Detect(ctx context.Context) (Detection, error) {
	return p.DetectDevice(ctx, "")
}

// DetectDevice shares setup detection while allowing foreground workers to
// inspect a selected CUDA card instead of requiring a single-card host.
func (p MachineProbe) DetectDevice(ctx context.Context, device string) (Detection, error) {
	if err := singleDeviceMask(device); err != nil {
		return Detection{}, err
	}
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
		b, e := p.Command(ctx, "/usr/sbin/system_profiler", "SPDisplaysDataType")
		if e != nil && !probeAbsent(e) {
			return d, preflightError("gpu_driver_unavailable", "metal", "cannot inspect Metal; repair the runtime and re-detect")
		}
		if e != nil || !strings.Contains(string(b), "Metal") {
			return d, preflightError("gpu_driver_unavailable", "metal", "Apple Silicon Metal is unavailable; repair runtime access and re-detect")
		}
		d.Platform = "metal"
		d.GPU = "Apple Silicon"
		d.MemoryMode = "unified"
		d.ReserveGB = 4
		return d, nil
	}
	nvidia, pciErr := p.nvidiaPresent()
	if pciErr != nil {
		return d, preflightError("gpu_inventory_unavailable", "gpu", "cannot inspect PCI inventory; restore access and re-detect")
	}
	args := []string{"--query-gpu=name,memory.total", "--format=csv,noheader,nounits"}
	if device != "" {
		args = append(args, "--id="+device)
	}
	b, e := p.Command(ctx, "nvidia-smi", args...)
	if e != nil && !probeAbsent(e) {
		if err := p.nvidiaProbeFailure(nvidia); err != nil {
			return d, err
		}
	}
	if e == nil && strings.TrimSpace(string(b)) != "" {
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(lines) != 1 {
			return d, errors.New("automatic setup requires exactly one GPU")
		}
		name, memory, ok := strings.Cut(lines[0], ",")
		if !ok {
			return d, preflightError("gpu_inventory_invalid", "cuda", "invalid nvidia-smi inventory")
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
			return d, preflightError("gpu_memory_unavailable", "cuda", "invalid CUDA memory inventory")
		}
		d.MemoryGB = n
		d.MemoryMode = "dedicated"
		return d, nil
	}
	sycl := p.Command
	if device != "" && p.CommandEnv != nil {
		bound, err := WithModelDevice(Model{}, "xpu", device)
		if err != nil {
			return d, err
		}
		sycl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
			return p.CommandEnv(ctx, bound.DeviceEnv, name, args...)
		}
	}
	b, e = sycl(ctx, "sycl-ls")
	if e != nil {
		if !probeAbsent(e) {
			return d, preflightError("gpu_driver_unavailable", "xpu", "Intel driver probe failed; repair the runtime and re-detect")
		}
		b, e = p.Command(ctx, "xpu-smi", "discovery")
		if e != nil && !probeAbsent(e) {
			return d, preflightError("gpu_driver_unavailable", "xpu", "Intel driver probe failed; repair the runtime and re-detect")
		}
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
		d.Platform, d.GPU = "xpu", strings.TrimSpace(intelGPU)
		// Integrated XPU uses OS memory. Discrete XPU needs a measured allowance:
		// do not silently substitute host RAM for its device memory.
		low := strings.ToLower(intelGPU)
		if !strings.Contains(low, "integrated") && !strings.Contains(low, "uhd") && !strings.Contains(low, "iris") {
			verbose, err := sycl(ctx, "sycl-ls", "--verbose")
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
			return d, preflightError("gpu_memory_unavailable", "xpu", "discrete Intel GPU memory unavailable: specify --gpu xpu --memory-mode fixed --memory-gb with an explicit device budget")
		}
		d.Platform = "xpu"
		d.GPU = strings.TrimSpace(intelGPU)
		d.MemoryMode = "unified"
		d.ReserveGB = 8
		return d, nil
	}
	present, err := p.gpuPresent()
	if err != nil {
		return d, preflightError("gpu_inventory_unavailable", "gpu", "cannot inspect PCI inventory; restore access and re-detect")
	}
	if present {
		return d, preflightError("gpu_driver_missing", "gpu", "supported GPU has no runtime inventory; install its driver and re-detect")
	}
	return d, fmt.Errorf("%w (nvidia-smi, sycl-ls/xpu-smi, or Apple Silicon Metal)", ErrNoGPU)
}

type ServiceConfig struct {
	DailyUse          bool            `json:"daily_use"`
	Laptop            bool            `json:"laptop"`
	Desktop           bool            `json:"desktop"`
	MachineDevices    []DeviceInstall `json:"machine_devices,omitempty"`
	FromWorkerConfigs []string        `json:"-"`

	ProductionInfrastructureRetries int `json:"production_infrastructure_retries"`

	Version int `json:"version"`

	DockerEndpoint        string         `json:"docker_endpoint"`
	WorkerUser            string         `json:"worker_user"`
	WorkerHome            string         `json:"worker_home"`
	LocalDisableReceipts  bool           `json:"local_disable_receipts"`
	OnboardingBenchmark   bool           `json:"onboarding_benchmark"`
	JoiningFile           string         `json:"joining_file"`
	EnvironmentFile       string         `json:"environment_file"`
	HostID                string         `json:"host_id"`
	DB                    string         `json:"db"`
	ProjectConfig         string         `json:"project_config"`
	HostConfig            string         `json:"host_config"`
	UploadSpoolDir        string         `json:"upload_spool_dir"`
	UploadSpoolBytes      int64          `json:"upload_spool_bytes"`
	ContainerRetention    *time.Duration `json:"container_retention"`
	LogRetention          time.Duration  `json:"log_retention"`
	Heartbeat             time.Duration  `json:"heartbeat"`
	RestartGrace          time.Duration  `json:"restart_grace"`
	ProductionLossTimeout time.Duration  `json:"production_loss_timeout"`
	DeadlineLossTimeout   time.Duration  `json:"deadline_loss_timeout"`
	ExperimentLossTimeout time.Duration  `json:"experiment_loss_timeout"`
	ReserveHostMemoryGB   float64        `json:"reserve_host_memory_gb"`
	PrivateDevices        bool           `json:"private_devices"`
	NoNewPrivileges       bool           `json:"no_new_privileges"`
	Nice                  int            `json:"nice"`
	CPUQuota              string         `json:"cpu_quota"`
	MemoryMax             string         `json:"memory_max"`
	TasksMax              string         `json:"tasks_max"`

	RequireMemoryCeilings bool    `json:"require_memory_ceilings"`
	RequireMountedNAS     bool    `json:"require_mounted_nas"`
	Force                 bool    `json:"-"`
	CPUReserveCores       int     `json:"cpu_reserve_cores"`
	CPUBudgetCores        int     `json:"cpu_budget_cores"`
	HostMemoryBudgetGB    float64 `json:"host_memory_budget_gb"`
	MemoryCeilingSlackGB  float64 `json:"memory_ceiling_slack_gb"`
	MemorySwapMaxBytes    int64   `json:"memory_swap_max_bytes"`
	AllowDeviceless       bool    `json:"allow_deviceless"`
	Device                string  `json:"device"`

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
	ModelCatalog          string    `json:"model_catalog"`
	EnvironmentCatalog    string    `json:"environment_catalog"`
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

// PlanInstall is pure. The command boundary supplies the OS account; omitted
// accounts support synthetic plans without consulting the host account database.
func PlanInstall(role, home, root, nas, queue, name string, d Detection, account ...WorkerAccount) (ServiceConfig, error) {
	c := ServiceConfig{Role: role, Detection: d, Name: name, Queue: queue, Listen: "127.0.0.1:7331", NAS: nas}
	c.Version = 1
	if role == "worker" {
		c.WorkerHome = home
		if len(account) == 1 && account[0].Home == home {
			c.WorkerUser = account[0].Name
		}
	}
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
	c.ModelRoot = filepath.Join(nas, "mycelium", "models")
	c.DB = filepath.Join(c.State, "myce.db")

	c.HostID, _ = os.Hostname()
	c.ReserveHostMemoryGB = 4
	c.MemoryCeilingSlackGB = .125
	c.Heartbeat = time.Second
	c.LogRetention = DefaultLogRetention
	c.RestartGrace = RestartGrace
	c.ProductionInfrastructureRetries = DefaultProductionInfrastructureRetries
	c.ProductionLossTimeout, c.DeadlineLossTimeout, c.ExperimentLossTimeout = ProductionLossAfter, DeadlineLossAfter, LossAfter
	c.UploadSpoolDir = filepath.Join(c.State, "uploads")
	c.UploadSpoolBytes = DefaultUploadSpoolBytes
	c.PrivateDevices, c.NoNewPrivileges = false, true
	c.CPUReserveCores = 1
	c.Nice, c.CPUQuota, c.MemoryMax, c.TasksMax = 10, "25%", "512M", "64"
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
	for _, p := range []*string{&c.Config, &c.Binary, &c.Credentials, &c.State, &c.ModelCache, &c.EnvironmentCache, &c.DB, &c.UploadSpoolDir} {
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
		stopSettings := ""
		if c.Role == "worker" {
			stopSettings = "EnvironmentFile=" + unitConditionPath(c.GPUEnvironmentFile()) + "\nDelegate=memory\nKillMode=control-group\nSendSIGKILL=yes\nTimeoutStopSec=10s\n"
		}
		settings := ""
		if c.EnvironmentFile != "" {
			settings += "EnvironmentFile=" + unitConditionPath(c.EnvironmentFile) + "\n"
		}
		if c.Role == "queue" {
			yesno := func(v bool) string {
				if v {
					return "yes"
				}
				return "no"
			}
			settings += fmt.Sprintf("Environment=GOMAXPROCS=1\nPrivateDevices=%s\nNoNewPrivileges=%s\nNice=%d\nCPUQuota=%s\nMemoryMax=%s\nTasksMax=%s\nTimeoutStopSec=150\n", yesno(c.PrivateDevices), yesno(c.NoNewPrivileges), c.Nice, c.CPUQuota, c.MemoryMax, c.TasksMax)
		}
		if c.RequireMemoryCeilings {
			settings += "RestartPreventExitStatus=78\n"
		}
		argv := ""
		for _, arg := range c.Args() {
			argv += " " + unitQuote(arg)
		}
		return "[Unit]\nDescription=Mycelium " + c.Role + "\nAfter=network-online.target\n\n[Service]\n" + "ExecStart=:" + unitQuote(c.Binary) + " service --config " + unitQuote(c.Config) + " --" + argv + "\n" + settings + stopSettings + "Restart=on-failure\nRestartSec=3\nUMask=0077\n\n[Install]\nWantedBy=default.target\n"
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
	return installWithFiles(ctx, c, executable, token, command, installFiles{})
}

func installWithFiles(ctx context.Context, c ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error), files installFiles) (result error) {
	if os.Geteuid() == 0 {
		return ErrHostIdentity
	}
	if c.Detection.OS == "linux" && (c.Role == "worker" && !ValidWorkerUnit(filepath.Base(c.Service)) || c.Role != "worker" && (c.Role != "queue" || filepath.Base(c.Service) != "myce-queue.service")) {
		return errors.New("refusing non-Mycelium service unit")
	}

	if !files.machineConversion {
		if err := CheckMachineInstallGate(c); err != nil {
			return err
		}
	}
	if err := c.ValidateInstallPolicies(); err != nil {
		return err
	}

	roots, err := c.StateRoots()
	if err != nil {
		return err
	}
	if err := CheckStateRoots(roots...); err != nil {
		return err
	}
	if command == nil {
		return errors.New("service runner unavailable")
	}
	if err := c.ValidateInstall(); err != nil {
		return err
	}
	c.Version = 1
	existing := false
	if _, err := os.Lstat(c.Config); err == nil {
		old, err := ReadServiceConfig(c.Config)
		if err != nil {
			return err
		}
		if err := ValidateInstallIdentity(old, c); err != nil {
			return err
		}
		existing = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var invitation *Invitation
	if c.JoiningFile != "" && !existing {
		if c.Role != "worker" || c.EnvironmentFile != "" {
			return errors.New("joining invitation requires a worker without EnvironmentFile")
		}
		i, err := ReadInvitation(c.JoiningFile)
		if err != nil {
			return err
		}
		invitation = &i
		token = i.Token
		c.JoiningFile = filepath.Join(filepath.Dir(c.Config), "joining.json")
	}
	for _, path := range []string{c.Config, c.Binary, c.Credentials, c.Service} {
		st, err := os.Lstat(path)
		if err == nil {
			if (!existing && !(files.machineConversion && (path == c.Credentials))) || !st.Mode().IsRegular() {
				return fmt.Errorf("refusing unexpected installed path %s", path)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	var extraPaths []string
	if invitation != nil {
		extraPaths = append(extraPaths, c.JoiningFile)
	}
	tx, err := snapshotInstall(ctx, c, files, command, extraPaths...)
	if err != nil {
		return err
	}
	defer func() {
		if result != nil && len(tx.published) > 0 {
			result = errors.Join(result, tx.restore(ctx))
		}
	}()
	if err := prepareInstallNAS(ctx, c, command); err != nil {
		return err
	}
	if err := PrepareStateRoots(roots...); err != nil {
		return err
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	for _, path := range []string{filepath.Dir(c.Config), filepath.Dir(c.Service), c.State} {
		if err = os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	// Publication and recovery share the complete pre-install snapshot.
	if err = tx.put(c.Binary, data, 0700); err != nil {
		return err
	}
	configData, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err = tx.put(c.Config, configData, 0600); err != nil {
		return err
	}
	if invitation != nil {
		invitationData, err := json.Marshal(invitation)
		if err != nil {
			return err
		}
		if err = tx.put(c.JoiningFile, invitationData, 0600); err != nil {
			return err
		}
	}
	if c.EnvironmentFile == "" && !existing && invitation == nil && !files.machineConversion {
		if token == "" {
			return errors.New("set MYCE_TOKEN before installing")
		}
		credentialData := []byte("MYCE_TOKEN=" + strconv.Quote(token) + "\nMYCE_QUEUE=" + strconv.Quote(c.Queue) + "\n")
		if err = tx.put(c.Credentials, credentialData, 0600); err != nil {
			return err
		}
	}
	if c.Role == "worker" && c.Detection.OS == "linux" {
		// Runtime settings are an installed artifact, not ambient upgrade input.
		if _, err = os.Lstat(c.GPUEnvironmentFile()); errors.Is(err, os.ErrNotExist) {
			if err = tx.put(c.GPUEnvironmentFile(), []byte(gpuRuntimeEnvironment(os.Environ())), 0600); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

	}
	if err = tx.put(c.Service, []byte(c.ServiceText()), 0600); err != nil {
		return err
	}
	if c.Detection.OS == "linux" {
		if err = tx.retireObsolete(ctx); err != nil {
			return err
		}
		if _, err = command(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
			return err
		}
		if c.Role == "worker" {
			if err = ValidateWorkerServiceEnvironment(ctx, c, RealClock{}, command); err != nil {
				return err
			}
		}
		if c.Role == "worker" {
			if err = validateInstallDropIns(ctx, c, command); err != nil {
				return err
			}
		}
		if _, err = command(ctx, "systemctl", "--user", "enable", "--now", filepath.Base(c.Service)); err != nil {
			return err
		}
		if existing {
			_, err = command(ctx, "systemctl", "--user", "restart", filepath.Base(c.Service))
		}
	} else {
		if err = ValidateWorkerServiceEnvironment(ctx, c, RealClock{}, command); err != nil {
			return err
		}
		if tx.units[0].state.loaded {
			if _, err = command(ctx, "launchctl", "bootout", tx.units[0].target()); err != nil {
				return err
			}
		}
		_, err = command(ctx, "launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), c.Service)
	}
	return err
}

func ReadServiceConfig(path string) (ServiceConfig, error) {
	c := ServiceConfig{ProductionInfrastructureRetries: DefaultProductionInfrastructureRetries}
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	if !json.Valid(b) {
		return c, errors.New("invalid installed config JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	// Decode only known retired settings for saved-config overlays. They have no
	// runtime representation and disappear when the current config is written.
	legacy := struct {
		*ServiceConfig
		HostStateDir    json.RawMessage `json:"host_state_dir"`
		StartDisabled   json.RawMessage `json:"start_disabled"`
		RequiredWorkers json.RawMessage `json:"required_workers"`
	}{ServiceConfig: &c}
	e = decoder.Decode(&legacy)
	if e == nil {
		_, e = deviceAllowance(c.Device)
	}
	if e == nil && (c.Version < 0 || c.Version > 1) {
		return c, errors.New("unsupported installed config version")
	}
	return c, e
}
func Uninstall(ctx context.Context, c ServiceConfig, dry bool, command func(context.Context, string, ...string) ([]byte, error)) error {
	if c.Detection.OS == "linux" && (c.Role == "worker" && !ValidWorkerUnit(filepath.Base(c.Service)) || c.Role != "worker" && (c.Role != "queue" || filepath.Base(c.Service) != "myce-queue.service")) {
		return errors.New("refusing non-Mycelium service unit")
	}

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
		client := Client{URL: c.Queue, Token: token}
		if c.JoiningFile != "" {
			i, err := ReadInvitation(c.JoiningFile)
			if err != nil {
				return err
			}
			client, err = i.Client(i.Address)
			if err != nil {
				return err
			}
		}
		return client.Call(ctx, "GET", "/api/status", nil, &Snapshot{})
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
		status := WorkerMemoryDelegationStatus(ctx, c, p.Machine.Command)
		// Enforcement is advisory on unsupported hosts; report its capability plainly.
		add("memory ceilings", nil, status)
		if c.Detection.Platform == "cpu" {
			add("CPU target", nil, "Linux/macOS logical cores and host RAM; GPU not required")
		} else {
			d, e := p.Machine.DetectDevice(ctx, c.Device)
			if e == nil && d.Platform != c.Detection.Platform {
				e = fmt.Errorf("detected %s; configured %s", d.Platform, c.Detection.Platform)
			}
			add("GPU visible", e, d.Platform)
		}
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
	if c.JoiningFile != "" {
		i, err := ReadInvitation(c.JoiningFile)
		return i.Token, err
	}
	if c.EnvironmentFile != "" {
		token, _, err := ReadFleetEnvironment(c.EnvironmentFile)
		return token, err
	}
	token, _, err := ReadFleetEnvironment(c.Credentials)
	if err != nil {
		return "", errors.New("service credentials require a literal EnvironmentFile; preserve the secret reference and do a fresh install")
	}
	return token, nil
}
func (c ServiceConfig) Args() []string {
	args := []string{}
	if c.Role == "queue" {
		args = []string{"queue", "--listen", c.Listen, "--db", c.DB, "--checkpoints", c.Checkpoints, "--queue", c.Queue}
	} else {
		args = []string{"join", "--reserve-cpu-cores", strconv.Itoa(c.CPUReserveCores), "--cpu-budget-cores", strconv.Itoa(c.CPUBudgetCores), "--host-memory-budget-gb", strconv.FormatFloat(c.HostMemoryBudgetGB, 'f', -1, 64), "--memory-ceiling-slack-gb", strconv.FormatFloat(c.MemoryCeilingSlackGB, 'f', -1, 64), "--memory-swap-max-bytes", strconv.FormatInt(c.MemorySwapMaxBytes, 10), "--name", c.Name, "--gpu", c.Detection.Platform, "--memory-mode", c.Detection.MemoryMode, "--memory-gb", strconv.FormatFloat(c.Detection.MemoryGB, 'f', -1, 64), "--reserve-memory-gb", strconv.FormatFloat(c.Detection.ReserveGB, 'f', -1, 64), "--state-dir", c.State, "--checkpoint-root", c.Checkpoints, "--model-root", c.ModelRoot, "--model-cache", c.ModelCache, "--model-cache-bytes", strconv.FormatInt(c.ModelCacheBytes, 10), "--environment-root", c.EnvironmentRoot, "--environment-cache", c.EnvironmentCache, "--environment-cache-bytes", strconv.FormatInt(c.EnvironmentCacheBytes, 10)}
	}
	if c.Role == "worker" {
		args = append(args, "--daily-use="+strconv.FormatBool(c.DailyUse), "--laptop="+strconv.FormatBool(c.Laptop), "--desktop="+strconv.FormatBool(c.Desktop))
		if len(c.MachineDevices) > 0 {
			data, _ := json.Marshal(c.MachineDevices)
			args = append(args, "--device-settings", string(data))
		}

		if c.DockerEndpoint != "" {
			args = append(args, "--docker-host", c.DockerEndpoint)
		}
		if c.OnboardingBenchmark {
			args = append(args, "--onboarding-benchmark")
		}
		if c.ContainerRetention != nil {
			args = append(args, "--container-retention", c.ContainerRetention.String())
		}
	}
	if c.LogRetention > 0 {
		args = append(args, "--log-retention", c.LogRetention.String())
	}
	if c.Role == "queue" {
		for _, v := range [][2]string{{"--project-config", c.ProjectConfig}, {"--host-config", c.HostConfig}, {"--upload-spool-dir", c.UploadSpoolDir}} {
			if v[1] != "" {
				args = append(args, v[0], v[1])
			}
		}
		args = append(args, "--upload-spool-bytes", strconv.FormatInt(c.UploadSpoolBytes, 10), "--restart-grace", c.RestartGrace.String(), "--production-infrastructure-retries", strconv.Itoa(c.ProductionInfrastructureRetries), "--production-loss-timeout", c.ProductionLossTimeout.String(), "--deadline-loss-timeout", c.DeadlineLossTimeout.String(), "--experiment-loss-timeout", c.ExperimentLossTimeout.String())
	}
	if c.Role == "worker" {
		if c.JoiningFile != "" {
			args = append(args, "--joining-file", c.JoiningFile)
		} else if c.EnvironmentFile == "" {
			args = append(args, "--queue", c.Queue)
		}
		args = append(args, "--host-id", c.HostID, "--reserve-host-memory-gb", strconv.FormatFloat(c.ReserveHostMemoryGB, 'f', -1, 64), "--heartbeat", c.Heartbeat.String())
		if c.RequireMemoryCeilings {
			args = append(args, "--require-memory-ceilings")
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
