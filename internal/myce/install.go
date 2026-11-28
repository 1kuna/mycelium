package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io/fs"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/probeguard"
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
	infoState *probeInfoState // Shared by local probes; diagnostics change, capacity is always reprobed.
	Files     fs.FS           // Entire filesystem view for injected probes.
	OS, Arch  string
	Info      func(string) // Optional informational diagnostics; never inventory output.
	Command   func(context.Context, string, ...string) ([]byte, error)
	// CommandEnv runs a probe with per-process overrides; nil keeps injected Command fixtures.
	CommandEnv func(context.Context, map[string]string, string, ...string) ([]byte, error)
	Glob       func(string) ([]string, error)
	ReadFile   func(string) ([]byte, error)
	// XeActivity reads every enumerated hardware engine without changing GPU settings.
	XeActivity func(context.Context, string) (float64, error)
	// XPUFreeMemory is a read-only, PCI-keyed kernel/broker source. It must
	// never initialize a vendor runtime; nil means unsupported, not fallback.
	XPUFreeMemory func(context.Context) (map[string]float64, error)
	// XeFreeMemory reads one card's free VRAM with the broker's read-only Xe
	// kernel query, for cards the broker reports none for; nil is unsupported.
	XeFreeMemory func(context.Context, string) (float64, error)
}

func LocalMachineProbe() MachineProbe {
	run := func(ctx context.Context, env map[string]string, n string, args ...string) ([]byte, error) {
		noteInventoryOperation(ctx, n, args)
		defer noteInventoryOperation(ctx, "", nil)
		// This runner also serves installer service managers and lazy tool
		// version checks. Those are not hardware probes; test CLI processes
		// supply their ordinary fake managers through PATH.
		switch filepath.Base(n) {
		case "systemctl", "systemd-run", "launchctl", "loginctl", "codesign", "sudo", "uv", "uvx", "pixi":
		default:
			probeguard.Check()
		}
		if n == cachedMountInventoryCommand {
			return cachedMountInventory(ctx)
		}
		cmd := exec.CommandContext(ctx, n, args...)
		// A probe owns its process group. Killing only the tool can leave
		// descendants holding stdout/stderr and defeat the caller's deadline.
		cmd.SysProcAttr = commandProcessAttributes()
		killGroup := func() error {
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
		cmd.Cancel = killGroup
		// Match the existing OS inventory drain allowance: shorter drains
		// falsely failed under contention. This also bounds inherited pipes.
		cmd.WaitDelay = time.Second
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
		if errors.Is(err, exec.ErrWaitDelay) {
			if cleanupErr := killGroup(); !errors.Is(cleanupErr, os.ErrProcessDone) {
				err = errors.Join(err, cleanupErr)
			}
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			exit.Stderr = stderr.Bytes()
		}
		if filepath.Base(n) == "codesign" {
			// codesign writes inspection metadata to stderr, even on success.
			return append(stdout.Bytes(), stderr.Bytes()...), err
		}
		return stdout.Bytes(), err
	}
	return MachineProbe{infoState: &localProbeInfo, XeActivity: nativeXeActivity, XeFreeMemory: nativeXeFreeMemory, Info: func(line string) { fmt.Fprintln(os.Stderr, line) }, OS: runtime.GOOS, Arch: runtime.GOARCH, Files: guardedProbeFS{os.DirFS("/")}, CommandEnv: run, Command: func(ctx context.Context, n string, args ...string) ([]byte, error) {
		return run(ctx, nil, n, args...)
	}}
}
func (p MachineProbe) Detect(ctx context.Context) (Detection, error) {
	return p.DetectDevice(ctx, "")
}

type ServiceConfig struct {
	storageWarning           *StoragePreflightFailure // Current invocation only; never configuration authority.
	SaveSettingsOnly         bool                     `json:"-"` // Prepare queue override retirement without activation.
	InstalledReleaseManifest string                   `json:"-"` // Signed incumbent artifact evidence for local recovery.

	DailyUse       bool            `json:"daily_use"`
	Laptop         bool            `json:"laptop"`
	Desktop        bool            `json:"desktop"`
	Enrollment     bool            `json:"enrollment,omitempty"`
	MachineDevices []DeviceInstall `json:"machine_devices,omitempty"`

	ProductionInfrastructureRetries int `json:"production_infrastructure_retries"`

	FileStoreBytes             int64 `json:"file_store_bytes"`
	FileStoreProductionReserve int64 `json:"file_store_production_reserve"`
	// Saved beside the config so the previous release's strict decoder can still start.
	OutputDeliveryRoot string `json:"-"`

	Version int `json:"version"`

	WorkerUser            string        `json:"worker_user"`
	WorkerHome            string        `json:"worker_home"`
	LocalDisableReceipts  bool          `json:"local_disable_receipts"`
	OnboardingBenchmark   bool          `json:"onboarding_benchmark"`
	JoiningFile           string        `json:"joining_file"`
	EnvironmentFile       string        `json:"environment_file"`
	HostID                string        `json:"host_id"`
	DB                    string        `json:"db"`
	ProjectConfig         string        `json:"project_config"`
	HostConfig            string        `json:"host_config"`
	UploadSpoolDir        string        `json:"upload_spool_dir"`
	UploadSpoolBytes      int64         `json:"upload_spool_bytes"`
	FileRetention         time.Duration `json:"file_retention"`
	LogRetention          time.Duration `json:"log_retention"`
	Heartbeat             time.Duration `json:"heartbeat"`
	RestartGrace          time.Duration `json:"restart_grace"`
	ProductionLossTimeout time.Duration `json:"production_loss_timeout"`
	DeadlineLossTimeout   time.Duration `json:"deadline_loss_timeout"`
	ExperimentLossTimeout time.Duration `json:"experiment_loss_timeout"`
	ReserveHostMemoryGB   float64       `json:"reserve_host_memory_gb"`
	PrivateDevices        bool          `json:"private_devices"`
	NoNewPrivileges       bool          `json:"no_new_privileges"`
	Nice                  int           `json:"nice"`
	CPUQuota              string        `json:"cpu_quota"`
	IOWeight              int           `json:"io_weight,omitempty"`
	MemoryMax             string        `json:"memory_max"`
	Slice                 string        `json:"slice,omitempty"`
	TasksMax              string        `json:"tasks_max"`

	RequireMemoryCeilings    bool    `json:"require_memory_ceilings"`
	RequireMountedNAS        bool    `json:"require_mounted_nas"`
	Force                    bool    `json:"-"`
	CPUReserveCores          int     `json:"cpu_reserve_cores"`
	CPUBudgetCores           int     `json:"cpu_budget_cores"`
	HostMemoryBudgetGB       float64 `json:"host_memory_budget_gb"`
	HostMemoryBudgetMeasured bool    `json:"host_memory_budget_measured,omitempty"`
	MemoryCeilingSlackGB     float64 `json:"memory_ceiling_slack_gb"`
	MemorySwapMaxBytes       int64   `json:"memory_swap_max_bytes"`
	GPUTelemetryHelper       string  `json:"gpu_telemetry_helper,omitempty"`
	AllowDeviceless          bool    `json:"allow_deviceless"`
	Device                   string  `json:"device"`

	NativeReadRoots    []string `json:"native_read_roots"`
	NativeExecuteRoots []string `json:"native_execute_roots"`

	Path                   string    `json:"path"`
	EnvironmentRoot        string    `json:"environment_root"`
	Role                   string    `json:"role"`
	Detection              Detection `json:"detection"`
	Name                   string    `json:"name"`
	Queue                  string    `json:"queue"`
	Listen                 string    `json:"listen"`
	DiscoveryListen        string    `json:"discovery_listen,omitempty"`
	NAS                    string    `json:"nas"`
	Checkpoints            string    `json:"checkpoint_root"`
	ModelRoot              string    `json:"model_root"`
	SecretRoot             string    `json:"secret_root,omitempty"`
	ModelCatalog           string    `json:"model_catalog"`
	ModelListen            string    `json:"model_listen,omitempty"`
	ModelAdvertise         string    `json:"model_advertise,omitempty"`
	EnvironmentCatalog     string    `json:"environment_catalog"`
	State                  string    `json:"state_dir"`
	ModelCache             string    `json:"model_cache"`
	EnvironmentCache       string    `json:"environment_cache"`
	FileCacheBytes         int64     `json:"file_cache_bytes,omitempty"`
	FileCacheBytesExplicit bool      `json:"file_cache_bytes_explicit,omitempty"`
	ModelCacheBytes        int64     `json:"model_cache_bytes"`
	EnvironmentCacheBytes  int64     `json:"environment_cache_bytes"`
	Binary                 string    `json:"binary"`
	Credentials            string    `json:"credentials"`
	Service                string    `json:"service"`
	Config                 string    `json:"config"`

	// A queue with a shared authority runs as a shared-queue member. The
	// password stays in its own private file, never in this config or argv.
	SharedAuthority             string `json:"shared_authority,omitempty"`
	SharedAuthorityPasswordFile string `json:"shared_authority_password_file,omitempty"`
	// Every shared-queue member must serve the same file store; empty keeps
	// the default beside the database.
	FileStore string `json:"file_store,omitempty"`
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
	c.FileCacheBytes = 0
	c.ModelCacheBytes = 100 * 1024 * 1024 * 1024
	c.EnvironmentCacheBytes = 20 * 1024 * 1024 * 1024
	c.Checkpoints = filepath.Join(nas, "mycelium", "checkpoints")
	c.ModelRoot = filepath.Join(nas, "mycelium", "models")
	c.DB = filepath.Join(c.State, "myce.db")

	c.HostID, _ = os.Hostname()
	// Live free memory already excludes the OS/driver working set. New
	// installations do not subtract a second, guessed reserve; explicit
	// operator reserves and ceiling slack remain supported and preserved.
	c.ReserveHostMemoryGB = 0
	c.MemoryCeilingSlackGB = 0
	c.Heartbeat = time.Second
	c.LogRetention = DefaultLogRetention
	c.FileRetention = DefaultFileRetention
	c.FileStoreBytes = 100 << 30
	c.RestartGrace = RestartGrace
	c.ProductionInfrastructureRetries = DefaultProductionInfrastructureRetries
	// Worker configs retain these unused schema values so older installers can
	// validate a saved worker config on rollback. Only queues decide worker loss.
	c.ProductionLossTimeout, c.DeadlineLossTimeout, c.ExperimentLossTimeout = ProductionLossAfter, DeadlineLossAfter, LossAfter
	c.UploadSpoolDir = filepath.Join(c.State, "uploads")
	c.UploadSpoolBytes = DefaultUploadSpoolBytes
	c.PrivateDevices, c.NoNewPrivileges = false, true
	// The OS preempts CPU work; an idle logical CPU is not a memory reserve.
	c.CPUReserveCores = 0
	c.Nice, c.CPUQuota, c.MemoryMax, c.TasksMax = 10, "25%", "512M", "64"
	if role == "queue" {
		c.ProductionLossTimeout, c.DeadlineLossTimeout, c.ExperimentLossTimeout = 0, 0, 0
		// Control transactions and heartbeats must not share job politeness limits.
		c.Nice, c.CPUQuota, c.MemoryMax = 0, "", "infinity"
	}
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
func (c ServiceConfig) workerContainmentSettings() string {
	return "OOMScoreAdjust=0\nSlice=" + c.workerSlice() + "\n"
}

func (c ServiceConfig) ServiceText() string {
	if c.Detection.OS == "linux" {
		stopSettings := ""
		if c.Role == "worker" {
			// TERM must go through the worker's recorded stop path. Direct TERM
			// to payloads races their exit observation and looks like command failure.
			// mixed retains the bounded final KILL of every delegated descendant.
			stopSettings = "EnvironmentFile=" + unitConditionPath(c.GPUEnvironmentFile()) + "\n" + c.workerContainmentSettings() + "MemoryAccounting=yes\nOOMPolicy=continue\nDelegate=memory cpu\nKillMode=mixed\nSendSIGKILL=yes\nTimeoutStopSec=45s\n"
		}
		settings := ""
		if c.IOWeight != 0 {
			// Relative service-subtree policy. Installed detached attempts live
			// in a separate jobs slice. Native I/O qualification is separate.
			settings += fmt.Sprintf("IOAccounting=yes\nIOWeight=%d\n", c.IOWeight)
		}
		if c.EnvironmentFile != "" {
			settings += "EnvironmentFile=" + unitConditionPath(c.EnvironmentFile) + "\n"
		}
		if c.Role == "queue" {
			// Retained-state startup has exceeded systemd's usual 90-second
			// default. Readiness must not introduce a kill during a valid load;
			// the installer retains explicit caller cancellation.
			settings += "Type=notify\nNotifyAccess=main\nTimeoutStartSec=infinity\n"
			yesno := func(v bool) string {
				if v {
					return "yes"
				}
				return "no"
			}
			settings += fmt.Sprintf("PrivateDevices=%s\nNoNewPrivileges=%s\nNice=%d\nCPUQuota=%s\nMemoryMax=%s\nTasksMax=%s\nTimeoutStopSec=150\n", yesno(c.PrivateDevices), yesno(c.NoNewPrivileges), c.Nice, c.CPUQuota, c.MemoryMax, c.TasksMax)
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
	if c.Role == "queue" && c.OutputDeliveryRoot != "" {
		vals = append(append(vals, "--"), c.Args()...)
	}
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

// InstallWithRollback retains the normal transaction until the caller completes
// enrollment readiness. The returned rollback restores files and manager state.
func InstallWithRollback(ctx context.Context, c ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error)) (func(context.Context) error, error) {
	var rollback func(context.Context) error
	err := installWithFiles(ctx, c, executable, token, command, installFiles{retain: func(tx *installTransaction) { rollback = tx.restore }})
	return rollback, err
}

// PreflightInstall checks the ordinary install's read-only prerequisites. The
// transaction repeats them at install time and keeps its own rollback snapshot.
func PreflightInstall(ctx context.Context, c ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error)) error {
	plan, err := prepareInstall(ctx, c, executable, token, command, installFiles{}, nil)
	if err != nil || c.SaveSettingsOnly {
		return err
	}
	mode := installStoragePreview
	if plan.existing && plan.config.PreservesWorkerStorage(plan.slotConfig) {
		mode |= installStoragePreserveNAS
	}
	_, err = prepareInstallStorage(ctx, plan.config, command, mode, nil)
	return err
}

type installPreparation struct {
	config         ServiceConfig
	slotConfig     ServiceConfig
	transaction    *installTransaction
	roots          []string
	existing       bool
	invitation     *Invitation
	reconciliation *slotReconciliation
	executable     []byte
}

func prepareInstall(ctx context.Context, c ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error), files installFiles, lockSlot func() error) (installPreparation, error) {
	if os.Geteuid() == 0 {
		return installPreparation{}, ErrHostIdentity
	}
	if c.Detection.OS == "linux" && (c.Role == "worker" && !ValidWorkerUnit(filepath.Base(c.Service)) || c.Role != "worker" && (c.Role != "queue" || filepath.Base(c.Service) != "myce-queue.service")) {
		return installPreparation{}, errors.New("refusing non-Mycelium service unit")
	}

	if err := CheckMachineInstallGate(c); err != nil {
		return installPreparation{}, err
	}
	if err := c.ValidateInstallPolicies(); err != nil {
		return installPreparation{}, err
	}

	roots, err := c.StateRoots()
	if err != nil {
		return installPreparation{}, err
	}
	if err := CheckStateRoots(roots...); err != nil {
		return installPreparation{}, err
	}
	if command == nil {
		return installPreparation{}, errors.New("service runner unavailable")
	}
	if err := c.ValidateInstall(); err != nil {
		return installPreparation{}, err
	}
	if lockSlot != nil {
		if err := lockSlot(); err != nil {
			return installPreparation{}, err
		}
	}
	if _, err := InstallUnitPlan(ctx, c, command); err != nil {
		return installPreparation{}, err
	}
	c.Version = 1
	existing := false
	var installed ServiceConfig
	if _, err := os.Lstat(c.Config); err == nil {
		old, err := ReadServiceConfig(c.Config)
		if err != nil {
			return installPreparation{}, err
		}
		if err := ValidateInstallIdentity(old, c); err != nil {
			return installPreparation{}, err
		}
		existing = true
		installed = old
	} else if !errors.Is(err, os.ErrNotExist) {
		return installPreparation{}, err
	}
	if c.SaveSettingsOnly {
		if !existing {
			return installPreparation{}, errors.New("--save-settings-only requires an existing queue installation")
		}
		tx, err := snapshotInstall(ctx, c, files, command)
		return installPreparation{config: c, transaction: tx}, err
	}
	if existing && c.Detection.OS == "darwin" {
		if err := validateDarwinUpgradeIdentity(ctx, c.Binary, executable, command); err != nil {
			return installPreparation{}, err
		}
	}
	var invitation *Invitation
	if existing && installed.JoiningFile != "" {
		if c.JoiningFile != installed.JoiningFile {
			return installPreparation{}, errors.New("address repair must preserve the installed joining authority and certificate pin")
		}
		i, err := ReadInvitation(installed.JoiningFile)
		if err != nil {
			return installPreparation{}, err
		}
		// Also repair a previous reinstall that changed config.Queue alone.
		if c.Queue != installed.Queue || strings.HasPrefix(c.Queue, "https://") && c.Queue != i.Address {
			client, err := ResolveJoining(ctx, i, c.Queue, nil)
			if err != nil {
				return installPreparation{}, fmt.Errorf("cannot update joining address using the saved certificate pin: %w", err)
			}
			client.HTTP.CloseIdleConnections()
			i.Address = c.Queue
			invitation = &i
		}
	}
	if c.JoiningFile != "" && !existing {
		if c.Role != "worker" || c.EnvironmentFile != "" {
			return installPreparation{}, errors.New("joining invitation requires a worker without EnvironmentFile")
		}
		i, err := ReadInvitation(c.JoiningFile)
		if err != nil {
			return installPreparation{}, err
		}
		i.Address = c.Queue // Save the verified origin only with the installed authority.
		invitation = &i
		c.JoiningFile = filepath.Join(filepath.Dir(c.Config), "joining.json")
	}
	for _, path := range []string{c.Config, c.Binary, c.Credentials, c.Service} {
		st, err := os.Lstat(path)
		if err == nil {
			if !existing || !st.Mode().IsRegular() {
				return installPreparation{}, fmt.Errorf("refusing unexpected installed path %s", path)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return installPreparation{}, err
		}
	}
	var reconciliation *slotReconciliation
	if existing {
		installed.Queue = c.Queue // Address repair already verified the saved certificate pin.
		installed.InstalledReleaseManifest = c.InstalledReleaseManifest
		reconciliation, err = preflightInstallSlot(ctx, installed, command, files.imageProbe)
		if err != nil {
			return installPreparation{}, err
		}
	}
	if c.EnvironmentFile == "" && !existing && invitation == nil && token == "" {
		return installPreparation{}, errors.New("set MYCE_TOKEN before installing")
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		return installPreparation{}, err
	}
	extraPaths := []string{filepath.Join(c.State, installStorageReceiptFile)}
	if invitation != nil {
		extraPaths = append(extraPaths, c.JoiningFile)
	}
	if reconciliation != nil {
		extraPaths = append(extraPaths, filepath.Join(c.State, "slot-reconciliation.json"))
	}
	tx, err := snapshotInstall(ctx, c, files, command, extraPaths...)
	if err != nil {
		return installPreparation{}, err
	}
	// Preview effective overrides before publication and check again after reload.
	if c.Detection.OS == "linux" {
		if err := validateInstallDropIns(ctx, c, command, tx.retired...); err != nil {
			return installPreparation{}, err
		}
	}
	if reconciliation != nil {
		// The rollback snapshot must contain exactly the qualified slot bytes.
		original := tx.originals[c.Binary]
		if fleetBytesHash(original.data) != reconciliation.Slot.SHA256 || int64(len(original.data)) != reconciliation.Slot.Size {
			return installPreparation{}, errors.New("fleet_install_changed")
		}
	}
	return installPreparation{
		config: c, slotConfig: installed, transaction: tx, roots: roots, existing: existing,
		invitation: invitation, reconciliation: reconciliation, executable: data,
	}, nil
}

func installWithFiles(ctx context.Context, c ServiceConfig, executable, token string, command func(context.Context, string, ...string) ([]byte, error), files installFiles) (result error) {
	var unlock func()
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	// Acquire before observing the unit. Otherwise a second install could see
	// recovery's temporary stop and proceed without the first install's fences.
	plan, err := prepareInstall(ctx, c, executable, token, command, files, func() error {
		if c.Role != "worker" || c.HostID == "" {
			return nil
		}
		if _, err := os.Lstat(filepath.Join(c.State, "fleet-publisher.pub")); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
		release, err := commandLock(c.State, "fleet-update.lock", false)
		if err != nil {
			return errors.New("fleet_helper_busy")
		}
		unlock = release
		return nil
	})
	if err != nil {
		return err
	}
	c, tx := plan.config, plan.transaction
	if c.SaveSettingsOnly {
		return saveQueueInstallSettings(ctx, c, tx)
	}
	if err := EnsureXPUHealthAccess(ctx, c, command); err != nil {
		return err
	}
	existing, invitation, reconciliation := plan.existing, plan.invitation, plan.reconciliation
	recovery := reconciliation != nil && reconciliation.Recovery != nil

	recoveryStopIssued := false
	defer func() {
		if result != nil && (len(tx.published) > 0 || recoveryStopIssued) {
			result = errors.Join(result, tx.restore(ctx))
		} else if result == nil {
			if err := tx.pruneBackups(); err != nil {
				log.Printf("install backup retention: %v", err)
			}
		}
	}()
	mode := installStorageWrite
	if existing && c.PreservesWorkerStorage(plan.slotConfig) {
		mode |= installStoragePreserveNAS
	}
	warning, err := prepareInstallStorage(ctx, c, command, mode, files.storageFS)
	if err != nil {
		return err
	}
	if err := PrepareStateRoots(plan.roots...); err != nil {
		return err
	}
	for _, path := range []string{filepath.Dir(c.Config), filepath.Dir(c.Service), c.State} {
		if err = os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	receipt, err := json.Marshal(installStorageReceipt{Warning: warning})
	if err != nil {
		return err
	}
	if recovery {
		// Recheck the original drain, local process and slot after storage work.
		// Stop under that evidence before publication, so an early failure can't
		// restart an unchecked service as a side effect of file rollback.
		if !tx.fileMatches(c.Config) || !tx.fileMatches(c.Service) {
			return errors.New("fleet_install_changed")
		}
		current, err := preflightInstallSlot(ctx, plan.slotConfig, command, files.imageProbe)
		if err != nil {
			return err
		}
		if !tx.units[0].state.active || current == nil || current.Recovery == nil || *current.Recovery != *reconciliation.Recovery || current.Slot != reconciliation.Slot {
			return errFleetRecoveryChanged
		}
		identity, err := EnrollmentServiceHealth(ctx, plan.slotConfig, command)
		if err != nil || identity != current.Recovery.ServiceIdentity {
			return errFleetRecoveryChanged
		}
		recoveryStopIssued = true
		if err := tx.manager.stop(ctx, tx.units[0]); err != nil {
			return fmt.Errorf("fleet_restart_failed: %w", err)
		}
	}
	if reconciliation != nil {
		current, err := readReleaseSlot(c.Binary)
		if err != nil || current.SHA256 != reconciliation.Slot.SHA256 {
			return errors.New("fleet_install_changed")
		}
		receipt, err := json.Marshal(reconciliation)
		if err != nil {
			return err
		}
		if err = tx.put(filepath.Join(c.State, "slot-reconciliation.json"), receipt, 0600); err != nil {
			return err
		}
	}
	if err := tx.put(filepath.Join(c.State, installStorageReceiptFile), receipt, 0600); err != nil {
		return err
	}

	// Publication and recovery share the complete pre-install snapshot.
	if err = tx.put(c.Binary, plan.executable, 0700); err != nil {
		return err
	}
	configData, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	// The service reconnects through --joining-file. Publish that address first
	// so an interrupted repair can reconnect; repeating the same repair then
	// reconciles a config publication that did not finish.
	if invitation != nil {
		invitationData, err := json.Marshal(invitation)
		if err != nil {
			return err
		}
		if err = tx.put(c.JoiningFile, invitationData, 0600); err != nil {
			return err
		}
	}
	if err = tx.put(c.Config, configData, 0600); err != nil {
		return err
	}
	if c.EnvironmentFile == "" && !existing && invitation == nil {
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
	if err = tx.putServiceDefinition(c); err != nil {
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
		if err = validateInstallDropIns(ctx, c, command); err != nil {
			return err
		}
		if _, err = command(ctx, "systemctl", "--user", "enable", "--now", filepath.Base(c.Service)); err != nil {
			return err
		}
		if existing && !recovery {
			_, err = command(ctx, "systemctl", "--user", "restart", filepath.Base(c.Service))
		}
	} else {
		if err = ValidateWorkerServiceEnvironment(ctx, c, RealClock{}, command); err != nil {
			return err
		}
		if tx.units[0].state.loaded && !recovery {
			if err = tx.manager.stop(ctx, tx.units[0]); err != nil {
				return err
			}
		}
		_, err = command(ctx, "launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), c.Service)
		if err == nil {
			err = tx.manager.waitActivity(ctx, tx.units[0], true)
		}
	}
	if err == nil && files.retain != nil {
		files.retain(tx)
	}
	return err
}

func ReadServiceConfig(path string) (ServiceConfig, error) {
	c := ServiceConfig{ProductionInfrastructureRetries: DefaultProductionInfrastructureRetries, FileRetention: DefaultFileRetention, FileStoreBytes: 100 << 30, FileCacheBytes: 0}
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	if !json.Valid(b) {
		return c, errors.New("invalid installed config JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	// Old saved configs can outlive a binary upgrade. Decode retired keys only
	// at this boundary; they are neither settings nor part of the saved config.
	e = decoder.Decode(&struct {
		*ServiceConfig
		DockerEndpoint     json.RawMessage `json:"docker_endpoint"`
		ContainerRetention json.RawMessage `json:"container_retention"`
	}{ServiceConfig: &c})
	if e == nil {
		_, e = deviceAllowance(c.Device)
	}
	if e == nil {
		e = c.validateWorkerSlice()
	}
	if e == nil && (c.Version < 0 || c.Version > 1) {
		return c, errors.New("unsupported installed config version")
	}
	if e == nil && c.Role == "queue" {
		b, err := os.ReadFile(queueOutputDeliverySettings(path))
		switch {
		case err == nil:
			if json.Unmarshal(b, &c.OutputDeliveryRoot) != nil {
				return c, errors.New("invalid saved output-delivery root")
			}
		case !errors.Is(err, os.ErrNotExist):
			return c, err
		}
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
		e = (installManager{os: "darwin", command: command}).stop(ctx, installUnit{path: c.Service, label: "local.mycelium." + c.Role})
	}
	if e != nil {
		return fmt.Errorf("stop service before removing setup: %w", e)
	}
	// Revoke only this installation's health access after stopping its service.
	// Keep the config/ownership identity available if cleanup needs a retry.
	if e = xpuHealthAccess(ctx, c, "remove", command); e != nil {
		return e
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
		// Read the saved joining bind at startup, rather than embedding an
		// automatic port in systemd's argv that can become stale after fallback.
		args = append(args, "--service-config", c.Config)
	} else {
		args = []string{"join", "--reserve-cpu-cores", strconv.Itoa(c.CPUReserveCores), "--cpu-budget-cores", strconv.Itoa(c.CPUBudgetCores), "--host-memory-budget-gb", strconv.FormatFloat(c.HostMemoryBudgetGB, 'f', -1, 64), "--memory-ceiling-slack-gb", strconv.FormatFloat(c.MemoryCeilingSlackGB, 'f', -1, 64), "--memory-swap-max-bytes", strconv.FormatInt(c.MemorySwapMaxBytes, 10), "--name", c.Name, "--gpu", c.Detection.Platform, "--memory-mode", c.Detection.MemoryMode, "--memory-gb", strconv.FormatFloat(c.Detection.MemoryGB, 'f', -1, 64), "--reserve-memory-gb", strconv.FormatFloat(c.Detection.ReserveGB, 'f', -1, 64), "--state-dir", c.State, "--checkpoint-root", c.Checkpoints, "--job-file-root", c.NAS, "--model-root", c.ModelRoot, "--model-cache", c.ModelCache, "--model-cache-bytes", strconv.FormatInt(c.ModelCacheBytes, 10), "--environment-root", c.EnvironmentRoot, "--environment-cache", c.EnvironmentCache, "--environment-cache-bytes", strconv.FormatInt(c.EnvironmentCacheBytes, 10)}
	}
	if c.Role == "worker" {
		if c.ModelListen != "" {
			args = append(args, "--model-listen", c.ModelListen)
		}
		if c.ModelAdvertise != "" {
			args = append(args, "--model-advertise", c.ModelAdvertise)
		}
		if c.SecretRoot != "" {
			args = append(args, "--secret-root", c.SecretRoot)
		}
		if c.FileCacheBytes > 0 {
			args = append(args, "--file-cache-bytes", strconv.FormatInt(c.FileCacheBytes, 10))
		}
		if c.GPUTelemetryHelper != "" {
			args = append(args, "--gpu-telemetry-helper", c.GPUTelemetryHelper)
		}
		args = append(args, "--daily-use="+strconv.FormatBool(c.DailyUse), "--laptop="+strconv.FormatBool(c.Laptop), "--desktop="+strconv.FormatBool(c.Desktop))
		for _, root := range c.NativeReadRoots {
			args = append(args, "--native-read-root", root)
		}
		for _, root := range c.NativeExecuteRoots {
			args = append(args, "--native-exec-root", root)
		}
		if len(c.MachineDevices) > 0 {
			data, _ := json.Marshal(c.MachineDevices)
			args = append(args, "--device-settings", string(data))
		}

		if c.Enrollment {
			args = append(args, "--enrollment")
		}
		if c.OnboardingBenchmark {
			args = append(args, "--onboarding-benchmark")
		}
	}
	if c.LogRetention > 0 {
		args = append(args, "--log-retention", c.LogRetention.String())
	}
	if c.Role == "queue" {
		// Always rendered: a saved zero means retain until release, not the default.
		args = append(args, "--file-retention", c.FileRetention.String())
		args = append(args, "--file-store-bytes", strconv.FormatInt(c.FileStoreBytes, 10), "--file-store-production-reserve", strconv.FormatInt(c.FileStoreProductionReserve, 10))
		for _, v := range [][2]string{{"--project-config", c.ProjectConfig}, {"--host-config", c.HostConfig}, {"--upload-spool-dir", c.UploadSpoolDir}, {"--file-store", c.FileStore}, {"--output-delivery-root", c.OutputDeliveryRoot}} {
			if v[1] != "" {
				args = append(args, v[0], v[1])
			}
		}
		if c.SharedAuthority != "" {
			args = append(args, "--shared-authority", c.SharedAuthority)
			if c.SharedAuthorityPasswordFile != "" {
				args = append(args, "--shared-authority-password-file", c.SharedAuthorityPasswordFile)
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
		if c.HostMemoryBudgetMeasured {
			args = append(args, "--host-memory-budget-measured")
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

// Guard filesystem discovery as well as vendor subprocesses.
type guardedProbeFS struct{ fs.FS }

func (f guardedProbeFS) Open(name string) (fs.File, error) {
	probeguard.Check()
	return f.FS.Open(name)
}

// ProtectServiceCredentials keeps an installed service's initial environment,
// which can carry the fleet token, from same-user jobs (#1309).
func ProtectServiceCredentials() error { return protectServiceCredentials() }
