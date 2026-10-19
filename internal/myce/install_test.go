package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func fakeProbe(osname, arch, nvidia, intel string) MachineProbe {
	return MachineProbe{OS: osname, Arch: arch, Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		switch name {
		case "nvidia-smi":
			if nvidia != "" {
				return []byte(nvidia), nil
			}
		case "sycl-ls":
			if intel != "" {
				return []byte(intel), nil
			}
		case "system_profiler":
			return []byte("Chipset Model: Apple M4\nMetal Support: Metal 3"), nil
		case "uv":
			return []byte("uv 0.8"), nil
		}
		return nil, errors.New("not installed")
	}}
}
func TestInstallDetectionDryPlans(t *testing.T) {
	cases := []struct {
		name           string
		p              MachineProbe
		platform, mode string
		reserve        float64
	}{
		{"4090", fakeProbe("linux", "amd64", "NVIDIA GeForce RTX 4090, 24564", ""), "cuda", "dedicated", 2},
		{"Spark", fakeProbe("linux", "arm64", "NVIDIA GB10, [N/A]", ""), "cuda", "unified", 16},
		{"Intel", fakeProbe("linux", "amd64", "", "[level_zero:gpu] Intel(R) UHD Integrated Graphics"), "xpu", "unified", 8},
		{"Mac", fakeProbe("darwin", "arm64", "", ""), "metal", "unified", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, e := tc.p.Detect(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if d.Platform != tc.platform || d.MemoryMode != tc.mode || d.ReserveGB != tc.reserve {
				t.Fatal(d)
			}
			home := t.TempDir()
			c, e := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue:7331", "worker", d)
			if e != nil {
				t.Fatal(e)
			}
			if strings.Contains(c.ServiceText(), "token") {
				t.Fatal("secret in unit")
			}
			if !strings.Contains(c.ServiceText(), "service") {
				t.Fatal("missing service command")
			}
			if _, e = os.Stat(c.Config); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("dry plan wrote config")
			}
			if e = Uninstall(context.Background(), c, true, func(context.Context, string, ...string) ([]byte, error) {
				t.Fatal("dry uninstall ran service command")
				return nil, nil
			}); e != nil {
				t.Fatal(e)
			}
		})
	}
	if _, e := fakeProbe("linux", "amd64", "", "").Detect(context.Background()); e == nil {
		t.Fatal("missing GPU accepted")
	}
}
func TestInstallAndUninstallTempOnly(t *testing.T) {
	home := t.TempDir()
	c, e := PlanInstall("worker", home, "", filepath.Join(home, "nas"), "http://queue:7331", "worker", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "dedicated"})
	if e != nil {
		t.Fatal(e)
	}
	executable := filepath.Join(home, "test-executable")
	os.WriteFile(executable, []byte("test binary"), 0700)
	commands := []string{}
	runner := func(ctx context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, name+" "+strings.Join(args, " "))
		if name == "systemd-run" {
			return json.Marshal(c.Detection)
		}
		return nil, nil
	}
	if e = Install(context.Background(), c, executable, "private-test-secret", runner); e != nil {
		t.Fatal(e)
	}
	if len(commands) != 7 {
		t.Fatal(commands)
	}
	token, e := ReadServiceToken(c)
	if e != nil || token != "private-test-secret" {
		t.Fatal(e)
	}
	if b, _ := os.ReadFile(c.Config); strings.Contains(string(b), token) {
		t.Fatal("token in public config")
	}
	if e = os.WriteFile(c.Service, []byte("[Service]\nKillMode=mixed\nTimeoutStopSec=infinity\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = Install(context.Background(), c, executable, "changed-token", runner); e != nil {
		t.Fatal(e)
	}
	if got, e := ReadServiceToken(c); e != nil || got != token {
		t.Fatal("upgrade changed credentials", e)
	}
	text, e := os.ReadFile(c.Service)
	if e != nil || !strings.Contains(string(text), "KillMode=control-group") {
		t.Fatal("unit not upgraded", e)
	}
	old, e := os.ReadDir(filepath.Join(filepath.Dir(c.Service), ".Trash"))
	foundBackup := false
	for _, entry := range old {
		if strings.HasSuffix(entry.Name(), "-"+filepath.Base(c.Service)) {
			foundBackup = true
		}
	}
	if e != nil || !foundBackup {
		t.Fatal("old unit not recoverable", e)
	}
	if e = Uninstall(context.Background(), c, false, runner); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(c.Config); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("config remained")
	}
	entries, _ := os.ReadDir(filepath.Join(home, ".local", "share", "mycelium", ".Trash"))
	if len(entries) != 2 {
		t.Fatal("uninstall not recoverable")
	}
}
func TestDoctorChecks(t *testing.T) {
	c := ServiceConfig{Role: "worker", Detection: Detection{Platform: "cuda"}, NAS: "/nas", State: "/cache"}
	p := DoctorProbe{Machine: fakeProbe("linux", "amd64", "RTX 4090, 24564", ""), NAS: func(context.Context, string) error { return nil }, Disk: func(string) (int64, error) { return 2 << 30, nil }, Queue: func(context.Context, ServiceConfig) error { return nil }}
	checks := Doctor(context.Background(), c, p)
	if len(checks) != 6 {
		t.Fatal(checks)
	}
	for _, v := range checks {
		if !v.OK {
			t.Fatal(v)
		}
	}
	p.NAS = func(context.Context, string) error { return errors.New("NAS offline") }
	p.Queue = func(context.Context, ServiceConfig) error { return errors.New("queue offline") }
	p.Disk = func(string) (int64, error) { return 1, nil }
	checks = Doctor(context.Background(), c, p)
	failed := 0
	for _, v := range checks {
		if !v.OK {
			failed++
		}
	}
	if failed != 3 {
		t.Fatal(checks)
	}
}

func TestInstallDiscreteIntelUsesConservativeDeviceMemory(t *testing.T) {
	p := MachineProbe{OS: "linux", Arch: "amd64", Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "sycl-ls" {
			if len(args) > 0 {
				return []byte("global_mem_size: 68719476736\nglobal_mem_size: 17179869184\n"), nil
			}
			return []byte("[level_zero:gpu] Intel Data Center GPU Max"), nil
		}
		return nil, errors.New("absent")
	}}
	d, e := p.Detect(context.Background())
	if e != nil || d.Platform != "xpu" || d.MemoryMode != "fixed" || d.MemoryGB != 16 {
		t.Fatalf("%+v %v", d, e)
	}
	p.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "sycl-ls" {
			return []byte("[opencl:cpu] Intel CPU\n[opencl:gpu] Other GPU"), nil
		}
		return nil, errors.New("absent")
	}
	if _, e = p.Detect(context.Background()); e == nil {
		t.Fatal("CPU vendor used as GPU vendor")
	}
}

func TestDoctorLinuxLingering(t *testing.T) {
	for _, tc := range []struct {
		name, os, role, result, detail string
		err                            error
		ok, wantCheck                  bool
	}{
		{name: "worker enabled", os: "linux", role: "worker", result: "yes\n", ok: true, wantCheck: true, detail: "starts at boot"},
		{name: "queue disabled", os: "linux", role: "queue", result: "no\n", wantCheck: true, detail: "loginctl enable-linger " + strconv.Itoa(os.Getuid())},
		{name: "unavailable", os: "linux", role: "queue", err: errors.New("loginctl unavailable"), wantCheck: true, detail: "loginctl unavailable"},
		{name: "unknown", os: "linux", role: "queue", result: "", wantCheck: true, detail: "unexpected loginctl result"},
		{name: "macOS", os: "darwin", role: "queue"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			machine := fakeProbe(tc.os, "amd64", "RTX 4090, 24564", "")
			base := machine.Command
			machine.Command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "loginctl" {
					return base(ctx, name, args...)
				}
				calls++
				want := "show-user " + strconv.Itoa(os.Getuid()) + " --property=Linger --value"
				if strings.Join(args, " ") != want {
					t.Fatalf("unexpected command loginctl %v", args)
				}
				return []byte(tc.result), tc.err
			}
			p := DoctorProbe{Machine: machine, NAS: func(context.Context, string) error { return nil }, Queue: func(context.Context, ServiceConfig) error { return nil }, Disk: func(string) (int64, error) { return 2 << 30, nil }}
			checks := Doctor(context.Background(), ServiceConfig{Role: tc.role, Detection: Detection{OS: tc.os, Platform: "cuda"}}, p)
			found := false
			for _, check := range checks {
				if check.Name == "Linux user lingering" {
					found = true
					if check.OK != tc.ok || !strings.Contains(check.Detail, tc.detail) {
						t.Fatal(check)
					}
				}
			}
			wantCalls := 0
			if tc.wantCheck {
				wantCalls = 1
			}
			if found != tc.wantCheck || calls != wantCalls {
				t.Fatalf("found=%v calls=%d checks=%v", found, calls, checks)
			}
		})
	}
	p := DoctorProbe{}
	for _, check := range Doctor(context.Background(), ServiceConfig{Detection: Detection{OS: "linux"}}, p) {
		if check.Name == "Linux user lingering" && (check.OK || !strings.Contains(check.Detail, "command probe missing")) {
			t.Fatal(check)
		}
	}
}

func TestInstallMissingRunnerDoesNotWrite(t *testing.T) {
	home := t.TempDir()
	c, err := PlanInstall("queue", home, "", filepath.Join(home, "nas"), "http://queue:7331", "queue", Detection{OS: "linux"})
	if err != nil {
		t.Fatal(err)
	}
	if err = Install(context.Background(), c, "missing-executable", "test-token", nil); err == nil || !strings.Contains(err.Error(), "runner unavailable") {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatalf("install wrote files: %v %v", entries, err)
	}
}

func TestDoctorNASMountFormats(t *testing.T) {
	for _, tc := range []struct {
		name, suffix string
		ok           bool
	}{
		{"Linux NFS", " type nfs (rw)", true},
		{"Linux SMB", " type cifs (rw)", true},
		{"macOS SMB", " (smbfs, rw)", true},
		{"local filesystem", " type ext4 (rw)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "nested")
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			p := LocalDoctorProbeWithMachine("", MachineProbe{Command: func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if name != "mount" {
					t.Fatalf("unexpected command %s", name)
				}
				return []byte("server:/share on " + root + tc.suffix), nil
			}})
			if err := p.NAS(context.Background(), path); (err == nil) != tc.ok {
				t.Fatalf("NAS probe: %v", err)
			}
		})
	}
}

func TestInstallDeploymentNASAndUpgrade(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		t.Run(role, func(t *testing.T) {
			home := t.TempDir()
			nas := filepath.Join(home, "nas")
			os.Mkdir(nas, 0700)
			c, _ := PlanInstall(role, home, "", nas, "http://queue:7331", "test", Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
			c.AllowDeviceless = true
			c.RequireMountedNAS = true
			c.EnvironmentFile = filepath.Join(home, "fleet.env")
			os.WriteFile(c.EnvironmentFile, []byte("MYCE_TOKEN='private-test-secret'\nMYCE_QUEUE=http://queue:7331\n"), 0600)
			executable := filepath.Join(home, "binary")
			os.WriteFile(executable, []byte("version 1"), 0700)
			mounted := false
			runner := func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name == "cat" {
					if !mounted {
						return []byte("root / ext4 rw 0 0\n"), nil
					}
					return []byte("server " + nas + " cifs rw 0 0\n"), nil
				}
				return nil, nil
			}
			if err := Install(context.Background(), c, executable, "", runner); err == nil || !strings.Contains(err.Error(), "mount") {
				t.Fatal("unmounted NAS accepted", err)
			}
			if _, err := os.Stat(filepath.Join(nas, "mycelium")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("wrote to unmounted NAS")
			}
			mounted = true
			if err := Install(context.Background(), c, executable, "", runner); err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{"checkpoints", "models", "environments"} {
				entries, err := os.ReadDir(filepath.Join(nas, "mycelium", dir))
				if err != nil || len(entries) != 0 {
					t.Fatal("NAS probe not removed", entries, err)
				}
			}
			for _, p := range []string{c.Service, c.Config, c.GPUEnvironmentFile()} {
				b, _ := os.ReadFile(p)
				if strings.Contains(string(b), "private-test-secret") {
					t.Fatal("secret persisted", p)
				}
			}
			state := filepath.Join(c.State, "custody")
			os.WriteFile(state, []byte("preserve"), 0600)
			os.WriteFile(executable, []byte("version 2"), 0700)
			if err := Install(context.Background(), c, executable, "", runner); err != nil {
				t.Fatal(err)
			}
			b, _ := os.ReadFile(c.Binary)
			if string(b) != "version 2" {
				t.Fatal("binary not upgraded")
			}
			b, _ = os.ReadFile(state)
			if string(b) != "preserve" {
				t.Fatal("state lost")
			}
			changed := c
			changed.LogRetention = 24 * 60 * 60 * 1e9
			if err := Install(context.Background(), changed, executable, "", runner); err != nil {
				t.Fatal("explicit configuration change required --force", err)
			}
			got, err := ReadServiceConfig(c.Config)
			if err != nil || got.LogRetention != changed.LogRetention {
				t.Fatal("explicit update was not saved", err)
			}
		})
	}
}

func TestInstallStartDisabledPreservesHostGate(t *testing.T) {
	home := t.TempDir()
	nas := filepath.Join(home, "nas")
	os.Mkdir(nas, 0700)
	c, _ := PlanInstall("worker", home, "", nas, "http://queue:7331", "card", Detection{OS: "linux", Arch: "amd64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	c.HostStateDir = filepath.Join(home, "host-control")
	c.AllowDeviceless = true
	c.StartDisabled = true
	c.WorkerInstance("gpu0")
	os.MkdirAll(c.HostStateDir, 0700)
	initial, e := InitializeHostIntent(c.HostStateDir, c.HostID, RealClock{})
	if e != nil {
		t.Fatal(e)
	}
	executable := filepath.Join(home, "binary")
	os.WriteFile(executable, []byte("test"), 0700)
	runner := func(_ context.Context, n string, a ...string) ([]byte, error) {
		text := strings.Join(a, " ")
		if strings.HasSuffix(text, filepath.Base(c.Service)) && (strings.Contains(text, "enable --now") || strings.Contains(text, "restart ")) {
			t.Fatal("started disabled worker")
		}
		if strings.Contains(text, "--property=DropInPaths") {
			return nil, nil
		}
		if strings.Contains(text, "show ") {
			return []byte("LoadState=loaded\nActiveState=inactive\nControlGroup=\nKillMode=control-group\nSendSIGKILL=yes\n"), nil
		}
		return nil, nil
	}
	if err := Install(context.Background(), c, executable, "test-token", runner); err != nil {
		t.Fatal(err)
	}
	unitDir := c.HostStateDir
	current, e := ReadHostIntent(unitDir)
	if e != nil || current.Desired != "closed" || current.HostRevision != initial.HostRevision {
		t.Fatal(current, e)
	}
	if current.Reason != initial.Reason || !current.AcceptedAt.Equal(initial.AcceptedAt) || !current.StopDeadline.Equal(*initial.StopDeadline) {
		t.Fatal("install changed accepted close or renewed its deadline", current)
	}
	scoped := c
	scoped.HostStateDir = unitDir
	if err := (Worker{HostStateDir: unitDir}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := Install(context.Background(), c, executable, "test-token", runner); err != nil {
		t.Fatal("disabled rerun", err)
	}
}

func TestInstallDeploymentDefaultUnitAndNASBoundary(t *testing.T) {
	home := t.TempDir()
	nas := filepath.Join(home, "nas")
	os.Mkdir(nas, 0700)
	c, _ := PlanInstall("queue", home, "", nas, "http://queue:7331", "q", Detection{OS: "linux", Arch: "amd64"})
	for _, want := range []string{"PrivateDevices=no", "NoNewPrivileges=yes", "Nice=10", "CPUQuota=25%", "MemoryMax=512M", "TasksMax=64", "Environment=GOMAXPROCS=1", "--restart-grace", "3m0s", "--production-loss-timeout", "5s", "--deadline-loss-timeout", "20s", "--experiment-loss-timeout", "1m0s", "--upload-spool-bytes", "17179869184", "--log-retention", "168h0m0s"} {
		if !strings.Contains(c.ServiceText(), want) {
			t.Error("missing default", want)
		}
	}
	outside := t.TempDir()
	os.Symlink(outside, filepath.Join(nas, "mycelium"))
	executable := filepath.Join(home, "binary")
	os.WriteFile(executable, []byte("test"), 0700)
	if err := Install(context.Background(), c, executable, "token", func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("service changed before NAS check")
		return nil, nil
	}); err == nil {
		t.Fatal("NAS symlink followed")
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Fatal("outside namespace touched")
	}
}

func TestFleetEnvironmentPrivacyAndLiteralValues(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		mode       os.FileMode
		ok         bool
	}{
		{"literal", "MYCE_TOKEN='test $literal'\nMYCE_QUEUE=\"http://queue:7331\"\n", 0600, true},
		{"public", "MYCE_TOKEN=test\nMYCE_QUEUE=http://queue:7331\n", 0644, false},
		{"missing", "MYCE_TOKEN=test\n", 0600, false},
		{"malformed", "MYCE_TOKEN='test\nMYCE_QUEUE=http://queue:7331\n", 0600, false},
		{"token URL", "MYCE_TOKEN=test\nMYCE_QUEUE=http://user:secret@queue:7331\n", 0600, false},
		{"extra", "MYCE_TOKEN=test\nMYCE_QUEUE=http://queue:7331\nOTHER=secret\n", 0600, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "fleet.env")
			os.WriteFile(p, []byte(tc.text), tc.mode)
			token, queue, err := ReadFleetEnvironment(p)
			if (err == nil) != tc.ok {
				t.Fatal(err)
			}
			if tc.ok && (token != "test $literal" || queue != "http://queue:7331") {
				t.Fatal("literal changed")
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("credential in error")
			}
		})
	}
}

func TestUserUnitLiteralPaths(t *testing.T) {
	for _, role := range []string{"queue", "worker"} {
		c, _ := PlanInstall(role, "/tmp/home%test", "", "/tmp/nas", "http://queue:7331", "test", Detection{OS: "linux", Arch: "amd64", Platform: "cuda", MemoryMode: "dedicated"})
		c.EnvironmentFile = "/tmp/fleet%test.env"
		text := c.ServiceText()
		for _, path := range []string{c.EnvironmentFile, c.GPUEnvironmentFile()} {
			if role == "queue" && path == c.GPUEnvironmentFile() {
				continue
			}
			if !strings.Contains(text, "EnvironmentFile="+strings.ReplaceAll(path, "%", "%%")+"\n") {
				t.Errorf("quoted or unescaped EnvironmentFile: %s", text)
			}
		}
		if role == "worker" && (!strings.Contains(text, "Delegate=memory\n") || strings.Contains(text, "PrivateDevices=") || strings.Contains(text, "PrivateUsers=")) {
			t.Fatal("worker device access/delegation changed")
		}
		for _, bad := range []string{" ", "\t", "'", "\"", "\\", "\n", "\r", "\u00a0"} {
			for _, field := range []string{"environment", "config", "host"} {
				broken := c
				switch field {
				case "environment":
					broken.EnvironmentFile += bad
				case "config":
					if role == "queue" {
						continue
					}
					broken.Config = filepath.Join(filepath.Dir(c.Config)+bad, "config.json")
				case "host":
					if role == "queue" {
						continue
					}
					broken.HostStateDir += bad
				}
				if err := broken.ValidateInstall(); err == nil {
					t.Errorf("accepted unsafe %s path %q", field, bad)
				}
			}
		}
	}
	c, _ := PlanInstall("queue", "/tmp/home", "", "/tmp/nas", "http://queue:7331", "test", Detection{OS: "linux"})
	c.PrivateDevices = true
	if err := c.ValidateInstall(); err == nil {
		t.Fatal("accepted unsupported user sandbox")
	}
}

func TestServiceConfigOnboardingBenchmark(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		c := ServiceConfig{Role: "worker", OnboardingBenchmark: enabled}
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		var restored ServiceConfig
		if err := json.Unmarshal(data, &restored); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, arg := range restored.Args() {
			if arg == "--onboarding-benchmark" {
				found = true
			}
		}
		if found != enabled {
			t.Fatalf("enabled=%v args=%v", enabled, restored.Args())
		}
	}
}

func TestQueueLocalDisableReceiptsServiceArgs(t *testing.T) {
	c, err := PlanInstall("queue", t.TempDir(), "", t.TempDir(), "http://queue:7331", "queue", Detection{OS: "linux"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(c.Args(), " "), "--local-disable-receipts") {
		t.Fatal("enabled by default")
	}
	c.LocalDisableReceipts = true
	c.HostID = "local-host"
	c.HostStateDir = filepath.Join(t.TempDir(), "host-state")
	if strings.Contains(strings.Join(c.Args(), " "), "--local-disable-receipts") || strings.Contains(c.ServiceText(), "--local-disable-receipts") {
		t.Fatal("retired receipt option emitted")
	}
	if err := c.ValidateInstall(); err == nil || !strings.Contains(err.Error(), "reinstall") {
		t.Fatal("retired config accepted", err)
	}
}

func TestPlanInstallUsesOnlySuppliedAccount(t *testing.T) {
	home := "/home/fixture-worker"
	d := Detection{OS: "linux", Platform: "fake"}
	account := WorkerAccount{Name: "fixture-worker", Home: home, UID: 12345, GID: 23456}
	c, e := PlanInstall("worker", home, "", "/fixture/nas", "http://queue:7331", "fixture", d, account)
	if e != nil || c.WorkerUser != account.Name || c.WorkerHome != home {
		t.Fatal(c, e)
	}
	c, e = PlanInstall("worker", home, "", "/fixture/nas", "http://queue:7331", "fixture", d)
	if e != nil || c.WorkerUser != "" || c.WorkerHome != home {
		t.Fatal(c, e)
	}
}
